"""保留期回收：按 UTC 截止时刻退役快照、隔离旧分片并安全释放 Parquet 文件。

语义（与需求逐条对应）
======================
1. 提交一个**不晚于当前时间**的 UTC 截止时刻 ``cutoff``：
   * 退役所有 ``created_at <= cutoff`` 的快照（状态置 ``retired``，拒绝再读）；
   * 此后创建的新快照只包含 ``uploaded_at > cutoff`` 且仍 active 的分片，
     cutoff 前上传的分片即使文件还在（被保留快照引用）也不会重新供数。
2. 仅当分片**不再被任何保留（active）快照引用**时，才允许删除其文件与登记；
   仍被引用的分片原样返回，等待后续回收。
3. 截止时刻在保留水位线上**单调推进**：
   * 与当前水位线相同 -> 幂等空转（``applied=false``），不恢复任何旧分片；
   * 早于当前水位线 -> 409 拒绝，已退役数据不能重新供数。

崩溃安全（两阶段删除）
======================
所有裁决都在调用方持有的进程写锁 + ``BEGIN IMMEDIATE`` 事务内完成，
事务内只把无引用的候选分片置为 ``pending_delete`` 并提交；
**物理文件删除与登记删除在事务外逐个执行**：

* 提交后、删文件前崩溃 -> 重启时 :func:`recover_pending_deletes` 找到
  ``pending_delete`` 分片，删除文件与登记，重试即可继续清理；
* 删完文件、删登记前崩溃 -> 重启时发现文件已不存在，补删登记；
* ``pending_delete`` 分片不可能再被任何 active 快照引用（新快照只选
  active 分片，退役快照的引用已在同一事务内移除），因此恢复删除永不误删
  保留快照所需文件。
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional

from .models import ConflictError


# ---------------------------------------------------------------------------
# 时间辅助：SQLite 既有时间戳精度不统一（%f 为毫秒，历史数据可能为秒），
# 统一解析成 aware datetime 后按 datetime 比较，避免字符串字典序陷阱。
# ---------------------------------------------------------------------------

def parse_utc(text: str) -> datetime:
    """解析库内 UTC 时间戳（``...Z`` 结尾，秒/毫秒/微秒精度均可）。"""
    normalized = text.strip()
    if normalized.endswith(("Z", "z")):
        normalized = normalized[:-1] + "+00:00"
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:  # 库内约定全部为 UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def canonical_cutoff(dt: datetime) -> str:
    """把请求的截止时刻规范化为带微秒与 ``Z`` 后缀的 UTC 字符串。"""
    if dt.tzinfo is None:
        raise ConflictError("截止时刻 cutoff 必须带时区信息（UTC 偏移或 Z 后缀），不接受无时区时刻")
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def validate_cutoff(dt: datetime) -> str:
    """校验并规范化截止时刻：必须带时区、且不晚于当前 UTC 时间。"""
    text = canonical_cutoff(dt)
    if parse_utc(text) > datetime.now(timezone.utc):
        raise ConflictError("截止时刻 cutoff 不能晚于当前 UTC 时间")
    return text


# ---------------------------------------------------------------------------
# 水位线读取
# ---------------------------------------------------------------------------

def get_active_cutoff(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT cutoff FROM retention_cutoff WHERE id = 1").fetchone()
    return row["cutoff"] if row else None


def _snapshot_rows(conn: sqlite3.Connection, status: str) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT snapshot_id, created_at FROM snapshots WHERE status = ? ORDER BY snapshot_id",
        (status,),
    ).fetchall()


def retention_overview(conn: sqlite3.Connection) -> dict:
    """当前保留状态：水位线、存续/退役快照、active/待删分片。"""
    return {
        "active_cutoff": get_active_cutoff(conn),
        "active_snapshot_ids": [r["snapshot_id"] for r in _snapshot_rows(conn, "active")],
        "retired_snapshot_ids": [r["snapshot_id"] for r in _snapshot_rows(conn, "retired")],
        "active_shard_ids": [
            r["shard_id"]
            for r in conn.execute(
                "SELECT shard_id FROM shards WHERE status = 'active' ORDER BY shard_id"
            ).fetchall()
        ],
        "pending_delete_shard_ids": [
            r["shard_id"]
            for r in conn.execute(
                "SELECT shard_id FROM shards WHERE status = 'pending_delete' ORDER BY shard_id"
            ).fetchall()
        ],
    }


# ---------------------------------------------------------------------------
# 回收裁决（事务内，只做元数据变更，不碰物理文件）
# ---------------------------------------------------------------------------

def reclaim(conn: sqlite3.Connection, raw_cutoff: datetime) -> dict:
    """在调用方立即事务内完成一次回收裁决。

    返回本次退役快照、删除/保留/待删分片明细；物理文件由调用方在事务外
    通过 :func:`delete_pending_files` 完成。
    """
    cutoff_text = validate_cutoff(raw_cutoff)
    cutoff_dt = parse_utc(cutoff_text)
    current = get_active_cutoff(conn)

    # 更早的截止时刻：拒绝，保证水位线单调、旧数据不能重新供数
    if current is not None and cutoff_dt < parse_utc(current):
        raise ConflictError(
            f"截止时刻 {cutoff_text} 早于当前保留水位线 {current}，"
            "已退役/隔离的数据不能重新供数"
        )

    # 相同截止时刻：幂等空转，不产生任何状态变更
    if current is not None and cutoff_dt == parse_utc(current):
        return {
            "cutoff": cutoff_text,
            "applied": False,
            "retired_snapshot_ids": [],
            "deleted_shards": [],
            "retained_shards": [],
            "pending_delete_shard_ids": [
                r["shard_id"]
                for r in conn.execute(
                    "SELECT shard_id FROM shards WHERE status = 'pending_delete' ORDER BY shard_id"
                ).fetchall()
            ],
            "active_cutoff": current,
        }

    # 1) 退役 cutoff 前（含）创建的全部快照
    retired_ids: List[int] = []
    for snap in conn.execute(
        "SELECT snapshot_id, created_at FROM snapshots WHERE status = 'active' ORDER BY snapshot_id"
    ).fetchall():
        if parse_utc(snap["created_at"]) <= cutoff_dt:
            retired_ids.append(int(snap["snapshot_id"]))

    # 先移除退役快照的分片引用（snapshot_shards 外键约束要求先于分片删除）
    if retired_ids:
        marks = ",".join("?" for _ in retired_ids)
        conn.execute(
            f"DELETE FROM snapshot_shards WHERE snapshot_id IN ({marks})", retired_ids
        )
        conn.executemany(
            "UPDATE snapshots SET status = 'retired', retired_at = ? WHERE snapshot_id = ?",
            [(cutoff_text, sid) for sid in retired_ids],
        )

    # 2) cutoff 前（含）上传、且仍 active 的分片，是本次释放候选
    candidates = [
        r
        for r in conn.execute(
            "SELECT shard_id, file_path, row_count, uploaded_at FROM shards "
            "WHERE status = 'active' ORDER BY shard_id"
        ).fetchall()
        if parse_utc(r["uploaded_at"]) <= cutoff_dt
    ]

    # 3) 仍被保留快照引用的候选 -> 保留；其余 -> pending_delete
    referenced: dict[int, List[int]] = {}
    for row in conn.execute(
        "SELECT ss.shard_id, ss.snapshot_id FROM snapshot_shards ss "
        "JOIN snapshots s ON s.snapshot_id = ss.snapshot_id "
        "WHERE s.status = 'active' ORDER BY ss.shard_id, ss.snapshot_id"
    ).fetchall():
        referenced.setdefault(int(row["shard_id"]), []).append(int(row["snapshot_id"]))

    retained_shards: List[dict] = []
    to_delete_ids: List[int] = []
    for shard in candidates:
        shard_id = int(shard["shard_id"])
        holders = referenced.get(shard_id, [])
        if holders:
            retained_shards.append(
                {
                    "shard_id": shard_id,
                    "file_path": shard["file_path"],
                    "row_count": shard["row_count"],
                    "uploaded_at": shard["uploaded_at"],
                    "referenced_by_snapshots": holders,
                }
            )
        else:
            to_delete_ids.append(shard_id)

    if to_delete_ids:
        marks = ",".join("?" for _ in to_delete_ids)
        conn.execute(
            f"UPDATE shards SET status = 'pending_delete' WHERE shard_id IN ({marks})",
            to_delete_ids,
        )

    # 4) 推进单调水位线
    conn.execute(
        "INSERT INTO retention_cutoff (id, cutoff) VALUES (1, ?) "
        "ON CONFLICT(id) DO UPDATE SET cutoff = excluded.cutoff, applied_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
        (cutoff_text,),
    )

    # 5) 审计记录
    conn.execute(
        "INSERT INTO reclamation_runs (cutoff, retired_snapshot_ids, deleted_shard_ids, retained_shard_ids) "
        "VALUES (?, ?, ?, ?)",
        (
            cutoff_text,
            json.dumps(retired_ids),
            json.dumps(to_delete_ids),
            json.dumps([s["shard_id"] for s in retained_shards]),
        ),
    )

    return {
        "cutoff": cutoff_text,
        "applied": True,
        "retired_snapshot_ids": retired_ids,
        "deleted_shards": [],  # 物理删除在事务外完成后由调用方填充
        "retained_shards": retained_shards,
        "pending_delete_shard_ids": to_delete_ids,
        "active_cutoff": cutoff_text,
    }


# ---------------------------------------------------------------------------
# 物理删除（事务外，逐个分片独立事务；崩溃后可重复执行）
# ---------------------------------------------------------------------------

def delete_pending_files(conn: sqlite3.Connection) -> List[dict]:
    """删除所有 ``pending_delete`` 分片的物理文件与登记，返回已删明细。

    每个分片独立提交：单个文件删除失败不影响其他分片，调用方/重启后重试
    本函数即可继续清理。删除前再次确认无 active 快照引用（防御性不变量）。
    """
    pending = conn.execute(
        "SELECT shard_id, file_path, row_count, uploaded_at FROM shards "
        "WHERE status = 'pending_delete' ORDER BY shard_id"
    ).fetchall()

    deleted: List[dict] = []
    for shard in pending:
        shard_id = int(shard["shard_id"])
        holders = conn.execute(
            "SELECT COUNT(*) AS n FROM snapshot_shards ss "
            "JOIN snapshots s ON s.snapshot_id = ss.snapshot_id "
            "WHERE ss.shard_id = ? AND s.status = 'active'",
            (shard_id,),
        ).fetchone()["n"]
        if holders:
            # 理论不可达：pending_delete 只能由无引用状态进入；绝不误删
            continue

        path = shard["file_path"]
        try:
            if os.path.exists(path):
                os.unlink(path)
        except OSError:
            # 文件暂时删不掉：保留 pending_delete 状态，下次重试
            continue

        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM shard_indicators WHERE shard_id = ?", (shard_id,))
            conn.execute("DELETE FROM shards WHERE shard_id = ?", (shard_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            continue
        deleted.append(
            {
                "shard_id": shard_id,
                "file_path": path,
                "row_count": shard["row_count"],
                "uploaded_at": shard["uploaded_at"],
            }
        )
    return deleted


def recover_pending_deletes() -> List[dict]:
    """进程启动时调用：清理上次崩溃遗留的 pending_delete 分片。"""
    from .db import connect

    conn = connect()
    try:
        return delete_pending_files(conn)
    finally:
        conn.close()
