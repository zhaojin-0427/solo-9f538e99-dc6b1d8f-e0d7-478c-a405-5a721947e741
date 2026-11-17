"""保留期回收自测：退役快照、分片隔离/释放、水位线单调、崩溃恢复与并发交错。"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from app.config import DATA_DIR, SHARD_DIR
from app.db import connect, immediate_transaction, init_db
from app.main import app
from app.retention import recover_pending_deletes
from fastapi.testclient import TestClient


UTC = timezone.utc


def upload_file(client, path, version: int):
    with open(path, "rb") as fh:
        return client.post(
            "/ingest/parquet",
            data={"schema_version": str(version)},
            files={"file": (path.name, fh.read(), "application/vnd.apache.parquet")},
        )


def make_indicator_and_version(client, name: str = "浓度") -> int:
    r = client.post("/schema/indicators", json={"name": name})
    assert r.status_code == 201, r.text
    return r.json()["new_version"]


def upload_one(client, make_parquet, version: int, station: str, value: float = 1.0):
    f = make_parquet(f"{station}.parquet", {
        "station": [station],
        "sampled_at": [f"2026-01-01T00:00:0{value}Z"],
        "浓度": [value],
    }, types={"sampled_at": pa.string()})
    r = upload_file(client, f, version)
    assert r.status_code == 200, r.text
    return r.json()["shard_id"]


def shard_path(shard_id: int):
    return SHARD_DIR / f"shard-{shard_id:010d}.parquet"


def set_uploaded_at(shard_id: int, text: str) -> None:
    conn = connect()
    try:
        with immediate_transaction(conn):
            conn.execute("UPDATE shards SET uploaded_at = ? WHERE shard_id = ?", (text, shard_id))
    finally:
        conn.close()


def set_snapshot_created(snapshot_id: int, text: str) -> None:
    conn = connect()
    try:
        with immediate_transaction(conn):
            conn.execute(
                "UPDATE snapshots SET created_at = ? WHERE snapshot_id = ?",
                (text, snapshot_id),
            )
    finally:
        conn.close()


def reclaim(client, cutoff: datetime):
    return client.post("/retention/reclaim", json={"cutoff": cutoff.isoformat()})


# ---------------------------------------------------------------------------
# 主流程：退役快照、删除无引用分片、新快照排除旧分片、旧数据不恢复供数
# ---------------------------------------------------------------------------

def test_reclaim_full_lifecycle(client, make_parquet):
    v = make_indicator_and_version(client)
    s1 = upload_one(client, make_parquet, v, "A", 1.0)
    s2 = upload_one(client, make_parquet, v, "B", 2.0)
    # 把前两个分片与快照固定到“过去”，与 cutoff 之后的新数据形成确定边界
    set_uploaded_at(s1, "2026-01-01T00:00:00.000Z")
    set_uploaded_at(s2, "2026-01-02T00:00:00.000Z")

    # 首个快照引用 s1、s2，创建时刻同样固定到过去
    snap1 = client.get("/rows").json()["snapshot_id"]
    set_snapshot_created(snap1, "2026-01-03T00:00:00.000Z")
    assert client.get("/rows", params={"snapshot_id": snap1}).json()["total_rows"] == 2

    # cutoff 之后再上传 s3（uploaded_at 为当前时间，严格晚于 cutoff）
    cutoff = datetime.now(UTC) - timedelta(seconds=10)
    s3 = upload_one(client, make_parquet, v, "C", 3.0)

    r = reclaim(client, cutoff)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["applied"] is True
    assert body["retired_snapshot_ids"] == [snap1]
    assert {d["shard_id"] for d in body["deleted_shards"]} == {s1, s2}
    assert body["retained_shards"] == []
    assert body["pending_delete_shard_ids"] == []
    assert body["active_cutoff"].endswith("Z")

    # 物理文件与登记均已删除；新分片保留
    assert not shard_path(s1).exists()
    assert not shard_path(s2).exists()
    assert shard_path(s3).exists()
    conn = connect()
    try:
        ids = {row["shard_id"] for row in conn.execute("SELECT shard_id FROM shards").fetchall()}
        assert ids == {s3}
    finally:
        conn.close()

    # 退役快照明确拒绝：分页 410、元数据 410
    r = client.get("/rows", params={"snapshot_id": snap1})
    assert r.status_code == 410 and "退役" in r.json()["detail"]
    r = client.get(f"/snapshots/{snap1}")
    assert r.status_code == 410

    # 新快照只含 cutoff 之后上传的 s3，旧分片不恢复供数
    fresh = client.get("/rows").json()
    snap2 = fresh["snapshot_id"]
    assert fresh["total_rows"] == 1
    assert fresh["rows"][0]["station"] == "C"
    meta = client.get(f"/snapshots/{snap2}").json()
    assert meta["shard_ids"] == [s3]
    assert meta["status"] == "active" and meta["retired_at"] is None

    # 保留快照仍可完整分页
    page = client.get("/rows", params={"snapshot_id": snap2, "page_size": 1}).json()
    assert page["total_rows"] == 1 and page["rows"][0]["station"] == "C"


def test_repeated_cutoff_is_idempotent_and_earlier_cutoff_rejected(client, make_parquet):
    v = make_indicator_and_version(client)
    upload_one(client, make_parquet, v, "A")
    snap = client.get("/rows").json()["snapshot_id"]

    cutoff = datetime.now(UTC)
    r = reclaim(client, cutoff)
    assert r.status_code == 200
    first = r.json()
    assert first["applied"] is True and first["retired_snapshot_ids"] == [snap]

    # 完全相同的 cutoff：幂等空转
    r2 = reclaim(client, cutoff)
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["applied"] is False
    assert body2["retired_snapshot_ids"] == []
    assert body2["deleted_shards"] == []
    assert body2["active_cutoff"] == first["active_cutoff"]

    # 更早的 cutoff：409，已退役数据不能重新供数
    r3 = reclaim(client, cutoff - timedelta(hours=1))
    assert r3.status_code == 409 and "水位线" in r3.json()["detail"]

    # 退役快照依旧 410，没有被恢复
    assert client.get("/rows", params={"snapshot_id": snap}).status_code == 410


def test_new_snapshot_excludes_pre_cutoff_shards_even_while_file_retained(client, make_parquet):
    """cutoff 前上传但仍被保留快照引用的分片：文件不删，但新快照绝不包含它。"""
    v = make_indicator_and_version(client)
    s_old = upload_one(client, make_parquet, v, "OLD", 1.0)
    set_uploaded_at(s_old, "2026-01-01T00:00:00.000Z")

    # 构造一个 cutoff 之后才创建、却引用旧分片的保留快照（模拟引用仍存续的情形）。
    # 注意：测试“当前”是 2026-11，保留快照时刻必须晚于本次 cutoff（当前时刻）。
    snap_kept = client.get("/rows").json()["snapshot_id"]
    future_created = (datetime.now(UTC) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    set_snapshot_created(snap_kept, future_created)
    s_new = upload_one(client, make_parquet, v, "NEW", 2.0)

    cutoff = datetime.now(UTC) - timedelta(seconds=10)
    body = reclaim(client, cutoff).json()
    # 旧分片被保留快照 snap_kept 引用 -> 保留不删
    retained = {d["shard_id"]: d for d in body["retained_shards"]}
    assert set(retained) == {s_old}
    assert retained[s_old]["referenced_by_snapshots"] == [snap_kept]
    # s_new 上传时刻晚于 cutoff，不参与本次回收
    assert {d["shard_id"] for d in body["deleted_shards"]} == set()
    assert shard_path(s_old).exists() and shard_path(s_new).exists()

    # 保留快照仍可完整分页（含旧分片数据）
    kept = client.get("/rows", params={"snapshot_id": snap_kept, "page_size": 100}).json()
    assert kept["total_rows"] == 1
    assert kept["rows"][0]["station"] == "OLD"

    # 新快照只能看到 cutoff 之后的 s_new，旧分片不恢复供数
    fresh = client.get("/rows").json()
    assert fresh["snapshot_id"] != snap_kept
    assert fresh["total_rows"] == 1
    assert fresh["rows"][0]["station"] == "NEW"
    assert client.get(f"/snapshots/{fresh['snapshot_id']}").json()["shard_ids"] == [s_new]

    # 再次推进 cutoff（当前时刻）：先把仍存续快照的创建时刻、以及 s_new 的上传时刻
    # 钉到过去，使两个快照退役、两个分片都不再被引用，可一并清理
    set_snapshot_created(snap_kept, "2026-02-01T00:00:00.000Z")
    set_snapshot_created(fresh["snapshot_id"], "2026-03-01T00:00:00.000Z")
    set_uploaded_at(s_new, "2026-05-01T00:00:00.000Z")
    later = datetime.now(UTC)
    body2 = reclaim(client, later).json()
    assert body2["applied"] is True
    assert body2["retired_snapshot_ids"] == sorted([snap_kept, fresh["snapshot_id"]])
    deleted_ids = {d["shard_id"] for d in body2["deleted_shards"]}
    assert deleted_ids == {s_old, s_new}
    assert not shard_path(s_old).exists() and not shard_path(s_new).exists()


def test_cutoff_validation_rules(client):
    # 晚于当前时间 -> 409
    future = datetime.now(UTC) + timedelta(minutes=5)
    r = reclaim(client, future)
    assert r.status_code == 409 and "不能晚于" in r.json()["detail"]

    # 无时区的 naive 时刻 -> 409（业务校验明确要求带时区）
    r = client.post("/retention/reclaim", json={"cutoff": "2026-01-01T00:00:00"})
    assert r.status_code == 409 and "时区" in r.json()["detail"]

    # 带偏移量的时刻可接受（自动转 UTC）
    r = client.post(
        "/retention/reclaim",
        json={"cutoff": (datetime.now(UTC) - timedelta(hours=2)).isoformat()},
    )
    assert r.status_code == 200, r.text
    assert r.json()["cutoff"].endswith("Z")


def test_retention_overview_endpoint(client, make_parquet):
    v = make_indicator_and_version(client)
    sid = upload_one(client, make_parquet, v, "A")
    snap = client.get("/rows").json()["snapshot_id"]

    overview = client.get("/retention").json()
    assert overview["active_cutoff"] is None
    assert overview["active_snapshot_ids"] == [snap]
    assert overview["retired_snapshot_ids"] == []
    assert overview["active_shard_ids"] == [sid]

    reclaim(client, datetime.now(UTC))
    overview = client.get("/retention").json()
    assert overview["active_cutoff"] is not None
    assert overview["active_snapshot_ids"] == []
    assert overview["retired_snapshot_ids"] == [snap]
    assert overview["active_shard_ids"] == []
    assert overview["pending_delete_shard_ids"] == []


# ---------------------------------------------------------------------------
# 崩溃/中断恢复：文件删除各阶段中断后重启，重试可继续且不误删
# ---------------------------------------------------------------------------

def test_restart_recovers_pending_delete_with_and_without_file(client, make_parquet):
    v = make_indicator_and_version(client)
    s1 = upload_one(client, make_parquet, v, "A", 1.0)
    s2 = upload_one(client, make_parquet, v, "B", 2.0)
    snap = client.get("/rows").json()["snapshot_id"]  # 引用 s1、s2

    # 模拟真实回收在“事务提交后、物理删除阶段”崩溃的现场：
    # 快照已退役、snapshot_shards 引用已移除、分片置 pending_delete
    conn = connect()
    try:
        with immediate_transaction(conn):
            conn.execute("DELETE FROM snapshot_shards WHERE snapshot_id = ?", (snap,))
            conn.execute(
                "UPDATE snapshots SET status = 'retired', retired_at = ? WHERE snapshot_id = ?",
                ("2026-01-01T00:00:00.000Z", snap),
            )
            conn.execute(
                "UPDATE shards SET status = 'pending_delete' WHERE shard_id IN (?, ?)", (s1, s2)
            )
        # s1 文件也被删掉（模拟“删完文件、删登记前崩溃”）；s2 文件仍在（模拟更早中断）
        shard_path(s1).unlink()
    finally:
        conn.close()

    # 重启：lifespan 触发恢复（init_db 后执行 recover_pending_deletes）
    del client
    init_db()
    recover_pending_deletes()
    with TestClient(app) as client2:
        conn = connect()
        try:
            rows = conn.execute("SELECT shard_id, status FROM shards").fetchall()
            assert rows == []  # 两个待删分片的登记都被清掉
        finally:
            conn.close()
        assert not shard_path(s2).exists()
        # 服务正常工作，可继续上传与建快照
        f = make_parquet("c.parquet", {
            "station": ["C"], "sampled_at": ["2026-09-01T00:00:00Z"], "浓度": [9.0],
        }, types={"sampled_at": pa.string()})
        assert upload_file(client2, f, v).status_code == 200
        fresh = client2.get("/rows").json()
        assert fresh["total_rows"] == 1 and fresh["rows"][0]["station"] == "C"


def test_pending_delete_never_touches_referenced_shard(client, make_parquet):
    """防御性不变量：即使手工把被保留快照引用的分片置为待删，恢复也不会删它。"""
    v = make_indicator_and_version(client)
    s1 = upload_one(client, make_parquet, v, "A", 1.0)
    snap = client.get("/rows").json()["snapshot_id"]

    conn = connect()
    try:
        with immediate_transaction(conn):
            conn.execute("UPDATE shards SET status = 'pending_delete' WHERE shard_id = ?", (s1,))
    finally:
        conn.close()

    del client
    init_db()
    recover_pending_deletes()  # 恢复：该分片仍被 active 快照引用，必须保留
    with TestClient(app) as client2:
        conn = connect()
        try:
            row = conn.execute("SELECT status FROM shards WHERE shard_id = ?", (s1,)).fetchone()
            assert row is not None and row["status"] == "pending_delete"
        finally:
            conn.close()
        assert shard_path(s1).exists()
        # 快照读取依赖该文件，数据完整
        page = client2.get("/rows", params={"snapshot_id": snap}).json()
        assert page["total_rows"] == 1


def test_repeated_reclaim_retries_cleanup_of_interrupted_pending(client, make_parquet):
    """首次回收后人为制造待删残留，重复/再次提交 cutoff 应继续清理。"""
    v = make_indicator_and_version(client)
    s1 = upload_one(client, make_parquet, v, "A", 1.0)
    snap = client.get("/rows").json()["snapshot_id"]
    cutoff = datetime.now(UTC)
    body = reclaim(client, cutoff).json()
    assert body["applied"] is True and body["retired_snapshot_ids"] == [snap]
    assert {d["shard_id"] for d in body["deleted_shards"]} == {s1}
    assert not shard_path(s1).exists()

    # 模拟后续一次回收在物理删除阶段中断：手工塞入一个无 active 引用的待删分片
    s2 = upload_one(client, make_parquet, v, "B", 2.0)
    conn = connect()
    try:
        with immediate_transaction(conn):
            conn.execute("UPDATE shards SET status = 'pending_delete' WHERE shard_id = ?", (s2,))
    finally:
        conn.close()
    assert shard_path(s2).exists()

    # 用相同 cutoff 幂等重试：applied=false，但残留分片被继续清理
    again = reclaim(client, cutoff).json()
    assert again["applied"] is False
    assert {d["shard_id"] for d in again["deleted_shards"]} == {s2}
    assert again["pending_delete_shard_ids"] == []
    assert not shard_path(s2).exists()


def test_reclaim_state_survives_restart(client, make_parquet):
    v = make_indicator_and_version(client)
    upload_one(client, make_parquet, v, "A")
    snap = client.get("/rows").json()["snapshot_id"]
    cutoff = datetime.now(UTC)
    reclaim(client, cutoff)

    del client
    init_db()
    with TestClient(app) as client2:
        # 退役状态持久化：重启后依旧 410
        assert client2.get("/rows", params={"snapshot_id": snap}).status_code == 410
        # 水位线持久化：更早 cutoff 仍被拒绝
        r = client2.post(
            "/retention/reclaim",
            json={"cutoff": (cutoff - timedelta(hours=1)).isoformat()},
        )
        assert r.status_code == 409
        overview = client2.get("/retention").json()
        assert overview["retired_snapshot_ids"] == [snap]


# ---------------------------------------------------------------------------
# 并发交错：回收与上传、建快照、读取同时进行，不出现误删/半份状态
# ---------------------------------------------------------------------------

def test_concurrent_reclaim_interleaved_with_upload_snapshot_read(client, make_parquet):
    v = make_indicator_and_version(client)
    for i in range(3):
        upload_one(client, make_parquet, v, f"P{i}", float(i + 1))

    barrier = threading.Barrier(4)
    errors: list[Exception] = []

    def reclaim_loop():
        try:
            barrier.wait()
            for _ in range(5):
                r = reclaim(client, datetime.now(UTC))
                assert r.status_code == 200, r.text
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def upload_loop():
        try:
            barrier.wait()
            for i in range(5):
                f = make_parquet(f"u{i}.parquet", {
                    "station": [f"U{i}"],
                    "sampled_at": [f"2026-10-01T00:00:0{i}Z"],
                    "浓度": [float(i)],
                }, types={"sampled_at": pa.string()})
                r = upload_file(client, f, v)
                assert r.status_code == 200, r.text
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def snapshot_loop():
        try:
            barrier.wait()
            for _ in range(5):
                created = client.get("/rows").json()
                snap_id = created["snapshot_id"]
                # 新建快照必须能完整读自己声明的分片；若读取前恰好被并发回收退役，
                # 410 也是明确且正确的拒绝
                page = client.get("/rows", params={"snapshot_id": snap_id, "page_size": 100})
                assert page.status_code in (200, 410), page.text
                if page.status_code == 200:
                    assert page.json()["total_rows"] == len(page.json()["rows"])
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=reclaim_loop),
        threading.Thread(target=upload_loop),
        threading.Thread(target=snapshot_loop),
        threading.Thread(target=snapshot_loop),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    # 最终不变量：每个仍 active 的快照，其引用的分片登记与物理文件都存在
    conn = connect()
    try:
        active_snaps = conn.execute(
            "SELECT snapshot_id FROM snapshots WHERE status = 'active'"
        ).fetchall()
        for snap_row in active_snaps:
            refs = conn.execute(
                "SELECT shard_id FROM snapshot_shards WHERE snapshot_id = ?",
                (snap_row["snapshot_id"],),
            ).fetchall()
            for ref in refs:
                shard = conn.execute(
                    "SELECT file_path, status FROM shards WHERE shard_id = ?",
                    (ref["shard_id"],),
                ).fetchone()
                assert shard is not None, "保留快照引用的分片登记丢失"
                assert shard["status"] == "active"
                from pathlib import Path
                assert Path(shard["file_path"]).exists(), "保留快照引用的物理文件丢失"
        # 不存在待删残留（delete_pending_files 在锁内同步完成）
        pending = conn.execute(
            "SELECT COUNT(*) AS n FROM shards WHERE status = 'pending_delete'"
        ).fetchone()["n"]
        assert pending == 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 老库迁移：回收功能上线前的库可以直接启动，历史快照/分片保持可读
# ---------------------------------------------------------------------------

def test_legacy_database_migrates_and_keeps_history(client, make_parquet, tmp_path):
    import shutil
    import sqlite3

    # 在配置的 DATA_DIR 位置造一个“回收功能上线前”的旧结构库
    shutil.rmtree(DATA_DIR, ignore_errors=True)
    SHARD_DIR.mkdir(parents=True, exist_ok=True)
    db_path = DATA_DIR / "warehouse.db"

    legacy = sqlite3.connect(db_path)
    legacy.executescript(
        """
        CREATE TABLE indicators (indicator_id INTEGER PRIMARY KEY, name TEXT NOT NULL, active INTEGER NOT NULL);
        CREATE TABLE schema_versions (version_id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, change_note TEXT NOT NULL);
        CREATE TABLE version_indicators (version_id INTEGER, indicator_id INTEGER, name TEXT, active INTEGER,
            PRIMARY KEY (version_id, indicator_id));
        CREATE TABLE shards (shard_id INTEGER PRIMARY KEY, version_id INTEGER, file_path TEXT UNIQUE,
            row_count INTEGER, original_name TEXT, uploaded_at TEXT NOT NULL);
        CREATE TABLE shard_indicators (shard_id INTEGER, indicator_id INTEGER, PRIMARY KEY (shard_id, indicator_id));
        CREATE TABLE snapshots (snapshot_id INTEGER PRIMARY KEY, created_at TEXT NOT NULL);
        CREATE TABLE snapshot_fields (snapshot_id INTEGER, indicator_id INTEGER, name TEXT, position INTEGER,
            PRIMARY KEY (snapshot_id, indicator_id));
        CREATE TABLE snapshot_shards (snapshot_id INTEGER, shard_id INTEGER, position INTEGER,
            PRIMARY KEY (snapshot_id, shard_id));
        INSERT INTO schema_versions VALUES (1, '2026-01-01T00:00:00.000Z', '初始化空字段版');
        INSERT INTO indicators VALUES (1, '浓度', 1);
        INSERT INTO version_indicators VALUES (1, 1, '浓度', 1);
        """
    )
    f = SHARD_DIR / "shard-0000000001.parquet"
    pq.write_table(pa.table({
        "station": pa.array(["L"], pa.string()),
        "sampled_at": pa.array(
            [datetime(2026, 1, 1, tzinfo=UTC)], pa.timestamp("us", tz="UTC")
        ),
        "i1": pa.array([7.5], pa.float64()),
    }), f)
    legacy.execute(
        "INSERT INTO shards VALUES (1, 1, ?, 1, 'legacy.parquet', '2026-01-01T00:00:00.000Z')",
        (str(f),),
    )
    legacy.execute("INSERT INTO shard_indicators VALUES (1, 1)")
    legacy.execute("INSERT INTO snapshots VALUES (1, '2026-02-01T00:00:00.000Z')")
    legacy.execute("INSERT INTO snapshot_fields VALUES (1, 1, '浓度', 0)")
    legacy.execute("INSERT INTO snapshot_shards VALUES (1, 1, 0)")
    legacy.commit()
    legacy.close()

    # 对旧库执行迁移（幂等增列/增表）
    init_db()

    migrated = sqlite3.connect(db_path)
    assert {"status", "retired_at"} <= {r[1] for r in migrated.execute("PRAGMA table_info(snapshots)")}
    assert "status" in {r[1] for r in migrated.execute("PRAGMA table_info(shards)")}
    assert migrated.execute("SELECT status FROM snapshots WHERE snapshot_id = 1").fetchone()[0] == "active"
    migrated.close()

    # 迁移后历史快照仍可完整分页（TestClient 每请求新建连接，指向同一库文件）
    page = client.get("/rows", params={"snapshot_id": 1, "page_size": 100})
    assert page.status_code == 200, page.text
    assert page.json()["rows"][0]["浓度"] == 7.5

    # 回收接口在老库上同样可用
    r = client.post("/retention/reclaim", json={"cutoff": datetime.now(UTC).isoformat()})
    assert r.status_code == 200, r.text
    assert r.json()["retired_snapshot_ids"] == [1]
    assert client.get("/rows", params={"snapshot_id": 1}).status_code == 410
