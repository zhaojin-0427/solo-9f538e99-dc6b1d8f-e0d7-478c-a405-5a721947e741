"""独立 HTTP 冒烟：保留期回收完整故事（与 pytest 夹具无关）。

用法：先启动 uvicorn（建议用全新 DATA_DIR），再运行
    python tests/manual_reclaim_smoke.py
"""
from __future__ import annotations

import io
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
DATA_DIR = os.environ.get("DATA_DIR", "./data")
UTC = timezone.utc


def parquet_bytes(station: str, value: float) -> bytes:
    buf = io.BytesIO()
    pq.write_table(pa.table({
        "station": pa.array([station], pa.string()),
        "sampled_at": pa.array(["2026-01-01T00:00:00Z"], pa.string()),
        "浓度": pa.array([value], pa.float64()),
    }), buf)
    return buf.getvalue()


def main() -> int:
    c = httpx.Client(base_url=BASE, timeout=10)
    assert c.get("/health").status_code == 200

    v = c.post("/schema/indicators", json={"name": "浓度"}).json()["new_version"]

    def upload(station: str, value: float) -> int:
        r = c.post(
            "/ingest/parquet",
            data={"schema_version": str(v)},
            files={"file": ("f.parquet", parquet_bytes(station, value), "application/octet-stream")},
        )
        r.raise_for_status()
        return r.json()["shard_id"]

    old1 = upload("A", 1.0)
    old2 = upload("B", 2.0)
    snap = c.get("/rows").json()["snapshot_id"]

    # 把历史分片与快照钉到过去，制造确定的 cutoff 边界
    db = os.path.join(DATA_DIR, "warehouse.db")
    conn = sqlite3.connect(db)
    conn.execute("UPDATE shards SET uploaded_at=? WHERE shard_id=?", ("2026-01-01T00:00:00.000Z", old1))
    conn.execute("UPDATE shards SET uploaded_at=? WHERE shard_id=?", ("2026-01-02T00:00:00.000Z", old2))
    conn.execute("UPDATE snapshots SET created_at=? WHERE snapshot_id=?", ("2026-01-03T00:00:00.000Z", snap))
    conn.commit()
    conn.close()

    new = upload("C", 3.0)

    # 1) 回收：退役旧快照、删除无引用旧分片、保留新分片
    cutoff = (datetime.now(UTC) - timedelta(seconds=10)).isoformat()
    body = c.post("/retention/reclaim", json={"cutoff": cutoff}).json()
    print("reclaim:", body)
    assert body["retired_snapshot_ids"] == [snap]
    assert {d["shard_id"] for d in body["deleted_shards"]} == {old1, old2}

    # 2) 退役快照明确拒绝
    assert c.get("/rows", params={"snapshot_id": snap}).status_code == 410
    assert c.get(f"/snapshots/{snap}").status_code == 410

    # 3) 新快照只见 cutoff 之后的新分片，旧数据不恢复供数
    fresh = c.get("/rows").json()
    assert fresh["total_rows"] == 1 and fresh["rows"][0]["station"] == "C"
    assert c.get(f"/snapshots/{fresh['snapshot_id']}").json()["shard_ids"] == [new]

    # 4) 水位线单调：重复幂等、更早拒绝、未来拒绝、无时区拒绝
    assert c.post("/retention/reclaim", json={"cutoff": cutoff}).json()["applied"] is False
    earlier = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    assert c.post("/retention/reclaim", json={"cutoff": earlier}).status_code == 409
    future = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
    assert c.post("/retention/reclaim", json={"cutoff": future}).status_code == 409
    assert c.post("/retention/reclaim", json={"cutoff": "2026-01-01T00:00:00"}).status_code == 409

    print("retention overview:", c.get("/retention").json())
    print("\nRECLAIM SMOKE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
