"""断网缓存与排水（gateway.md §6，ADR-003 硬指标 3 的软件化）。

- 存储形态：SQLite WAL 单文件内 ``sample_cache`` 表，rowid 序 = FIFO 序；
- 补传（§6.2）：严格 oldest-first，PUBACK 到达才删行（at-least-once，
  重复行由 TSDB (point_id, ts) upsert 幂等吸收）；补传与新数据不交叉
  （全部样本统一走缓存序发送——同网关消息 Kafka 同分区保序的边缘义务）；
- 保留（§6.1）：FIFO 清理默认 4 天（> 硬指标 3 天，< ingest cagg 5 天窗）；
- seq（§5.1）：gw_meta 落盘自增，跨重启不断裂。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .db import Tx


@dataclass(frozen=True)
class CachedSample:
    cache_id: int
    raw_name: str
    ts: str
    value: float | None
    value_text: str | None
    quality: str
    unit: str | None


def enqueue_samples(
    db: sqlite3.Connection,
    rows: Iterable[tuple[str, str, float | None, str | None, str, str | None]],
) -> int:
    """成批入队（每轮询周期一个事务，§6.1）。返回入队行数。"""
    payload = [
        (name, ts, val, vtxt, q, unit, datetime.now(UTC).isoformat())
        for (name, ts, val, vtxt, q, unit) in rows
    ]
    if not payload:
        return 0
    with Tx(db) as t:
        t.executemany(
            "INSERT INTO sample_cache "
            "(raw_name, ts, value, value_text, quality, unit, enqueued_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            payload,
        )
    return len(payload)


def peek_oldest(db: sqlite3.Connection, limit_points: int) -> list[CachedSample]:
    """排水取批（§6.2 严格 oldest-first，rowid 升序）。PUBACK 后 delete_samples。"""
    rows = db.execute(
        "SELECT cache_id, raw_name, ts, value, value_text, quality, unit "
        "FROM sample_cache ORDER BY cache_id LIMIT ?",
        (limit_points,),
    ).fetchall()
    return [
        CachedSample(
            cache_id=r["cache_id"],
            raw_name=r["raw_name"],
            ts=r["ts"],
            value=r["value"],
            value_text=r["value_text"],
            quality=r["quality"],
            unit=r["unit"],
        )
        for r in rows
    ]


def delete_samples(db: sqlite3.Connection, cache_ids: Sequence[int]) -> None:
    if not cache_ids:
        return
    with Tx(db) as t:
        t.executemany("DELETE FROM sample_cache WHERE cache_id = ?", [(i,) for i in cache_ids])


def cache_size(db: sqlite3.Connection) -> int:
    row = db.execute("SELECT COUNT(*) AS n FROM sample_cache").fetchone()
    return int(row["n"])


def cache_oldest_age_s(db: sqlite3.Connection) -> float:
    row = db.execute("SELECT enqueued_at FROM sample_cache ORDER BY cache_id LIMIT 1").fetchone()
    if not row:
        return 0.0
    try:
        oldest = datetime.fromisoformat(row["enqueued_at"])
        return max(0.0, (datetime.now(UTC) - oldest).total_seconds())
    except ValueError:
        return 0.0


def cleanup_retention(db: sqlite3.Connection, retention_days: int) -> int:
    """FIFO 清理（§6.1 保留 N 天，默认 4）：按入队时刻删超期行。"""
    cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat()
    cur = db.execute("DELETE FROM sample_cache WHERE enqueued_at < ?", (cutoff,))
    return cur.rowcount or 0


def next_seq(db: sqlite3.Connection) -> int:
    """上行 seq：落盘自增跨重启不断裂（§5.1）。单写者下事务内读改写。"""
    with Tx(db) as t:
        cur = t.execute(
            "UPDATE gw_meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) "
            "WHERE key = 'last_seq'"
        )
        if cur.rowcount != 1:  # schema 自建键必在；防御性显式失败
            raise RuntimeError("gw_meta.last_seq 缺失（库损坏？）")
        row = t.execute(
            "SELECT CAST(value AS INTEGER) AS seq FROM gw_meta WHERE key = 'last_seq'"
        ).fetchone()
        return int(row["seq"])
