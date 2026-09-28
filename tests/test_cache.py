"""L1 断网缓存（§6）：FIFO 严格序、PUBACK 后删行、保留期清理、seq 单调。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from thermio_gateway.cache import (
    cache_size,
    cleanup_retention,
    delete_samples,
    enqueue_samples,
    next_seq,
    peek_oldest,
)
from thermio_gateway.db import open_db


@pytest.fixture()
def db(tmp_path):
    conn = open_db(str(tmp_path / "cache.db"))
    yield conn
    conn.close()


def row(name: str, i: int):
    return (name, f"2026-09-28T10:00:{i:02d}+00:00", float(i), None, "good", "degC")


class TestFifo:
    def test_peek_returns_oldest_first(self, db):
        enqueue_samples(db, [row("A", 0), row("B", 1), row("C", 2)])
        samples = peek_oldest(db, 2)
        assert [s.raw_name for s in samples] == ["A", "B"]

    def test_delete_then_peek_continues_fifo(self, db):
        enqueue_samples(db, [row("A", 0), row("B", 1), row("C", 2)])
        first = peek_oldest(db, 1)
        delete_samples(db, [s.cache_id for s in first])
        # 新数据永远排在既有行之后（补传与新数据不交叉，§6.2）
        enqueue_samples(db, [row("D", 3)])
        samples = peek_oldest(db, 10)
        assert [s.raw_name for s in samples] == ["B", "C", "D"]

    def test_delete_only_pubacked_rows(self, db):
        """PUBACK 前行必在（崩溃一致性：at-least-once 由未删行重发保证）。"""
        enqueue_samples(db, [row("A", 0), row("B", 1)])
        first = peek_oldest(db, 1)
        # 未删（模拟 PUBACK 未到）→ 再 peek 仍含 A
        assert peek_oldest(db, 1)[0].raw_name == "A"
        delete_samples(db, [first[0].cache_id])
        assert peek_oldest(db, 1)[0].raw_name == "B"

    def test_batch_enqueue_is_one_transaction_shape(self, db):
        n = enqueue_samples(db, [row(f"P{i}", 0) for i in range(50)])
        assert n == 50
        assert cache_size(db) == 50

    def test_empty_enqueue_noop(self, db):
        assert enqueue_samples(db, []) == 0


class TestRetention:
    def test_old_rows_purged_fifo(self, db):
        enqueue_samples(db, [row("OLD", 0)])
        # 直接改写 enqueued_at 模拟 5 天前行（保留 4 天，§6.1）
        db.execute(
            "UPDATE sample_cache SET enqueued_at = ?",
            ((datetime.now(UTC) - timedelta(days=5)).isoformat(),),
        )
        enqueue_samples(db, [row("NEW", 1)])
        removed = cleanup_retention(db, retention_days=4)
        assert removed == 1
        remaining = peek_oldest(db, 10)
        assert [s.raw_name for s in remaining] == ["NEW"]


class TestSeq:
    def test_monotonic(self, db):
        assert [next_seq(db) for _ in range(5)] == [0, 1, 2, 3, 4]

    def test_values_survive_rows(self, db):
        enqueue_samples(db, [row("A", 0)])
        assert next_seq(db) == 0
        delete_samples(db, [peek_oldest(db, 1)[0].cache_id])
        assert next_seq(db) == 1  # seq 不随行删除回退
