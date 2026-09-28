"""SQLite 连接与 schema 底座（pointmap/cache 共库单文件 WAL，gateway.md §3/§6）。

单进程 asyncio 单写者：所有调用发生在事件循环线程，无跨线程并发写。
表清单见各域模块（pointmap.py / cache.py）。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS gw_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pointmap (
    raw_name        TEXT PRIMARY KEY,           -- §3.3 生成规则产出，云端会合键
    bacnet_device_id INTEGER NOT NULL,
    obj_type        TEXT NOT NULL,              -- §4.2 收录集（analog-*/binary-*/multi-state-*）
    obj_instance    INTEGER NOT NULL,
    unit_raw        TEXT,                       -- ingest 单位族 token（§4.3；空=直通）
    writable        INTEGER NOT NULL DEFAULT 0, -- 1=可写（部署期工程决策，方向从严 §4.1）
    interval_s      INTEGER,                    -- NULL=默认周期（边缘本地配置，§14.2 差异 1）
    enabled         INTEGER NOT NULL DEFAULT 1,
    UNIQUE (bacnet_device_id, obj_type, obj_instance)
);
CREATE TABLE IF NOT EXISTS pointmap_staging (
    raw_name        TEXT PRIMARY KEY,
    bacnet_device_id INTEGER NOT NULL,
    obj_type        TEXT NOT NULL,
    obj_instance    INTEGER NOT NULL,
    unit_raw        TEXT,
    description     TEXT,
    discovered_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sample_cache (
    cache_id    INTEGER PRIMARY KEY AUTOINCREMENT, -- rowid 序 = 入队序 = FIFO 排水序
    raw_name    TEXT NOT NULL,
    ts          TEXT NOT NULL,                 -- 采集原始时刻 RFC3339（断网补传不改）
    value       REAL,
    value_text  TEXT,
    quality     TEXT NOT NULL,
    unit        TEXT,
    enqueued_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS sample_cache_enq ON sample_cache (enqueued_at);
"""


def open_db(path: str) -> sqlite3.Connection:
    """打开/初始化数据库：WAL + 显式事务（isolation_level=None）。"""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(p), isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.executescript(SCHEMA)
    db.execute(
        "INSERT OR IGNORE INTO gw_meta (key, value) VALUES ('schema_version', ?)",
        (str(SCHEMA_VERSION),),
    )
    db.execute("INSERT OR IGNORE INTO gw_meta (key, value) VALUES ('last_seq', '-1')")
    db.execute("INSERT OR IGNORE INTO gw_meta (key, value) VALUES ('applied_job_id', '')")
    return db


class Tx:
    """显式事务小助手（单写者，无嵌套）。"""

    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db

    def __enter__(self) -> sqlite3.Connection:
        self._db.execute("BEGIN IMMEDIATE")
        return self._db

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: Any | None
    ) -> None:
        if exc_type is None:
            self._db.execute("COMMIT")
        else:
            self._db.execute("ROLLBACK")


def meta_get(db: sqlite3.Connection, key: str) -> str | None:
    row = db.execute("SELECT value FROM gw_meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def meta_set(db: sqlite3.Connection, key: str, value: str) -> None:
    db.execute(
        "INSERT INTO gw_meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
