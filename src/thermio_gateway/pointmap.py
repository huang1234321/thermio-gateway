"""本地点表 pointmap（gateway.md §3）：raw_name 会合模型 + 生成规则 + adopt 迁移。

- raw_name = 云端与边缘唯一会合键（§3.1：BACnet 地址不出边缘）；
- 生成规则（§3.3）：object_name 收敛到 ingest §3.2 name（1..64，[A-Za-z0-9_.:-]）
  ⊂ M2 §5.1 raw_name（1..128）的严格侧交集，替换/去重/截断/空名全部记标记；
- ``adopt_staging``：发现 → pointmap 的版本迁移（§14.3-3 定稿，规则见函数注）。
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime

from .db import Tx

# ingest.md §3.2 name 字符集与长度（严格侧）
_NAME_RE = re.compile(r"[A-Za-z0-9_.:-]")

# §4.2 v1 收录的对象类型（其他类型发现时跳过 + 报告标记）
COLLECTABLE_TYPES = frozenset(
    {
        "analog-input",
        "analog-output",
        "analog-value",
        "binary-input",
        "binary-output",
        "binary-value",
        "multi-state-input",
        "multi-state-value",
    }
)


@dataclass(frozen=True)
class PointRow:
    raw_name: str
    bacnet_device_id: int
    obj_type: str
    obj_instance: int
    unit_raw: str | None
    writable: bool
    interval_s: int | None
    enabled: bool


@dataclass(frozen=True)
class NameDecision:
    """§3.3 逐点导出决策：原始名 → 导出名 + 标记（工程核对件）。"""

    raw_name: str
    marks: tuple[str, ...]


def generate_raw_name(
    original: str, device_id: int, obj_type: str, obj_instance: int, taken: set[str]
) -> NameDecision:
    """object_name → raw_name（§3.3 全分支）。

    taken 为本设备已用导出名集合（函数会将其更新为新占用的名字）。
    标记封闭集：sanitized / deduped / truncated / generated。
    """
    marks: list[str] = []
    name = (original or "").strip()
    if not name:
        name = f"DEV{device_id}_{obj_type}_{obj_instance}"
        marks.append("generated")
    else:
        cleaned = "".join(ch if _NAME_RE.match(ch) else "_" for ch in name)
        if cleaned != name:
            marks.append("sanitized")
            name = cleaned
    if len(name) > 64:
        name = name[:64]
        marks.append("truncated")
    if name in taken:
        suffix = 2
        while f"{name}_{suffix}" in taken:
            suffix += 1
        # 截断后再加后缀可能超 64：压缩本体保总长 ≤64
        base = name[: 64 - len(f"_{suffix}")]
        candidate = f"{base}_{suffix}"
        while candidate in taken and suffix < 10_000:
            suffix += 1
            base = name[: 64 - len(f"_{suffix}")]
            candidate = f"{base}_{suffix}"
        name = candidate
        marks.append("deduped")
    taken.add(name)
    return NameDecision(raw_name=name, marks=tuple(marks))


def _row_to_point(row: sqlite3.Row) -> PointRow:
    return PointRow(
        raw_name=row["raw_name"],
        bacnet_device_id=row["bacnet_device_id"],
        obj_type=row["obj_type"],
        obj_instance=row["obj_instance"],
        unit_raw=row["unit_raw"],
        writable=bool(row["writable"]),
        interval_s=row["interval_s"],
        enabled=bool(row["enabled"]),
    )


def load_pointmap(db: sqlite3.Connection) -> list[PointRow]:
    rows = db.execute("SELECT * FROM pointmap ORDER BY raw_name").fetchall()
    return [_row_to_point(r) for r in rows]


def get_point(db: sqlite3.Connection, raw_name: str) -> PointRow | None:
    row = db.execute("SELECT * FROM pointmap WHERE raw_name = ?", (raw_name,)).fetchone()
    return _row_to_point(row) if row else None


def replace_staging(
    db: sqlite3.Connection,
    records: list[tuple[str, int, str, int, str | None, str | None]],
) -> None:
    """发现产物整体落台（staging 全量替换，§4.1）。"""
    now = datetime.now(UTC).isoformat()
    with Tx(db) as t:
        t.execute("DELETE FROM pointmap_staging")
        t.executemany(
            "INSERT INTO pointmap_staging "
            "(raw_name, bacnet_device_id, obj_type, obj_instance, unit_raw, description, discovered_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(r[0], r[1], r[2], r[3], r[4], r[5], now) for r in records],
        )


def load_staging(
    db: sqlite3.Connection,
) -> list[tuple[str, int, str, int, str | None, str | None]]:
    rows = db.execute(
        "SELECT raw_name, bacnet_device_id, obj_type, obj_instance, unit_raw, description "
        "FROM pointmap_staging ORDER BY raw_name"
    ).fetchall()
    return [
        (
            r["raw_name"],
            r["bacnet_device_id"],
            r["obj_type"],
            r["obj_instance"],
            r["unit_raw"],
            r["description"],
        )
        for r in rows
    ]


def adopt_staging(db: sqlite3.Connection) -> dict[str, list]:
    """``pointmap adopt``：staging → pointmap 版本迁移（§14.3-3 定稿口径）。

    - 新点：插入（writable=0 / interval NULL / enabled=1 全默认——方向与节奏
      是部署期工程决策，§4.1）；
    - **地址漂移**：同名（raw_name 同）但 (device_id, obj_type, instance) 变了
      → 物理真相赢：更新地址、raw_name 保持稳定（云端零感知），漂移清单
      交工程师核对（发现报告同标 address_drift）；
    - **工程调参列不覆盖**：unit_raw / writable / interval_s / enabled 是
      sqlite3 直改的本地运维列（§3.2），adopt 只跟随物理世界（地址/新增）；
    - 消失的点：不删不改（missing 清单；poll 诚实产 bad 行，处置归现场）。
    """
    report: dict[str, list] = {"added": [], "address_drift": [], "missing": [], "unchanged": []}
    staging = load_staging(db)
    existing = {p.raw_name: p for p in load_pointmap(db)}
    staging_names: set[str] = set()
    with Tx(db) as t:
        for raw_name, dev, otype, inst, _unit, _desc in staging:
            staging_names.add(raw_name)
            cur = existing.get(raw_name)
            if cur is None:
                t.execute(
                    "INSERT INTO pointmap "
                    "(raw_name, bacnet_device_id, obj_type, obj_instance, unit_raw, writable, interval_s, enabled) "
                    "VALUES (?, ?, ?, ?, ?, 0, NULL, 1)",
                    (raw_name, dev, otype, inst, _unit),
                )
                report["added"].append(raw_name)
            elif (cur.bacnet_device_id, cur.obj_type, cur.obj_instance) != (dev, otype, inst):
                t.execute(
                    "UPDATE pointmap SET bacnet_device_id = ?, obj_type = ?, obj_instance = ? "
                    "WHERE raw_name = ?",
                    (dev, otype, inst, raw_name),
                )
                report["address_drift"].append(
                    {
                        "raw_name": raw_name,
                        "old": [cur.bacnet_device_id, cur.obj_type, cur.obj_instance],
                        "new": [dev, otype, inst],
                    }
                )
            else:
                report["unchanged"].append(raw_name)
        for name in existing:
            if name not in staging_names:
                report["missing"].append(name)
    return report
