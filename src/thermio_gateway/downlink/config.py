"""down/config 应用（gateway.md §7.1；M2-import.md §8.4——采集配置唯一来源）。

收到即：解析 points 快照 → 与 pointmap 求交（§3.2 采集集定义）→
原子切换采集集（进行中的周期用旧集跑完）→ 回 ack。

- 采集集 = **config 激活集（云端已注册点）∩ pointmap（物理可达点）**；
  不在 config 的点不采（停用语义归云端）；不在 pointmap 的 config 点
  不可采 → ``failed[] POINT_NOT_IN_MAP``（failure.rows 云端可见）；
- reason 封闭集 v1：``POINT_NOT_IN_MAP`` / ``POINT_DISABLED``；部分失败
  **不回滚**已应用点（全量替换语义 + 最大化可用集）；
- ``applied_job_id`` 落 gw_meta（§3.2）；retained 天然覆盖离线补课；
- ``offline_action`` 原样透传本地执行器（§7.4）；引用不可解析点 → 容错
  忽略 + WARN（M2 §8.4 既定口径）。
"""

from __future__ import annotations

import logging
import sqlite3

from .. import pointmap as pm
from ..contracts import ConfigAck, ConfigAckFailure, GatewayConfig, OfflineAction
from ..pointmap import PointRow

log = logging.getLogger("gw.down.config")


class ActiveSet:
    """当前生效采集集（config ∩ pointmap）与各点 unit 语义。"""

    def __init__(
        self,
        points: list[PointRow],
        units: dict[str, tuple[str | None, str | None]],
        job_id: str,
        offline_action: OfflineAction | None,
    ) -> None:
        self.points = points
        self.units = units  # raw_name → (unit_raw, unit_std)（下行换算用 §7.3）
        self.job_id = job_id
        self.offline_action = offline_action


def apply_config(
    db: sqlite3.Connection,
    cfg: GatewayConfig,
    current_addresses: dict[int, str],
) -> tuple[ActiveSet, ConfigAck]:
    """应用全量配置快照。current_addresses 为发现台账的 device_id → 地址。"""
    pointmap_rows = {p.raw_name: p for p in pm.load_pointmap(db)}
    active: list[PointRow] = []
    failed: list[ConfigAckFailure] = []
    units_by_name: dict[str, tuple[str | None, str | None]] = {}
    for cp in cfg.points:
        row = pointmap_rows.get(cp.raw_name)
        if row is None:
            failed.append(ConfigAckFailure(raw_name=cp.raw_name, reason="POINT_NOT_IN_MAP"))
            continue
        if not row.enabled:
            failed.append(ConfigAckFailure(raw_name=cp.raw_name, reason="POINT_DISABLED"))
            continue
        active.append(row)
        units_by_name[cp.raw_name] = (row.unit_raw or cp.unit_raw, cp.unit_std)
    ack = ConfigAck(job_id=cfg.job_id, ok_count=len(active), failed=failed)
    active_set = ActiveSet(active, units_by_name, cfg.job_id, cfg.offline_action)
    if cfg.offline_action:
        known = set(pointmap_rows)
        for w in cfg.offline_action.writes:
            if w.raw_name not in known:
                log.warning(
                    "offline_action 引用不可解析点 %s（容错忽略，云端 dry-run 已示警）",
                    w.raw_name,
                )
    _ = current_addresses  # 地址台账由 service 层在切换时校验（缺址设备周期跳过并告警）
    return active_set, ack
