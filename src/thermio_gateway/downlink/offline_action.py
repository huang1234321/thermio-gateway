"""offline_action 断链安全值（gateway.md §7.4；ADR-003 硬指标 4 的软件化）。

状态机：

    LINK_UP ──MQTT 断──▶ GRACE(60s，重连即取消) ──▶ SAFE_WRITE(按序写安全值，
    失败重试 3 次退避) ──完成──▶ ARMED（单次触发，不在断网期间反复写）
    LINK_UP ◀──────────── MQTT 恢复 ──────────────────┘

- 触发窗默认 60s（防抖：闪断不触发；OFFLINE_ACTION_DELAY_S 可调）；
- 安全值单位口径 = unit_std（与 write_cmd 同纪律；M1 §3.8 无 unit 字段的
  既定解读）——边缘换算成设备原生单位再写；
- 换算失败/点不在 pointmap → 容错跳过 + 本地 ERROR 日志（事后可查；
  云端 M2 dry-run 的 offline_action_ref_unresolved 警告是第一道防线）；
- **重连不自动回写原值**：恢复秩序归云端（ADR-009 租约回滚/人工指令）；
- ARMED 后回到 LINK_UP 需先经历一次重连（单次触发语义）。
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from ..bacnet.stack import BacnetError, BacnetStack
from ..contracts import OfflineAction
from ..pointmap import PointRow
from ..units import std_to_raw

log = logging.getLogger("gw.down.offline_action")

RETRY_MAX = 3
RETRY_BACKOFF_S = 2.0

STATE_LINK_UP = "LINK_UP"
STATE_GRACE = "GRACE"
STATE_SAFE_WRITE = "SAFE_WRITE"
STATE_ARMED = "ARMED"


class OfflineActionExecutor:
    """断链安全值执行器（service 桥接 MQTT 连接事件与最新 offline_action）。"""

    def __init__(
        self,
        stack: BacnetStack,
        addresses: dict[int, str],
        get_point: Callable[[str], PointRow | None],
        unit_lookup: Callable[[str], tuple[str | None, str | None]],
        delay_s: int,
        metrics: Any = None,
        retry_backoff_s: float = RETRY_BACKOFF_S,
    ) -> None:
        self._stack = stack
        self._addresses = addresses
        self._get_point = get_point
        self._unit_lookup = unit_lookup
        self._delay_s = delay_s
        self._retry_backoff_s = retry_backoff_s
        self._metrics = metrics
        self._action: OfflineAction | None = None
        self._state = STATE_LINK_UP
        self._timer_task: asyncio.Task | None = None
        self._armed_latch = False  # ARMED 单次触发：断网期内只写一遍

    def bind(
        self,
        addresses: dict[int, str],
        get_point: Callable[[str], PointRow | None],
        unit_lookup: Callable[[str], tuple[str | None, str | None]],
    ) -> None:
        self._addresses = addresses
        self._get_point = get_point
        self._unit_lookup = unit_lookup

    def update_action(self, action: OfflineAction | None) -> None:
        """down/config 透传最新 offline_action（§7.1）。"""
        self._action = action
        self._armed_latch = False  # 新配置重置触发闩

    def state(self) -> str:
        return self._state

    # ── 连接事件驱动 ───────────────────────────────────────────────────────

    def on_mqtt_disconnected(self) -> None:
        if self._state != STATE_LINK_UP or self._timer_task is not None:
            return
        log.warning("MQTT 断链，GRACE %ss（重连即取消）", self._delay_s)
        self._state = STATE_GRACE
        self._timer_task = asyncio.create_task(self._grace_then_write())

    def on_mqtt_connected(self) -> None:
        if self._timer_task is not None:
            self._timer_task.cancel()
            self._timer_task = None
            log.info("GRACE 期内重连，安全值触发取消")
        # ARMED → LINK_UP（重连不自动回写原值，§7.4：恢复秩序归云端）
        self._state = STATE_LINK_UP

    async def _grace_then_write(self) -> None:
        try:
            await asyncio.sleep(self._delay_s)
        except asyncio.CancelledError:
            return
        self._timer_task = None
        if self._action is None or not self._action.writes:
            self._state = STATE_ARMED  # 无配置也置 ARMED（单次触发语义）
            log.info("offline_action 无安全值清单，直接 ARMED")
            return
        if self._armed_latch:
            self._state = STATE_ARMED
            return
        self._state = STATE_SAFE_WRITE
        log.warning("GRACE 到期，按序写安全值（%d 项）", len(self._action.writes))
        for w in self._action.writes:
            await self._write_safe_value(w.raw_name, w.value)
        self._armed_latch = True
        self._state = STATE_ARMED
        log.warning("安全值写毕，ARMED（断网期间不再反复写；重连不自动回写）")

    async def _write_safe_value(self, raw_name: str, value_std: float) -> None:
        point = self._get_point(raw_name)
        if point is None:
            log.error("offline_action 点 %s 不在 pointmap（容错跳过）", raw_name)
            self._count("skipped")
            return
        if not point.writable:
            log.error("offline_action 点 %s 不可写（容错跳过）", raw_name)
            self._count("skipped")
            return
        address = self._addresses.get(point.bacnet_device_id)
        if address is None:
            log.error("offline_action 点 %s 无设备地址（容错跳过）", raw_name)
            self._count("skipped")
            return
        unit_raw, unit_std = self._unit_lookup(raw_name)
        converted, ok = std_to_raw(value_std, unit_std, unit_raw)
        if not ok:
            log.error(
                "offline_action 点 %s 无单位转换对（std=%r raw=%r），容错跳过",
                raw_name,
                unit_std,
                unit_raw,
            )
            self._count("skipped")
            return
        for attempt in range(1, RETRY_MAX + 1):
            try:
                await self._stack.write_property(
                    address, point.obj_type, point.obj_instance, converted
                )
                self._count("ok")
                log.info(
                    "安全值写入 %s ← %s %s（%s %s）",
                    raw_name,
                    value_std,
                    unit_std or "?",
                    converted,
                    unit_raw or "?",
                )
                return
            except BacnetError as exc:
                log.error(
                    "安全值写 %s 失败（%s/%d 次）",
                    raw_name,
                    exc,
                    attempt,
                )
                await asyncio.sleep(self._retry_backoff_s * attempt)
        self._count("failed")
        log.error("安全值写 %s 重试耗尽（值域已被云端 clamp，余仅日志+指标）", raw_name)

    def _count(self, result: str) -> None:
        if self._metrics:
            self._metrics.inc("gateway_offline_action_writes_total", result=result)

    async def stop(self) -> None:
        if self._timer_task is not None:
            self._timer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._timer_task
            self._timer_task = None


def _now() -> datetime:  # 观测辅助（日志时间戳由 logging 层给）
    return datetime.now(UTC)
