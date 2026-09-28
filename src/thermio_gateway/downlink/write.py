"""down/write 处理（gateway.md §7.3；control-safety.md §4.2/§4.3）。

``write_cmd``：
  过期检查（expires_in_s）→ 过期 ⇒ rejected CMD_EXPIRED
  → pointmap 查点 → 无此点 ⇒ rejected POINT_UNKNOWN
  → input 类/不可写 ⇒ rejected WRITE_REFUSED
  → unit_std→unit_raw 换算 → 无转换对 ⇒ rejected WRITE_REFUSED（宁拒不写错）
  → 入队 WriteProperty（同对象在途写 ≤1）⇒ accepted（受理即答，不候 APDU 回程）

``read_cmd``：ReadProperty → read_result{value(unit_std), quality, ts}
（READ_TIMEOUT_S=10s 内：APDU 3s×2 足容）。

- ``accepted`` = 传输回执（§4.4「ack 只是回执、回读仲裁事实」）——写失败
  不另行上报，云端回读验证捕获；**边缘不制造第二套仲裁**；
- cmd_id 幂等：最近 N=256 缓存，首个应答定局，重复投递仅记日志；
- 单位双向：换算对集 = ingest.md §5.2 同一对集（units.py，三栈同源）。
"""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Literal, cast

from ..bacnet.stack import BacnetError, BacnetStack
from ..contracts import ReadCmd, ReadResult, WriteAck, WriteCmd
from ..pointmap import PointRow
from ..units import raw_to_std, std_to_raw

log = logging.getLogger("gw.down.write")

CMD_ID_CACHE = 256
READ_TIMEOUT_S = 10.0


class WriteHandler:
    """write_cmd / read_cmd 处理器（service 层注入地址台账与点查表）。"""

    def __init__(
        self,
        stack: BacnetStack,
        gateway_serial: str,
        addresses: dict[int, str],
        get_point: Callable[[str], PointRow | None],
        unit_lookup: Callable[[str], tuple[str | None, str | None]],
        metrics: Any = None,
    ) -> None:
        self._stack = stack
        self._serial = gateway_serial
        self._addresses = addresses
        self._get_point = get_point
        self._unit_lookup = unit_lookup
        self._metrics = metrics
        self._recent: OrderedDict[str, str] = OrderedDict()  # cmd_id → 定局结果

    def bind(
        self,
        addresses: dict[int, str],
        get_point: Callable[[str], PointRow | None],
        unit_lookup: Callable[[str], tuple[str | None, str | None]],
    ) -> None:
        self._addresses = addresses
        self._get_point = get_point
        self._unit_lookup = unit_lookup

    # ── cmd_id 幂等（§7.3：首个应答定局，重复投递仅记日志）────────────────

    def seen_settled(self, cmd_id: str) -> str | None:
        return self._recent.get(cmd_id) or None if self._recent.get(cmd_id) != "pending" else None

    def _remember(self, cmd_id: str, result: str) -> None:
        self._recent[cmd_id] = result
        while len(self._recent) > CMD_ID_CACHE:
            self._recent.popitem(last=False)

    def is_duplicate(self, cmd_id: str, settled: str | None) -> bool:
        if settled is not None:
            log.info("cmd_id %s 重复投递（已定局 %s），忽略", cmd_id, settled)
            return True
        self._remember(cmd_id, "pending")
        return False

    # ── write_cmd ──────────────────────────────────────────────────────────

    async def handle_write(self, cmd: WriteCmd) -> WriteAck:
        settled = self._recent.get(cmd.cmd_id)
        if settled is not None and settled != "pending":
            log.info("cmd_id %s 重复投递（已定局 %s），忽略", cmd.cmd_id, settled)
            return self._replay_ack(cmd, settled)
        self._remember(cmd.cmd_id, "pending")

        expired = cmd.expires_in_s is not None and cmd.expires_in_s <= 0
        if expired:
            return self._settle_ack(cmd.cmd_id, False, "CMD_EXPIRED")
        point = self._get_point(cmd.point_ref)
        if point is None:
            return self._settle_ack(cmd.cmd_id, False, "POINT_UNKNOWN")
        if not point.writable:
            return self._settle_ack(cmd.cmd_id, False, "WRITE_REFUSED")
        unit_raw, unit_std = self._unit_lookup(cmd.point_ref)
        converted, ok = std_to_raw(cmd.value, unit_std, unit_raw)
        if not ok:
            log.warning(
                "写点 %s 无单位转换对（std=%r raw=%r），宁拒不写错",
                cmd.point_ref,
                unit_std,
                unit_raw,
            )
            return self._settle_ack(cmd.cmd_id, False, "WRITE_REFUSED")
        address = self._addresses.get(point.bacnet_device_id)
        if address is None:
            return self._settle_ack(cmd.cmd_id, False, "POINT_UNKNOWN")
        # 受理即答（§7.3：accepted 不候 APDU 回程）
        ack = self._settle_ack(cmd.cmd_id, True, None)
        try:
            await self._stack.write_property(address, point.obj_type, point.obj_instance, converted)
        except BacnetError as exc:
            # 写失败不另发事件：云端回读验证捕获（边缘不制造第二套仲裁）
            log.error("write_cmd %s 物理写失败（%s）——待云端回读仲裁", cmd.cmd_id, exc)
        return ack

    # ── read_cmd ───────────────────────────────────────────────────────────

    async def handle_read(self, cmd: ReadCmd) -> ReadResult:
        now = datetime.now(UTC)
        point = self._get_point(cmd.point_ref)
        if point is None:
            self._remember(cmd.cmd_id, "rejected")
            self._count("rejected")
            return ReadResult(
                cmd_id=cmd.cmd_id,
                gw=self._serial,
                value=None,
                unit=None,
                quality="bad",
                ts=now,
                at=now,
            )
        address = self._addresses.get(point.bacnet_device_id)
        if address is None:
            self._remember(cmd.cmd_id, "rejected")
            self._count("rejected")
            return ReadResult(
                cmd_id=cmd.cmd_id,
                gw=self._serial,
                value=None,
                unit=None,
                quality="bad",
                ts=now,
                at=now,
            )
        unit_raw, unit_std = self._unit_lookup(cmd.point_ref)
        try:
            result = await asyncio.wait_for(
                self._stack.read_property(
                    address, point.obj_type, point.obj_instance, "presentValue"
                ),
                READ_TIMEOUT_S,
            )
        except (TimeoutError, BacnetError) as exc:
            log.warning("read_cmd %s 读失败（%s）", cmd.cmd_id, exc)
            self._remember(cmd.cmd_id, "rejected")
            self._count("rejected")
            return ReadResult(
                cmd_id=cmd.cmd_id,
                gw=self._serial,
                value=None,
                unit=unit_std,
                quality="bad",
                ts=now,
                at=now,
            )
        raw_value = float(getattr(result.value, "value", result.value))
        std_value, ok = raw_to_std(raw_value, unit_raw, unit_std)
        self._remember(cmd.cmd_id, "accepted")
        self._count("accepted")
        return ReadResult(
            cmd_id=cmd.cmd_id,
            gw=self._serial,
            value=std_value if ok else raw_value,
            unit=unit_std if ok else unit_raw,
            quality="good",
            ts=now,
            at=now,
        )

    # ── 辅助 ───────────────────────────────────────────────────────────────

    def _replay_ack(self, cmd: WriteCmd, settled: str) -> WriteAck:
        return WriteAck(
            cmd_id=cmd.cmd_id,
            gw=self._serial,
            result=settled,  # type: ignore[arg-type]
            code=None,
            at=datetime.now(UTC),
        )

    def _settle_ack(self, cmd_id: str, accepted: bool, code: str | None) -> WriteAck:
        result: Literal["accepted", "rejected"] = "accepted" if accepted else "rejected"
        self._remember(cmd_id, result)
        self._count(result)
        code_literal = cast(Any, code)
        return WriteAck(
            cmd_id=cmd_id,
            gw=self._serial,
            result=result,
            code=code_literal,
            at=datetime.now(UTC),
        )

    def _count(self, result: str) -> None:
        if self._metrics:
            self._metrics.inc("gateway_write_cmd_total", result=result)
