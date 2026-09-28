"""轮询调度器（gateway.md §4.4）。

- 分组：按 ``(device_id, interval_s)`` 分组调度；interval 取 pointmap 逐点
  ``interval_s``（NULL → POLL_DEFAULT_INTERVAL_S，边缘本地配置，§14.2 差异 1）；
- 组内 **ReadPropertyMultiple 优先**（批 ≤20 对象/请求，防超 MAX_APDU）；
  设备不支持 RPM（error 回包）→ 自动降级逐对象 ReadProperty 并**记忆**；
- 超时/错误/APDU error → 该点 ``value=null, quality=bad``——L1a 职责，
  不丢行（ingest.md §5.3 边界表第一行）；
- Reliability ≠ no-fault-detected（可读设备）→ ``quality=uncertain``（§5.1）；
- 采样完成即成样本 ``(raw_name, ts=完成时刻, value/value_text, quality, unit)``
  交给 sink（cache 入队；§6.1 每轮询周期一个事务）。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from ..contracts import TelemetryPoint
from ..pointmap import PointRow
from .stack import BacnetError, BacnetStack

log = logging.getLogger("gw.poll")

SampleSink = Callable[[list[TelemetryPoint]], Awaitable[None]]

# §4.4：RPM 批 ≤20 对象/请求（防超 MAX_APDU）
RPM_BATCH_OBJECTS = 20


def _kebab(prop: str) -> str:
    return prop[:1].lower() + "".join(("-" + ch.lower()) if ch.isupper() else ch for ch in prop[1:])


class PollScheduler:
    """按组周期采集；同一时刻每组一个周期任务在跑（周期堆积时跳过下一槽）。"""

    def __init__(
        self,
        stack: BacnetStack,
        sink: SampleSink,
        default_interval_s: int,
        metrics: Any = None,
    ) -> None:
        self._stack = stack
        self._sink = sink
        self._default_interval_s = default_interval_s
        self._metrics = metrics
        self._rpm_unsupported: set[str] = set()  # "device_id|address" 记忆（§4.4）
        self._addresses: dict[int, str] = {}
        self._active: list[PointRow] = []
        self._tasks: list[asyncio.Task] = []
        self._cycle_tasks: set[asyncio.Task] = set()
        self._stop = asyncio.Event()
        self._wakeup: asyncio.Queue[tuple[str, list[PointRow]]] = asyncio.Queue()
        # down/read 自检插队（§5.3：立即一次性补采，优先级高于常规周期）
        self._urgent: asyncio.Queue[list[PointRow]] = asyncio.Queue()

    # ── 采集集管理（§3.2：config 激活集 ∩ pointmap）────────────────────────

    def set_active(self, points: list[PointRow], addresses: dict[int, str]) -> None:
        """down/config 应用后原子切换采集集（进行中的周期用旧集跑完，§7.1）。"""
        self._active = points
        self._addresses = addresses

    def active_points(self) -> list[PointRow]:
        return list(self._active)

    def addresses(self) -> dict[int, str]:
        return dict(self._addresses)

    def request_urgent(self, points: list[PointRow]) -> None:
        """自检读指令清单入队（§5.3）。"""
        if points:
            self._urgent.put_nowait(points)

    # ── 生命周期 ───────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._stop.clear()
        self._tasks.append(asyncio.create_task(self._group_loop(), name="poll-groups"))
        self._tasks.append(asyncio.create_task(self._urgent_loop(), name="poll-urgent"))

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        for task in self._cycle_tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, *self._cycle_tasks, return_exceptions=True)
        self._tasks.clear()
        self._cycle_tasks.clear()

    async def _group_loop(self) -> None:
        """组调度主循环：按 next_due 时间轮转（组数少，亚秒扫描成本可忽略）。"""
        dues: dict[tuple[int, int], float] = {}
        while not self._stop.is_set():
            groups = self._group()
            if not groups:
                await self._sleep_stoppable(0.5)
                continue
            now = time.monotonic()
            fired = False
            for key, (points, _addr) in self._group_items(groups):
                due = dues.setdefault(key, now)
                if now >= due:
                    dues[key] = now + self._interval_of(points)
                    task = asyncio.create_task(self._run_cycle_safe(points))
                    self._cycle_tasks.add(task)
                    task.add_done_callback(self._cycle_tasks.discard)
                    fired = True
            await self._sleep_stoppable(0.2 if fired else 0.5)

    def _group(self) -> dict[tuple[int, int], list[PointRow]]:
        groups: dict[tuple[int, int], list[PointRow]] = {}
        for p in self._active:
            interval = p.interval_s or self._default_interval_s
            groups.setdefault((p.bacnet_device_id, interval), []).append(p)
        return groups

    def _group_items(self, groups):
        for (device_id, interval), points in groups.items():
            addr = self._addresses.get(device_id)
            if addr is None:
                log.warning("device %s 无地址（发现台账缺失），周期跳过", device_id)
                continue
            yield (device_id, interval), (points, addr)

    def _interval_of(self, points: list[PointRow]) -> float:
        return float(points[0].interval_s or self._default_interval_s)

    async def _sleep_stoppable(self, seconds: float) -> None:
        with_later = asyncio.create_task(self._stop.wait())
        try:
            await asyncio.wait([with_later], timeout=seconds)
        finally:
            with_later.cancel()

    async def _run_cycle_safe(self, points: list[PointRow]) -> None:
        started = time.monotonic()
        try:
            await self.run_cycle(points)
        except Exception:  # 周期任务自身故障不拖垮调度器
            log.exception("poll cycle 异常（points=%d）", len(points))
        finally:
            if self._metrics:
                self._metrics.observe("gateway_poll_cycle_duration_s", time.monotonic() - started)

    # ── 单周期：同组同设备一轮采集 ────────────────────────────────────────

    async def run_cycle(self, points: list[PointRow]) -> None:
        if not points:
            return
        device_id = points[0].bacnet_device_id
        address = self._addresses.get(device_id)
        if address is None:
            return
        # 同一设备可能混档 interval 组，但 run_cycle 内部以整个清单跑
        batch: list[PointRow] = list(points)
        samples: list[TelemetryPoint] | None
        if f"{device_id}|{address}" not in self._rpm_unsupported:
            samples = await self._cycle_rpm(address, device_id, batch)
        else:
            samples = await self._cycle_rp(address, device_id, batch)
        # RPM 首次失败（不支持）→ 降级重跑并记忆
        if samples is None:
            self._rpm_unsupported.add(f"{device_id}|{address}")
            log.info("device %s RPM 不支持，降级逐对象 ReadProperty 并记忆", device_id)
            samples = await self._cycle_rp(address, device_id, batch)
        if samples:
            await self._sink(samples)

    async def _cycle_rpm(
        self, address: str, device_id: int, points: list[PointRow]
    ) -> list[TelemetryPoint] | None:
        """RPM 路径：批 ≤20 对象，每对象带 present-value + reliability。"""
        samples: list[TelemetryPoint] = []
        ts = datetime.now(UTC)
        for chunk_start in range(0, len(points), RPM_BATCH_OBJECTS):
            chunk = points[chunk_start : chunk_start + RPM_BATCH_OBJECTS]
            requests = [
                (p.obj_type, p.obj_instance, ["presentValue", "reliability"]) for p in chunk
            ]
            try:
                results = await self._stack.read_property_multiple(address, requests)
            except BacnetError as exc:
                if exc.kind in ("error", "reject"):
                    # RPM 不支持（error/reject 回包）→ 由调用方降级并记忆（§4.4）
                    return None
                # 超时/中止：整批 bad（不丢行）
                self._count(device_id, "timeout")
                samples.extend(self._bad_chunk(chunk, ts))
                continue
            by_object: dict[tuple[str, int], dict[str, Any]] = {}
            for obj_type, instance, prop, value in results:
                by_object.setdefault((obj_type, instance), {})[prop] = value
            for p in chunk:
                found = by_object.get((p.obj_type, p.obj_instance), {})
                if "present-value" not in found:
                    self._count(device_id, "error")
                    samples.append(self._bad_point(p, ts))
                    continue
                sample = self._sample_from(p, found["present-value"], ts)
                if self._reliability_faulted(found.get("reliability")):
                    sample = sample.model_copy(update={"quality": "uncertain"})
                self._count(device_id, "ok")
                samples.append(sample)
        return samples

    @staticmethod
    def _reliability_faulted(reliability: Any) -> bool:
        """Reliability ≠ no-fault-detected → uncertain（§5.1）。

        RPM 内嵌错误项（对象无 reliability 属性） bacpypes3 以 ErrorType
        对象回——那不是设备自报故障，忽略（保持 good）。
        """
        from bacpypes3.basetypes import ErrorType

        if reliability is None or isinstance(reliability, ErrorType):
            return False
        return str(getattr(reliability, "value", reliability)) != "no-fault-detected"

    async def _cycle_rp(
        self, address: str, device_id: int, points: list[PointRow]
    ) -> list[TelemetryPoint]:
        """逐对象 ReadProperty 降级路径（含 reliability 追问，PV 失败即 bad）。"""
        samples: list[TelemetryPoint] = []
        ts = datetime.now(UTC)
        for p in points:
            try:
                result = await self._stack.read_property(
                    address, p.obj_type, p.obj_instance, "presentValue"
                )
            except BacnetError as exc:
                self._count(device_id, "timeout" if exc.kind == "timeout" else "error")
                samples.append(self._bad_point(p, ts))
                continue
            sample = self._sample_from(p, result.value, ts)
            reliability = await self._stack.read_reliability(address, p.obj_type, p.obj_instance)
            if reliability is not None and reliability != "no-fault-detected":
                sample = sample.model_copy(update={"quality": "uncertain"})
            self._count(device_id, "ok")
            samples.append(sample)
        return samples

    # ── 样本构造（§4.2 对象类型映射 / §5.1 上行字段）───────────────────────

    def _sample_from(self, p: PointRow, value: Any, ts: datetime) -> TelemetryPoint:
        if p.obj_type.startswith("binary-"):
            text = str(getattr(value, "value", value))
            return TelemetryPoint(
                name=p.raw_name, value_text=text, ts=ts, quality="good", unit=None
            )
        if p.obj_type.startswith("multi-state-"):
            state = int(getattr(value, "value", value))
            return TelemetryPoint(
                name=p.raw_name, value=float(state), ts=ts, quality="good", unit=None
            )
        # analog 族：float 原样（BACnet Real 为 32 位，伪精度是物理事实，不人为舍入）
        num = float(getattr(value, "value", value))
        return TelemetryPoint(name=p.raw_name, value=num, ts=ts, quality="good", unit=p.unit_raw)

    def _bad_point(self, p: PointRow, ts: datetime) -> TelemetryPoint:
        return TelemetryPoint(
            name=p.raw_name,
            value=None,
            value_text=None,
            ts=ts,
            quality="bad",
            unit=p.unit_raw if p.obj_type.startswith("analog-") else None,
        )

    def _bad_chunk(self, chunk: list[PointRow], ts: datetime) -> list[TelemetryPoint]:
        return [self._bad_point(p, ts) for p in chunk]

    def _count(self, device_id: int, result: str) -> None:
        if self._metrics:
            self._metrics.inc("gateway_poll_total", device=str(device_id), result=result)

    # ── 自检插队（§5.3）─────────────────────────────────────────────────────

    async def _urgent_loop(self) -> None:
        while not self._stop.is_set():
            try:
                points = await asyncio.wait_for(self._urgent.get(), timeout=0.5)
            except TimeoutError:
                continue
            await self._run_cycle_safe(points)
