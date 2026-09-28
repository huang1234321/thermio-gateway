"""常驻服务总装（gateway.md §5/§6/§7；``thermio-gateway run``）。

组件协作（单进程 asyncio，§12）：

- poll 调度器产样本 → **一律入缓存**（§6.2 严格 FIFO：补传与新数据不交叉）；
- sender 排水循环：oldest-first 取批 → 组消息（≤500 点）→ QoS1 发布 →
  PUBACK 到达才删行 → 窗口（REPLAY_INFLIGHT_MSGS）推进；未连接即等待
  （MQTT 不可达 → 采集不停 → 缓存增长）；
- down/{config,read,write} 三面下行处理（§7）；
- offline_action 状态机由 MQTT 连接事件驱动（§7.4）；
- 保留期清理（§6.1）与指标刷新（§10）为低频后台任务；
- 优雅退出：停 poll → 冲缓存排水尽力（限时）→ 断 MQTT → 关库（§12）。

设备地址台账：启动/低频 Who-Is 解析 pointmap 内全部 device_id → 当前地址
（BACnet 常规姿势；地址不落库，重启后重解析）。解析不到的设备周期产出
``quality=bad`` 行——不可达也是数据，不丢行（§4.4）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import UTC, datetime

from . import cache as cache_mod
from . import pointmap as pm
from .bacnet.poll import PollScheduler
from .bacnet.stack import BacnetStack
from .config import Settings
from .contracts import (
    GatewayConfig,
    ReadCmd,
    SelfCheckRead,
    TelemetryBatch,
    TelemetryPoint,
    WriteCmd,
    parse_write_or_read,
    rfc3339,
)
from .db import meta_set
from .downlink.config import ActiveSet, apply_config
from .downlink.offline_action import OfflineActionExecutor
from .downlink.read import split_read_batches
from .downlink.write import WriteHandler
from .metrics import Registry
from .mqtt_agent import MqttAgent

log = logging.getLogger("gw.service")

ADDRESS_RESOLVE_INTERVAL_S = 60.0
RETENTION_SWEEP_INTERVAL_S = 3600.0
METRICS_REFRESH_INTERVAL_S = 5.0
SHUTDOWN_DRAIN_TIMEOUT_S = 10.0


class GatewayService:
    def __init__(
        self,
        settings: Settings,
        stack: BacnetStack,
        agent: MqttAgent,
        registry: Registry | None = None,
        db: sqlite3.Connection | None = None,
    ) -> None:
        self._s = settings
        self._stack = stack
        self._agent = agent
        self._metrics = registry or Registry()
        self._db = db
        self._addresses: dict[int, str] = {}
        self._units: dict[str, tuple[str | None, str | None]] = {}
        self._active: ActiveSet | None = None
        self._poll = PollScheduler(
            stack, self._sink, settings.poll_default_interval_s, self._metrics
        )
        self._write_handler = WriteHandler(
            stack,
            settings.gateway_serial,
            self._addresses,
            self._lookup_point,
            self._lookup_units,
            self._metrics,
        )
        self._offline = OfflineActionExecutor(
            stack,
            self._addresses,
            self._lookup_point,
            self._lookup_units,
            settings.offline_action_delay_s,
            self._metrics,
        )
        self._tasks: list[asyncio.Task] = []
        self._stopping = asyncio.Event()
        agent.register_handler(agent.topic_down_config(), self._on_down_config)
        agent.register_handler(agent.topic_down_read(), self._on_down_read)
        agent.register_handler(agent.topic_down_write(), self._on_down_write)
        agent.on_connect_event(self._on_conn_state)

    # ── 查表（点/单位）─────────────────────────────────────────────────────

    def _lookup_point(self, raw_name: str) -> pm.PointRow | None:
        return pm.get_point(self._conn(), raw_name)

    def _lookup_units(self, raw_name: str) -> tuple[str | None, str | None]:
        return self._units.get(raw_name, (None, None))

    # ── 生命周期 ───────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self._resolve_addresses()
        await self._agent.start()
        self._tasks = [
            asyncio.create_task(self._sender_loop(), name="sender"),
            asyncio.create_task(self._address_loop(), name="addr-resolve"),
            asyncio.create_task(self._retention_loop(), name="retention"),
            asyncio.create_task(self._metrics_loop(), name="metrics-refresh"),
        ]
        await self._poll.start()
        log.info(
            "service 就绪（points=%d devices=%d；无 down/config 前采集集为空——config 唯一权威）",
            len(self._poll.active_points()),
            len(self._addresses),
        )

    async def stop(self) -> None:
        """优雅退出（§12）：停 poll → 冲排水尽力 → 断 MQTT → 关库。"""
        self._stopping.set()
        await self._poll.stop()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._offline.stop()
        try:
            await asyncio.wait_for(self._drain_all(), SHUTDOWN_DRAIN_TIMEOUT_S)
        except TimeoutError:
            log.warning("退出排水超时（%ss），余量留库下次启动续排", SHUTDOWN_DRAIN_TIMEOUT_S)
        await self._agent.stop()
        if self._db is not None:
            self._db.close()

    # ── 样本面：poll → 缓存 → 排水 ─────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        assert self._db is not None  # stop() 前必在（start 前不产样本）
        return self._db

    async def _sink(self, samples: list[TelemetryPoint]) -> None:
        rows = [
            (
                s.name,
                rfc3339(s.ts),
                s.value,
                s.value_text,
                s.quality,
                s.unit,
            )
            for s in samples
        ]
        cache_mod.enqueue_samples(self._conn(), rows)

    async def _sender_loop(self) -> None:
        while not self._stopping.is_set():
            if not self._agent.is_connected():
                await self._sleep(0.5)
                continue
            size = cache_mod.cache_size(self._conn())
            if size == 0:
                await self._sleep(0.1)
                continue
            try:
                await self._drain_window()
            except ConnectionError:
                await self._sleep(0.5)  # 排水中途断链：行未删，回头重排（§6.2）
            except asyncio.CancelledError:
                raise

    async def _drain_window(self) -> None:
        """按窗口排水：一次取 window×batch 行（peek 不删行，防止窗口重读），
        分 ≤500 点/条依次发布；全部 PUBACK 后统删——中途断链异常上抛，
        行留库重排（at-least-once，重复由 TSDB upsert 幂等吸收，§6.2）。

        paho 单连接发布顺序 = publish 调用序 = rowid 序——严格 FIFO。
        """
        window = self._s.replay_inflight_msgs
        batch_points = self._s.replay_batch_points
        rows = cache_mod.peek_oldest(self._conn(), window * batch_points)
        if not rows:
            return
        tasks: list[asyncio.Task] = []
        for start in range(0, len(rows), batch_points):
            chunk = rows[start : start + batch_points]
            tasks.append(asyncio.create_task(self._publish_rows(chunk)))
        for task in tasks:  # create 顺序 = publish 调用顺序 = 线上消息序
            await task
        cache_mod.delete_samples(self._conn(), [r.cache_id for r in rows])

    async def _publish_rows(self, rows: list[cache_mod.CachedSample]) -> None:
        points = [
            TelemetryPoint(
                name=r.raw_name,
                value=r.value,
                value_text=r.value_text,
                ts=datetime.fromisoformat(r.ts),
                quality=r.quality,  # type: ignore[arg-type]
                unit=r.unit,
            )
            for r in rows
        ]
        seq = cache_mod.next_seq(self._conn())
        batch = TelemetryBatch(
            gw=self._s.gateway_serial,
            seq=seq,
            sent_at=datetime.now(UTC),
            points=points,
        )
        payload = json.dumps(
            batch.model_dump(mode="json"), ensure_ascii=False, separators=(",", ":")
        ).encode()
        await self._agent.publish_qos1(self._agent.topic_up_data(), payload)

    async def _drain_all(self) -> None:
        while cache_mod.cache_size(self._conn()) > 0:
            if not self._agent.is_connected():
                raise ConnectionError("排水要求 MQTT 连接")
            await self._drain_window()

    # ── 地址解析（Who-Is 台账）─────────────────────────────────────────────

    async def _resolve_addresses(self) -> None:
        rows = pm.load_pointmap(self._conn())
        device_ids = sorted({p.bacnet_device_id for p in rows})
        if not device_ids:
            log.info("pointmap 为空（先 discover + pointmap adopt + down/config）")
            return
        try:
            iams = await self._stack.who_is(timeout_s=2.0)
        except Exception as exc:  # 解析失败不阻断启动（bad 行兜底）
            log.warning("地址解析 who-is 失败: %s", exc)
            return
        by_id = {r.device_id: r.address for r in iams}
        for device_id in device_ids:
            addr = by_id.get(device_id)
            if addr is not None:
                self._addresses[device_id] = addr
            else:
                log.warning("device %s 未应答 Who-Is（周期产 bad 行）", device_id)
        self._rebind_all()

    async def _address_loop(self) -> None:
        while not self._stopping.is_set():
            await self._sleep(ADDRESS_RESOLVE_INTERVAL_S)
            missing = [
                d
                for d in {p.bacnet_device_id for p in pm.load_pointmap(self._conn())}
                if d not in self._addresses
            ]
            if not missing:
                continue
            try:
                iams = await self._stack.who_is(timeout_s=2.0)
            except asyncio.CancelledError:
                raise
            except Exception:
                continue
            for rec in iams:
                self._addresses.setdefault(rec.device_id, rec.address)
            self._rebind_all()

    def _rebind_all(self) -> None:
        self._write_handler.bind(self._addresses, self._lookup_point, self._lookup_units)
        self._offline.bind(self._addresses, self._lookup_point, self._lookup_units)

    # ── 下行三面（§7）──────────────────────────────────────────────────────

    def _on_conn_state(self, connected: bool) -> None:
        self._metrics.set("gateway_mqtt_connected", 1.0 if connected else 0.0)
        if connected:
            self._offline.on_mqtt_connected()
        else:
            self._offline.on_mqtt_disconnected()

    async def _on_down_config(self, topic: str, payload: bytes) -> None:
        try:
            cfg = GatewayConfig.model_validate_json(payload)
        except ValueError as exc:
            log.error("down/config 解析失败（忽略整条）: %s", exc)
            return
        active_set, ack = apply_config(self._conn(), cfg, self._addresses)
        self._units = active_set.units
        self._active = active_set
        meta_set(self._conn(), "applied_job_id", cfg.job_id)
        # 原子切换采集集（进行中周期用旧集跑完，§7.1）
        self._poll.set_active(active_set.points, self._addresses)
        self._rebind_all()
        self._offline.update_action(active_set.offline_action)
        self._metrics.set("gateway_config_applied_job", 1.0, job_id=cfg.job_id)
        await self._publish_json(
            self._agent.topic_up_config_ack(),
            ack.model_dump(mode="json"),
        )
        log.info(
            "down/config 应用 job=%s ok=%d failed=%d",
            cfg.job_id,
            ack.ok_count,
            len(ack.failed),
        )

    async def _on_down_read(self, topic: str, payload: bytes) -> None:
        try:
            msg = SelfCheckRead.model_validate_json(payload)
        except ValueError as exc:
            log.error("down/read 解析失败（忽略整条）: %s", exc)
            return
        batches = split_read_batches(msg, lambda n: pm.get_point(self._conn(), n))
        for batch in batches:
            self._poll.request_urgent(batch)  # 立即补采 → 正常 up/data（§5.3）
        log.info("down/read job=%s 插队 %d 点", msg.job_id, len(msg.points))

    async def _on_down_write(self, topic: str, payload: bytes) -> None:
        cmd = parse_write_or_read(payload)
        if cmd is None:
            log.error("down/write 解析失败或 msg_type 不识别（忽略）")
            return
        if isinstance(cmd, WriteCmd):
            ack = await self._write_handler.handle_write(cmd)
            await self._publish_json(self._agent.topic_up_event(), ack.model_dump(mode="json"))
        elif isinstance(cmd, ReadCmd):
            result = await self._write_handler.handle_read(cmd)
            await self._publish_json(self._agent.topic_up_event(), result.model_dump(mode="json"))

    async def _publish_json(self, topic: str, body: dict) -> None:
        payload = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
        await self._agent.publish_qos1(topic, payload)

    # ── 低频维护 ───────────────────────────────────────────────────────────

    async def _retention_loop(self) -> None:
        while not self._stopping.is_set():
            await self._sleep(RETENTION_SWEEP_INTERVAL_S)
            removed = cache_mod.cleanup_retention(self._conn(), self._s.cache_retention_days)
            if removed:
                log.info("保留期清理删除 %d 行（FIFO，§6.1）", removed)

    async def _metrics_loop(self) -> None:
        while not self._stopping.is_set():
            self._metrics.set("gateway_cache_rows", float(cache_mod.cache_size(self._conn())))
            self._metrics.set(
                "gateway_cache_oldest_age_s", cache_mod.cache_oldest_age_s(self._conn())
            )
            backlog = cache_mod.cache_size(self._conn())
            self._metrics.set(
                "gateway_replay_lag_s",
                cache_mod.cache_oldest_age_s(self._conn()) if backlog else 0.0,
            )
            await self._sleep(METRICS_REFRESH_INTERVAL_S)

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            raise
