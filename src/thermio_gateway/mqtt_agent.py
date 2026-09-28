"""MQTT 接入（gateway.md §2/§5；emqx.md §2 一致性纪律）。

- MQTT 3.1.1 + QoS 1、``clean_start=false``（3.1.1 = clean_session=False）、
  keepalive 30s、单实例运行（takeover = 配置事故）；
- paho-mqtt 2.x 回调线程经 ``call_soon_threadsafe`` 桥入事件循环（§12）；
- PUBACK 驱动 inflight 窗口（默认 4，§5.2/§6.2 背压服从云端）；
- 上行三面：``up/data``（遥测）、``up/event``（write_ack/read_result）、
  ``up/config/ack``；下行订阅 ``down/{config,read,write}``。

topic 空间（§1.1）：``thermio/gw/{clientid}/...``，零新增。
"""

from __future__ import annotations

import asyncio
import logging
import ssl
from collections.abc import Awaitable, Callable
from typing import Any

import paho.mqtt.client as mqtt

log = logging.getLogger("gw.mqtt")

TopicHandler = Callable[[str, bytes], Awaitable[None]]


class MqttAgent:
    """paho 客户端门面：asyncio 侧只见任务与队列。"""

    def __init__(
        self,
        broker_host: str,
        broker_port: int,
        client_id: str,
        username: str,
        password: str,
        tls: bool = False,
        ca_path: str | None = None,
        keepalive_s: int = 30,
        max_inflight: int = 4,
    ) -> None:
        self._client_id = client_id
        self._connected = asyncio.Event()
        self._disconnected_evt = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._puback_futures: dict[int, asyncio.Future[bool]] = {}
        self._handlers: dict[str, TopicHandler] = {}
        self._connect_events: list[Callable[[bool], None]] = []

        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv311,
            clean_session=False,  # 会话保持（emqx.md §2：session expiry ≥ 1h 由 broker 承载）
        )
        self._client.username_pw_set(username, password)
        if tls:
            ctx = ssl.create_default_context(cafile=ca_path)
            self._client.tls_set_context(ctx)  # 显式信任链，不关校验（SEC-NET-04）
        self._client.max_inflight_messages_set(max_inflight)  # PUBACK 驱动窗口（§5.2）
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.on_publish = self._on_publish
        self._host = broker_host
        self._port = broker_port
        self._keepalive = keepalive_s

    # ── topic 帮手（§1.1 六 topic）─────────────────────────────────────────

    def topic_up_data(self) -> str:
        return f"thermio/gw/{self._client_id}/up/data"

    def topic_up_event(self) -> str:
        return f"thermio/gw/{self._client_id}/up/event"

    def topic_up_config_ack(self) -> str:
        return f"thermio/gw/{self._client_id}/up/config/ack"

    def topic_down_config(self) -> str:
        return f"thermio/gw/{self._client_id}/down/config"

    def topic_down_read(self) -> str:
        return f"thermio/gw/{self._client_id}/down/read"

    def topic_down_write(self) -> str:
        return f"thermio/gw/{self._client_id}/down/write"

    # ── 装配 ───────────────────────────────────────────────────────────────

    def on_connect_event(self, cb: Callable[[bool], None]) -> None:
        """注册连接状态回调（offline_action 状态机消费，§7.4）。"""
        self._connect_events.append(cb)

    def register_handler(self, topic: str, handler: TopicHandler) -> None:
        self._handlers[topic] = handler

    async def start(self) -> None:
        """连网并启动网络循环线程。不阻塞等待连接（重连节奏归 paho）。"""
        self._loop = asyncio.get_running_loop()
        self._client.connect_async(self._host, self._port, keepalive=self._keepalive)
        self._client.loop_start()
        await asyncio.sleep(0)  # 让 connect 任务启动

    async def stop(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()

    async def wait_connected(self, timeout_s: float | None = None) -> bool:
        try:
            await asyncio.wait_for(self._connected.wait(), timeout_s)
            return True
        except TimeoutError:
            return False

    def is_connected(self) -> bool:
        return self._connected.is_set()

    async def simulate_outage(self) -> None:
        """测试/运维钩子：主动断开（模拟断网；采集不停、缓存增长，§6.2）。

        显式 disconnect 后 paho 网络循环线程退出——真实断网也走同型路径
        （循环内重连由 paho retry 承载；显式断开不重连是 MQTT 语义）。
        """
        self._client.disconnect()

    def simulate_restore(self) -> None:
        """测试/运维钩子：网络恢复后重连（重启网络循环）。"""
        self._client.loop_stop()
        self._client.connect_async(self._host, self._port, keepalive=self._keepalive)
        self._client.loop_start()

    # ── paho 回调（网络线程 → call_soon_threadsafe 桥入事件循环，§12）─────

    def _bridge(self, coro_factory: Callable[[], Any]) -> None:
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._spawn, coro_factory)

    def _spawn(self, coro_factory: Callable[[], Any]) -> None:
        task = self._loop.create_task(coro_factory())  # type: ignore[union-attr]
        _BACKGROUND.add(task)
        task.add_done_callback(_BACKGROUND.discard)

    def _on_connect(
        self, client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any
    ) -> None:
        self._bridge(lambda: self._handle_connect(reason_code))

    def _on_disconnect(
        self, client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any
    ) -> None:
        self._bridge(lambda: self._handle_disconnect())

    def _on_message(self, client: Any, userdata: Any, msg: Any) -> None:
        payload = bytes(msg.payload)
        topic = str(msg.topic)
        self._bridge(lambda: self._handle_message(topic, payload))

    def _on_publish(
        self,
        client: Any,
        userdata: Any,
        mid: Any,
        reason_code: Any = None,
        properties: Any = None,
    ) -> None:
        self._bridge(lambda: self._handle_puback(mid))

    async def _handle_connect(self, reason_code: Any) -> None:
        # paho 2.x 回调给 ReasonCode 对象（不可 int()）；旧形态按数值判
        if reason_code is None:
            ok = True
        elif hasattr(reason_code, "is_failure"):
            ok = not reason_code.is_failure
        else:
            ok = int(reason_code) == 0
        if ok:
            log.info("MQTT 已连接（clientid=%s）", self._client_id)
            self._connected.set()
            self._disconnected_evt.clear()
            # 会话恢复后重订阅（clean_session=False 下 broker 仍可能丢订阅状态）
            for topic in self._handlers:
                self._client.subscribe(topic, qos=1)
            for cb in self._connect_events:
                cb(True)
        else:
            log.error("MQTT 连接拒绝 rc=%s（凭证/ACL/网络排查）", reason_code)

    async def _handle_disconnect(self) -> None:
        was = self._connected.is_set()
        self._connected.clear()
        self._disconnected_evt.set()
        if was:
            log.warning("MQTT 断链（进入重连节奏；采集不停，缓存增长——§6.2）")
            for cb in self._connect_events:
                cb(False)
        for future in self._puback_futures.values():
            if not future.done():
                future.set_exception(ConnectionError("断链未及 PUBACK"))
        self._puback_futures.clear()

    async def _handle_message(self, topic: str, payload: bytes) -> None:
        handler = None
        for pattern, candidate in self._handlers.items():
            if pattern == topic:
                handler = candidate
                break
        if handler is None:
            log.warning("未注册 topic %s 消息（忽略）", topic)
            return
        try:
            await handler(topic, payload)
        except Exception:
            log.exception("下行消息处理异常 topic=%s", topic)

    async def _handle_puback(self, mid: Any) -> None:
        future = self._puback_futures.pop(mid, None)
        if future is not None and not future.done():
            future.set_result(True)

    # ── 发布（QoS 1；PUBACK 到达才返回，§6.2 删行语义的上游）──────────────

    async def publish_qos1(self, topic: str, payload: bytes) -> None:
        if not self._connected.is_set():
            raise ConnectionError("MQTT 未连接")
        loop = self._loop
        assert loop is not None
        future: asyncio.Future[bool] = loop.create_future()
        info = self._client.publish(topic, payload, qos=1)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise ConnectionError(f"publish 失败 rc={info.rc}")
        self._puback_futures[info.mid] = future
        await future


_BACKGROUND: set[asyncio.Task] = set()
