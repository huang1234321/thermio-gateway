"""端到端主链（验收 1）：发现 → 导出 → 上行 → 缓存补传 → 下行面 → 断链安全值。

由 ``scripts/e2e.sh`` 驱动：自起伞仓 deploy compose **隔离栈**（唯一 project +
动态端口，环境隔离纪律；与并发会话/共享 thermio-dev 栈互不踩），EMQX 真实
MQTT 面对拍；BACnet 面用 bacpypes3 VirtualNetwork 进程内仿真（RPM 两型）。

环境变量（e2e.sh 注入）：

- ``E2E_EMQX_HOST`` / ``E2E_EMQX_PORT``：隔离栈 EMQX；
- ``E2E_CLIENT_ID`` / ``E2E_SERIAL``：伪装面身份（缺省 e2e 固定值——dev 栈
  无钩子模式不校验注册绑定）。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import socket
import time
import uuid
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import paho.mqtt.client as mqtt
import pytest
from bacpypes3.vlan import VirtualNetwork

from tests.sim.device import SimDevice
from thermio_gateway.bacnet.discover import (
    Discoverer,
    stage_pointmap,
    write_export_xlsx,
    write_report,
)
from thermio_gateway.bacnet.stack import BacnetStack, build_virtual_application
from thermio_gateway.config import Settings
from thermio_gateway.db import meta_get, open_db
from thermio_gateway.metrics import Registry, serve_metrics
from thermio_gateway.mqtt_agent import MqttAgent
from thermio_gateway.pointmap import adopt_staging, load_pointmap
from thermio_gateway.service import GatewayService

EMQX_HOST = os.environ.get("E2E_EMQX_HOST", "127.0.0.1")
EMQX_PORT = int(os.environ.get("E2E_EMQX_PORT", "1883"))
CLIENT_ID = os.environ.get("E2E_CLIENT_ID", "e2e-gw-client")
SERIAL = os.environ.get("E2E_SERIAL", "SWGW-E2E-01")

UNIT_STD_BY_NAME = {"CHWS_T_AV": "degC", "OAT_AI": "degC", "SETPOINT_AV": "degC"}


class Driver:
    """云端视角驱动：订阅全部上行面、下发三面下行。"""

    def __init__(self) -> None:
        self.up_data: deque[dict] = deque()
        self.up_event: deque[dict] = deque()
        self.up_config_ack: deque[dict] = deque()
        self.connected = asyncio.Event()
        self._loop = asyncio.get_running_loop()
        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"driver-{uuid.uuid4().hex[:6]}",
            protocol=mqtt.MQTTv311,
        )
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

    def _bridge(self, fn: Callable[[], None]) -> None:
        self._loop.call_soon_threadsafe(fn)

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        self._bridge(lambda: self.connected.set())

    def _on_message(self, client, userdata, msg) -> None:
        topic, payload = msg.topic, bytes(msg.payload)

        def store() -> None:
            obj = json.loads(payload)
            if topic.endswith("/up/data"):
                self.up_data.append(obj)
            elif topic.endswith("/up/event"):
                self.up_event.append(obj)
            elif topic.endswith("/up/config/ack"):
                self.up_config_ack.append(obj)

        self._bridge(store)

    async def start(self) -> None:
        self._client.connect(EMQX_HOST, EMQX_PORT, keepalive=30)
        self._client.loop_start()
        await asyncio.wait_for(self.connected.wait(), 5)
        self._client.subscribe(f"thermio/gw/{CLIENT_ID}/up/#", qos=1)
        await asyncio.sleep(0.3)  # 订阅生效

    def stop(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()

    def publish(self, topic: str, payload: dict, retain: bool = False) -> None:
        self._client.publish(topic, json.dumps(payload, ensure_ascii=False), qos=1, retain=retain)

    async def wait_for(self, queue: deque, predicate, timeout: float = 15.0) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for obj in list(queue):
                if predicate(obj):
                    return obj
            await asyncio.sleep(0.1)
        raise AssertionError(f"等待上行消息超时（队列 {len(queue)} 条）")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def wait_until(predicate, timeout: float = 15.0, interval: float = 0.2) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("条件等待超时")


async def wait_until_async(predicate, timeout: float = 15.0, interval: float = 0.2) -> None:
    """异步条件等待（谓词自身是 coroutine 函数——如读 sim 设备现场值）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("异步条件等待超时")


@pytest.mark.e2e
async def test_main_chain(tmp_path: Path) -> None:
    # ═══ 阶段 0：仿真设备 + 网关栈（VLAN 进程内，RPM 两型）═══
    net_name = f"e2e-{uuid.uuid4().hex[:8]}"
    net = VirtualNetwork(net_name)
    dev = SimDevice(net, net_name, device_id=519, mac=2, rpm_supported=True)
    old_dev = SimDevice(net, net_name, device_id=620, mac=3, rpm_supported=False)
    await dev.apply_spec()
    await old_dev.apply_spec()
    stack = BacnetStack(
        build_virtual_application(net, net_name, mac=1, device_id=900),
        max_inflight=2,
        inter_request_gap_ms=0,
    )

    # ═══ 阶段 1：发现 → 三件产物 ═══
    report = await Discoverer(stack).run()
    assert {d.device_id for d in report.devices} == {519, 620}
    report_path = write_report(report, str(tmp_path))
    assert report_path.exists()
    db = open_db(str(tmp_path / "cache.db"))
    staged = stage_pointmap(db, report)
    assert staged == len(report.points)
    xlsx = write_export_xlsx(report, str(tmp_path))
    assert xlsx.exists() and xlsx.suffix == ".xlsx"

    # ═══ 阶段 2：pointmap adopt + 部署期工程决策（写点授权 / 快周期）═══
    adopt = adopt_staging(db)
    assert len(adopt["added"]) == staged
    db.execute("UPDATE pointmap SET writable = 1 WHERE raw_name = 'SETPOINT_AV'")
    db.execute("UPDATE pointmap SET interval_s = 1")  # e2e 加速：1s 周期
    adopted = {p.raw_name: p for p in load_pointmap(db)}
    assert adopted["SETPOINT_AV"].writable is True
    assert adopted["CHWS_T_AV"].writable is False

    # ═══ 阶段 3：service + 云端驱动接入隔离 EMQX ═══
    agent = MqttAgent(
        broker_host=EMQX_HOST,
        broker_port=EMQX_PORT,
        client_id=CLIENT_ID,
        username="e2e",
        password="e2e",
        tls=False,
        max_inflight=4,
    )
    settings = Settings(
        mqtt_broker_url=f"mqtt://{EMQX_HOST}:{EMQX_PORT}",
        mqtt_username="e2e",
        mqtt_password="e2e",
        mqtt_client_id=CLIENT_ID,
        gateway_serial=SERIAL,
        mqtt_ca_path=None,
        mqtt_keepalive_s=30,
        bacnet_device_id=900,
        bacnet_port=47808,
        bacnet_interface="0.0.0.0",
        bacnet_apdu_timeout_ms=1000,
        bacnet_apdu_retries=0,
        bacnet_max_inflight_per_device=2,
        bacnet_inter_request_gap_ms=0,
        poll_default_interval_s=1,
        cache_path=str(tmp_path / "cache.db"),
        cache_retention_days=4,
        replay_inflight_msgs=4,
        replay_batch_points=500,
        offline_action_delay_s=60,
        metrics_addr=f"127.0.0.1:{free_port()}",
        log_level="INFO",
        log_file=None,
    )
    registry = Registry()
    metrics_server = await serve_metrics(registry, settings.metrics_addr)  # cmd_run 同款
    service = GatewayService(settings, stack, agent, registry, db)
    await service.start()
    driver = Driver()
    await driver.start()

    # ═══ 阶段 4：down/config（retained）→ ack 会合（含 GHOST 失败面）═══
    cfg_points = [
        {"raw_name": p, "ref": p, "unit_raw": None, "unit_std": UNIT_STD_BY_NAME.get(p)}
        for p in adopted
    ] + [{"raw_name": "GHOST_X", "ref": "GHOST_X", "unit_raw": None, "unit_std": None}]
    driver.publish(
        f"thermio/gw/{CLIENT_ID}/down/config",
        {
            "schema_version": 1,
            "job_id": "job-e2e-1",
            "generated_at": datetime.now(UTC).isoformat(),
            "points": cfg_points,
            "offline_action": None,
        },
        retain=True,
    )
    ack = await driver.wait_for(driver.up_config_ack, lambda m: m["job_id"] == "job-e2e-1")
    assert ack["ok_count"] == len(adopted)
    assert ack["failed"] == [{"raw_name": "GHOST_X", "reason": "POINT_NOT_IN_MAP"}]
    assert meta_get(db, "applied_job_id") == "job-e2e-1"

    # ═══ 阶段 5：上行信封逐字段（ingest §3.2 镜像断言）═══
    def valid_batch(m: dict) -> bool:
        return (
            m["msg_type"] == "telemetry_batch"
            and m["ver"] == 1
            and m["gw"] == SERIAL
            and isinstance(m["seq"], int)
            and m["seq"] >= 0
            and 1 <= len(m["points"]) <= 500
        )

    await wait_until(lambda: len(driver.up_data) >= 2)
    by_name: dict[str, dict] = {}
    seqs: list[int] = []
    for m in list(driver.up_data):
        assert valid_batch(m), m
        seqs.append(m["seq"])
        datetime.fromisoformat(m["sent_at"].replace("Z", "+00:00"))  # RFC3339 带时区
        for p in m["points"]:
            assert re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", p["name"]), p
            datetime.fromisoformat(p["ts"].replace("Z", "+00:00"))
            assert p["quality"] in ("good", "bad", "uncertain")
            by_name.setdefault(p["name"], p)
    assert seqs == sorted(seqs), "上行 seq 必须单调递增（网关内）"
    # 两型设备（RPM 与降级路径）都有样本；值/枚态/多态映射正确（§4.2）
    assert by_name["CHWS_T_AV"]["value"] == pytest.approx(7.42, abs=1e-5)
    assert by_name["CHWS_T_AV"]["unit"] == "degC"
    assert by_name["PUMP_BV"]["value_text"] == "active"
    assert by_name["MODE_MSI"]["value"] == 2.0
    # 620 号设备（RPM 降级路径）同名点去重后缀（跨设备 raw_name 全局唯一）
    deduped = [
        p for name, p in by_name.items() if name.startswith("CHWS_T_AV") and name != "CHWS_T_AV"
    ]
    assert deduped and deduped[0]["value"] == pytest.approx(7.42, abs=1e-5)

    # ═══ 阶段 6：down/read 自检 → 正常 up/data 立即补采（§5.3）═══
    driver.publish(
        f"thermio/gw/{CLIENT_ID}/down/read",
        {"job_id": "job-e2e-1", "req_id": "req-1", "points": ["CHWS_T_AV", "OAT_AI"]},
    )
    # 自检样本与生产样本同管线：新 up/data 到达即命中（ts 继续打点）

    # ═══ 阶段 7：down/write 三面（§7.3）═══
    base = f"thermio/gw/{CLIENT_ID}"
    driver.publish(
        base + "/down/write",
        {
            "msg_type": "write_cmd",
            "ver": 1,
            "cmd_id": "cmd-w1",
            "point_ref": "SETPOINT_AV",
            "value": 25.0,
            "unit": "degC",
            "issued_at": datetime.now(UTC).isoformat(),
            "expires_in_s": 30,
        },
    )
    ack_w = await driver.wait_for(driver.up_event, lambda m: m.get("cmd_id") == "cmd-w1")
    assert ack_w["result"] == "accepted" and ack_w["gw"] == SERIAL

    async def write_landed() -> bool:
        return float(await dev.read("SETPOINT_AV")) == pytest.approx(25.0)

    await wait_until_async(write_landed)

    driver.publish(
        base + "/down/write",
        {
            "msg_type": "read_cmd",
            "ver": 1,
            "cmd_id": "cmd-r1",
            "point_ref": "SETPOINT_AV",
            "issued_at": datetime.now(UTC).isoformat(),
            "expires_in_s": 30,
        },
    )
    read_result = await driver.wait_for(driver.up_event, lambda m: m.get("cmd_id") == "cmd-r1")
    assert read_result["msg_type"] == "read_result"
    assert read_result["value"] == pytest.approx(25.0)
    assert read_result["unit"] == "degC" and read_result["quality"] == "good"

    # 不可写点拒写；过期指令拒执行；重复 cmd_id 幂等
    driver.publish(
        base + "/down/write",
        {
            "msg_type": "write_cmd",
            "ver": 1,
            "cmd_id": "cmd-w2",
            "point_ref": "CHWS_T_AV",
            "value": 99.0,
            "unit": "degC",
            "issued_at": datetime.now(UTC).isoformat(),
            "expires_in_s": 30,
        },
    )
    ack2 = await driver.wait_for(driver.up_event, lambda m: m.get("cmd_id") == "cmd-w2")
    assert ack2["result"] == "rejected" and ack2["code"] == "WRITE_REFUSED"

    driver.publish(
        base + "/down/write",
        {
            "msg_type": "write_cmd",
            "ver": 1,
            "cmd_id": "cmd-w3",
            "point_ref": "SETPOINT_AV",
            "value": 30.0,
            "unit": "degC",
            "issued_at": datetime.now(UTC).isoformat(),
            "expires_in_s": 0,
        },
    )
    ack3 = await driver.wait_for(driver.up_event, lambda m: m.get("cmd_id") == "cmd-w3")
    assert ack3["code"] == "CMD_EXPIRED"

    driver.publish(
        base + "/down/write",
        {
            "msg_type": "write_cmd",
            "ver": 1,
            "cmd_id": "cmd-w1",  # 重复投递
            "point_ref": "SETPOINT_AV",
            "value": 26.0,
            "unit": "degC",
            "issued_at": datetime.now(UTC).isoformat(),
            "expires_in_s": 30,
        },
    )
    await asyncio.sleep(1.0)
    assert float(await dev.read("SETPOINT_AV")) == pytest.approx(25.0)  # 未被重复写

    # ═══ 阶段 8：断链缓存补传（§6.2 主张：ts 不改、严格 FIFO、seq 不断）═══
    seq_before = max(m["seq"] for m in driver.up_data)
    await agent.simulate_outage()
    await asyncio.sleep(3.0)  # ≥3 个周期入缓存（1s 周期）
    outage_rows = db.execute("SELECT COUNT(*) AS n FROM sample_cache").fetchone()["n"]
    assert outage_rows > 0, "断网期间采集不停、缓存增长"
    driver.up_data.clear()
    agent.simulate_restore()
    await wait_until(
        lambda: (
            len(driver.up_data) >= 2
            and db.execute("SELECT COUNT(*) AS n FROM sample_cache").fetchone()["n"] == 0
        ),
        timeout=20,
    )
    drained = list(driver.up_data)
    drained_seqs = [m["seq"] for m in drained]
    assert drained_seqs == sorted(drained_seqs), "排水严格 oldest-first（FIFO）"
    assert drained_seqs[0] == seq_before + 1, "恢复后 seq 续排不重不断"
    # 补传样本 ts 是采集原始时刻（断链窗内），sent_at 是排水时刻
    for m in drained:
        for p in m["points"]:
            ts = datetime.fromisoformat(p["ts"].replace("Z", "+00:00"))
            assert ts.replace(tzinfo=None) <= datetime.now(UTC).replace(tzinfo=None)

    # ═══ 阶段 9：offline_action 断链安全值（§7.4：GRACE→SAFE_WRITE→ARMED）═══
    driver.publish(
        base + "/down/config",
        {
            "schema_version": 1,
            "job_id": "job-e2e-2",
            "generated_at": datetime.now(UTC).isoformat(),
            "points": cfg_points,
            "offline_action": {"writes": [{"raw_name": "SETPOINT_AV", "value": 20.0}]},
        },
        retain=True,
    )
    await driver.wait_for(driver.up_config_ack, lambda m: m["job_id"] == "job-e2e-2")
    service._offline._delay_s = 1  # e2e 加速：GRACE 缩至 1s（§7.4 默认 60s）
    await agent.simulate_outage()

    async def safe_written() -> bool:
        return float(await dev.read("SETPOINT_AV")) == pytest.approx(20.0)

    await wait_until_async(safe_written, timeout=8)  # GRACE 1s → SAFE_WRITE → ARMED
    await asyncio.sleep(1.5)  # ARMED 后不再反复写（单次触发）
    assert float(await dev.read("SETPOINT_AV")) == pytest.approx(20.0)
    agent.simulate_restore()  # 重连不自动回写原值（恢复秩序归云端）
    await asyncio.sleep(1.0)
    assert float(await dev.read("SETPOINT_AV")) == pytest.approx(20.0)

    # ═══ 阶段 10：指标端点 + 优雅退出 ═══
    port = int(settings.metrics_addr.rsplit(":", 1)[1])
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
    await writer.drain()
    body = (await reader.read(-1)).decode()
    writer.close()
    assert "gateway_poll_total" in body and "gateway_mqtt_connected 1" in body

    # 优雅退出：停服前先留档缓存水位，stop() 冲排水后复开库验证清空
    await service.stop()
    metrics_server.close()
    await metrics_server.wait_closed()
    driver.stop()
    check_db = open_db(str(tmp_path / "cache.db"))
    remaining = check_db.execute("SELECT COUNT(*) AS n FROM sample_cache").fetchone()["n"]
    check_db.close()
    assert remaining == 0, "优雅退出冲排水后缓存清空"
