"""L2（VLAN 进程内）：轮询调度（§4.4）——RPM 主路径 / 降级路径 / 质量戳 / 插队。

真实 bacpypes3 协议栈对拍（RPM 支持与不支持两型设备，§12 测试分层）；
无 UDP 套接字、无外部服务（CODE-TST-02）。
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from bacpypes3.vlan import VirtualNetwork

from tests.sim.device import SIM_OBJECTS, SimDevice
from thermio_gateway.bacnet.poll import PollScheduler
from thermio_gateway.bacnet.stack import BacnetStack, build_virtual_application


class Sink:
    def __init__(self) -> None:
        self.batches: list[list] = []

    async def __call__(self, samples):
        self.batches.append(list(samples))

    def flat(self):
        return [s for b in self.batches for s in b]

    def by_name(self):
        return {s.name: s for s in self.flat()}


@pytest.fixture()
async def env():
    net_name = f"poll-{uuid.uuid4().hex[:8]}"
    net = VirtualNetwork(net_name)
    dev_rpm = SimDevice(net, net_name, device_id=519, mac=2, rpm_supported=True)
    dev_old = SimDevice(net, net_name, device_id=620, mac=3, rpm_supported=False)
    await dev_rpm.apply_spec()
    await dev_old.apply_spec()
    app = build_virtual_application(net, net_name, mac=1, device_id=900)
    stack = BacnetStack(app, max_inflight=2, inter_request_gap_ms=0)
    return stack, dev_rpm, dev_old


def point_rows(dev: SimDevice):
    """把 sim 对象映射成 pointmap 行（instance 从 1 起与 SimDevice 构造一致）。"""
    from thermio_gateway.pointmap import PointRow

    rows = []
    instance = 1
    for name, cfg in SIM_OBJECTS.items():
        obj_type = cfg["kind"]
        unit = {"degrees-celsius": "degC", "degrees-fahrenheit": "degF"}.get(cfg.get("units", ""))
        rows.append(
            PointRow(
                raw_name=name,
                bacnet_device_id=dev.device_id,
                obj_type=obj_type,
                obj_instance=instance,
                unit_raw=unit,
                writable=False,
                interval_s=None,
                enabled=True,
            )
        )
        instance += 1
    return rows


class TestPollCycle:
    async def test_rpm_path_samples_and_mapping(self, env):
        stack, dev, _ = env
        sink = Sink()
        sched = PollScheduler(stack, sink, default_interval_s=60)
        sched.set_active(point_rows(dev), {dev.device_id: dev.address})
        await sched.run_cycle(point_rows(dev))
        by_name = sink.by_name()
        # §4.2 映射：analog → value+unit；binary → value_text active/inactive；
        # multi-state → 状态序号
        chws = by_name["CHWS_T_AV"]
        assert chws.value == pytest.approx(7.42, abs=1e-5)
        assert chws.unit == "degC"
        assert chws.quality == "good"
        pump = by_name["PUMP_BV"]
        assert pump.value_text == "active"
        assert pump.value is None
        mode = by_name["MODE_MSI"]
        assert mode.value == 2.0 and mode.value_text is None

    async def test_rpm_unsupported_falls_back_and_memorizes(self, env):
        stack, _, dev_old = env
        sink = Sink()
        sched = PollScheduler(stack, sink, default_interval_s=60)
        rows = point_rows(dev_old)
        sched.set_active(rows, {dev_old.device_id: dev_old.address})
        await sched.run_cycle(rows)
        # 降级路径也产全量样本
        by_name = sink.by_name()
        assert by_name["CHWS_T_AV"].value == pytest.approx(7.42, abs=1e-5)
        assert "620|" + dev_old.address in sched._rpm_unsupported  # 记忆（§4.4）

    async def test_dead_device_yields_bad_rows(self, env):
        """不可达设备：value=null + quality=bad，不丢行（§4.4/ingest §5.3 L1a）。"""
        stack, dev, _ = env
        sink = Sink()
        sched = PollScheduler(stack, sink, default_interval_s=60)
        rows = point_rows(dev)
        # 指向不存在的工作站地址（VLAN 上无节点 9）
        sched.set_active(rows, {dev.device_id: "9"})
        # 换短超时栈以加速失败：直接走 RP 降级（把 RPM 记忆预置避开 RPM 等待）
        sched._rpm_unsupported.add(f"{dev.device_id}|9")
        await sched.run_cycle(rows)
        assert sink.batches, "不可达也必须有样本行"
        for s in sink.flat():
            assert s.quality == "bad"
            assert s.value is None

    async def test_urgent_read_jumps_queue(self, env):
        stack, dev, _ = env
        sink = Sink()
        sched = PollScheduler(stack, sink, default_interval_s=60)
        sched.set_active([], {dev.device_id: dev.address})  # 无常规采集集（地址仍在册）
        await sched.start()
        sched.request_urgent(point_rows(dev)[:2])  # §5.3 立即补采
        for _ in range(50):
            if sink.batches:
                break
            await asyncio.sleep(0.05)
        await sched.stop()
        names = {s.name for s in sink.flat()}
        assert {"CHWS_T_AV", "OAT_AI"} <= names
