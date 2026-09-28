"""L1 offline_action 状态机（§7.4）：

LINK_UP → GRACE(可取消) → SAFE_WRITE(按序/重试) → ARMED(单次触发)；
重连不自动回写；不可解析点容错跳过。
"""

from __future__ import annotations

import asyncio

import pytest

from thermio_gateway.contracts import OfflineAction, OfflineActionWrite
from thermio_gateway.downlink.offline_action import (
    STATE_ARMED,
    STATE_GRACE,
    STATE_LINK_UP,
    OfflineActionExecutor,
)
from thermio_gateway.pointmap import PointRow


class StackStub:
    def __init__(self):
        self.writes: list[float] = []
        self.fail_times = 0  # 前 N 次失败（重试路径）

    async def write_property(self, address, obj_type, obj_instance, value):
        if self.fail_times > 0:
            self.fail_times -= 1
            from thermio_gateway.bacnet.stack import BacnetError

            raise BacnetError("error", "stub")
        self.writes.append(value)


def point(name="SP_01", writable=True):
    return PointRow(
        raw_name=name,
        bacnet_device_id=519,
        obj_type="analog-value",
        obj_instance=5,
        unit_raw="degC",
        writable=writable,
        interval_s=None,
        enabled=True,
    )


def make_executor(points, units, stack, delay_s=0):
    return OfflineActionExecutor(
        stack=stack,
        addresses={519: "2"},
        get_point=points.get,
        unit_lookup=lambda n: units.get(n, (None, None)),
        delay_s=delay_s,
        retry_backoff_s=0,
    )


class TestStateMachine:
    async def test_grace_canceled_by_reconnect(self):
        stack = StackStub()
        ex = make_executor({"SP_01": point()}, {"SP_01": ("degC", "degC")}, stack, delay_s=60)
        ex.update_action(OfflineAction(writes=[OfflineActionWrite(raw_name="SP_01", value=7.5)]))
        ex.on_mqtt_disconnected()
        assert ex.state() == STATE_GRACE
        await asyncio.sleep(0.01)
        ex.on_mqtt_connected()
        await asyncio.sleep(0.05)
        assert ex.state() == STATE_LINK_UP
        assert stack.writes == []  # 闪断不触发（防抖）

    async def test_grace_expiry_writes_in_order_with_conversion(self):
        stack = StackStub()
        ex = make_executor(
            {"A": point("A"), "B": point("B")},
            {"A": ("degC", "degC"), "B": ("degF", "degC")},
            stack,
            delay_s=0,
        )
        ex.update_action(
            OfflineAction(
                writes=[
                    OfflineActionWrite(raw_name="A", value=7.5),  # degC → 7.5
                    OfflineActionWrite(raw_name="B", value=100.0),  # degC 100 → degF 212
                ]
            )
        )
        ex.on_mqtt_disconnected()
        await asyncio.sleep(0.05)
        assert ex.state() == STATE_ARMED
        assert stack.writes == [pytest.approx(7.5), pytest.approx(212.0)]  # 按序

    async def test_single_shot_no_rewrite_during_outage(self):
        stack = StackStub()
        ex = make_executor({"A": point("A")}, {"A": ("degC", "degC")}, stack, delay_s=0)
        ex.update_action(OfflineAction(writes=[OfflineActionWrite(raw_name="A", value=1.0)]))
        ex.on_mqtt_disconnected()
        await asyncio.sleep(0.02)
        assert ex.state() == STATE_ARMED
        # 再断一次（长断网期内第二次 GRACE 到期）也不反复写（单次触发）
        ex._state = STATE_LINK_UP  # 模拟状态机复位于另一断链边沿
        ex.on_mqtt_disconnected()
        await asyncio.sleep(0.02)
        assert ex.state() == STATE_ARMED
        assert len(stack.writes) == 1

    async def test_unresolvable_point_skipped(self):
        stack = StackStub()
        ex = make_executor({}, {}, stack, delay_s=0)
        ex.update_action(OfflineAction(writes=[OfflineActionWrite(raw_name="GHOST", value=1.0)]))
        ex.on_mqtt_disconnected()
        await asyncio.sleep(0.05)
        assert ex.state() == STATE_ARMED
        assert stack.writes == []

    async def test_no_conversion_skipped(self):
        stack = StackStub()
        ex = make_executor({"A": point("A")}, {"A": ("psi", "degC")}, stack, delay_s=0)
        ex.update_action(OfflineAction(writes=[OfflineActionWrite(raw_name="A", value=1.0)]))
        ex.on_mqtt_disconnected()
        await asyncio.sleep(0.05)
        assert stack.writes == []

    async def test_retry_then_success(self):
        stack = StackStub()
        stack.fail_times = 1  # 首写失败，第二次成功
        ex = make_executor({"A": point("A")}, {"A": ("degC", "degC")}, stack, delay_s=0)
        ex.update_action(OfflineAction(writes=[OfflineActionWrite(raw_name="A", value=5.0)]))
        ex.on_mqtt_disconnected()
        await asyncio.sleep(0.1)
        assert stack.writes == [pytest.approx(5.0)]

    async def test_reconnect_after_armed_resets_to_link_up(self):
        stack = StackStub()
        ex = make_executor({"A": point("A")}, {"A": ("degC", "degC")}, stack, delay_s=0)
        ex.update_action(OfflineAction(writes=[OfflineActionWrite(raw_name="A", value=1.0)]))
        ex.on_mqtt_disconnected()
        await asyncio.sleep(0.02)
        assert ex.state() == STATE_ARMED
        ex.on_mqtt_connected()
        assert ex.state() == STATE_LINK_UP
        # 重连不自动回写原值（恢复秩序归云端，§7.4）
        assert stack.writes == [pytest.approx(1.0)]
