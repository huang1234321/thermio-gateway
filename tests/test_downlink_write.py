"""L1 下行 write（§7.3）：cmd_id 幂等、四类 rejected、单位换算、受理即答。

栈用记录桩（纯逻辑在单元层，CODE-TST-03；VLAN 真栈路径见 test_poll_vlan）。
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from thermio_gateway.contracts import ReadCmd, WriteCmd
from thermio_gateway.downlink.write import WriteHandler
from thermio_gateway.pointmap import PointRow


class StackStub:
    def __init__(self):
        self.writes: list[tuple[str, str, int, float]] = []
        self.fail = False

    async def write_property(self, address, obj_type, obj_instance, value):
        if self.fail:
            from thermio_gateway.bacnet.stack import BacnetError

            raise BacnetError("error", "stub failure")
        self.writes.append((address, obj_type, obj_instance, value))


def make_point(raw_name="SP_01", writable=True, dev=519, otype="analog-value", inst=5):
    return PointRow(
        raw_name=raw_name,
        bacnet_device_id=dev,
        obj_type=otype,
        obj_instance=inst,
        unit_raw="degC",
        writable=writable,
        interval_s=None,
        enabled=True,
    )


def make_handler(points: dict[str, PointRow], units: dict[str, tuple], stack=None):
    stack = stack or StackStub()
    handler = WriteHandler(
        stack=stack,
        gateway_serial="SWGW-1",
        addresses={519: "2"},
        get_point=points.get,
        unit_lookup=lambda n: units.get(n, (None, None)),
    )
    return handler, stack


def cmd(point_ref="SP_01", value=7.5, expires=30, cmd_id="cmd-1") -> WriteCmd:
    return WriteCmd(
        msg_type="write_cmd",
        ver=1,
        cmd_id=cmd_id,
        point_ref=point_ref,
        value=value,
        unit="degC",
        issued_at=datetime.now(UTC),
        expires_in_s=expires,
    )


class TestWriteCmd:
    async def test_accepted_converts_std_to_raw(self):
        handler, stack = make_handler({"SP_01": make_point()}, {"SP_01": ("degF", "degC")})
        ack = await handler.handle_write(cmd(value=100.0))
        assert ack.result == "accepted"
        assert ack.code is None
        # degC 100 → degF 212 写到设备（值一律 unit_std 的边缘换算面）
        assert stack.writes == [("2", "analog-value", 5, pytest.approx(212.0))]

    async def test_expired_rejected_cmd_expired(self):
        handler, _ = make_handler({"SP_01": make_point()}, {})
        ack = await handler.handle_write(cmd(expires=0))
        assert (ack.result, ack.code) == ("rejected", "CMD_EXPIRED")

    async def test_unknown_point_rejected(self):
        handler, stack = make_handler({}, {})
        ack = await handler.handle_write(cmd(point_ref="GHOST"))
        assert (ack.result, ack.code) == ("rejected", "POINT_UNKNOWN")
        assert stack.writes == []

    async def test_not_writable_rejected(self):
        handler, stack = make_handler(
            {"AI_1": make_point("AI_1", writable=False, otype="analog-input", inst=1)},
            {"AI_1": ("degC", "degC")},
        )
        ack = await handler.handle_write(cmd(point_ref="AI_1"))
        assert (ack.result, ack.code) == ("rejected", "WRITE_REFUSED")
        assert stack.writes == []

    async def test_no_conversion_pair_rejected(self):
        handler, stack = make_handler({"X_1": make_point("X_1")}, {"X_1": ("degF", "kPa")})
        ack = await handler.handle_write(cmd(point_ref="X_1"))
        assert (ack.result, ack.code) == ("rejected", "WRITE_REFUSED")
        assert stack.writes == []

    async def test_physical_failure_still_accepted(self):
        """受理即答（§4.4）：物理写失败不改 accepted——回读仲裁归云端。"""
        stack = StackStub()
        stack.fail = True
        handler, _ = make_handler({"SP_01": make_point()}, {"SP_01": ("degC", "degC")}, stack)
        ack = await handler.handle_write(cmd())
        assert ack.result == "accepted"

    async def test_cmd_id_idempotent_first_settles(self):
        handler, stack = make_handler({"SP_01": make_point()}, {"SP_01": ("degC", "degC")})
        first = await handler.handle_write(cmd(cmd_id="dup-1"))
        assert first.result == "accepted"
        replay = await handler.handle_write(cmd(cmd_id="dup-1"))
        assert replay.result == "accepted"
        assert len(stack.writes) == 1  # 重复投递不重复写


class TestReadCmd:
    async def test_read_result_converted_to_std(self):
        class ReadStack(StackStub):
            async def read_property(self, address, obj_type, obj_instance, prop):
                from thermio_gateway.bacnet.stack import ReadResult

                return ReadResult(value=212.0, quality="good")

        handler, _ = make_handler({"SP_01": make_point()}, {"SP_01": ("degF", "degC")}, ReadStack())
        result = await handler.handle_read(
            ReadCmd(
                msg_type="read_cmd",
                ver=1,
                cmd_id="c-r",
                point_ref="SP_01",
                issued_at=datetime.now(UTC),
                expires_in_s=10,
            )
        )
        assert result.msg_type == "read_result"
        assert result.quality == "good"
        assert result.value == pytest.approx(100.0)  # degF 212 → degC 100
        assert result.unit == "degC"

    async def test_read_failure_bad_quality(self):
        class FailStack(StackStub):
            async def read_property(self, address, obj_type, obj_instance, prop):
                from thermio_gateway.bacnet.stack import BacnetError

                raise BacnetError("timeout")

        handler, _ = make_handler({"SP_01": make_point()}, {}, FailStack())
        result = await handler.handle_read(
            ReadCmd(
                msg_type="read_cmd",
                ver=1,
                cmd_id="c-r2",
                point_ref="SP_01",
                issued_at=datetime.now(UTC),
                expires_in_s=10,
            )
        )
        assert result.quality == "bad"
        assert result.value is None
