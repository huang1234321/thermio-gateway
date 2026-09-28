"""L1 上行信封字段规则（§5.1 对齐表 = ingest.md §3.2 的镜像面）。

gw-sim B 表同源的镜像断言：产出侧保证 ingest 解码零拒收——
UNKNOWN_MSG_TYPE/UNSUPPORTED_VER/GW_MISMATCH/PAYLOAD_TOO_LARGE/
charset 拒收/TS_INVALID 全部不可达。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from thermio_gateway.contracts import TelemetryBatch, TelemetryPoint, rfc3339


def point(name: str = "CHWS_T_1", **kw) -> TelemetryPoint:
    base = dict(name=name, value=7.42, ts=datetime.now(UTC), quality="good", unit="degC")
    base.update(kw)
    return TelemetryPoint(**base)


def batch(points, seq=0) -> TelemetryBatch:
    return TelemetryBatch(
        gw="SWGW-BLDG-A-01",
        seq=seq,
        sent_at=datetime.now(UTC),
        points=points,
    )


class TestPointFieldRules:
    @pytest.mark.parametrize(
        "name",
        [
            "A",
            "a.b",
            "a:b",
            "a-b",
            "a_b",
            "X" * 64,
            "9F_CHWS.T-1:2",
        ],
    )
    def test_valid_names_accepted(self, name):
        assert point(name=name).name == name

    @pytest.mark.parametrize(
        "name",
        [
            "",
            "X" * 65,
            "带中文",
            "with space",
            "tab\tname",
            "sym#bol",
        ],
    )
    def test_invalid_names_rejected(self, name):
        with pytest.raises(ValidationError):
            point(name=name)

    def test_null_value_allowed_for_bad_quality(self):
        p = point(name="DEAD_SENSOR", value=None, quality="bad")
        assert p.value is None and p.quality == "bad"

    def test_value_text_enum_path(self):
        p = point(name="PUMP_1", value=None, value_text="active")
        assert p.value_text == "active"

    def test_quality_domain(self):
        assert point(quality="uncertain").quality == "uncertain"
        with pytest.raises(ValidationError):
            point(quality="broken")

    def test_ts_serializes_rfc3339_with_tz(self):
        ts = datetime(2026, 9, 28, 14, 3, 4, tzinfo=UTC)
        assert rfc3339(ts) == "2026-09-28T14:03:04+00:00"
        p = point(ts=ts)
        dumped = json.loads(p.model_dump_json())
        # pydantic v2 UTC 序列化为 Z 后缀——Z 与 +00:00 同为 RFC3339 带时区
        assert dumped["ts"].endswith(("Z", "+00:00"))


class TestBatchFieldRules:
    def test_envelope_constants(self):
        b = batch([point()])
        assert b.msg_type == "telemetry_batch"
        assert b.ver == 1

    def test_points_upper_bound_500(self):
        assert batch([point(name=f"P{i}") for i in range(500)]).points.__len__() == 500
        with pytest.raises(ValidationError):
            batch([point(name=f"P{i}") for i in range(501)])

    def test_points_lower_bound_1(self):
        with pytest.raises(ValidationError):
            batch([])

    def test_seq_ge_zero(self):
        assert batch([point()], seq=4821).seq == 4821
        with pytest.raises(ValidationError):
            batch([point()], seq=-1)

    def test_json_shape_field_order_and_names(self):
        """逐字段对齐 ingest §3.1 示例（§5.1 对齐表的产出面证据）。"""
        b = batch(
            [point(), point(name="PUMP_1", value=None, value_text="active", unit=None)], seq=7
        )
        data = json.loads(b.model_dump_json())
        assert list(data.keys()) == ["msg_type", "ver", "gw", "seq", "sent_at", "points"]
        assert data["msg_type"] == "telemetry_batch"
        assert data["ver"] == 1
        assert data["gw"] == "SWGW-BLDG-A-01"
        assert data["seq"] == 7
        assert set(data["points"][0].keys()) == {
            "name",
            "value",
            "value_text",
            "ts",
            "quality",
            "unit",
        }

    def test_gw_serial_mismatch_shape_note(self):
        """gw 字段 = env 注入 serial（与 clientid 同源于 M1 登记）——GW_MISMATCH
        不可达的本地前提：serial 唯一注入点在 service 装配。"""
        b = batch([point()])
        assert b.gw == "SWGW-BLDG-A-01"
