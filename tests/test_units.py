"""L1 单位域（§12 测试分层）：

- BACnet engineering-units → ingest token 映射只收量值恒等项（§4.3/§14.3-1）；
- 双向换算与 ingest.md §5.2 / ingest 仓 units.go 公式逐对一致（三栈同源锚点）；
- 无收录对 → ok=False（上行直通 / 下行宁拒不写错，§7.3）。
"""

from __future__ import annotations

import pytest

from thermio_gateway.units import BACNET_UNITS_TO_TOKEN, bacnet_unit_to_token, convert


class TestBacnetUnitMapping:
    def test_mapping_keys_exist_in_bacpypes3_enum(self):
        """全集映射表的键必须都是 bacpypes3 EngineeringUnits 真实枚举标签。

        str(实例) 即设备读回的 kebab 标签（bacpypes3 Enumerated 语义），
        逐键构造实例做往返验证——未知标签会抛 LookupError。
        """
        from bacpypes3.basetypes import EngineeringUnits

        for key in BACNET_UNITS_TO_TOKEN:
            assert str(EngineeringUnits(key)) == key, key

    @pytest.mark.parametrize(
        ("bacnet", "token"),
        [
            ("degrees-celsius", "degC"),
            ("degrees-fahrenheit", "degF"),
            ("degrees-kelvin", "K"),
            ("kilopascals", "kPa"),
            ("pascals", "Pa"),
            ("bars", "bar"),
            ("pounds-force-per-square-inch", "psi"),
            ("millimeters-of-water", "mmH2O"),
            ("kilowatts", "kW"),
            ("watts", "W"),
            ("megawatts", "MW"),
            ("kilowatt-hours", "kWh"),
            ("watt-hours", "Wh"),
            ("liters-per-second", "L/s"),
            ("cubic-meters-per-hour", "m³/h"),
            ("percent", "%"),
            ("hertz", "Hz"),
            ("volts", "V"),
            ("amperes", "A"),
            ("revolutions-per-minute", "rpm"),
        ],
    )
    def test_gateway_md_samples(self, bacnet: str, token: str):
        """§4.3 样例逐对（设计文档锚点）。"""
        assert BACNET_UNITS_TO_TOKEN[bacnet] == token
        assert bacnet_unit_to_token(bacnet) == token

    def test_near_neighbors_not_mapped(self):
        """近邻但不恒等的单位不映射（宁空不错——量纲纪律）。"""
        for name in (
            "centimeters-of-water",  # 10× mmH2O
            "inches-of-water",
            "inches-of-mercury",
            "millibars",  # 0.1 kPa，无恒等 token
            "hectopascals",
            "millivolts",  # 需换算
            "kilovolts",
            "milliamperes",
            "kilohertz",
            "btus",  # 能量族不同制
            "us-gallons-per-minute",
        ):
            assert bacnet_unit_to_token(name) is None, name

    def test_empty_and_unknown(self):
        assert bacnet_unit_to_token(None) is None
        assert bacnet_unit_to_token("") is None
        assert bacnet_unit_to_token("not-a-unit") is None


class TestConvert:
    @pytest.mark.parametrize(
        ("frm", "to", "value", "expected"),
        [
            ("degC", "degF", 100.0, 212.0),
            ("degF", "degC", 212.0, 100.0),
            ("degF", "degC", 32.0, 0.0),
            ("K", "degC", 273.15, 0.0),
            ("degC", "K", -273.15, 0.0),
            ("kPa", "Pa", 1.0, 1000.0),
            ("psi", "kPa", 1.0, 6.894757293168),
            ("mmH2O", "kPa", 1000.0, 9.80665),
            ("bar", "kPa", 1.0, 100.0),
            ("W", "kW", 1000.0, 1.0),
            ("MW", "kW", 1.0, 1000.0),
            ("Wh", "kWh", 1000.0, 1.0),
            ("m³/h", "L/s", 3.6, 1.0),
            ("L/s", "m³/h", 1.0, 3.6),
            ("m3/h", "L/s", 3.6, 1.0),  # ASCII 别名
        ],
    )
    def test_ingest_pairs_bidirectional(self, frm, to, value, expected):
        out, ok = convert(value, frm, to)
        assert ok
        assert out == pytest.approx(expected)

    def test_identity_units_same_token_passthrough(self):
        out, ok = convert(55.0, "%", "%")
        assert ok and out == 55.0
        out, ok = convert(50.0, "Hz", "Hz")
        assert ok and out == 50.0

    def test_empty_unit_is_passthrough(self):
        """§5.2 空单位 = 已归一直通。"""
        out, ok = convert(7.4, None, "degC")
        assert ok and out == 7.4
        out, ok = convert(7.4, "degF", None)
        assert ok and out == 7.4

    def test_same_unit_passthrough(self):
        out, ok = convert(7.4, "degC", "degC")
        assert ok and out == 7.4

    def test_no_pair_across_families(self):
        _, ok = convert(1.0, "degC", "kPa")  # 跨族
        assert not ok
        _, ok = convert(1.0, "%", "Hz")  # 恒等族互跨
        assert not ok
        _, ok = convert(1.0, "degC", "cmH2O")  # 未收录 token
        assert not ok

    def test_std_raw_round_trip(self):
        """unit_std ↔ unit_raw 往返不丢精度（下行换算正确性的根）。"""
        from thermio_gateway.units import raw_to_std, std_to_raw

        std_value, ok = std_to_raw(25.0, "degC", "degF")
        assert ok and std_value == pytest.approx(77.0)
        back, ok = raw_to_std(std_value, "degF", "degC")
        assert ok and back == pytest.approx(25.0)
