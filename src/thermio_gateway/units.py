"""单位域（gateway.md §4.3/§7.3；§14.3-1 建仓期收口项）。

两件事，一张权威表都不复制：

1. **BACnet engineering-units → ingest 单位族 token 全集映射**（§4.3）：
   只有「量值恒等」的枚举才映射（如 degrees-celsius→degC）；无对应
   token 的留空（直通语义，M2 §6.5「unit_raw 空则不换算」）+ 报告标记
   `unit_unmapped`。cmH2O/inHg 之类与 mmH2O/psi 只是近邻、不是同一单位，
   一律不映射——宁空不错。
2. **unit_std ↔ unit_raw 双向换算**（§7.3）：换算对集 = ingest.md §5.2
   内置表 v1 的**同一对集**（表本体以 ingest.md 为唯一权威，本文不复制
   第二份表——本文件是实现面的一份翻译，评审时与 ingest 仓 units.go
   逐对核对，三栈同源）。
"""

from __future__ import annotations

# ── §14.3-1：BACnet engineering-units 枚举（bacpypes3 kebab-case 标签）→ ingest token ──
# 只收录量值恒等的映射；未收录 = 留空 + unit_unmapped。
BACNET_UNITS_TO_TOKEN: dict[str, str] = {
    # 温度族
    "degrees-celsius": "degC",
    "degrees-fahrenheit": "degF",
    "degrees-kelvin": "K",
    # 压力族
    "kilopascals": "kPa",
    "pascals": "Pa",
    "bars": "bar",
    "pounds-force-per-square-inch": "psi",
    "millimeters-of-water": "mmH2O",
    # 功率族
    "kilowatts": "kW",
    "watts": "W",
    "megawatts": "MW",
    # 能量族
    "kilowatt-hours": "kWh",
    "watt-hours": "Wh",
    # 流量族
    "liters-per-second": "L/s",
    "cubic-meters-per-hour": "m³/h",
    # 恒等族
    "percent": "%",
    "percent-relative-humidity": "%",
    "hertz": "Hz",
    "volts": "V",
    "amperes": "A",
    "revolutions-per-minute": "rpm",
}


def bacnet_unit_to_token(bacnet_unit: str | None) -> str | None:
    """BACnet units 枚举标签 → ingest token；无对应 → None（报告侧标 unit_unmapped）。"""
    if not bacnet_unit:
        return None
    return BACNET_UNITS_TO_TOKEN.get(bacnet_unit)


# ── ingest.md §5.2 转换对集（同一对集双向；族基准：degC/kPa/kW/kWh/L/s）──────
# 公式与 ingest 仓 internal/units/units.go 逐对一致（三栈同源，评审锚点）：
#   degF→degC (v−32)×5/9；K→degC v−273.15；Pa ×0.001；bar ×100；
#   psi ×6.894757293168；mmH2O ×0.00980665；W ×0.001；MW ×1000；
#   Wh ×0.001；m³/h ÷3.6；恒等族仅同单位恒等。

# 仿射族（温度）：base = a·v + b
_AFFINE_TO_BASE: dict[str, tuple[str, float, float]] = {
    "degC": ("temperature", 1.0, 0.0),
    "degF": ("temperature", 5.0 / 9.0, -32.0 * 5.0 / 9.0),
    "K": ("temperature", 1.0, -273.15),
}
# 线性族：base = s·v
_LINEAR_TO_BASE: dict[str, float] = {
    "kPa": 1.0,
    "Pa": 0.001,
    "bar": 100.0,
    "psi": 6.894757293168,
    "mmH2O": 0.00980665,
    "kW": 1.0,
    "W": 0.001,
    "MW": 1000.0,
    "kWh": 1.0,
    "Wh": 0.001,
    "L/s": 1.0,
    "m³/h": 1.0 / 3.6,
    "m3/h": 1.0 / 3.6,  # ASCII 别名（ingest 表同款：网关模板常无法打出 ³）
}
# 恒等族（跨单位不换算；同单位直通）
_IDENTITY: set[str] = {"%", "Hz", "V", "A", "rpm"}

# 线性族所属（用于判族相等）
_LINEAR_FAMILY: dict[str, str] = {
    "kPa": "pressure",
    "Pa": "pressure",
    "bar": "pressure",
    "psi": "pressure",
    "mmH2O": "pressure",
    "kW": "power",
    "W": "power",
    "MW": "power",
    "kWh": "energy",
    "Wh": "energy",
    "L/s": "flow",
    "m³/h": "flow",
    "m3/h": "flow",
}


def _to_base(token: str, v: float) -> tuple[str, float]:
    if token in _AFFINE_TO_BASE:
        family, a, b = _AFFINE_TO_BASE[token]
        return family, a * v + b
    family = _LINEAR_FAMILY[token]
    return family, _LINEAR_TO_BASE[token] * v


def _from_base(token: str, base: float) -> float:
    if token in _AFFINE_TO_BASE:
        _, a, b = _AFFINE_TO_BASE[token]
        return (base - b) / a
    return base / _LINEAR_TO_BASE[token]


def convert(value: float, unit_from: str | None, unit_to: str | None) -> tuple[float, bool]:
    """按 ingest.md §5.2 对集换算；ok=False 表示无收录对（调用方按场景处置：
    上行/读回 → 原值直通；下行写 → 宁拒不写错，§7.3）。

    空单位约定（§5.2）：任一侧为空 → 已归一直通，返回原值 ok=True。
    转换不做人为舍入（double 原精度）。
    """
    src = (unit_from or "").strip()
    dst = (unit_to or "").strip()
    if not src or not dst:
        return value, True
    if src == dst:
        return value, True
    if src in _IDENTITY or dst in _IDENTITY:
        # 恒等族没有跨单位对（%→Hz 无意义）
        return value, False
    known = src in _AFFINE_TO_BASE or src in _LINEAR_TO_BASE
    if not known:
        return value, False
    known = dst in _AFFINE_TO_BASE or dst in _LINEAR_TO_BASE
    if not known:
        return value, False
    fam_src, base = _to_base(src, value)
    fam_dst = _AFFINE_TO_BASE[dst][0] if dst in _AFFINE_TO_BASE else _LINEAR_FAMILY[dst]
    if fam_src != fam_dst:
        return value, False
    return _from_base(dst, base), True


def std_to_raw(value: float, unit_std: str | None, unit_raw: str | None) -> tuple[float, bool]:
    """下行写路径（§7.3）：云端 unit_std 值 → 设备原生单位值。"""
    return convert(value, unit_std, unit_raw)


def raw_to_std(value: float, unit_raw: str | None, unit_std: str | None) -> tuple[float, bool]:
    """上行/读回路径：设备原生值 → unit_std（read_result 契约）。"""
    return convert(value, unit_raw, unit_std)
