"""进程内 BACnet 仿真设备（gateway.md §12 L2：RPM 支持/不支持两型）。

基于 bacpypes3 VirtualNetwork——无 UDP 套接字、确定性、可进 CI
（CODE-TST-02：无外部服务真依赖）。

- ``rpm_supported=True``  常规 DDC（RPM 可用）；
- ``rpm_supported=False`` 老旧 DDC 降级型：设备应用不含 RPM 服务 mixin，
  对 ReadPropertyMultiple 回 unrecognized-service（§4.4 降级路径的真实对端）。
"""

from __future__ import annotations

import contextlib
from typing import Any

from bacpypes3.app import Application
from bacpypes3.errors import UnrecognizedService
from bacpypes3.local.analog import AnalogInputObject, AnalogValueObject
from bacpypes3.local.binary import BinaryValueObject
from bacpypes3.local.device import DeviceObject
from bacpypes3.local.multistate import MultiStateInputObject
from bacpypes3.local.networkport import NetworkPortObject
from bacpypes3.vlan import VirtualNetwork

# 仿真对象清单：name → (类型, 参数)
SIM_OBJECTS: dict[str, dict[str, Any]] = {
    "CHWS_T_AV": {"kind": "analog-value", "value": 7.42, "units": "degrees-celsius"},
    "OAT_AI": {"kind": "analog-input", "value": 28.5, "units": "degrees-fahrenheit"},
    "PUMP_BV": {"kind": "binary-value", "value": "active"},
    "MODE_MSI": {"kind": "multi-state-input", "value": 2, "states": ["stop", "auto", "hand"]},
    "SETPOINT_AV": {"kind": "analog-value", "value": 12.0, "units": "degrees-celsius"},
}


class NoRpmApplication(Application):
    """老旧 DDC 型设备应用：RPM 服务缺位 → unrecognized-service。

    覆写 ``do_ReadPropertyMultipleRequest`` 直接抛 ``UnrecognizedService``——
    Application.indication 会把它转成 ErrorPDU 回给客户端（§4.4 降级路径
    的真实对端行为）。
    """

    async def do_ReadPropertyMultipleRequest(self, apdu) -> None:
        raise UnrecognizedService("sim: RPM not supported")


def _build_app(device_id: int, mac: int, net_name: str, rpm: bool) -> Application:
    device = DeviceObject(
        objectIdentifier=("device", device_id),
        objectName=f"SIM-{device_id}",
        maxApduLengthAccepted=1024,
        segmentationSupported="segmentedBoth",
        vendorIdentifier=15,
    )
    net_port = NetworkPortObject(
        objectIdentifier=("network-port", 1),
        objectName="NP-1",
        networkType="virtual",
        protocolLevel="bacnet-application",
        networkInterfaceName=net_name,
        macAddress=bytes([mac]),
    )
    cls = Application if rpm else NoRpmApplication
    return cls.from_object_list([device, net_port])


class SimDevice:
    """一台仿真设备：持有应用与对象实例，供测试改值/断言。"""

    def __init__(
        self,
        net: VirtualNetwork,
        net_name: str,
        device_id: int,
        mac: int,
        rpm_supported: bool = True,
        objects: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.device_id = device_id
        self.address = str(mac)  # VirtualNetwork LocalStation 字符串形
        self.rpm_supported = rpm_supported
        self.app = _build_app(device_id, mac, net_name, rpm_supported)
        self.objects: dict[str, Any] = {}
        spec = objects if objects is not None else SIM_OBJECTS
        instance = 1
        for name, cfg in spec.items():
            kind = cfg["kind"]
            if kind == "analog-value":
                obj = AnalogValueObject(
                    objectIdentifier=("analog-value", instance), objectName=name
                )
            elif kind == "analog-input":
                obj = AnalogInputObject(
                    objectIdentifier=("analog-input", instance), objectName=name
                )
            elif kind == "binary-value":
                obj = BinaryValueObject(
                    objectIdentifier=("binary-value", instance), objectName=name
                )
            elif kind == "multi-state-input":
                obj = MultiStateInputObject(
                    objectIdentifier=("multi-state-input", instance), objectName=name
                )
            else:
                raise ValueError(f"unknown sim kind: {kind}")
            self.app.add_object(obj)
            self.objects[name] = obj
            instance += 1

    async def write(self, name: str, value: Any) -> None:
        await self.objects[name].write_property("presentValue", value)

    async def read(self, name: str) -> Any:
        return await self.objects[name].read_property("presentValue")

    async def apply_spec(self, spec: dict[str, dict[str, Any]] | None = None) -> None:
        """按 SIM_OBJECTS 形态写入初始值/单位（multi-state 的 stateText 等静态
        属性在构造期给定不了，presentValue 与 units 在此落）。"""
        for name, obj in self.objects.items():
            cfg = (spec or SIM_OBJECTS).get(name, {})
            if "value" in cfg:
                await obj.write_property("presentValue", cfg["value"])
            if "units" in cfg and hasattr(obj, "units"):
                with contextlib.suppress(Exception):
                    # units 只读型对象忽略（PY-02：非静默——报告侧无标记需求）
                    await obj.write_property("units", cfg["units"])
