"""BACnet 协议栈封装（bacpypes3，gateway.md §4/§12；§14.1 选型说明）。

bacpypes3 是 bacpypes 同作者的 asyncio 重写维护线（§12 单进程 asyncio 模型
配套）；错误/超时/拒绝一律以 ``ErrorRejectAbortNack``（BaseException 子类）
抛出——本模块统一归一为 ``BacnetError``，调用侧不裸接第三方异常型。

节流纪律（§4.4）：每设备在途请求 ≤ ``max_inflight`` + 帧间隔 ≥
``inter_request_gap_ms``——BA 网络上的礼貌（防扫描风暴是软采集的第一
网络纪律）。写硬件级保护：同对象在途写 ≤1（§7.3）。
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from bacpypes3.apdu import ErrorRejectAbortNack
from bacpypes3.app import Application
from bacpypes3.local.device import DeviceObject
from bacpypes3.local.networkport import NetworkPortObject
from bacpypes3.pdu import Address, LocalStation
from bacpypes3.primitivedata import ObjectIdentifier
from bacpypes3.vlan import VirtualNetwork


class BacnetError(Exception):
    """BACnet 读写失败（超时/错误/拒绝/中止）——该点 quality=bad 的来源。"""

    def __init__(self, kind: str, detail: str = "") -> None:
        super().__init__(f"{kind}: {detail}" if detail else kind)
        self.kind = kind  # timeout / error / reject / abort / unknown


@dataclass(frozen=True)
class IAmRecord:
    device_id: int
    address: str
    vendor_id: int | None
    max_apdu: int | None


@dataclass(frozen=True)
class ReadResult:
    value: Any
    quality: str  # good / uncertain（reliability 非 no-fault）


def _normalize_error(exc: BaseException) -> BacnetError:
    name = type(exc).__name__
    if "Abort" in name:
        return BacnetError("timeout", repr(exc))  # 重试耗尽 → AbortPDU（实测路径）
    if "Error" in name:
        return BacnetError("error", repr(exc))
    if "Reject" in name:
        return BacnetError("reject", repr(exc))
    return BacnetError("unknown", repr(exc))


def build_application(
    device_id: int,
    interface: str,
    port: int,
    apdu_timeout_ms: int,
    apdu_retries: int,
) -> Application:
    """构建真网口栈（UDP ``interface:port``）。

    interface 形如 ``192.168.10.5`` 或 ``192.168.10.5/24``（带掩码时
    bacpypes3 据此推导本地广播地址）。
    """
    addr = f"{interface}:{port}"  # IPv4Address 接受 "ip[/mask]:port"
    device = DeviceObject(
        objectIdentifier=("device", device_id),
        objectName=f"thermio-gateway-{device_id}",
        maxApduLengthAccepted=1024,
        segmentationSupported="segmentedBoth",
        vendorIdentifier=999,
        apduTimeout=apdu_timeout_ms,
        numberOfApduRetries=apdu_retries,
    )
    net_port = NetworkPortObject(
        Address(addr),
        objectIdentifier=("network-port", 1),
        objectName="NetworkPort-1",
    )
    return Application.from_object_list([device, net_port])


def build_virtual_application(
    net: VirtualNetwork, net_name: str, mac: int, device_id: int
) -> Application:
    """进程内虚拟网栈（测试/仿真基座，§12 L2 用 in-process 模拟 device）。"""
    device = DeviceObject(
        objectIdentifier=("device", device_id),
        objectName=f"thermio-gateway-{device_id}",
        maxApduLengthAccepted=1024,
        segmentationSupported="segmentedBoth",
        vendorIdentifier=999,
        apduTimeout=1000,
        numberOfApduRetries=0,
    )
    net_port = NetworkPortObject(
        objectIdentifier=("network-port", 1),
        objectName="NetworkPort-1",
        networkType="virtual",
        protocolLevel="bacnet-application",
        networkInterfaceName=net_name,
        macAddress=bytes([mac]),
    )
    return Application.from_object_list([device, net_port])


def make_virtual_network(name: str) -> VirtualNetwork:
    return VirtualNetwork(name)


class BacnetStack:
    """客户端门面：who_is / read / rpm / write，带每设备节流。"""

    def __init__(
        self,
        app: Application,
        max_inflight: int = 2,
        inter_request_gap_ms: int = 50,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._app = app
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._gaps: dict[str, float] = {}  # key → 下次可发时刻（monotonic）
        self._gap_s = inter_request_gap_ms / 1000.0
        self._max_inflight = max_inflight
        self._write_locks: dict[str, asyncio.Lock] = {}  # 同对象在途写 ≤1（§7.3）

    def _sem(self, key: str) -> asyncio.Semaphore:
        if key not in self._semaphores:
            self._semaphores[key] = asyncio.Semaphore(self._max_inflight)
        return self._semaphores[key]

    def _write_lock(self, key: str) -> asyncio.Lock:
        if key not in self._write_locks:
            self._write_locks[key] = asyncio.Lock()
        return self._write_locks[key]

    @contextlib.asynccontextmanager
    async def _throttled(self, key: str) -> AsyncIterator[None]:
        async with self._sem(key):
            now = asyncio.get_running_loop().time()
            wait = self._gaps.get(key, 0.0) - now
            if wait > 0:
                await asyncio.sleep(wait)
            try:
                yield
            finally:
                self._gaps[key] = asyncio.get_running_loop().time() + self._gap_s

    async def who_is(
        self,
        timeout_s: float = 3.0,
        address: str | None = None,
        low: int | None = None,
        high: int | None = None,
    ) -> list[IAmRecord]:
        """Phase 1（§4.1）：全广播（或定向）Who-Is → I-Am 台账。"""
        dest = Address(address) if address else None
        future = self._app.who_is(low_limit=low, high_limit=high, address=dest, timeout=timeout_s)
        iams = await future
        records: list[IAmRecord] = []
        for iam in iams:
            device_instance = int(iam.iAmDeviceIdentifier[1])
            records.append(
                IAmRecord(
                    device_id=device_instance,
                    address=str(iam.pduSource),
                    vendor_id=int(iam.vendorID) if iam.vendorID is not None else None,
                    max_apdu=int(iam.maxAPDULengthAccepted)
                    if iam.maxAPDULengthAccepted is not None
                    else None,
                )
            )
        return records

    async def read_property(
        self, address: str, obj_type: str, obj_instance: int, prop: str = "presentValue"
    ) -> ReadResult:
        key = f"rp:{address}"
        async with self._throttled(key):
            try:
                value = await self._app.read_property(
                    Address(address), ObjectIdentifier(f"{obj_type}:{obj_instance}"), prop
                )
            except ErrorRejectAbortNack as exc:
                raise _normalize_error(exc) from exc
            except TimeoutError as exc:
                raise BacnetError("timeout") from exc
            except OSError as exc:
                raise BacnetError("error", str(exc)) from exc
        return ReadResult(value=value, quality="good")

    async def read_property_multiple(
        self, address: str, requests: list[tuple[str, int, list[str]]]
    ) -> list[tuple[str, int, str, Any]]:
        """RPM：单请求带多对象多属性。requests = [(obj_type, instance, [props])]。

        返回 [(obj_type, instance, prop, value)]。设备不支持 RPM → BacnetError
        （调用方记忆并降级，§4.4）。
        """
        key = f"rpm:{address}"
        params: list[Any] = []
        for obj_type, instance, props in requests:
            params.append(ObjectIdentifier(f"{obj_type}:{instance}"))
            params.append(list(props))
        async with self._throttled(key):
            try:
                result = await self._app.read_property_multiple(Address(address), params)
            except ErrorRejectAbortNack as exc:
                raise _normalize_error(exc) from exc
            except TimeoutError as exc:
                raise BacnetError("timeout") from exc
            except OSError as exc:
                raise BacnetError("error", str(exc)) from exc
        return [
            (str(objid[0]), int(objid[1]), str(prop), value) for objid, prop, _idx, value in result
        ]

    async def read_reliability(self, address: str, obj_type: str, obj_instance: int) -> str | None:
        """Reliability 属性（§5.1：≠ no-fault-detected → uncertain）。不可读返回 None。"""
        try:
            async with self._throttled(f"rp:{address}"):
                value = await self._app.read_property(
                    Address(address),
                    ObjectIdentifier(f"{obj_type}:{obj_instance}"),
                    "reliability",
                )
        except ErrorRejectAbortNack:
            return None
        except (TimeoutError, OSError):
            return None
        return str(value) if value is not None else None

    async def write_property(
        self, address: str, obj_type: str, obj_instance: int, value: Any
    ) -> None:
        """WriteProperty(present_value)（§4.4 写路径 / §7.3 / §7.4 安全值）。"""
        obj_key = f"{address}|{obj_type}:{obj_instance}"
        async with self._write_lock(obj_key), self._throttled(f"wp:{address}"):
            try:
                await self._app.write_property(
                    Address(address),
                    ObjectIdentifier(f"{obj_type}:{obj_instance}"),
                    "presentValue",
                    value,
                )
            except ErrorRejectAbortNack as exc:
                raise _normalize_error(exc) from exc
            except TimeoutError as exc:
                raise BacnetError("timeout") from exc
            except OSError as exc:
                raise BacnetError("error", str(exc)) from exc

    # ── 地址解析辅助 ───────────────────────────────────────────────────────

    @staticmethod
    def local_station(mac: int) -> LocalStation:
        return LocalStation(mac)
