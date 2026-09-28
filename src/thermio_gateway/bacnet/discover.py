"""点位发现与点表导出（gateway.md §4.1——喂 M2 导入向导，验收路径第一棒）。

部署期工具（``thermio-gateway discover``），非常驻循环，对 OT 网络零持续负担：

- Phase 1  Who-Is（全广播或 --at 定向，可限 device-id 范围）→ I-Am 台账；
- Phase 2  逐设备 ReadProperty(device_object_list)；不可读 → 按
  (obj_type, instance=0,1,2…) 逐序枚举至首个 error（仅 §4.2 收录类型）；
- Phase 3  逐对象 ReadProperty object_name / units / description（可选）。

产出三件：
① ``discovery-<date>.json``  台账 + 逐点 原始名→导出名+标记（工程核对件，
   物理身份不静默改动——§3.3）；
② pointmap staging 落盘（``pointmap adopt`` 确认后生效）；
③ ``export-<date>.xlsx``  M2 §5.1 canonical 四列：A 点号 B 描述 C 单位 D 方向
   （D 留空——可写性是工程决策不是对象属性推断，误判写点比漏判危险）。
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from .. import pointmap as pm
from .. import units
from ..bacnet.stack import BacnetError, BacnetStack, IAmRecord

log = logging.getLogger("gw.discover")

# Phase 2 兜底枚举的对象类型顺序（仅 §4.2 收录集）
_FALLBACK_ENUM_TYPES = list(pm.COLLECTABLE_TYPES)


@dataclass
class DiscoveredPoint:
    device_id: int
    obj_type: str
    obj_instance: int
    original_name: str
    raw_name: str
    marks: list[str] = field(default_factory=list)
    unit_token: str | None = None
    description: str | None = None


@dataclass
class DiscoveredDevice:
    device_id: int
    address: str
    vendor_id: int | None
    model: str | None
    name: str | None
    object_count: int = 0
    rpm_supported: bool = True  # 发现阶段逐设备探测并记忆（§9.4）
    object_list_read: bool = True  # False = 兜底枚举路径


@dataclass
class DiscoveryReport:
    generated_at: str
    devices: list[DiscoveredDevice] = field(default_factory=list)
    points: list[DiscoveredPoint] = field(default_factory=list)
    skipped: list[dict] = field(default_factory=list)  # 跳过对象（非收录类型）

    def to_json(self) -> str:
        return json.dumps(
            {
                "generated_at": self.generated_at,
                "devices": [d.__dict__ for d in self.devices],
                "points": [p.__dict__ for p in self.points],
                "skipped": self.skipped,
            },
            ensure_ascii=False,
            indent=2,
        )


def _kebab_prop(prop: str) -> str:
    """属性名转 bacpypes3 kebab 标签（presentValue → present-value）。"""
    return prop[:1].lower() + "".join(("-" + ch.lower()) if ch.isupper() else ch for ch in prop[1:])


class Discoverer:
    def __init__(self, stack: BacnetStack) -> None:
        self._stack = stack

    async def run(
        self,
        whois_timeout_s: float = 3.0,
        at: str | None = None,
        low: int | None = None,
        high: int | None = None,
    ) -> DiscoveryReport:
        report = DiscoveryReport(generated_at=datetime.now(UTC).isoformat(timespec="seconds"))
        # Phase 1
        iams = await self._stack.who_is(timeout_s=whois_timeout_s, address=at, low=low, high=high)
        if not iams:
            log.warning("who-is 无应答（网络可达性/防火墙 UDP 47808）")
        devices: dict[int, IAmRecord] = {}
        for rec in iams:
            if rec.device_id in devices:
                continue  # 多网卡重复应答去重
            devices[rec.device_id] = rec
        # Phase 2 + 3（taken 为**整轮**共享集：raw_name 是网关全局键，
        # 跨设备同名同样要去重——staging/pointmap 主键与云端注册键均全局）
        taken: set[str] = set()
        for device_id, rec in sorted(devices.items()):
            dev = DiscoveredDevice(
                device_id=device_id,
                address=rec.address,
                vendor_id=rec.vendor_id,
                model=None,
                name=None,
            )
            dev.model, dev.name = await self._read_device_meta(rec.address, device_id)
            objects = await self._enumerate_objects(rec.address, device_id)
            dev.object_list_read = objects is not None
            if objects is None:
                objects = await self._fallback_enumerate(rec.address, device_id)
                dev.object_list_read = False
            dev.object_count = len(objects)
            report.devices.append(dev)
            await self._read_points(rec.address, device_id, objects, report, taken)
        return report

    async def _read_device_meta(
        self, address: str, device_id: int
    ) -> tuple[str | None, str | None]:
        name: str | None = None
        model: str | None = None
        try:
            result = await self._stack.read_property(address, "device", device_id, "objectName")
            name = str(getattr(result, "value", result))
        except BacnetError as exc:
            log.warning("device %s objectName 不可读: %s", device_id, exc)
        try:
            result = await self._stack.read_property(address, "device", device_id, "modelName")
            model = str(getattr(result, "value", result))
        except BacnetError as exc:
            log.warning("device %s modelName 不可读: %s", device_id, exc)
        return model, name

    async def _enumerate_objects(
        self, address: str, device_id: int
    ) -> list[tuple[str, int]] | None:
        """Phase 2 主路径：object_list。不可读返回 None（走兜底）。"""
        try:
            rpm_results = await self._stack.read_property_multiple(
                address, [("device", device_id, ["objectList"])]
            )
        except BacnetError:
            try:
                result = await self._stack.read_property(address, "device", device_id, "objectList")
            except BacnetError as exc:
                log.warning("device %s objectList 不可读（兜底枚举）: %s", device_id, exc)
                return None
            object_list = getattr(result, "value", None) or []
            return [(str(oid[0]), int(oid[1])) for oid in object_list]
        objects: list[tuple[str, int]] = []
        for _otype, _inst, _prop, value in rpm_results:
            for oid in value or []:
                objects.append((str(oid[0]), int(oid[1])))
        return objects

    async def _fallback_enumerate(self, address: str, device_id: int) -> list[tuple[str, int]]:
        """Phase 2 兜底：按 (obj_type, instance=0,1,2…) 枚举至首个 error（§4.1）。"""
        objects: list[tuple[str, int]] = []
        for obj_type in _FALLBACK_ENUM_TYPES:
            instance = 0
            while True:
                try:
                    await self._stack.read_property(address, obj_type, instance, "objectName")
                except BacnetError:
                    break
                objects.append((obj_type, instance))
                instance += 1
        return objects

    async def _read_points(
        self,
        address: str,
        device_id: int,
        objects: list[tuple[str, int]],
        report: DiscoveryReport,
        taken: set[str],
    ) -> None:
        for obj_type, obj_instance in objects:
            if obj_type not in pm.COLLECTABLE_TYPES:
                report.skipped.append(
                    {"device_id": device_id, "obj_type": obj_type, "obj_instance": obj_instance}
                )
                continue
            original_name = ""
            description: str | None = None
            unit_token: str | None = None
            extra_marks: list[str] = []
            try:
                raw = await self._stack.read_property(address, obj_type, obj_instance, "objectName")
                original_name = str(raw.value) if hasattr(raw, "value") else str(raw)
            except BacnetError as exc:
                log.warning("object 名不可读 %s:%s:%s: %s", device_id, obj_type, obj_instance, exc)
                original_name = ""
            try:
                desc = await self._stack.read_property(
                    address, obj_type, obj_instance, "description"
                )
                description = str(desc.value) if hasattr(desc, "value") else str(desc)
            except BacnetError:
                description = None  # 可选属性，缺失不标记
            if obj_type.startswith("analog-"):
                try:
                    u = await self._stack.read_property(address, obj_type, obj_instance, "units")
                    unit_enum = str(u.value) if hasattr(u, "value") else str(u)
                    unit_token = units.bacnet_unit_to_token(unit_enum)
                    if unit_enum and not unit_token:
                        extra_marks.append("unit_unmapped")
                except BacnetError:
                    unit_token = None  # units 不可读 = 留空直通
            decision = pm.generate_raw_name(original_name, device_id, obj_type, obj_instance, taken)
            report.points.append(
                DiscoveredPoint(
                    device_id=device_id,
                    obj_type=obj_type,
                    obj_instance=obj_instance,
                    original_name=original_name,
                    raw_name=decision.raw_name,
                    marks=list(decision.marks) + extra_marks,
                    unit_token=unit_token,
                    description=description,
                )
            )


# ── 产物落盘 ────────────────────────────────────────────────────────────────


def write_report(report: DiscoveryReport, out_dir: str) -> Path:
    """① discovery-<date>.json（工程核对件：原始名→导出名+标记逐点可核）。"""
    path = Path(out_dir) / f"discovery-{date.today().isoformat()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report.to_json(), encoding="utf-8")
    return path


def stage_pointmap(db: sqlite3.Connection, report: DiscoveryReport) -> int:
    """② pointmap staging 落盘（adopt 前不生效，§4.1）。返回 staging 行数。"""
    records = [
        (p.raw_name, p.device_id, p.obj_type, p.obj_instance, p.unit_token, p.description)
        for p in report.points
    ]
    pm.replace_staging(db, records)
    return len(records)


def write_export_xlsx(report: DiscoveryReport, out_dir: str) -> Path:
    """③ export-<date>.xlsx：M2 §5.1 canonical 四列，列头逐字对齐。

    A 点号(=raw_name) B 描述(=description，带 [AV] 对象类型提示) C 单位
    (=unit_raw token) D 方向(=空，全部默认 read——§4.1 方向从严)。
    """
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    assert ws is not None
    ws.title = "points"
    ws.append(["点号", "描述", "单位", "方向"])
    for p in report.points:
        abbrev = _type_abbrev(p.obj_type)
        desc = f"[{abbrev}] {p.description}" if p.description else f"[{abbrev}] {p.original_name}"
        ws.append([p.raw_name, desc, p.unit_token or "", ""])
    path = Path(out_dir) / f"export-{date.today().isoformat()}.xlsx"
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    return path


def _type_abbrev(obj_type: str) -> str:
    mapping = {
        "analog-input": "AI",
        "analog-output": "AO",
        "analog-value": "AV",
        "binary-input": "BI",
        "binary-output": "BO",
        "binary-value": "BV",
        "multi-state-input": "MSI",
        "multi-state-value": "MSV",
    }
    return mapping.get(obj_type, obj_type[:3].upper())
