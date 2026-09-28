"""消息契约模型层（PY-05）：三套信封解析与产出共用同一模型，防字段漂移。

- ingest.md §3（up/data telemetry_batch 信封与点位字段规则）；
- M2-import.md §8.4/§9.2（down/config、up/config/ack、down/read）；
- control-safety.md §4.2/§4.3（down/write 的 write_cmd/read_cmd、up/event 的
  write_ack/read_result）。

字段规则以对端文档为准逐字对齐；本模块产出的上行消息即 ingest 眼中的
「普通成品网关」（伪装面，gateway.md §5.1 对齐表的代码面）。
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ingest.md §3.2：name 1..64 字符，[A-Za-z0-9_.:-]+
RAW_NAME_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
# 更宽的云点号上限（M2 §5.1 raw_name trim 后 1..128）——导出侧取严格侧 64
RAW_NAME_CLOUD_MAX = 128
RAW_NAME_STRICT_MAX = 64

Quality = Literal["good", "bad", "uncertain"]


def rfc3339(dt: datetime) -> str:
    """RFC3339 带时区（naive 视作 UTC——正常运行时钟来自 chrony 同步的本地钟）。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.isoformat()


def parse_rfc3339(text: str) -> datetime:
    # Python 3.11 起 fromisoformat 覆盖 RFC3339 全形（含 Z 后缀）
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def is_valid_uplink_name(name: str) -> bool:
    """上行 name 合法性 = ingest.md §3.2 点位元素规则（charset + 长度）。"""
    return bool(RAW_NAME_RE.match(name))


# ── ingest.md §3：up/data 信封 ────────────────────────────────────────────────


class TelemetryPoint(BaseModel):
    """单个点位样本。value/value_text 二选一（§3.2 数值量/枚态量）。"""

    model_config = ConfigDict(extra="forbid")

    name: str
    value: float | None = None
    value_text: str | None = None
    ts: datetime
    quality: Quality = "good"
    unit: str | None = None

    @field_validator("name")
    @classmethod
    def _name_charset(cls, v: str) -> str:
        if not is_valid_uplink_name(v):
            raise ValueError(f"name {v!r} 违反 ingest §3.2（1..64 字符，[A-Za-z0-9_.:-]）")
        return v


class TelemetryBatch(BaseModel):
    """thermio/gw/{clientid}/up/data 载荷（§3.1/§3.2）。points 1..500。"""

    model_config = ConfigDict(extra="forbid")

    msg_type: Literal["telemetry_batch"] = "telemetry_batch"
    ver: int = 1
    gw: str
    seq: int = Field(ge=0)
    sent_at: datetime
    points: list[TelemetryPoint] = Field(min_length=1, max_length=500)


# ── M2-import.md §8.4：down/config 与 up/config/ack ──────────────────────────


class ConfigPoint(BaseModel):
    """配置快照点位元素：云端契约**无 BACnet 地址字段**（§3.1 会合模型）。"""

    model_config = ConfigDict(extra="ignore")

    raw_name: str
    ref: str
    unit_raw: str | None = None
    unit_std: str | None = None


class OfflineActionWrite(BaseModel):
    """M1 §3.8 loose schema：value 单位口径 = unit_std（与 down/write 同纪律）。"""

    model_config = ConfigDict(extra="ignore")

    raw_name: str
    value: float


class OfflineAction(BaseModel):
    model_config = ConfigDict(extra="ignore")

    writes: list[OfflineActionWrite] = Field(default_factory=list, max_length=64)


class GatewayConfig(BaseModel):
    """down/config retained 全量快照（全量替换语义，§7.1）。"""

    model_config = ConfigDict(extra="ignore")

    schema_version: int = 1
    job_id: str
    generated_at: datetime | None = None
    points: list[ConfigPoint]
    offline_action: OfflineAction | None = None


class ConfigAckFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_name: str
    reason: Literal["POINT_NOT_IN_MAP", "POINT_DISABLED"]


class ConfigAck(BaseModel):
    """up/config/ack 载荷（§7.1）。"""

    model_config = ConfigDict(extra="forbid")

    job_id: str
    ok_count: int
    failed: list[ConfigAckFailure] = Field(default_factory=list)


# ── M2-import.md §9.2：down/read 自检指令 ────────────────────────────────────


class SelfCheckRead(BaseModel):
    """down/read 载荷：{job_id, req_id, points}（批次 ≤500 由云端保证，本地防御切分）。"""

    model_config = ConfigDict(extra="ignore")

    job_id: str
    req_id: str
    points: list[str] = Field(min_length=1, max_length=500)


# ── control-safety.md §4.2/§4.3：down/write 与 up/event ──────────────────────


class WriteCmd(BaseModel):
    """down/write 的 write_cmd（值一律 unit_std）。"""

    model_config = ConfigDict(extra="ignore")

    msg_type: Literal["write_cmd"]
    ver: int = 1
    cmd_id: str
    point_ref: str
    value: float
    unit: str
    issued_at: datetime
    expires_in_s: int


class ReadCmd(BaseModel):
    """down/write 的 read_cmd（不带值）。"""

    model_config = ConfigDict(extra="ignore")

    msg_type: Literal["read_cmd"]
    ver: int = 1
    cmd_id: str
    point_ref: str
    issued_at: datetime
    expires_in_s: int


WriteAckCode = Literal["POINT_UNKNOWN", "WRITE_REFUSED", "CMD_EXPIRED", None]


class WriteAck(BaseModel):
    """up/event 的 write_ack：accepted = 校验通过并受理（传输回执语义 §4.4）。"""

    model_config = ConfigDict(extra="forbid")

    msg_type: Literal["write_ack"] = "write_ack"
    ver: int = 1
    cmd_id: str
    gw: str
    result: Literal["accepted", "rejected"]
    code: WriteAckCode = None
    at: datetime


class ReadResult(BaseModel):
    """up/event 的 read_result（unit_std 口径，quality ≠ good 视同读失败）。"""

    model_config = ConfigDict(extra="forbid")

    msg_type: Literal["read_result"] = "read_result"
    ver: int = 1
    cmd_id: str
    gw: str
    value: float | None = None
    unit: str | None = None
    quality: Quality = "good"
    ts: datetime
    at: datetime


def parse_write_or_read(payload: bytes | str) -> WriteCmd | ReadCmd | None:
    """down/write 载荷解析：msg_type 区分 write_cmd/read_cmd；不认识的返回 None。"""
    import json

    try:
        obj = json.loads(payload)
        kind = obj.get("msg_type") if isinstance(obj, dict) else None
        if kind == "write_cmd":
            return WriteCmd.model_validate(obj)
        if kind == "read_cmd":
            return ReadCmd.model_validate(obj)
        return None
    except (json.JSONDecodeError, ValueError):
        return None
