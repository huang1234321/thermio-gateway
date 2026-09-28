"""环境变量配置（gateway.md §11；SEC-KEY-01 密钥只来自环境）。

结构性纪律（§1 边界的可执行化）：本模型**不存在**任何 PG/TSDB/Kafka DSN
字段——配置唯一来源 = down/config retained 消息 + 本地点表。tests 中的
结构守护测试断言这一点（algo.md「无从连起」同款结构性落地）。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields


class ConfigError(Exception):
    """配置缺失/非法——进程 fail-fast（不猜、不带默认硬跑）。"""


def _env_int(name: str, default: int | None = None) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        if default is None:
            raise ConfigError(f"缺少必填环境变量 {name}")
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name}={raw!r} 不是整数") from exc


def _env_str(name: str, default: str | None = None) -> str:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        if default is None:
            raise ConfigError(f"缺少必填环境变量 {name}")
        return default
    return raw


def _env_opt(name: str) -> str | None:
    raw = os.environ.get(name)
    return raw or None


@dataclass(frozen=True)
class Settings:
    """全部配置项（§11 全表）。"""

    # MQTT 上行与身份（§2）
    mqtt_broker_url: str
    mqtt_username: str
    mqtt_password: str
    mqtt_client_id: str
    gateway_serial: str
    mqtt_ca_path: str | None
    mqtt_keepalive_s: int

    # BACnet 本机身份与栈参数（§4.4）
    bacnet_device_id: int
    bacnet_port: int
    bacnet_interface: str
    bacnet_apdu_timeout_ms: int
    bacnet_apdu_retries: int
    bacnet_max_inflight_per_device: int
    bacnet_inter_request_gap_ms: int

    # 采集与缓存（§4.4/§6）
    poll_default_interval_s: int
    cache_path: str
    cache_retention_days: int
    replay_inflight_msgs: int
    replay_batch_points: int

    # 断链安全值触发窗（§7.4）
    offline_action_delay_s: int

    # 可观测（§10）
    metrics_addr: str
    log_level: str
    log_file: str | None

    @classmethod
    def from_env(cls) -> Settings:
        broker = _env_str("MQTT_BROKER_URL")
        if not (broker.startswith("mqtt://") or broker.startswith("mqtts://")):
            raise ConfigError("MQTT_BROKER_URL 须以 mqtt:// 或 mqtts:// 开头")
        ca = _env_opt("MQTT_CA_PATH")
        if broker.startswith("mqtts://") and not ca:
            raise ConfigError("mqtts:// 必须显式给 MQTT_CA_PATH（不关校验，SEC-NET-04）")
        inflight = _env_int("BACNET_MAX_INFLIGHT_PER_DEVICE", 2)
        if inflight < 1:
            raise ConfigError("BACNET_MAX_INFLIGHT_PER_DEVICE 须 ≥1")
        batch = _env_int("REPLAY_BATCH_POINTS", 500)
        if not 1 <= batch <= 500:
            raise ConfigError("REPLAY_BATCH_POINTS 须在 1..500（ingest.md §3.2 单消息上限）")
        return cls(
            mqtt_broker_url=broker,
            mqtt_username=_env_str("MQTT_USERNAME"),
            mqtt_password=_env_str("MQTT_PASSWORD"),
            mqtt_client_id=_env_str("MQTT_CLIENT_ID"),
            gateway_serial=_env_str("GATEWAY_SERIAL"),
            mqtt_ca_path=ca,
            mqtt_keepalive_s=30,
            bacnet_device_id=_env_int("BACNET_DEVICE_ID"),
            bacnet_port=_env_int("BACNET_PORT", 47808),
            bacnet_interface=_env_str("BACNET_INTERFACE", "0.0.0.0"),
            bacnet_apdu_timeout_ms=_env_int("BACNET_APDU_TIMEOUT_MS", 3000),
            bacnet_apdu_retries=_env_int("BACNET_APDU_RETRIES", 1),
            bacnet_max_inflight_per_device=inflight,
            bacnet_inter_request_gap_ms=_env_int("BACNET_INTER_REQUEST_GAP_MS", 50),
            poll_default_interval_s=_env_int("POLL_DEFAULT_INTERVAL_S", 60),
            cache_path=_env_str("CACHE_PATH", "/var/lib/thermio-gateway/cache.db"),
            cache_retention_days=_env_int("CACHE_RETENTION_DAYS", 4),
            replay_inflight_msgs=_env_int("REPLAY_INFLIGHT_MSGS", 4),
            replay_batch_points=batch,
            offline_action_delay_s=_env_int("OFFLINE_ACTION_DELAY_S", 60),
            metrics_addr=_env_str("METRICS_ADDR", "0.0.0.0:9101"),
            log_level=_env_str("LOG_LEVEL", "INFO"),
            log_file=_env_opt("LOG_FILE"),
        )

    def mqtt_host_port(self) -> tuple[str, int]:
        """从 broker URL 解析 (host, port)；mqtt(mqs)://host:port。"""
        from urllib.parse import urlparse

        parsed = urlparse(self.mqtt_broker_url)
        if not parsed.hostname:
            raise ConfigError(f"MQTT_BROKER_URL 无法解析主机: {self.mqtt_broker_url!r}")
        tls = parsed.scheme == "mqtts"
        return parsed.hostname, parsed.port or (8883 if tls else 1883)

    def mqtt_tls(self) -> bool:
        return self.mqtt_broker_url.startswith("mqtts://")


def assert_no_database_fields(settings_type: type[Settings] = Settings) -> None:
    """结构守护入口：配置模型不得出现任何数据库/消息中间件连接字段。

    由 tests/test_config_guard.py 调用（§11 尾注）；新增配置项若撞上
    禁词前缀，说明边界被破坏（对 PG/TSDB/Kafka 零连接，§1）。
    """

    forbidden = ("pg", "postgres", "tsdb", "timescale", "kafka", "dsn", "jdbc", "amqp")
    for f in fields(settings_type):
        name = f.name.lower()
        if any(word in name for word in forbidden):
            raise AssertionError(f"配置字段 {f.name} 违反 §1 边界（疑似数据库连接项）")
