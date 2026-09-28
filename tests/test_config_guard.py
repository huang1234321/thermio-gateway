"""结构守护（§11 尾注 / §1 边界的可执行化）：配置模型零数据库连接字段。

algo.md §1.2「无从连起」同款结构性落地——不是约定，是断言。
"""

from __future__ import annotations

from pathlib import Path

from thermio_gateway.config import Settings, assert_no_database_fields


class TestNoDatabaseFields:
    def test_structure_guard_passes_on_current_model(self):
        assert_no_database_fields()  # 现模型干净

    def test_guard_catches_smuggled_dsn(self):
        import dataclasses

        @dataclasses.dataclass(frozen=True)
        class Smuggled:
            pg_dsn: str = ""
            kafka_brokers: str = ""

        try:
            assert_no_database_fields(Smuggled)  # type: ignore[arg-type]
        except AssertionError as exc:
            assert "pg_dsn" in str(exc)
        else:
            raise AssertionError("结构守护未拦住私带 DSN 字段")

    def test_all_env_keys_have_example_placeholders(self):
        """SEC-KEY-06：.env.example 占位值——不含真实凭据形状。"""
        example = Path(__file__).parent.parent / ".env.example"
        text = example.read_text(encoding="utf-8")
        assert "MQTT_PASSWORD=change-me" in text
        # 占位主机而非真实地址
        assert "broker.example.local" in text


class TestSettings:
    def test_fail_fast_on_missing_required(self, monkeypatch):
        for key in list(__import__("os").environ):
            if key.startswith(("MQTT_", "GATEWAY_", "BACNET_")):
                monkeypatch.delenv(key, raising=False)
        import pytest

        from thermio_gateway.config import ConfigError

        with pytest.raises(ConfigError):
            Settings.from_env()

    def test_mqtts_requires_ca(self, monkeypatch):
        import pytest

        from thermio_gateway.config import ConfigError

        env = {
            "MQTT_BROKER_URL": "mqtts://b.example:8883",
            "MQTT_USERNAME": "u",
            "MQTT_PASSWORD": "p",
            "MQTT_CLIENT_ID": "c",
            "GATEWAY_SERIAL": "s",
            "BACNET_DEVICE_ID": "100",
        }
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        monkeypatch.delenv("MQTT_CA_PATH", raising=False)
        with pytest.raises(ConfigError):
            Settings.from_env()
        monkeypatch.setenv("MQTT_CA_PATH", "/etc/ssl/ca.pem")
        s = Settings.from_env()
        assert s.mqtt_tls() and s.mqtt_host_port() == ("b.example", 8883)

    def test_replay_batch_bounds(self, monkeypatch):
        import pytest

        from thermio_gateway.config import ConfigError

        base = {
            "MQTT_BROKER_URL": "mqtt://b:1883",
            "MQTT_USERNAME": "u",
            "MQTT_PASSWORD": "p",
            "MQTT_CLIENT_ID": "c",
            "GATEWAY_SERIAL": "s",
            "BACNET_DEVICE_ID": "100",
            "REPLAY_BATCH_POINTS": "501",
        }
        for k, v in base.items():
            monkeypatch.setenv(k, v)
        with pytest.raises(ConfigError):
            Settings.from_env()
        monkeypatch.setenv("REPLAY_BATCH_POINTS", "500")
        assert Settings.from_env().replay_batch_points == 500


class TestMetrics:
    def test_render_prometheus_text(self):
        from thermio_gateway.metrics import Registry

        reg = Registry()
        reg.inc("gateway_poll_total", device="519", result="ok")
        reg.inc("gateway_poll_total", device="519", result="ok")
        reg.set("gateway_mqtt_connected", 1.0)
        text = reg.render()
        assert 'gateway_poll_total{device="519",result="ok"} 2' in text
        assert "gateway_mqtt_connected 1" in text
        assert "# TYPE gateway_poll_total counter" in text
