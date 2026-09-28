"""本地 Prometheus 指标（gateway.md §10）：文本端点 ``:9101/metrics``。

OT 隔离下由运维按需刮取/本地留档。指标全集（§10 清单）：

- gateway_poll_total{device, result=ok|timeout|error}
- gateway_poll_cycle_duration_s（gauge，最近一轮周期耗时）
- gateway_cache_rows / gateway_cache_oldest_age_s
- gateway_replay_lag_s（排水进度）
- gateway_mqtt_connected
- gateway_config_applied_job{job_id}（值 = 1；job_id 为最近应用作业）
- gateway_write_cmd_total{result}
- gateway_offline_action_writes_total{result}

不引 prometheus-client：手写最小文本暴露格式（依赖面钉死为
bacpypes3/paho-mqtt/openpyxl/pydantic，gateway.md §9.2）。
"""

from __future__ import annotations

import asyncio
import contextlib

_HELP = {
    "gateway_poll_total": ("counter", "轮询结果计数（device × result）"),
    "gateway_poll_cycle_duration_s": ("gauge", "最近一轮轮询周期耗时（秒）"),
    "gateway_cache_rows": ("gauge", "断网缓存行数"),
    "gateway_cache_oldest_age_s": ("gauge", "缓存最旧行龄（秒）"),
    "gateway_replay_lag_s": ("gauge", "排水滞后（排水期间新数据延迟 = 排水时长）"),
    "gateway_mqtt_connected": ("gauge", "MQTT 连接态（1/0）"),
    "gateway_config_applied_job": ("gauge", "最近应用的配置作业（job_id 标签，值恒 1）"),
    "gateway_write_cmd_total": ("counter", "下行写指令结果计数"),
    "gateway_offline_action_writes_total": ("counter", "断链安全值写结果计数"),
}


class Registry:
    """极简指标注册表：counter/gauge + 标签维度，Prometheus 文本格式输出。"""

    def __init__(self) -> None:
        self._values: dict[str, dict[tuple[tuple[str, str], ...], float]] = {}

    def inc(self, name: str, value: float = 1.0, **labels: str) -> None:
        key = tuple(sorted(labels.items()))
        bucket = self._values.setdefault(name, {})
        bucket[key] = bucket.get(key, 0.0) + value

    def set(self, name: str, value: float, **labels: str) -> None:
        key = tuple(sorted(labels.items()))
        self._values.setdefault(name, {})[key] = value

    def observe(self, name: str, value: float) -> None:
        """gauge 型单值观测（cycle_duration 等）。"""
        self.set(name, value)

    def render(self) -> str:
        lines: list[str] = []
        for name in sorted(self._values):
            kind, help_text = _HELP.get(name, ("gauge", name))
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} {kind}")
            for key, value in sorted(self._values[name].items()):
                label_str = ""
                if key:
                    rendered = ",".join(f'{k}="{v}"' for k, v in key)
                    label_str = "{" + rendered + "}"
                lines.append(f"{name}{label_str} {value:g}")
        return "\n".join(lines) + "\n"


async def serve_metrics(registry: Registry, addr: str) -> asyncio.AbstractServer:
    """``GET /metrics`` 文本端点（§10 :9101）。addr 形如 0.0.0.0:9101。"""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.readline()  # 请求行（只服务 GET /metrics，其余忽略）
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
            body = registry.render().encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
                + f"Content-Length: {len(body)}\r\n".encode()
                + b"Connection: close\r\n\r\n"
                + body
            )
            await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass  # 客户端早断/服务关闭：无日志噪音必要
        finally:
            if not writer.is_closing():
                writer.close()
            with contextlib.suppress(ConnectionError, RuntimeError):
                await writer.wait_closed()

    host, port = addr.rsplit(":", 1)
    return await asyncio.start_server(handle, host, int(port))
