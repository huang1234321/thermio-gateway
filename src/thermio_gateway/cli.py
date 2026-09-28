"""CLI 入口（gateway.md §12）：``run`` / ``discover`` / ``pointmap adopt`` / ``export`` / ``doctor``。

- ``run``        常驻服务（采集 + 上行 + 缓存排水 + 下行面）；
- ``discover``   部署期发现工具（§4.1 三相），产出报告/staging/xlsx 三件；
- ``pointmap adopt``  staging → pointmap 版本迁移（§14.3-3）；
- ``export``     从发现报告重建 M2 导入 xlsx（列头 canonical）；
- ``doctor``     启动前自检：device-id 冲突（Who-Is 自检，§9.4）/ DB / MQTT / 配置。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from pathlib import Path

from . import __version__
from .bacnet.discover import (
    Discoverer,
    DiscoveryReport,
    stage_pointmap,
    write_export_xlsx,
    write_report,
)
from .bacnet.stack import BacnetStack, build_application
from .config import ConfigError, Settings
from .db import open_db
from .logging_setup import setup_logging
from .metrics import Registry, serve_metrics
from .mqtt_agent import MqttAgent
from .pointmap import adopt_staging

log = logging.getLogger("gw.cli")


def build_stack_from_settings(settings: Settings) -> BacnetStack:
    app = build_application(
        device_id=settings.bacnet_device_id,
        interface=settings.bacnet_interface,
        port=settings.bacnet_port,
        apdu_timeout_ms=settings.bacnet_apdu_timeout_ms,
        apdu_retries=settings.bacnet_apdu_retries,
    )
    return BacnetStack(
        app,
        max_inflight=settings.bacnet_max_inflight_per_device,
        inter_request_gap_ms=settings.bacnet_inter_request_gap_ms,
    )


# ── run ─────────────────────────────────────────────────────────────────────


async def cmd_run(args: argparse.Namespace) -> int:
    from .service import GatewayService

    settings = Settings.from_env()
    setup_logging(settings.log_level, settings.log_file)
    db = open_db(settings.cache_path)
    stack = build_stack_from_settings(settings)
    host, port = settings.mqtt_host_port()
    agent = MqttAgent(
        broker_host=host,
        broker_port=port,
        client_id=settings.mqtt_client_id,
        username=settings.mqtt_username,
        password=settings.mqtt_password,
        tls=settings.mqtt_tls(),
        ca_path=settings.mqtt_ca_path,
        keepalive_s=settings.mqtt_keepalive_s,
        max_inflight=settings.replay_inflight_msgs,
    )
    registry = Registry()
    service = GatewayService(settings, stack, agent, registry, db)
    metrics_server = await serve_metrics(registry, settings.metrics_addr)

    stop_signal = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_signal.set)
        except NotImplementedError:  # 非 POSIX 宿主退化（生产宿主为 Linux）
            signal.signal(sig, lambda *_: stop_signal.set())

    await service.start()
    log.info("thermio-gateway %s 运行中（Ctrl-C 优雅退出）", __version__)
    await stop_signal.wait()
    log.info("收到停止信号，优雅退出……")
    await service.stop()
    metrics_server.close()
    await metrics_server.wait_closed()
    return 0


# ── discover ────────────────────────────────────────────────────────────────


async def cmd_discover(args: argparse.Namespace) -> int:
    settings = _partial_settings()
    setup_logging("INFO", None)
    stack = build_stack_from_settings(settings)
    discoverer = Discoverer(stack)
    report = await discoverer.run(
        whois_timeout_s=args.whois_timeout,
        at=args.at,
        low=args.low,
        high=args.high,
    )
    out_dir = args.out_dir
    report_path = write_report(report, out_dir)
    db = open_db(settings.cache_path)
    staged = stage_pointmap(db, report)
    xlsx_path = write_export_xlsx(report, out_dir)
    db.close()
    marks = [p for p in report.points if p.marks]
    print(
        json.dumps(
            {
                "devices": len(report.devices),
                "points": len(report.points),
                "staged": staged,
                "report": str(report_path),
                "xlsx": str(xlsx_path),
                "points_with_marks": len(marks),
                "skipped_objects": len(report.skipped),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if marks:
        log.warning("存在标记点（sanitized/deduped/truncated/unit_unmapped）——人工核对报告")
    return 0


# ── pointmap adopt ──────────────────────────────────────────────────────────


async def cmd_pointmap_adopt(args: argparse.Namespace) -> int:
    settings = _partial_settings()
    db = open_db(settings.cache_path)
    report = adopt_staging(db)
    db.close()
    print(
        json.dumps(
            {
                "added": report["added"],
                "address_drift": report["address_drift"],
                "missing": report["missing"],
                "unchanged": len(report["unchanged"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if report["address_drift"]:
        log.warning("检测到同名对象地址漂移（已跟随物理地址，raw_name 稳定）——核对清单")
    return 0


# ── export ──────────────────────────────────────────────────────────────────


async def cmd_export(args: argparse.Namespace) -> int:
    path = Path(args.report)
    if not path.exists():
        # 缺省取 out_dir 下最新 discovery-*.json
        candidates = sorted(Path(args.out_dir).glob("discovery-*.json"))
        if not candidates:
            print(f"未找到发现报告（{path} 不存在且无 discovery-*.json）", file=sys.stderr)
            return 2
        path = candidates[-1]
    data = json.loads(path.read_text(encoding="utf-8"))
    report = _report_from_json(data)
    out = write_export_xlsx(report, args.out_dir)
    print(str(out))
    return 0


def _report_from_json(data: dict) -> DiscoveryReport:
    from .bacnet.discover import DiscoveredDevice, DiscoveredPoint

    report = DiscoveryReport(generated_at=data.get("generated_at", ""))
    report.devices = [DiscoveredDevice(**d) for d in data.get("devices", [])]
    report.points = [DiscoveredPoint(**p) for p in data.get("points", [])]
    report.skipped = data.get("skipped", [])
    return report


# ── doctor ──────────────────────────────────────────────────────────────────


async def cmd_doctor(args: argparse.Namespace) -> int:
    settings = Settings.from_env()
    setup_logging("INFO", None)
    failures: list[str] = []

    # 1) device-id 冲突自检（§9.4：软采集自身是一个 BACnet device）
    stack = build_stack_from_settings(settings)
    try:
        iams = await stack.who_is(
            timeout_s=args.whois_timeout,
            low=settings.bacnet_device_id,
            high=settings.bacnet_device_id,
        )
    except Exception as exc:
        failures.append(f"who-is 自检异常: {exc}")
        iams = []
    conflicts = [r for r in iams if r.device_id == settings.bacnet_device_id]
    if conflicts:
        failures.append(
            f"device-id {settings.bacnet_device_id} 冲突：网络已有 "
            f"{[c.address for c in conflicts]} 应答（找 BA 厂商分配段，§9.4）"
        )

    # 2) DB 可写
    try:
        db = open_db(settings.cache_path)
        db.close()
    except Exception as exc:
        failures.append(f"缓存库不可写（{settings.cache_path}）: {exc}")

    # 3) MQTT TCP 可达
    host, port = settings.mqtt_host_port()
    try:
        _reader, writer = await asyncio.open_connection(host, port)
        writer.close()
        await writer.wait_closed()
    except OSError as exc:
        failures.append(f"MQTT broker 不可达 {host}:{port}: {exc}")

    if failures:
        for f in failures:
            print(f"FAIL {f}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "ok": True,
                "device_id": settings.bacnet_device_id,
                "mqtt": f"{host}:{port}",
                "cache": settings.cache_path,
            },
            ensure_ascii=False,
        )
    )
    return 0


# ── 装配 ────────────────────────────────────────────────────────────────────


def _partial_settings() -> Settings:
    """discover/adopt 等部署期工具不需要 MQTT 凭据——补占位满足模型。"""
    import os

    for key, placeholder in (
        ("MQTT_BROKER_URL", "mqtt://placeholder.local:1883"),
        ("MQTT_USERNAME", "placeholder"),
        ("MQTT_PASSWORD", "placeholder"),
        ("MQTT_CLIENT_ID", "placeholder"),
        ("GATEWAY_SERIAL", "placeholder"),
    ):
        os.environ.setdefault(key, placeholder)
    return Settings.from_env()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="thermio-gateway",
        description="thermio BACnet/IP 软采集服务（设计真源 docs/design/gateway.md）",
    )
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="常驻服务")
    p_run.set_defaults(func=cmd_run)

    p_disc = sub.add_parser("discover", help="点位发现与点表导出（部署期工具）")
    p_disc.add_argument("--at", help="定向 Who-Is 地址（缺省全广播）")
    p_disc.add_argument("--low", type=int, help="device-id 范围下限")
    p_disc.add_argument("--high", type=int, help="device-id 范围上限")
    p_disc.add_argument("--whois-timeout", type=float, default=3.0)
    p_disc.add_argument("--out-dir", default=".", help="报告/xlsx 输出目录")
    p_disc.set_defaults(func=cmd_discover)

    p_adopt = sub.add_parser("pointmap", help="pointmap 管理")
    p_adopt_sub = p_adopt.add_subparsers(dest="sub", required=True)
    p_adopt_run = p_adopt_sub.add_parser("adopt", help="staging → pointmap 生效")
    p_adopt_run.set_defaults(func=cmd_pointmap_adopt)

    p_export = sub.add_parser("export", help="从发现报告重建 M2 导入 xlsx")
    p_export.add_argument("--report", default="", help="discovery-*.json 路径（缺省取最新）")
    p_export.add_argument("--out-dir", default=".")
    p_export.set_defaults(func=cmd_export)

    p_doc = sub.add_parser("doctor", help="启动前自检（device-id 冲突/DB/MQTT）")
    p_doc.add_argument("--whois-timeout", type=float, default=3.0)
    p_doc.set_defaults(func=cmd_doctor)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(args.func(args))
    except ConfigError as exc:
        print(f"配置错误: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
