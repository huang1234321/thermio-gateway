"""结构化 JSON 日志（gateway.md §10；CODE-LOG-01 / SEC-KEY-02）。

- 统一 JSON 行到 stderr；LOG_FILE 设定时叠加轮转文件（大小 + 份数上限）；
- **凭据与 payload 全文不落日志**：本模块只提供结构，调用方负责不传
  密码/secret/原始 payload；trace 标（raw_name/device_id/cmd_id/job_id）
  由调用方以 extra 传入。

字段序固定：ts / level / event / msg（+extra 平铺）。
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

_RESERVED = set(
    logging.LogRecord("x", 0, "x", 0, "x", None, None).__dict__  # type: ignore[arg-type]
) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "event": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                entry[key] = value
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False, default=str)


def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    for handler in list(root.handlers):
        root.removeHandler(handler)
    fmt = JsonFormatter()
    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(fmt)
    root.addHandler(stream)
    if log_file:
        from logging.handlers import RotatingFileHandler

        rotating = RotatingFileHandler(
            log_file, maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        rotating.setFormatter(fmt)
        root.addHandler(rotating)
