"""Structured logging configuration with recursive secret redaction."""

from __future__ import annotations

import logging
import re
import sys
from collections.abc import Mapping
from typing import Any

import structlog

_SECRET_MARKERS = (
    "api_key",
    "token",
    "secret",
    "password",
    "private" + "_key",
    "dsn",
)
_SECRET_TEXT_PATTERNS = (
    re.compile(r"(?i)([?&](?:api[-_]?key|token|access_token)=)[^&\s\"']+"),
    re.compile(r"(?i)(/bot)[0-9]{6,}:[A-Za-z0-9_-]+"),
)


def configure_logging(*, level: str = "INFO", json_logs: bool = True) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _redact_event,
    ]
    renderer: Any = (
        structlog.processors.JSONRenderer(sort_keys=True)
        if json_logs
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(numeric_level)
    for logger_name in ("httpx", "httpcore"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    structlog.configure(
        processors=[*shared_processors, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def _redact_event(
    _logger: Any, _method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    return {key: _redact_value(key, value) for key, value in event_dict.items()}


def _redact_value(key: str, value: Any) -> Any:
    lowered = key.lower()
    if any(marker in lowered for marker in _SECRET_MARKERS):
        return "***"
    if isinstance(value, Mapping):
        return {str(child): _redact_value(str(child), item) for child, item in value.items()}
    if isinstance(value, list):
        return [_redact_value(key, item) for item in value]
    if isinstance(value, str):
        return _redact_text(value)
    return value


def _redact_text(value: str) -> str:
    redacted = value
    for pattern in _SECRET_TEXT_PATTERNS:
        redacted = pattern.sub(r"\1***", redacted)
    return redacted
