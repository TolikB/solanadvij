"""Deterministic journal for read-only external API responses."""

from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class ExternalJournal:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._records: dict[str, list[dict[str, Any]]] = {}
        self._cursors: dict[str, int] = {}
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as stream:
                for line in stream:
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    key = record.get("key")
                    if isinstance(key, str):
                        self._records.setdefault(key, []).append(record)

    @staticmethod
    def key(provider: str, operation: str, request: object) -> str:
        payload = json.dumps(
            {"provider": provider, "operation": operation, "request": request},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def get(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            cursor = self._cursors.get(key, 0)
            records = self._records.get(key, [])
            if cursor >= len(records):
                return None
            self._cursors[key] = cursor + 1
            return dict(records[cursor])

    def record(self, key: str, response: object, *, observed_at: datetime | None = None) -> None:
        observed_at = observed_at or datetime.now(tz=timezone.utc)
        record = {"key": key, "observed_at": observed_at.isoformat(), "response": response}
        encoded = json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)
        with self._lock:
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(encoded + "\n")
            self._records.setdefault(key, []).append(record)
