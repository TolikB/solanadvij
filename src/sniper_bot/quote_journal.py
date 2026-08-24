from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any


class QuoteJournal:
    """Simple file-backed store for Jupiter quote responses."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._entries: dict[str, list[dict[str, Any]]] = {}
        self._cursors: dict[str, int] = {}
        self._lock = threading.Lock()
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as file:
            for line in file:
                text = line.strip()
                if not text:
                    continue
                payload = json.loads(text)
                key = payload.get("request_key")
                if not key or not isinstance(payload.get("response"), dict):
                    continue
                self._entries.setdefault(key, []).append(payload)

    def has(self, request_key: str) -> bool:
        return self._cursors.get(request_key, 0) < len(
            self._entries.get(request_key, [])
        )

    def get(self, request_key: str) -> dict[str, Any] | None:
        record = self.get_record(request_key)
        response = record.get("response") if record else None
        return dict(response) if isinstance(response, dict) else None

    def get_record(self, request_key: str) -> dict[str, Any] | None:
        with self._lock:
            cursor = self._cursors.get(request_key, 0)
            records = self._entries.get(request_key, [])
            if cursor >= len(records):
                return None
            self._cursors[request_key] = cursor + 1
            return dict(records[cursor])

    def record(
        self,
        request_key: str,
        response: dict[str, Any],
        *,
        requested_at: datetime | None = None,
        received_at: datetime | None = None,
        latency_ms: int | None = None,
    ) -> None:
        record: dict[str, Any] = {
            "request_key": request_key,
            "response": dict(response),
        }
        if requested_at is not None:
            record["requested_at"] = requested_at.isoformat()
        if received_at is not None:
            record["received_at"] = received_at.isoformat()
        if latency_ms is not None:
            record["latency_ms"] = latency_ms
        with self._lock:
            self._entries.setdefault(request_key, []).append(record)
            with self.path.open("a", encoding="utf-8") as file:
                json.dump(record, file, ensure_ascii=False)
                file.write("\n")

    def size(self) -> int:
        return sum(len(records) for records in self._entries.values())
