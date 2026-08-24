"""Bounded retention for raw archives."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path


class RawRetentionManager:
    def __init__(self, root: str | Path, retention_days: int) -> None:
        self.root = Path(root).resolve()
        self.retention_days = max(90, retention_days)

    def run(self, *, now: datetime | None = None) -> int:
        if not self.root.exists():
            return 0
        cutoff = (now or datetime.now(tz=timezone.utc)) - timedelta(days=self.retention_days)
        removed = 0
        for path in self.root.rglob("*.ndjson.zst"):
            resolved = path.resolve()
            if not resolved.is_relative_to(self.root):
                continue
            modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            if modified < cutoff:
                path.unlink()
                removed += 1
        return removed
