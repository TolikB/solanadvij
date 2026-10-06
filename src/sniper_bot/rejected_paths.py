"""Bounded counterfactual price paths of rejected candidates.

Live state discards the activity of a pool once its candidate is rejected, and
ingest stops recording it, so whether a rejection avoided a loss or missed a
winner is otherwise unknowable. For a fixed horizon after each rejection this
recorder keeps a compact reserve-price path of the pool, bucketed in time, and
appends one NDJSON record per pool once the horizon ends.

It reads the events ingest already drops and never feeds live state, scoring,
the ledger or the database; the file is offline calibration evidence only.
Paths still open at shutdown are not written.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .candidates import Candidate
from .events import ChainEventType, EventEnvelope
from .registry import PoolRecord, PoolState

logger = logging.getLogger(__name__)

REJECTED_PATH_LOG = "rejected_paths.ndjson"
REJECTED_PATH_LOG_BYTES = 200_000_000
# Covers a would-be entry anywhere in the entry window plus the maximum hold.
REJECTED_PATH_HORIZON = timedelta(minutes=15)
REJECTED_PATH_BUCKET_SECONDS = 10
MAX_OPEN_REJECTED_PATHS = 2048
SCHEMA_VERSION = 1

_RESERVE_EVENT_TYPES = frozenset(
    {
        ChainEventType.SWAP_BUY,
        ChainEventType.SWAP_SELL,
        ChainEventType.LIQUIDITY_ADDED,
        ChainEventType.LIQUIDITY_REMOVED,
    }
)


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


@dataclass(slots=True)
class _Bucket:
    events: int = 0
    high: Decimal = Decimal("0")
    low: Decimal = Decimal("0")
    close: Decimal = Decimal("0")
    close_at: datetime | None = None
    effective_quote: Decimal = Decimal("0")
    raw_quote: Decimal = Decimal("0")


@dataclass(slots=True)
class _Path:
    candidate_id: str
    mint: str
    pool_address: str
    quote_mint: str
    reject_reason: str
    rejected_at: datetime
    until: datetime
    pool_created_at: datetime
    reversed_orientation: bool
    base_scale: Decimal
    quote_scale: Decimal
    raw_base: Decimal
    raw_quote: Decimal
    virtual_base: Decimal
    virtual_quote: Decimal
    start_price: Decimal
    start_price_usd: Decimal
    start_quote_liquidity_usd: Decimal
    start_effective_quote: Decimal
    start_raw_quote: Decimal
    counts: dict[str, int] = field(default_factory=dict)
    buckets: dict[int, _Bucket] = field(default_factory=dict)


class RejectedPathRecorder:
    def __init__(
        self,
        path: Path,
        *,
        horizon: timedelta = REJECTED_PATH_HORIZON,
        bucket_seconds: int = REJECTED_PATH_BUCKET_SECONDS,
        max_open: int = MAX_OPEN_REJECTED_PATHS,
        max_bytes: int = REJECTED_PATH_LOG_BYTES,
    ) -> None:
        self.path = path
        self.horizon = horizon
        self.bucket_seconds = bucket_seconds
        self.max_open = max_open
        self.max_bytes = max_bytes
        self._open: dict[str, _Path] = {}
        self.dropped = 0
        self.written = 0

    def __len__(self) -> int:
        return len(self._open)

    def start(
        self,
        candidate: Candidate,
        pool: PoolRecord | None,
        state: PoolState | None,
        at: datetime,
    ) -> bool:
        """Begin the path of a just-rejected candidate from its live pool state."""
        if pool is None or state is None or candidate.pool_address in self._open:
            return False
        if len(self._open) >= self.max_open:
            self.dropped += 1
            return False
        base_scale = Decimal(10) ** pool.base_decimals
        quote_scale = Decimal(10) ** pool.quote_decimals
        effective_quote = max(state.effective_quote_reserves, Decimal("0")) / quote_scale
        effective_base = max(state.effective_base_reserves, Decimal("0")) / base_scale
        reason = candidate.reject_reason.value if candidate.reject_reason else "UNKNOWN"
        self._open[candidate.pool_address] = _Path(
            candidate_id=candidate.candidate_id,
            mint=candidate.mint,
            pool_address=candidate.pool_address,
            quote_mint=pool.quote_mint,
            reject_reason=reason,
            rejected_at=at,
            until=at + self.horizon,
            pool_created_at=pool.creation_time,
            reversed_orientation=pool.source_orientation_reversed,
            base_scale=base_scale,
            quote_scale=quote_scale,
            raw_base=state.raw_base_reserves,
            raw_quote=state.raw_quote_reserves,
            virtual_base=state.virtual_base_reserves,
            virtual_quote=state.virtual_quote_reserves,
            start_price=(effective_quote / effective_base if effective_base > 0 else Decimal("0")),
            start_price_usd=state.marginal_price_usd,
            start_quote_liquidity_usd=state.quote_reserve_usd,
            start_effective_quote=effective_quote,
            start_raw_quote=max(state.raw_quote_reserves, Decimal("0")) / quote_scale,
        )
        return True

    def observe(self, event: EventEnvelope) -> None:
        """Fold one dropped pool event into its open path, if it has one."""
        path = self._open.get(event.pool_address or "")
        if (
            path is None
            or event.event_type not in _RESERVE_EVENT_TYPES
            or not path.rejected_at <= event.block_time <= path.until
        ):
            return
        payload = event.payload
        base, quote = (
            ("pool_quote_token_reserves", "pool_base_token_reserves")
            if path.reversed_orientation
            else ("pool_base_token_reserves", "pool_quote_token_reserves")
        )
        raw_base = _decimal(payload.get(base))
        raw_quote = _decimal(payload.get(quote))
        if raw_base is not None:
            path.raw_base = raw_base
        if raw_quote is not None:
            path.raw_quote = raw_quote
        virtual = _decimal(payload.get("virtual_quote_reserves"))
        if virtual is not None:
            if path.reversed_orientation:
                path.virtual_base, path.virtual_quote = virtual, Decimal("0")
            else:
                path.virtual_base, path.virtual_quote = Decimal("0"), virtual
        kind = event.event_type.value
        path.counts[kind] = path.counts.get(kind, 0) + 1
        effective_base = max(path.raw_base + path.virtual_base, Decimal("0")) / path.base_scale
        effective_quote = max(path.raw_quote + path.virtual_quote, Decimal("0")) / path.quote_scale
        if effective_base <= 0 or path.start_price <= 0:
            return
        relative = effective_quote / effective_base / path.start_price
        index = int((event.block_time - path.rejected_at).total_seconds()) // self.bucket_seconds
        bucket = path.buckets.get(index)
        if bucket is None:
            bucket = path.buckets[index] = _Bucket(high=relative, low=relative)
        bucket.events += 1
        bucket.high = max(bucket.high, relative)
        bucket.low = min(bucket.low, relative)
        if bucket.close_at is None or event.block_time >= bucket.close_at:
            bucket.close = relative
            bucket.close_at = event.block_time
            bucket.effective_quote = effective_quote
            bucket.raw_quote = max(path.raw_quote, Decimal("0")) / path.quote_scale

    def finalize_due(self, now: datetime) -> int:
        """Write and release every path whose horizon has ended."""
        due = [address for address, path in self._open.items() if path.until <= now]
        if not due:
            return 0
        records = [self._record(self._open.pop(address)) for address in due]
        try:
            if self.path.exists() and self.path.stat().st_size > self.max_bytes:
                self.path.replace(self.path.with_suffix(self.path.suffix + ".1"))
            with self.path.open("a", encoding="utf-8") as stream:
                for record in records:
                    stream.write(json.dumps(record, sort_keys=True) + "\n")
        except OSError:
            logger.warning("rejected path log unavailable", exc_info=True)
            return 0
        self.written += len(records)
        return len(records)

    def _record(self, path: _Path) -> dict[str, Any]:
        def number(value: Decimal) -> float:
            return float(round(value, 6))

        return {
            "schema": SCHEMA_VERSION,
            "candidate_id": path.candidate_id,
            "mint": path.mint,
            "pool_address": path.pool_address,
            "quote_mint": path.quote_mint,
            "reject_reason": path.reject_reason,
            "rejected_at": path.rejected_at.isoformat(),
            "pool_created_at": path.pool_created_at.isoformat(),
            "horizon_seconds": int(self.horizon.total_seconds()),
            "bucket_seconds": self.bucket_seconds,
            "start": {
                "price_quote": str(path.start_price),
                "price_usd": str(path.start_price_usd),
                "quote_liquidity_usd": str(path.start_quote_liquidity_usd),
                "effective_quote": str(path.start_effective_quote),
                "raw_quote": str(path.start_raw_quote),
            },
            "counts": dict(sorted(path.counts.items())),
            # [bucket, events, high, low, close] as price relative to the
            # rejection, then effective and raw quote reserves at the close.
            "path": [
                [
                    index,
                    bucket.events,
                    number(bucket.high),
                    number(bucket.low),
                    number(bucket.close),
                    number(bucket.effective_quote),
                    number(bucket.raw_quote),
                ]
                for index, bucket in sorted(path.buckets.items())
            ],
        }
