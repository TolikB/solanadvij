"""Canonical chain events, deduplication, and compressed raw-event storage."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections import OrderedDict
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any

import zstandard
from pydantic import BaseModel, Field, field_validator, model_validator


class EventSource(StrEnum):
    HELIUS_WSS = "helius_wss"
    SOLANA_WSS = "solana_wss"
    BASELINE_WSS = "baseline_wss"
    RPC_RECOVERY = "rpc_recovery"
    REPLAY = "replay"


class Protocol(StrEnum):
    PUMP = "pump"
    PUMPSWAP = "pumpswap"


class ChainEventType(StrEnum):
    TOKEN_CREATED = "token_created"
    SWAP_BUY = "swap_buy"
    SWAP_SELL = "swap_sell"
    BONDING_CURVE_COMPLETED = "bonding_curve_completed"
    MIGRATION = "migration"
    POOL_CREATED = "pool_created"
    LIQUIDITY_ADDED = "liquidity_added"
    LIQUIDITY_REMOVED = "liquidity_removed"
    POOL_STATE_CHANGED = "pool_state_changed"
    UNKNOWN = "unknown"


class EventEnvelope(BaseModel):
    event_id: str = ""
    source: EventSource
    network: str = "solana-mainnet"
    protocol: Protocol
    event_type: ChainEventType
    slot: int = Field(ge=0)
    signature: str = Field(min_length=1)
    instruction_index: int = Field(ge=0)
    inner_instruction_index: int = Field(default=-1, ge=-1)
    block_time: datetime
    observed_at: datetime
    commitment: str = "confirmed"
    mint: str | None = None
    pool_address: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("block_time", "observed_at")
    @classmethod
    def require_aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("event timestamps must be timezone-aware")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def populate_and_validate_event_id(self) -> "EventEnvelope":
        expected = make_event_id(
            self.signature,
            self.instruction_index,
            self.inner_instruction_index,
            self.event_type,
        )
        if self.event_id and self.event_id != expected:
            raise ValueError("event_id does not match canonical deduplication key")
        self.event_id = expected
        return self


def make_event_id(
    signature: str,
    instruction_index: int,
    inner_instruction_index: int,
    event_type: ChainEventType | str,
) -> str:
    event_name = event_type.value if isinstance(event_type, ChainEventType) else str(event_type)
    canonical = f"{signature}:{instruction_index}:{inner_instruction_index}:{event_name}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class EventDeduplicator:
    """Bounded process-local dedupe; durable uniqueness is enforced by persistence."""

    def __init__(self, capacity: int = 100_000) -> None:
        if capacity <= 0:
            raise ValueError("dedupe capacity must be positive")
        self._capacity = capacity
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._lock = asyncio.Lock()
        self.duplicates_total = 0

    async def accept(self, event_id: str) -> bool:
        async with self._lock:
            if event_id in self._seen:
                self._seen.move_to_end(event_id)
                self.duplicates_total += 1
                return False
            self._seen[event_id] = None
            if len(self._seen) > self._capacity:
                self._seen.popitem(last=False)
            return True

    async def forget(self, event_id: str) -> None:
        """Release a failed event so the same process can retry it."""
        async with self._lock:
            self._seen.pop(event_id, None)


class RawEventRecorder:
    """Append each event as an independent zstd frame in date/hour partitions."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self._lock = asyncio.Lock()
        self._compressor = zstandard.ZstdCompressor(level=3)

    def path_for(self, event: EventEnvelope) -> Path:
        day = event.block_time.strftime("%Y-%m-%d")
        hour = event.block_time.strftime("%H")
        return self.root / day / f"{event.protocol.value}-events-{hour}.ndjson.zst"

    async def record(self, event: EventEnvelope) -> Path:
        return (await self.record_many([event]))[0]

    async def record_many(
        self,
        events: list[EventEnvelope],
    ) -> list[Path]:
        if not events:
            return []
        async with self._lock:
            operation = asyncio.create_task(
                asyncio.to_thread(
                    self._record_many_sync,
                    tuple(events),
                )
            )
            cancellation: asyncio.CancelledError | None = None
            while not operation.done():
                try:
                    await asyncio.shield(operation)
                except asyncio.CancelledError as error:
                    cancellation = cancellation or error
            paths = operation.result()
            if cancellation is not None:
                raise cancellation
            return paths

    def _record_many_sync(
        self,
        events: tuple[EventEnvelope, ...],
    ) -> list[Path]:
        paths: list[Path] = []
        payloads: dict[Path, bytearray] = {}
        for event in events:
            target = self.path_for(event)
            paths.append(target)
            serialized = event.model_dump_json(exclude_none=True) + "\n"
            compressed = self._compressor.compress(serialized.encode("utf-8"))
            payloads.setdefault(target, bytearray()).extend(compressed)
        for target, archive_payload in payloads.items():
            target.parent.mkdir(parents=True, exist_ok=True)
            _append_bytes(target, bytes(archive_payload))
        return paths

class RawEventReader:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def iter_events(self) -> Iterator[EventEnvelope]:
        if not self.root.exists():
            return
        for path in sorted(self.root.glob("*/*-events-*.ndjson.zst")):
            yield from _read_zstd_events(path)

    async def events(self) -> AsyncIterator[EventEnvelope]:
        for event in await asyncio.to_thread(lambda: list(self.iter_events())):
            yield event


def _append_bytes(path: Path, payload: bytes) -> None:
    with path.open("ab") as stream:
        stream.write(payload)
        stream.flush()


def _read_zstd_events(path: Path) -> Iterator[EventEnvelope]:
    decompressor = zstandard.ZstdDecompressor()
    with path.open("rb") as compressed:
        with decompressor.stream_reader(compressed, read_across_frames=True) as reader:
            text = reader.read().decode("utf-8")
    for line in text.splitlines():
        if line.strip():
            payload = json.loads(line)
            yield EventEnvelope.model_validate(payload)
