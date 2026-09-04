"""Canonical chain events, deduplication, and compressed raw-event storage."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections import OrderedDict
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4

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
    ingest_sequence: int | None = Field(default=None, ge=1)
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


@dataclass(frozen=True, slots=True)
class RawArchiveSegment:
    path: Path
    start_sequence: int
    end_sequence: int
    event_count: int
    checksum_sha256: str
    protocol: Protocol


class RawEventRecorder:
    """Write immutable, checksummed zstd segments with atomic publication."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self._lock = asyncio.Lock()
        self._compressor = zstandard.ZstdCompressor(level=3)

    def path_for(self, event: EventEnvelope) -> Path:
        day = event.block_time.strftime("%Y-%m-%d")
        hour = event.block_time.strftime("%H")
        if event.ingest_sequence is not None:
            sequence = f"{event.ingest_sequence:020d}"
            return (
                self.root
                / day
                / hour
                / event.protocol.value
                / f"{sequence}-{sequence}.ndjson.zst"
            )
        return self.root / day / f"{event.protocol.value}-events-{hour}.ndjson.zst"

    async def record(self, event: EventEnvelope) -> Path:
        return (await self.record_many([event]))[0]

    async def record_many(self, events: list[EventEnvelope]) -> list[Path]:
        return [segment.path for segment in await self.write_segments(events)]

    async def write_segments(
        self,
        events: list[EventEnvelope],
    ) -> list[RawArchiveSegment]:
        if not events:
            return []
        async with self._lock:
            operation = asyncio.create_task(
                asyncio.to_thread(self._write_segments_sync, tuple(events))
            )
            cancellation: asyncio.CancelledError | None = None
            while not operation.done():
                try:
                    await asyncio.shield(operation)
                except asyncio.CancelledError as error:
                    cancellation = cancellation or error
            segments = operation.result()
            if cancellation is not None:
                raise cancellation
            return segments

    def _write_segments_sync(
        self,
        events: tuple[EventEnvelope, ...],
    ) -> list[RawArchiveSegment]:
        grouped: dict[tuple[str, str, Protocol], list[EventEnvelope]] = {}
        for event in events:
            key = (
                event.block_time.strftime("%Y-%m-%d"),
                event.block_time.strftime("%H"),
                event.protocol,
            )
            grouped.setdefault(key, []).append(event)

        segments: list[RawArchiveSegment] = []
        for (day, hour, protocol), grouped_events in grouped.items():
            sequenced = all(
                event.ingest_sequence is not None for event in grouped_events
            )
            if sequenced:
                ordered = sorted(
                    grouped_events,
                    key=lambda event: int(event.ingest_sequence or 0),
                )
                start_sequence = int(ordered[0].ingest_sequence or 0)
                end_sequence = int(ordered[-1].ingest_sequence or 0)
                target = (
                    self.root
                    / day
                    / hour
                    / protocol.value
                    / f"{start_sequence:020d}-{end_sequence:020d}.ndjson.zst"
                )
            elif any(
                event.ingest_sequence is not None for event in grouped_events
            ):
                raise ValueError(
                    "raw archive batch cannot mix sequenced and legacy events"
                )
            else:
                ordered = list(grouped_events)
                start_sequence = 0
                end_sequence = 0
                identity = hashlib.sha256(
                    "".join(event.event_id for event in ordered).encode("ascii")
                ).hexdigest()[:16]
                target = (
                    self.root
                    / day
                    / f"{protocol.value}-events-{hour}-legacy-{identity}.ndjson.zst"
                )

            serialized = "".join(
                event.model_dump_json(exclude_none=True) + "\n"
                for event in ordered
            ).encode("utf-8")
            compressed = self._compressor.compress(serialized)
            checksum = hashlib.sha256(compressed).hexdigest()
            _write_atomic_segment(target, compressed, checksum)
            segments.append(
                RawArchiveSegment(
                    path=target,
                    start_sequence=start_sequence,
                    end_sequence=end_sequence,
                    event_count=len(ordered),
                    checksum_sha256=checksum,
                    protocol=protocol,
                )
            )
        return segments


class RawEventReader:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def iter_events(self) -> Iterator[EventEnvelope]:
        if not self.root.exists():
            return
        for path in sorted(self.root.rglob("*.ndjson.zst")):
            yield from _read_zstd_events(path)

    async def events(self) -> AsyncIterator[EventEnvelope]:
        for event in await asyncio.to_thread(lambda: list(self.iter_events())):
            yield event


def _write_atomic_segment(
    path: Path,
    payload: bytes,
    checksum_sha256: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing_checksum = hashlib.sha256(path.read_bytes()).hexdigest()
        if existing_checksum != checksum_sha256:
            raise RuntimeError("raw archive segment identity collision")
        return
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        if hasattr(os, "O_DIRECTORY"):
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_zstd_events(path: Path) -> Iterator[EventEnvelope]:
    decompressor = zstandard.ZstdDecompressor()
    with path.open("rb") as compressed:
        with decompressor.stream_reader(compressed, read_across_frames=True) as reader:
            text = reader.read().decode("utf-8")
    for line in text.splitlines():
        if line.strip():
            payload = json.loads(line)
            yield EventEnvelope.model_validate(payload)
