"""Replay helpers for deterministic, offline execution checks."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from .candidates import CandidateState
from .config import AppMode
from .database import Database
from .events import RawEventReader
from .runtime import SniperRuntime

if TYPE_CHECKING:
    from .service import PaperService


@dataclass(frozen=True)
class ReplayAction:
    """Single replay action."""

    kind: str
    token_mint: str
    amount: str | None = None
    order_id: str | None = None
    exit_reason: str | None = None
    delay_seconds: int = 0

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class ReplayRunRecord:
    """Stored replay run metadata."""

    run_id: str
    strategy_version: str
    config_hash: str
    input_hash: str
    output_hash: str
    action_count: int
    input_digest: str
    started_at: str
    finished_at: str
    replay_seed: int | None = None
    strategy_label: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "ReplayRunRecord":
        return cls(
            run_id=payload["run_id"],  # type: ignore[arg-type]
            strategy_version=payload["strategy_version"],  # type: ignore[arg-type]
            config_hash=payload["config_hash"],  # type: ignore[arg-type]
            input_hash=payload["input_hash"],  # type: ignore[arg-type]
            output_hash=payload["output_hash"],  # type: ignore[arg-type]
            action_count=payload["action_count"],  # type: ignore[arg-type]
            input_digest=payload["input_digest"],  # type: ignore[arg-type]
            started_at=payload["started_at"],  # type: ignore[arg-type]
            finished_at=payload["finished_at"],  # type: ignore[arg-type]
            replay_seed=payload.get("replay_seed"),  # type: ignore[arg-type]
            strategy_label=payload.get("strategy_label"),  # type: ignore[arg-type]
        )


@dataclass
class ReplayRunResult:
    """Result returned after replay actions are executed."""

    run_id: str
    input_hash: str
    output_hash: str
    started_at: datetime
    finished_at: datetime
    actions_executed: int
    final_reconcile: dict[str, object]


class VirtualClock:
    """A minimal deterministic clock for replay runs."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2000, 1, 1, tzinfo=timezone.utc)

    @property
    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)

    async def sleep(self, seconds: float) -> None:
        self.advance(seconds)


class ReplaySpeed(StrEnum):
    ONE_X = "1x"
    REALTIME = "realtime"
    FIVE_X = "5x"
    TEN_X = "10x"
    MAXIMUM = "maximum"
    MAX = "max"

    @property
    def wall_time_divisor(self) -> float | None:
        if self in {ReplaySpeed.ONE_X, ReplaySpeed.REALTIME}:
            return 1.0
        if self == ReplaySpeed.FIVE_X:
            return 5.0
        if self == ReplaySpeed.TEN_X:
            return 10.0
        return None


@dataclass
class RawReplayResult:
    run_id: str
    input_hash: str
    output_hash: str
    events_executed: int
    started_at: datetime
    finished_at: datetime
    candidate_states: dict[str, str]
    final_reconcile: dict[str, object]


class ReplayRunStore:
    """Store replay run metadata in a local JSON file."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._records: dict[str, ReplayRunRecord] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as file:
            raw = json.load(file)
        for payload in raw if isinstance(raw, list) else []:
            try:
                record = ReplayRunRecord.from_dict(payload)
            except Exception:
                continue
            self._records[record.run_id] = record

    def _persist(self) -> None:
        with self.path.open("w", encoding="utf-8") as file:
            json.dump([record.to_dict() for record in self._records.values()], file, ensure_ascii=False, indent=2)

    def upsert(self, record: ReplayRunRecord) -> None:
        self._records[record.run_id] = record
        self._persist()

    def list_runs(self) -> list[ReplayRunRecord]:
        return list(self._records.values())

    def get_run_by_signature(self, *, strategy_version: str, input_hash: str) -> ReplayRunRecord | None:
        for record in self._records.values():
            if record.strategy_version == strategy_version and record.input_hash == input_hash:
                return record
        return None


class ReplayRunner:
    """Executes deterministic actions and emits replay run metrics."""

    def __init__(
        self,
        runtime: SniperRuntime,
        *,
        clock: VirtualClock | None = None,
        store: ReplayRunStore | None = None,
        strategy_label: str | None = None,
    ) -> None:
        self.runtime = runtime
        self._clock = clock or VirtualClock()
        self._store = store
        self._strategy_label = strategy_label
        self.runtime.quote_provider.set_clock(lambda: self._clock.now)
        self.runtime.rpc.set_clock(lambda: self._clock.now)
        if self.runtime.broker is not None:
            self.runtime.broker.set_clock(lambda: self._clock.now, self._clock.sleep)

    async def run(self, actions: list[ReplayAction]) -> ReplayRunResult:
        from .service import PaperService

        service = PaperService(self.runtime)
        started_at = self._clock.now
        for action in actions:
            if action.delay_seconds > 0:
                await self._clock.sleep(float(action.delay_seconds))
            amount = _to_decimal(action.amount) if action.amount is not None else None
            if action.kind == "open":
                await self._open(service, action, amount)
            elif action.kind == "close":
                await self._close(service, action, amount)
            elif action.kind == "close_half":
                await self._close_half(service, action)
            else:
                raise ValueError(f"unknown replay action {action.kind!r}")

        finished_at = self._clock.now
        output = self._output_state_hash(self.runtime, at=self._clock.now)
        input_payload = {
            "strategy_version": self.runtime.config.strategy_version,
            "actions": [action.to_payload() for action in actions],
        }
        input_hash = _payload_hash(input_payload)
        run_id = _run_id(
            strategy_version=self.runtime.config.strategy_version,
            input_hash=input_hash,
            output_hash=output,
            strategy_label=self._strategy_label,
        )
        final_reconcile = {**self.runtime.ledger.reconcile()}

        result = ReplayRunResult(
            run_id=run_id,
            input_hash=input_hash,
            output_hash=output,
            started_at=started_at,
            finished_at=finished_at,
            actions_executed=len(actions),
            final_reconcile=final_reconcile,
        )
        if self._store is not None:
            self._store.upsert(
                ReplayRunRecord(
                    run_id=run_id,
                    strategy_version=self.runtime.config.strategy_version,
                    config_hash=self.runtime.config.config_hash,
                    input_hash=input_hash,
                    output_hash=output,
                    action_count=len(actions),
                    input_digest=input_payload_to_digest(input_payload),
                    started_at=started_at.isoformat(),
                    finished_at=finished_at.isoformat(),
                    replay_seed=self.runtime.config.replay_seed,
                    strategy_label=self._strategy_label,
                )
            )
        return result

    async def _open(self, service: "PaperService", action: ReplayAction, amount: Decimal | None) -> None:
        if amount is None:
            raise ValueError("open action requires amount")
        await service.open_position(action.token_mint, amount, order_id=action.order_id)

    async def _close(self, service: "PaperService", action: ReplayAction, amount: Decimal | None) -> None:
        if amount is None:
            raise ValueError("close action requires amount")
        await service.close_position(action.token_mint, amount, order_id=action.order_id, exit_reason=action.exit_reason)

    async def _close_half(self, service: "PaperService", action: ReplayAction) -> None:
        await service.close_half(action.token_mint, order_id=action.order_id)

    @staticmethod
    def _output_state_hash(
        runtime: SniperRuntime, *, at: datetime | None = None
    ) -> str:
        fills = _sorted_records(
            (_normalize(record.model_dump(mode="json")) for record in runtime.ledger.iter_fills()),
            sort_key=("fill_id", "order_id", "quote_id", "token_mint"),
        )
        positions = _sorted_records(
            (
                _normalize(
                    record.model_dump(mode="json", exclude={"strategy_version"})
                )
                for record in runtime.ledger.state.positions.values()
            ),
            sort_key=("position_id", "token_mint"),
        )
        snapshot = runtime.ledger.snapshot()
        reconcile = runtime.ledger.reconcile()
        report_time = at or datetime(2000, 1, 1, tzinfo=timezone.utc)
        candidate_states = {
            candidate.candidate_id: candidate.state.value
            for candidate in sorted(
                runtime.pipeline.list_candidates(),
                key=lambda item: item.candidate_id,
            )
        }
        payload = {
            "equity_usd": snapshot["equity_usd"],
            "realized_pnl_usd": snapshot["realized_pnl_usd"],
            "unrealized_pnl_usd": snapshot["unrealized_pnl_usd"],
            "reconcile": _normalize(reconcile),
            "fills": fills,
            "positions": positions,
            "candidate_states": candidate_states,
            "daily_report": runtime.report_builder.daily(
                report_time.astimezone(runtime.ledger._time_zone).date().isoformat(),
                as_of=report_time,
            ),
            "all_time_report": runtime.report_builder.all_time(as_of=report_time),
            "outbox_events": [],
        }
        normalized = _normalize_output(payload)
        return hashlib.sha256(json.dumps(normalized, sort_keys=True).encode("utf-8")).hexdigest()


class RawEventReplayRunner:
    """Runs the real normalization/feature/decision pipeline without network calls."""

    def __init__(
        self,
        runtime: SniperRuntime,
        *,
        clock: VirtualClock | None = None,
        store: ReplayRunStore | None = None,
        database: Database | None = None,
        sleeper: Any = asyncio.sleep,
    ) -> None:
        if not runtime.config.replay_mode:
            raise ValueError("raw replay requires replay_mode=true")
        self.runtime = runtime
        self.clock = clock
        self.store = store
        self.database = database
        self.sleeper = sleeper

    async def run(
        self,
        source: str | Path | RawEventReader,
        *,
        speed: ReplaySpeed = ReplaySpeed.MAX,
    ) -> RawReplayResult:
        reader = source if isinstance(source, RawEventReader) else RawEventReader(source)
        events = sorted(
            list(reader.iter_events()),
            key=lambda item: (
                item.block_time, item.slot, item.signature,
                item.instruction_index, item.inner_instruction_index,
            ),
        )
        if not events:
            raise ValueError("raw replay input contains no events")
        clock = self.clock or VirtualClock(events[0].block_time)
        self.runtime.quote_provider.set_clock(lambda: clock.now)
        self.runtime.rpc.set_clock(lambda: clock.now)
        if self.runtime.broker is not None:
            self.runtime.broker.set_clock(lambda: clock.now, clock.sleep)
        for reason in (
            "startup",
            "warmup",
            "stream_disconnected",
            "stream_stale",
            "database_unavailable",
        ):
            self.runtime.entry_gate.unblock(reason)
        started_at = clock.now
        previous = events[0].block_time
        for event in events:
            delay = max(0.0, (event.block_time - previous).total_seconds())
            divisor = speed.wall_time_divisor
            if divisor is not None:
                await self.sleeper(delay / divisor)
            clock.advance(delay)
            replay_event = event.model_copy(update={"observed_at": event.block_time})
            self.runtime.stream_gateway.last_observed_at = event.block_time
            self.runtime.stream_gateway.last_slot = event.slot
            await self.runtime.pipeline.process_event(replay_event)
            await self._tick(clock.now)
            previous = event.block_time
        if self.runtime.config.app_mode == AppMode.PAPER:
            horizon = max(
                self.runtime.config.candidate.max_pool_age_seconds,
                self.runtime.config.exits.maximum_holding_seconds,
            )
            deadline = clock.now + timedelta(seconds=horizon)
            while clock.now < deadline and self._has_active_lifecycle():
                clock.advance(1)
                await self._tick(clock.now)
        finished_at = clock.now
        input_payload = [event.model_dump(mode="json") for event in events]
        input_hash = _payload_hash(input_payload)
        candidate_states = {
            item.candidate_id: item.state.value
            for item in sorted(
                self.runtime.pipeline.list_candidates(), key=lambda candidate: candidate.candidate_id
            )
        }
        base_output_hash = ReplayRunner._output_state_hash(
            self.runtime, at=clock.now
        )
        output_hash = _payload_hash(
            {"ledger": base_output_hash, "candidate_states": candidate_states}
        )
        run_id = _run_id(
            self.runtime.config.strategy_version,
            input_hash,
            output_hash,
            speed.value,
        )
        reconcile = cast(dict[str, object], _normalize(self.runtime.ledger.reconcile()))
        result = RawReplayResult(
            run_id=run_id, input_hash=input_hash, output_hash=output_hash,
            events_executed=len(events), started_at=started_at, finished_at=finished_at,
            candidate_states=candidate_states, final_reconcile=reconcile,
        )
        if self.store is not None:
            self.store.upsert(
                ReplayRunRecord(
                    run_id=run_id,
                    strategy_version=self.runtime.config.strategy_version,
                    config_hash=self.runtime.config.config_hash,
                    input_hash=input_hash,
                    output_hash=output_hash,
                    action_count=len(events),
                    input_digest=_payload_hash(input_payload),
                    started_at=started_at.isoformat(),
                    finished_at=finished_at.isoformat(),
                    replay_seed=self.runtime.config.replay_seed,
                    strategy_label=f"raw:{speed.value}",
                )
            )
        if self.database is not None:
            await self.database.record_replay_run(
                run_id=run_id,
                strategy_version_id=self.runtime.config.strategy_version,
                config_hash=self.runtime.config.config_hash,
                random_seed=self.runtime.config.replay_seed,
                input_hash=input_hash,
                output_hash=output_hash,
                speed=speed.value,
                started_at=started_at,
                finished_at=finished_at,
                result_json={
                    "events_executed": len(events),
                    "candidate_states": candidate_states,
                    "reconcile": reconcile,
                },
            )
        return result

    async def _tick(self, at: datetime) -> None:
        await self.runtime.pipeline.evaluate_candidates(at=at)
        if self.runtime.broker is not None:
            await self.runtime.evaluate_and_close_exits(now=at)

    def _has_active_lifecycle(self) -> bool:
        active_candidate_states = {
            CandidateState.DISCOVERED,
            CandidateState.COLLECTING,
            CandidateState.SECURITY_CHECK,
            CandidateState.ELIGIBLE,
            CandidateState.WAITING_PULLBACK,
            CandidateState.ARMED,
            CandidateState.ENTRY_PENDING,
            CandidateState.POSITION_OPEN,
            CandidateState.POSITION_PARTIAL,
            CandidateState.EXIT_PENDING,
            CandidateState.RETRYING_EXIT,
        }
        return bool(self.runtime.ledger.open_positions) or any(
            candidate.state in active_candidate_states
            for candidate in self.runtime.pipeline.list_candidates()
        )


def input_payload_to_digest(actions: object) -> str:
    return _payload_hash(actions)


def _to_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(value)


def _payload_hash(payload: object) -> str:
    normalized = _normalize(payload)
    return hashlib.sha256(json.dumps(normalized, sort_keys=True).encode("utf-8")).hexdigest()


def _run_id(strategy_version: str, input_hash: str, output_hash: str, strategy_label: str | None = None) -> str:
    base = "".join([
        strategy_version,
        input_hash[:12],
        output_hash[:12],
    ])
    if strategy_label:
        base = f"{strategy_label}:{base}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:24]


def _normalize(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        filtered = {
            key: _normalize(item)
            for key, item in value.items()
            if key not in {"opened_at", "created_at"}
        }
        return {key: filtered[key] for key in sorted(filtered)}
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize(item) for item in value]
    return value


def _normalize_output(value: object) -> object:
    excluded = {
        "opened_at",
        "created_at",
        "closed_at",
        "first_run_at",
        "period_start_utc",
        "period_end_utc",
        "report_id",
        "date",
        "timezone",
        "strategy_version",
        "config_hash",
    }
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {
            key: _normalize_output(item)
            for key, item in sorted(value.items())
            if key not in excluded
        }
    if isinstance(value, (list, tuple)):
        return [_normalize_output(item) for item in value]
    return value


def _sorted_records(
    records: Iterable[object], sort_key: tuple[str, ...]
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            raise TypeError("normalized replay record must be an object")
        normalized.append(record)
    return sorted(normalized, key=lambda record: tuple(str(record.get(key, "")) for key in sort_key))
