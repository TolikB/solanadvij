from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from sniper_bot.candidates import Candidate, CandidateState
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.features import FeatureSnapshot
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.registry import WSOL_MINT, PoolRecord, PoolState, QuoteAssetPrice
from sniper_bot.rejected_paths import REJECTED_PATH_LOG, RejectedPathRecorder
from sniper_bot.security import RejectReason, SecurityContext
from sniper_bot.stream import EntryGate

NOW = datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc)
HORIZON = timedelta(minutes=15)


def _candidate(pool: str = "POOL", reason: RejectReason = RejectReason.LOW_QUOTE_LIQUIDITY) -> Candidate:
    return Candidate(
        candidate_id=f"candidate-{pool}", mint=f"MINT-{pool}", pool_address=pool,
        detected_at=NOW - timedelta(seconds=45), updated_at=NOW, strategy_version="strategy-v1",
        config_hash="config-hash", state=CandidateState.REJECTED, reject_reason=reason, rejected_at=NOW,
    )


def _pool(pool: str = "POOL", *, reversed_orientation: bool = False) -> PoolRecord:
    return PoolRecord(
        pool_address=pool, base_mint=f"MINT-{pool}", quote_mint=WSOL_MINT, creation_signature="create",
        creation_slot=1, creation_time=NOW - timedelta(seconds=45), base_decimals=6, quote_decimals=9,
        source_orientation_reversed=reversed_orientation, updated_at=NOW,
    )


def _state(pool: str = "POOL", *, base: int = 1_000_000_000_000, quote: int = 100_000_000_000,
           virtual_quote: int = 0, reversed_orientation: bool = False) -> PoolState:
    return PoolState(
        pool_address=pool, base_mint=f"MINT-{pool}", quote_mint=WSOL_MINT,
        raw_base_reserves=Decimal(base), raw_quote_reserves=Decimal(quote),
        virtual_quote_reserves=Decimal(virtual_quote), effective_base_reserves=Decimal(base),
        effective_quote_reserves=Decimal(quote + virtual_quote), quote_reserve_usd=Decimal("12000"),
        marginal_price_usd=Decimal("0.012"), last_update_time=NOW, quote_price_updated_at=NOW,
        source_orientation_reversed=reversed_orientation,
    )


def _event(pool: str, seconds: float, *, base: int | None, quote: int | None,
           event_type: ChainEventType = ChainEventType.SWAP_BUY, **extra: Any) -> EventEnvelope:
    at = NOW + timedelta(seconds=seconds)
    payload: dict[str, Any] = dict(extra)
    if base is not None:
        payload["pool_base_token_reserves"] = base
    if quote is not None:
        payload["pool_quote_token_reserves"] = quote
    return EventEnvelope(
        source=EventSource.HELIUS_WSS, protocol=Protocol.PUMPSWAP, event_type=event_type, slot=2,
        signature=f"sig-{pool}-{seconds}-{event_type.value}", instruction_index=1, block_time=at,
        observed_at=at, pool_address=pool, payload=payload,
    )


def _records(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_path_buckets_relative_prices_and_writes_once_after_horizon(tmp_path: Path) -> None:
    recorder = RejectedPathRecorder(tmp_path / REJECTED_PATH_LOG)
    assert recorder.start(_candidate(), _pool(), _state(), NOW)
    # Price doubles in bucket 0, then halves to the start in bucket 3.
    recorder.observe(_event("POOL", -1, base=1, quote=10**15))  # before the rejection
    recorder.observe(_event("POOL", 2, base=1_000_000_000_000, quote=150_000_000_000))
    recorder.observe(_event("POOL", 5, base=1_000_000_000_000, quote=200_000_000_000,
                            event_type=ChainEventType.SWAP_SELL))
    recorder.observe(_event("POOL", 35, base=1_000_000_000_000, quote=100_000_000_000))
    recorder.observe(_event("OTHER", 3, base=1, quote=1))
    recorder.observe(_event("POOL", 4, base=1, quote=10**15, event_type=ChainEventType.TOKEN_CREATED))
    recorder.observe(_event("POOL", HORIZON.total_seconds() + 1, base=1, quote=10**15))
    assert recorder.finalize_due(NOW + HORIZON - timedelta(seconds=1)) == 0
    assert not (tmp_path / REJECTED_PATH_LOG).exists()
    assert recorder.finalize_due(NOW + HORIZON) == 1
    assert recorder.finalize_due(NOW + HORIZON + timedelta(minutes=1)) == 0
    [record] = _records(tmp_path / REJECTED_PATH_LOG)
    assert record["reject_reason"] == "LOW_QUOTE_LIQUIDITY"
    assert record["candidate_id"] == "candidate-POOL" and record["schema"] == 1
    assert record["horizon_seconds"] == 900 and record["bucket_seconds"] == 10
    assert record["counts"] == {"swap_buy": 2, "swap_sell": 1}
    assert record["start"]["quote_liquidity_usd"] == "12000"
    assert Decimal(record["start"]["price_quote"]) == Decimal("0.0001")
    assert record["path"] == [[0, 2, 2.0, 1.5, 2.0, 200.0, 200.0], [3, 1, 1.0, 1.0, 1.0, 100.0, 100.0]]
    assert len(recorder) == 0 and recorder.written == 1


def test_reversed_pools_and_virtual_quote_follow_the_registry_orientation(tmp_path: Path) -> None:
    recorder = RejectedPathRecorder(tmp_path / REJECTED_PATH_LOG)
    recorder.start(_candidate("REV"), _pool("REV", reversed_orientation=True),
                   _state("REV", reversed_orientation=True), NOW)
    recorder.start(_candidate("VIRT"), _pool("VIRT"), _state("VIRT", virtual_quote=100_000_000_000), NOW)
    # Reversed source: the payload's quote side holds the token.
    recorder.observe(_event("REV", 1, base=300_000_000_000, quote=1_000_000_000_000))
    # Virtual quote counts in the effective price but not in the raw quote.
    recorder.observe(_event("VIRT", 1, base=1_000_000_000_000, quote=100_000_000_000,
                            virtual_quote_reserves=300_000_000_000))
    recorder.observe(_event("VIRT", 11, base=None, quote=None))
    recorder.finalize_due(NOW + HORIZON)
    records = {record["pool_address"]: record for record in _records(tmp_path / REJECTED_PATH_LOG)}
    assert records["REV"]["path"] == [[0, 1, 3.0, 3.0, 3.0, 300.0, 300.0]]
    assert Decimal(records["VIRT"]["start"]["raw_quote"]) == Decimal("100")
    assert records["VIRT"]["path"] == [
        [0, 1, 2.0, 2.0, 2.0, 400.0, 100.0],
        [1, 1, 2.0, 2.0, 2.0, 400.0, 100.0],
    ]


def test_out_of_order_events_keep_the_latest_close_and_full_range(tmp_path: Path) -> None:
    recorder = RejectedPathRecorder(tmp_path / REJECTED_PATH_LOG)
    recorder.start(_candidate(), _pool(), _state(), NOW)
    recorder.observe(_event("POOL", 8, base=1_000_000_000_000, quote=120_000_000_000))
    recorder.observe(_event("POOL", 3, base=1_000_000_000_000, quote=50_000_000_000))
    recorder.finalize_due(NOW + HORIZON)
    [record] = _records(tmp_path / REJECTED_PATH_LOG)
    assert record["path"] == [[0, 2, 1.2, 0.5, 1.2, 120.0, 120.0]]


def test_start_requires_live_pool_state_is_idempotent_and_bounded(tmp_path: Path) -> None:
    recorder = RejectedPathRecorder(tmp_path / REJECTED_PATH_LOG, max_open=2)
    assert not recorder.start(_candidate("A"), None, _state("A"), NOW)
    assert not recorder.start(_candidate("A"), _pool("A"), None, NOW)
    assert recorder.start(_candidate("A"), _pool("A"), _state("A"), NOW)
    assert not recorder.start(_candidate("A"), _pool("A"), _state("A"), NOW + timedelta(seconds=5))
    assert recorder.start(_candidate("B"), _pool("B"), _state("B"), NOW)
    assert not recorder.start(_candidate("C"), _pool("C"), _state("C"), NOW)
    assert len(recorder) == 2 and recorder.dropped == 1
    # A pool without a usable start price is recorded with counts only.
    assert recorder.finalize_due(NOW + HORIZON) == 2
    empty = RejectedPathRecorder(tmp_path / "empty.ndjson")
    empty.start(_candidate("Z"), _pool("Z"), _state("Z", base=0), NOW)
    empty.observe(_event("Z", 1, base=1_000_000_000_000, quote=100_000_000_000))
    empty.finalize_due(NOW + HORIZON)
    [record] = _records(tmp_path / "empty.ndjson")
    assert record["counts"] == {"swap_buy": 1} and record["path"] == []


def test_log_rotates_past_its_size_bound(tmp_path: Path) -> None:
    path = tmp_path / REJECTED_PATH_LOG
    path.write_text("x" * 20)
    recorder = RejectedPathRecorder(path, max_bytes=10)
    recorder.start(_candidate(), _pool(), _state(), NOW)
    recorder.finalize_due(NOW + HORIZON)
    assert (tmp_path / f"{REJECTED_PATH_LOG}.1").read_text() == "x" * 20
    assert len(_records(path)) == 1


def _pipeline(tmp_path: Path, **kwargs: Any) -> ConfirmationPipeline:
    metrics = BotMetrics()
    return ConfirmationPipeline(
        data_dir=str(tmp_path), strategy_version="strategy-v1", config_hash="config-hash",
        entry_gate=EntryGate(metrics), metrics=metrics, record_raw=False, **kwargs,
    )


def _pool_created(pool: str, mint: str, at: datetime, quote_mint: str = WSOL_MINT) -> EventEnvelope:
    return EventEnvelope(
        source=EventSource.HELIUS_WSS, protocol=Protocol.PUMPSWAP, event_type=ChainEventType.POOL_CREATED,
        slot=1, signature=f"create-{pool}", instruction_index=1, block_time=at, observed_at=at,
        mint=mint, pool_address=pool,
        payload={"base_mint": mint, "quote_mint": quote_mint, "base_mint_decimals": 6, "quote_mint_decimals": 9,
                 "pool_base_amount": 1_000_000_000_000, "pool_quote_amount": 10_000_000_000},
    )


@pytest.mark.asyncio
async def test_market_rejected_pool_records_the_activity_ingest_drops(tmp_path: Path) -> None:
    async def provider(_candidate: Candidate, _snapshot: FeatureSnapshot) -> SecurityContext:
        pytest.fail("a candidate failing market filters must not call providers")

    pipeline = _pipeline(tmp_path, record_rejected_paths=True, security_provider=provider)
    pipeline.pools.set_quote_price(QuoteAssetPrice(mint=WSOL_MINT, price_usd=Decimal("150"), observed_at=NOW))
    await pipeline.process_event(_pool_created("THIN", "TOKEN", NOW))
    for seconds in (1, 46, 47):
        await pipeline.evaluate_candidates(NOW + timedelta(seconds=seconds))
    [candidate] = pipeline.candidates.values()
    assert candidate.state == CandidateState.REJECTED
    assert candidate.reject_reason == RejectReason.LOW_QUOTE_LIQUIDITY
    assert pipeline.rejected_paths is not None and len(pipeline.rejected_paths) == 1
    rejected_at = candidate.rejected_at
    assert rejected_at is not None
    later = _event("THIN", 0, base=500_000_000_000, quote=20_000_000_000)
    later = later.model_copy(update={"block_time": rejected_at + timedelta(seconds=30)})
    assert pipeline._admit_for_ingest(later) is False
    await pipeline.evaluate_candidates(rejected_at + HORIZON)
    [record] = _records(tmp_path / REJECTED_PATH_LOG)
    assert record["reject_reason"] == "LOW_QUOTE_LIQUIDITY" and record["mint"] == "TOKEN"
    assert record["path"] == [[3, 1, 4.0, 4.0, 4.0, 20.0, 20.0]]
    assert Decimal(record["start"]["quote_liquidity_usd"]) == Decimal("1500")


@pytest.mark.asyncio
async def test_unsupported_pool_rejected_on_sight_is_recorded_from_creation(tmp_path: Path) -> None:
    pipeline = _pipeline(tmp_path, record_rejected_paths=True)
    await pipeline.process_event(_pool_created("ODD", "TOKEN", NOW, quote_mint="OtherQuote111"))
    [candidate] = pipeline.candidates.values()
    assert candidate.reject_reason == RejectReason.UNSUPPORTED_QUOTE_MINT
    assert pipeline.rejected_paths is not None and len(pipeline.rejected_paths) == 1
    await pipeline.evaluate_candidates(NOW + HORIZON)
    [record] = _records(tmp_path / REJECTED_PATH_LOG)
    assert record["reject_reason"] == "UNSUPPORTED_QUOTE_MINT"


@pytest.mark.asyncio
async def test_replay_pipelines_record_no_paths(tmp_path: Path) -> None:
    pipeline = _pipeline(tmp_path)
    assert pipeline.rejected_paths is None
    await pipeline.process_event(_pool_created("ODD", "TOKEN", NOW, quote_mint="OtherQuote111"))
    await pipeline.evaluate_candidates(NOW + HORIZON)
    assert not (tmp_path / REJECTED_PATH_LOG).exists()
