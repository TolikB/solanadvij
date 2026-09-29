from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from scripts.benchmark_postgres_capacity import BorshEventEncoder, _address, _idl_path
from sniper_bot.candidates import Candidate, CandidateState
from sniper_bot.config import AppConfig
from sniper_bot.database import Database
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.features import FeatureSnapshot
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import ConfirmationPipeline, SecurityProvider
from sniper_bot.protocols import pump as pump_package
from sniper_bot.protocols import pumpswap as pumpswap_package
from sniper_bot.protocols.pump import PUMP_PROGRAM_ID
from sniper_bot.protocols.pumpswap import PUMPSWAP_PROGRAM_ID
from sniper_bot.registry import WSOL_MINT, PoolRecord, QuoteAssetPrice, TokenRecord
from sniper_bot.security import RejectReason, SecurityContext
from sniper_bot.stream import EntryGate

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


def _config(**overrides: Any) -> AppConfig:
    return AppConfig(
        **{
            "APP_MODE": "paper",
            "APP_REVISION": "",
            "HELIUS_API_KEY": "helius-key",
            "JUPITER_API_KEY": "jupiter-key",
            "POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db",
            "TELEGRAM_BOT_TOKEN": "telegram-token",
            "TELEGRAM_ADMIN_CHAT_ID": 123456,
            **overrides,
        }
    )


def _pipeline(
    tmp_path: Any,
    *,
    security_provider: SecurityProvider | None = None,
    config: AppConfig | None = None,
    metrics: BotMetrics | None = None,
) -> ConfirmationPipeline:
    metrics = metrics or BotMetrics()
    return ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="strategy-v1",
        config_hash="config-hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        security_provider=security_provider,
        record_raw=False,
        config=config,
    )


def _pool_created(
    pool: str,
    mint: str,
    at: datetime,
    *,
    quote_lamports: int = 300_000_000_000,
    source: EventSource = EventSource.HELIUS_WSS,
) -> EventEnvelope:
    return EventEnvelope(
        source=source,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.POOL_CREATED,
        slot=1,
        signature=f"create-{pool}",
        instruction_index=1,
        block_time=at,
        observed_at=at,
        mint=mint,
        pool_address=pool,
        payload={
            "base_mint": mint,
            "quote_mint": WSOL_MINT,
            "base_mint_decimals": 6,
            "quote_mint_decimals": 9,
            "pool_base_amount": 1_000_000_000_000,
            "pool_quote_amount": quote_lamports,
        },
    )


def _swap(pool: str, at: datetime, signature: str) -> EventEnvelope:
    return EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.SWAP_BUY,
        slot=2,
        signature=signature,
        instruction_index=1,
        block_time=at,
        observed_at=at,
        pool_address=pool,
        payload={},
    )


async def _advance_to_security_check(
    pipeline: ConfirmationPipeline, detected_at: datetime
) -> None:
    await pipeline.evaluate_candidates(detected_at + timedelta(seconds=1))
    await pipeline.evaluate_candidates(detected_at + timedelta(seconds=46))
    assert {candidate.state for candidate in pipeline.candidates.values()} == {
        CandidateState.SECURITY_CHECK
    }


def _notification(
    encoder: BorshEventEncoder,
    program_id: str,
    event_name: str,
    signature: str,
    overrides: dict[str, Any],
) -> dict[str, Any]:
    # Streaming shape: signature and slot but no blockTime.
    return {
        "slot": 400_000_001,
        "signature": signature,
        "meta": {
            "err": None,
            "logMessages": [
                f"Program {program_id} invoke [1]",
                f"Program data: {encoder.encode(event_name, overrides)}",
                f"Program {program_id} success",
            ],
        },
    }


@pytest.mark.asyncio
async def test_stream_transactions_are_dated_and_untracked_activity_never_reaches_ingest(
    tmp_path,
) -> None:
    metrics = BotMetrics()
    pipeline = _pipeline(tmp_path, metrics=metrics)
    swap = BorshEventEncoder(_idl_path(pumpswap_package))
    pump = BorshEventEncoder(_idl_path(pump_package))
    timestamp = int(NOW.timestamp())
    tracked_pool, other_pool, mint = _address(7, 1), _address(7, 2), _address(11, 1)
    pool_fields = {
        "timestamp": timestamp,
        "pool": tracked_pool,
        "base_mint": mint,
        "quote_mint": WSOL_MINT,
        "base_mint_decimals": 6,
        "quote_mint_decimals": 9,
        "pool_base_amount": 1_000_000_000,
        "pool_quote_amount": 300_000_000_000,
    }
    transactions = [
        _notification(swap, PUMPSWAP_PROGRAM_ID, "CreatePoolEvent", "create", pool_fields),
        _notification(
            swap,
            PUMPSWAP_PROGRAM_ID,
            "BuyEvent",
            "tracked-buy",
            {"timestamp": timestamp + 2, "pool": tracked_pool},
        ),
        _notification(
            swap,
            PUMPSWAP_PROGRAM_ID,
            "BuyEvent",
            "untracked-buy",
            {"timestamp": timestamp + 2, "pool": other_pool},
        ),
        _notification(
            pump,
            PUMP_PROGRAM_ID,
            "TradeEvent",
            "curve-trade",
            {"timestamp": timestamp + 3, "mint": mint, "is_buy": True},
        ),
    ]
    items = [
        (Protocol.PUMPSWAP, transactions[0], EventSource.HELIUS_WSS),
        (Protocol.PUMPSWAP, transactions[1], EventSource.HELIUS_WSS),
        (Protocol.PUMPSWAP, transactions[2], EventSource.HELIUS_WSS),
        (Protocol.PUMP, transactions[3], EventSource.HELIUS_WSS),
    ]

    await pipeline.process_transactions(items)

    assert [item["blockTime"] for item in transactions] == [
        timestamp,
        timestamp + 2,
        timestamp + 2,
        timestamp + 3,
    ]
    assert metrics.chain_events_received._value.get() == 2
    assert (
        metrics.chain_events_filtered_before_ingest.labels(
            reason="untracked_pool"
        )._value.get()
        == 1
    )
    [candidate] = pipeline.candidates.values()
    assert candidate.pool_address == tracked_pool
    assert candidate.detected_at == NOW
    assert pipeline.pools.pool(other_pool) is None


def test_ingest_filter_follows_the_candidate_lifecycle(tmp_path) -> None:
    pipeline = _pipeline(tmp_path)
    assert pipeline._admit_for_ingest(_pool_created("POOL", "TOKEN", NOW)) is True
    candidate = Candidate(
        candidate_id=pipeline._ingest_tracked_pools["POOL"][0],
        mint="TOKEN",
        pool_address="POOL",
        detected_at=NOW,
        updated_at=NOW,
        strategy_version="strategy-v1",
        config_hash="config-hash",
    )
    # The candidate is materialized later by the state stage.
    assert pipeline._admit_for_ingest(_swap("POOL", NOW + timedelta(seconds=1), "a")) is True

    pipeline.candidates[candidate.candidate_id] = candidate
    late = _swap("POOL", NOW + timedelta(seconds=241), "late")
    assert pipeline._admit_for_ingest(late) is False

    pipeline.candidates[candidate.candidate_id] = candidate.model_copy(
        update={"state": CandidateState.POSITION_OPEN}
    )
    assert pipeline._admit_for_ingest(late) is True

    pipeline.candidates[candidate.candidate_id] = candidate.model_copy(
        update={"state": CandidateState.REJECTED}
    )
    assert pipeline._admit_for_ingest(_swap("POOL", NOW + timedelta(seconds=2), "b")) is False
    assert pipeline._admit_for_ingest(_pool_created("NEXT", "OTHER", NOW)) is True
    assert "POOL" not in pipeline._ingest_tracked_pools

    for source in (EventSource.BASELINE_WSS, EventSource.RPC_RECOVERY):
        event = _pool_created(f"POOL-{source.value}", "TOKEN", NOW, source=source)
        assert pipeline._admit_for_ingest(event) is True
        assert event.pool_address not in pipeline._ingest_tracked_pools


@pytest.mark.asyncio
async def test_unknown_event_layout_blocks_the_protocol_without_stopping_ingestion(
    tmp_path,
) -> None:
    metrics = BotMetrics()
    pipeline = _pipeline(tmp_path, metrics=metrics)
    swap = BorshEventEncoder(_idl_path(pumpswap_package))
    unknown = base64.b64encode(bytes([255]) * 16).decode()
    transaction = _notification(
        swap,
        PUMPSWAP_PROGRAM_ID,
        "CreatePoolEvent",
        "mixed",
        {
            "timestamp": int(NOW.timestamp()),
            "pool": _address(7, 9),
            "base_mint": _address(11, 9),
            "quote_mint": WSOL_MINT,
            "base_mint_decimals": 6,
            "quote_mint_decimals": 9,
        },
    )
    transaction["meta"]["logMessages"].insert(2, f"Program data: {unknown}")

    await pipeline.process_transactions(
        [(Protocol.PUMPSWAP, transaction, EventSource.HELIUS_WSS)]
    )

    assert "protocol:pumpswap" in pipeline.entry_gate.reasons
    assert (
        metrics.protocol_layout_quarantines.labels(
            protocol="pumpswap", kind="unknown_discriminator"
        )._value.get()
        == 1
    )
    assert pipeline.pools.pool(_address(7, 9)) is not None

    truncated = base64.b64decode(swap.encode("CreatePoolEvent", {}))[:20]
    broken = {
        "slot": 400_000_002,
        "signature": "broken",
        "meta": {
            "err": None,
            "logMessages": [
                f"Program {PUMPSWAP_PROGRAM_ID} invoke [1]",
                f"Program data: {base64.b64encode(truncated).decode()}",
                f"Program {PUMPSWAP_PROGRAM_ID} success",
            ],
        },
    }
    await pipeline.process_transactions(
        [(Protocol.PUMPSWAP, broken, EventSource.HELIUS_WSS)]
    )
    assert (
        metrics.protocol_layout_quarantines.labels(
            protocol="pumpswap", kind="decode_error"
        )._value.get()
        == 1
    )


@pytest.mark.asyncio
async def test_market_only_reject_spends_no_provider_quota(tmp_path) -> None:
    async def provider(_candidate: Candidate, _snapshot: FeatureSnapshot) -> SecurityContext:
        pytest.fail("a candidate failing market filters must not call providers")

    pipeline = _pipeline(tmp_path, security_provider=provider)
    pipeline.pools.set_quote_price(
        QuoteAssetPrice(mint=WSOL_MINT, price_usd=Decimal("150"), observed_at=NOW)
    )
    await pipeline.process_event(
        _pool_created("THIN", "TOKEN", NOW, quote_lamports=10_000_000_000)
    )
    await _advance_to_security_check(pipeline, NOW)

    await pipeline.evaluate_candidates(NOW + timedelta(seconds=47))

    [candidate] = pipeline.candidates.values()
    assert candidate.state == CandidateState.REJECTED
    assert candidate.reject_reason == RejectReason.LOW_QUOTE_LIQUIDITY


@pytest.mark.asyncio
async def test_candidate_without_a_usable_quote_price_waits_and_then_expires(
    tmp_path,
) -> None:
    calls: list[str] = []

    async def provider(candidate: Candidate, _snapshot: FeatureSnapshot) -> SecurityContext:
        calls.append(candidate.pool_address)
        raise AssertionError("unreachable")

    metrics = BotMetrics()
    pipeline = _pipeline(tmp_path, security_provider=provider, metrics=metrics)
    await pipeline.process_event(_pool_created("NO-PRICE", "TOKEN", NOW))
    await _advance_to_security_check(pipeline, NOW)

    await pipeline.evaluate_candidates(NOW + timedelta(seconds=47))
    [candidate] = pipeline.candidates.values()
    assert candidate.state == CandidateState.SECURITY_CHECK
    assert calls == []
    assert (
        metrics.candidate_evaluation_failures.labels(stage="market_data")._value.get()
        == 1
    )

    await pipeline.evaluate_candidates(NOW + timedelta(seconds=181))
    [candidate] = pipeline.candidates.values()
    assert candidate.reject_reason == RejectReason.ENTRY_WINDOW_EXPIRED


@pytest.mark.asyncio
async def test_security_failure_of_one_candidate_does_not_stall_the_others(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    async def provider(candidate: Candidate, _snapshot: FeatureSnapshot) -> SecurityContext:
        calls.append(candidate.pool_address)
        raise RuntimeError("token-account index is too stale")

    metrics = BotMetrics()
    pipeline = _pipeline(tmp_path, security_provider=provider, metrics=metrics)
    monkeypatch.setattr(pipeline, "_market_reject_reason", lambda *_args: (True, None))
    await pipeline.process_event(_pool_created("POOL-A", "TOKEN-A", NOW))
    await pipeline.process_event(_pool_created("POOL-B", "TOKEN-B", NOW))
    await _advance_to_security_check(pipeline, NOW)

    assert await pipeline.evaluate_candidates(NOW + timedelta(seconds=47)) == []
    assert sorted(calls) == ["POOL-A", "POOL-B"]
    assert (
        metrics.candidate_evaluation_failures.labels(stage="security")._value.get() == 2
    )

    calls.clear()
    changed = await pipeline.evaluate_candidates(NOW + timedelta(seconds=181))
    assert calls == []
    assert len(changed) == 2
    assert {candidate.reject_reason for candidate in changed} == {
        RejectReason.ENTRY_WINDOW_EXPIRED
    }


@pytest.mark.asyncio
async def test_terminal_candidates_leave_the_evaluation_set(tmp_path) -> None:
    pipeline = _pipeline(tmp_path)
    await pipeline.process_event(
        _pool_created("OLD", "TOKEN", NOW, source=EventSource.BASELINE_WSS)
    )
    assert len(pipeline.candidates) == 1

    await pipeline.evaluate_candidates(NOW + timedelta(minutes=59))
    assert len(pipeline.candidates) == 1
    await pipeline.evaluate_candidates(NOW + timedelta(minutes=61))
    assert pipeline.candidates == {}


@pytest.mark.asyncio
async def test_collection_window_closes_candidates_before_it_ends(tmp_path) -> None:
    ends_at = NOW + timedelta(hours=1)
    config = _config(collection={"ends_at": ends_at.isoformat(), "entry_cutoff_seconds": 1800})
    pipeline = _pipeline(tmp_path, config=config)
    closes_at = ends_at - timedelta(seconds=1800)

    await pipeline.process_event(_pool_created("OPEN", "TOKEN-A", closes_at - timedelta(minutes=2)))
    await pipeline.process_event(_pool_created("LATE", "TOKEN-B", closes_at + timedelta(seconds=5)))

    late = next(c for c in pipeline.candidates.values() if c.pool_address == "LATE")
    assert late.state == CandidateState.REJECTED
    assert late.reject_reason == RejectReason.COLLECTION_WINDOW_CLOSED
    assert late.rejected_at == closes_at + timedelta(seconds=5)
    assert "LATE" not in pipeline._ingest_tracked_pools

    open_candidate = next(c for c in pipeline.candidates.values() if c.pool_address == "OPEN")
    assert open_candidate.state == CandidateState.DISCOVERED
    await pipeline.evaluate_candidates(closes_at + timedelta(seconds=1))
    open_candidate = pipeline.candidates[open_candidate.candidate_id]
    assert open_candidate.state == CandidateState.REJECTED
    assert open_candidate.reject_reason == RejectReason.COLLECTION_WINDOW_CLOSED
    assert open_candidate.rejected_at is not None
    assert open_candidate.rejected_at < ends_at


def test_collection_window_is_part_of_the_strategy_identity_only_when_set() -> None:
    unset = _config()
    frozen = _config(collection={"ends_at": "2026-10-29T12:00:00Z"})

    assert unset.collection.ends_at is None
    assert "collection" not in unset._normalized_payload()
    assert frozen.config_hash != unset.config_hash
    assert frozen.collection.closes_at == datetime(2026, 10, 29, 11, 30, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="entry_cutoff_seconds"):
        _config(collection={"ends_at": "2026-10-29T12:00:00Z", "entry_cutoff_seconds": 600})
    with pytest.raises(ValueError, match="UTC offset"):
        _config(collection={"ends_at": "2026-10-29T12:00:00"})


def test_default_config_carries_the_frozen_infrastructure_cost(monkeypatch) -> None:
    for name, value in {
        "APP_MODE": "paper",
        "HELIUS_API_KEY": "helius-key",
        "JUPITER_API_KEY": "jupiter-key",
        "POSTGRES_DSN": "postgresql://user:pass@localhost:5432/db",
        "TELEGRAM_BOT_TOKEN": "telegram-token",
        "TELEGRAM_ADMIN_CHAT_ID": "123456",
        "APP_REVISION": "",
    }.items():
        monkeypatch.setenv(name, value)
    config = AppConfig.load("configs/default.yaml")

    assert config.reporting.monthly_infrastructure_cost_usd == Decimal("15")
    assert config.collection.ends_at is None


@pytest.mark.asyncio
async def test_restart_loads_open_and_recent_terminal_candidates_only(tmp_path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'restore.db'}")
    await database.create_schema_for_tests()
    await database.register_strategy(
        strategy_id="strategy-v1",
        version="strategy-v1",
        config_hash="hash",
        config_json={},
        now=NOW,
    )
    rows = {
        "old-open": (NOW - timedelta(days=2), CandidateState.POSITION_OPEN),
        "old-rejected": (NOW - timedelta(days=2), CandidateState.REJECTED),
        "recent-rejected": (NOW - timedelta(minutes=5), CandidateState.REJECTED),
    }
    for name, (detected_at, state) in rows.items():
        await database.upsert_token(TokenRecord(mint=f"mint-{name}", updated_at=detected_at))
        await database.upsert_pool(
            PoolRecord(
                pool_address=f"pool-{name}",
                base_mint=f"mint-{name}",
                quote_mint=WSOL_MINT,
                creation_signature=f"sig-{name}",
                creation_slot=1,
                creation_time=detected_at,
                base_decimals=6,
                quote_decimals=9,
                updated_at=detected_at,
            )
        )
        await database.upsert_candidate(
            Candidate(
                candidate_id=name,
                mint=f"mint-{name}",
                pool_address=f"pool-{name}",
                state=state,
                detected_at=detected_at,
                updated_at=detected_at,
                rejected_at=detected_at if state == CandidateState.REJECTED else None,
                reject_reason=(
                    RejectReason.LOW_QUOTE_LIQUIDITY
                    if state == CandidateState.REJECTED
                    else None
                ),
                strategy_version="strategy-v1",
                config_hash="hash",
            ),
            "strategy-v1",
        )

    everything = await database.load_active_candidates("strategy-v1")
    resumable = await database.load_active_candidates(
        "strategy-v1", terminal_since=NOW - timedelta(hours=1)
    )

    assert {candidate.candidate_id for candidate in everything} == set(rows)
    assert {candidate.candidate_id for candidate in resumable} == {
        "old-open",
        "recent-rejected",
    }
    pipeline = _pipeline(tmp_path)
    pipeline.restore_candidates(resumable)
    assert set(pipeline._ingest_tracked_pools) == {"pool-old-open"}
    await database.close()
