from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select

import sniper_bot.wallet_analysis as wallet_analysis_module
from sniper_bot.candidates import CandidateState
from sniper_bot.config import AppConfig
from sniper_bot.database import Database
from sniper_bot.db_models import WalletProfileRow
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.features import EventTimeFeatureEngine, LiquidityObservation
from sniper_bot.metrics import BotMetrics
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.registry import WSOL_MINT, TokenRecord, TokenRegistry
from sniper_bot.runtime import SniperRuntime
from sniper_bot.stream import EntryGate
from sniper_bot.wallet_analysis import WalletAnalyzer, WalletProfile

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


def _event(
    event_type: ChainEventType,
    at: datetime,
    *,
    signature: str,
    mint: str | None = "TOKEN",
    pool: str | None = "POOL",
    slot: int | None = None,
    payload: dict[str, object] | None = None,
    protocol: Protocol = Protocol.PUMPSWAP,
) -> EventEnvelope:
    return EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=protocol,
        event_type=event_type,
        slot=slot if slot is not None else int(at.timestamp()),
        signature=signature,
        instruction_index=0,
        block_time=at,
        observed_at=at,
        mint=mint,
        pool_address=pool,
        payload=payload or {},
    )


def _created(mint: str, creator: str, at: datetime) -> EventEnvelope:
    return _event(
        ChainEventType.TOKEN_CREATED,
        at,
        signature=f"create-{mint}",
        mint=mint,
        pool=None,
        protocol=Protocol.PUMP,
        payload={"creator": creator, "user": creator},
    )


def test_idle_traders_mints_and_relations_leave_memory() -> None:
    analyzer = WalletAnalyzer(reloadable_history=True)
    for index, wallet in enumerate(("wallet-a", "wallet-b")):
        analyzer.observe(
            _event(
                ChainEventType.SWAP_BUY,
                NOW + timedelta(seconds=index),
                signature=f"buy-{wallet}",
                slot=7,
                payload={"user": wallet, "quote_amount_in": 10},
            )
        )
    assert analyzer.memory_size()["relations"] == 1
    assert analyzer.cluster_for("wallet-a", {"wallet-b"}) >= {"wallet-a"}

    later = NOW + wallet_analysis_module.RELATION_MEMORY_RETENTION + timedelta(minutes=1)
    analyzer.observe(_created("OTHER", "someone", later))

    size = analyzer.memory_size()
    assert size["relations"] == 0
    assert size["mints"] == 0
    # Only the new creator stays resident.
    assert size["profiles"] == 1


def test_creator_history_leaves_memory_only_when_it_can_be_reloaded() -> None:
    kept = WalletAnalyzer()
    evicting = WalletAnalyzer(reloadable_history=True)
    for analyzer in (kept, evicting):
        analyzer.observe(_created("TOKEN-1", "dev", NOW))
        analyzer.observe(_created("TOKEN-X", "other", NOW + timedelta(days=2)))

    kept_profile = kept.profile("dev", at=NOW + timedelta(days=2))
    assert kept_profile is not None and kept_profile.tokens_created_total == 1
    assert evicting.profile("dev", at=NOW + timedelta(days=2)) is None
    assert evicting.needs_history("dev")


def test_reloaded_creator_history_counts_new_launches_once() -> None:
    analyzer = WalletAnalyzer(reloadable_history=True)
    baseline = WalletProfile(
        wallet_address="dev",
        first_seen_at=NOW - timedelta(days=20),
        known_creator=True,
        tokens_created_total=5,
        tokens_created_7d=2,
        tokens_created_30d=5,
        tokens_with_liquidity_rug=1,
        profile_updated_at=NOW - timedelta(days=3),
    )
    assert analyzer.needs_history("dev")
    analyzer.restore_history("dev", baseline, at=NOW)
    assert not analyzer.needs_history("dev")

    profiles, _ = analyzer.observe(_created("TOKEN-NEW", "dev", NOW))
    profile = next(item for item in profiles if item.wallet_address == "dev")
    assert profile.tokens_created_total == 6
    assert profile.tokens_created_7d == 3
    assert profile.tokens_with_liquidity_rug == 1
    assert profile.first_seen_at == NOW - timedelta(days=20)


def test_stale_baseline_rolling_counts_expire() -> None:
    analyzer = WalletAnalyzer(reloadable_history=True)
    analyzer.restore_history(
        "dev",
        WalletProfile(
            wallet_address="dev",
            first_seen_at=NOW - timedelta(days=40),
            known_creator=True,
            tokens_created_total=4,
            tokens_created_7d=4,
            tokens_created_30d=4,
            profile_updated_at=NOW - timedelta(days=10),
        ),
    )
    profile = analyzer.profile("dev", at=NOW)
    assert profile is not None
    assert (profile.tokens_created_total, profile.tokens_created_7d) == (4, 0)
    assert profile.tokens_created_30d == 4


def test_developer_history_only_reads_the_creators_own_launches() -> None:
    analyzer = WalletAnalyzer()
    for index in range(200):
        analyzer.observe(_created(f"T{index}", f"creator-{index % 50}", NOW))
    profile = analyzer.profile("creator-7", at=NOW + timedelta(minutes=1))
    assert profile is not None and profile.tokens_created_total == 4
    assert analyzer._outcomes_by_creator["creator-7"] == ["T7", "T57", "T107", "T157"]


def test_seen_event_ids_stay_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(wallet_analysis_module, "MAX_SEEN_EVENT_IDS", 10)
    analyzer = WalletAnalyzer()
    for index in range(50):
        analyzer.observe(
            _event(
                ChainEventType.SWAP_SELL,
                NOW + timedelta(seconds=index),
                signature=f"sell-{index}",
                payload={"user": "trader"},
            )
        )
    assert analyzer.memory_size()["seen_events"] == 10


def test_token_registry_reports_persisted_changes_and_sweeps() -> None:
    registry = TokenRegistry()
    created, changed = registry.apply_tracked(_created("MINT", "dev", NOW))
    assert created is not None and changed

    swap = _event(
        ChainEventType.SWAP_BUY, NOW + timedelta(seconds=5), signature="s", mint="MINT"
    )
    touched, changed = registry.apply_tracked(swap)
    assert touched is not None and not changed
    assert touched.updated_at == NOW + timedelta(seconds=5)

    registry.apply_tracked(_created("KEEP", "dev", NOW))
    assert registry.sweep(NOW + timedelta(hours=25), keep={"KEEP"}) == 1
    assert registry.get("MINT") is None and registry.get("KEEP") is not None


def test_feature_engine_forgets_a_pool_and_its_seen_ids() -> None:
    engine = EventTimeFeatureEngine()
    engine.register_pool("POOL", NOW)
    observation = LiquidityObservation(
        event_id="e1",
        pool_address="POOL",
        event_time=NOW,
        quote_liquidity_usd=Decimal("1000"),
    )
    assert engine.ingest_liquidity(observation)
    assert not engine.ingest_liquidity(observation)
    engine.forget_pool("POOL")
    assert engine.pool_count() == 0
    assert engine.ingest_liquidity(observation)


def _config() -> AppConfig:
    return AppConfig(
        APP_MODE="paper",
        APP_REVISION="",
        HELIUS_API_KEY="helius-key",
        JUPITER_API_KEY="jupiter-key",
        POSTGRES_DSN="postgresql://user:pass@localhost:5432/db",
        TELEGRAM_BOT_TOKEN="telegram-token",
        TELEGRAM_ADMIN_CHAT_ID=123456,
        STARTING_EQUITY_USD=Decimal("500"),
    )


def _pool_created(pool: str, mint: str, at: datetime) -> EventEnvelope:
    return _event(
        ChainEventType.POOL_CREATED,
        at,
        signature=f"pool-{pool}",
        mint=mint,
        pool=pool,
        payload={
            "base_mint": mint,
            "quote_mint": "UNSUPPORTED-QUOTE",
            "base_mint_decimals": 6,
            "quote_mint_decimals": 9,
            "pool_base_amount": 1_000_000_000_000,
            "pool_quote_amount": 300_000_000_000,
        },
    )


@pytest.mark.asyncio
async def test_settled_pools_and_tokens_leave_pipeline_memory(tmp_path: Path) -> None:
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="strategy-v1",
        config_hash="config-hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        record_raw=False,
        config=_config(),
    )
    await pipeline.process_event(_created("MINT", "dev", NOW))
    await pipeline.process_event(_pool_created("POOL", "MINT", NOW))
    candidate = next(iter(pipeline.candidates.values()))
    assert candidate.state == CandidateState.REJECTED
    assert pipeline.pools.pool("POOL") is not None

    await pipeline.evaluate_candidates(NOW + timedelta(hours=2))

    assert pipeline.candidates == {}
    assert pipeline.pools.pool("POOL") is None
    assert pipeline.features.pool_count() == 0
    assert pipeline.tokens.get("MINT") is None


@pytest.mark.asyncio
async def test_pool_creation_reloads_an_evicted_token_with_its_creator(
    tmp_path: Path,
) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'tokens.db'}")
    await database.create_schema_for_tests()
    metrics = BotMetrics()
    pipeline = ConfirmationPipeline(
        data_dir=str(tmp_path),
        strategy_version="strategy-v1",
        config_hash="config-hash",
        entry_gate=EntryGate(metrics),
        metrics=metrics,
        database=database,
        record_raw=False,
        config=_config(),
    )
    await database.upsert_token(
        TokenRecord(
            mint="MINT",
            creator_address="dev",
            creation_time=NOW - timedelta(days=3),
            updated_at=NOW - timedelta(days=3),
        )
    )
    assert pipeline.tokens.get("MINT") is None

    await pipeline._apply_event(_pool_created("POOL", "MINT", NOW), persist=True, observe=False)

    token = pipeline.tokens.get("MINT")
    assert token is not None and token.creator_address == "dev"
    # The pool's token row is rewritten with the creator it already had.
    stored = await database.load_token("MINT")
    assert stored is not None and stored.creator_address == "dev"
    await database.close()


@pytest.mark.asyncio
async def test_wallet_rows_merge_instead_of_losing_creator_history(tmp_path: Path) -> None:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'wallets.db'}")
    await database.create_schema_for_tests()
    await database.upsert_wallet_profile(
        WalletProfile(
            wallet_address="dev",
            first_seen_at=NOW - timedelta(days=5),
            known_creator=True,
            tokens_created_total=7,
            tokens_with_liquidity_rug=2,
            profile_updated_at=NOW - timedelta(days=1),
        )
    )
    # A process that evicted the creator sees it again only as a trader.
    await database.upsert_wallet_profile(
        WalletProfile(
            wallet_address="dev",
            first_seen_at=NOW,
            initial_funder="funder",
            profile_updated_at=NOW,
        )
    )
    async with database.sessions() as session:
        row = (await session.scalars(select(WalletProfileRow))).one()
    assert row.known_creator is True
    assert (row.tokens_created, row.tokens_with_liquidity_rug) == (7, 2)
    assert row.first_seen_at.replace(tzinfo=timezone.utc) == NOW - timedelta(days=5)
    assert row.initial_funder == "funder"

    loaded = await database.load_wallet_profile("dev")
    assert loaded is not None and loaded.tokens_created_total == 7
    assert await database.load_wallet_profile("nobody") is None
    await database.close()


@pytest.mark.asyncio
async def test_launch_loads_persisted_creator_history_first(tmp_path: Path) -> None:
    runtime = SniperRuntime(_config(), data_dir=tmp_path)
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'runtime.db'}")
    await database.create_schema_for_tests()
    runtime.database = database
    await database.upsert_wallet_profile(
        WalletProfile(
            wallet_address="dev",
            first_seen_at=NOW - timedelta(days=9),
            known_creator=True,
            tokens_created_total=3,
            profile_updated_at=NOW - timedelta(days=2),
        )
    )

    await runtime._observe_event(_created("MINT", "dev", NOW))

    stored = await database.load_wallet_profile("dev")
    assert stored is not None and stored.tokens_created_total == 4
    await database.close()


def test_quote_cache_stays_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    import sniper_bot.jupiter as jupiter_module
    from sniper_bot.models import QuoteResponse

    monkeypatch.setattr(jupiter_module, "MAX_QUOTE_CACHE_ENTRIES", 3)
    provider = jupiter_module.JupiterQuoteProvider("key")
    quote = QuoteResponse(
        request_id="r",
        requested_at=NOW,
        received_at=NOW,
        latency_ms=1,
        token_in=WSOL_MINT,
        token_out="MINT",
        in_amount=Decimal("1"),
        out_amount=Decimal("1"),
        expires_at=NOW + timedelta(seconds=30),
    )
    for index in range(10):
        provider._set_cached_quote({"amount": index}, 5.0, quote)
    assert len(provider._quote_cache) == 3
