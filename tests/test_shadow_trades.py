from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select

from sniper_bot.acceptance import (
    ClosedTrade,
    StatisticalProtocol,
    evaluate_statistical_stage,
    load_statistical_stage_data,
)
from sniper_bot.candidates import Candidate, CandidateState
from sniper_bot.config import AppConfig
from sniper_bot.database import Database
from sniper_bot.db_models import ShadowFillRow, ShadowPositionRow
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.exit_engine import ExitPolicy
from sniper_bot.metrics import BotMetrics
from sniper_bot.models import PositionRecord, QuoteResponse
from sniper_bot.pipeline import ConfirmationPipeline
from sniper_bot.registry import USDC_MINT, WSOL_MINT, PoolRecord, TokenRecord
from sniper_bot.shadow import ShadowBook, ShadowEntry
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
            "STARTING_EQUITY_USD": Decimal("500"),
            **overrides,
        }
    )


class _Quotes:
    def __init__(self) -> None:
        self.tokens_per_usd = Decimal("10")
        self.usd_per_token = Decimal("0.1")

    def _quote(self, amount_in: Decimal, amount_out: Decimal, usd: Decimal) -> QuoteResponse:
        return QuoteResponse(
            token_in="A",
            token_out="B",
            in_amount=amount_in,
            out_amount=amount_out,
            in_amount_usd=usd,
            out_amount_usd=usd,
            estimated_network_fee_usd=Decimal("0.01"),
        )

    async def get_buy_quote(self, quote_token: str, token: str, usdc_amount: Decimal) -> QuoteResponse:
        return self._quote(usdc_amount, usdc_amount * self.tokens_per_usd, usdc_amount)

    async def get_sell_quote(self, token: str, quote_token: str, token_amount: Decimal) -> QuoteResponse:
        usd = token_amount * self.usd_per_token
        return self._quote(token_amount, usd, usd)

    async def get_sell_quote_mark_to_market(
        self, token: str, quote_token: str, token_amount: Decimal
    ) -> QuoteResponse:
        return await self.get_sell_quote(token, quote_token, token_amount)


async def _database(tmp_path: Path) -> Database:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'shadow.db'}")
    await database.create_schema_for_tests()
    await database.register_strategy(
        strategy_id="strategy-v1", version="strategy-v1", config_hash="hash",
        config_json={}, now=NOW,
    )
    await database.upsert_token(TokenRecord(mint="TOKEN", creation_time=NOW, updated_at=NOW))
    await database.upsert_pool(
        PoolRecord(
            pool_address="POOL", base_mint="TOKEN", quote_mint=USDC_MINT,
            creation_signature="pool", creation_slot=1, creation_time=NOW,
            base_decimals=6, quote_decimals=6, updated_at=NOW,
        )
    )
    await database.upsert_candidate(
        Candidate(
            candidate_id="candidate", mint="TOKEN", pool_address="POOL",
            detected_at=NOW, updated_at=NOW, strategy_version="strategy-v1",
            config_hash="hash",
        ),
        "strategy-v1",
    )
    return database


def _book(
    config: AppConfig,
    quotes: _Quotes,
    database: Database | None,
    held: set[str],
) -> ShadowBook:
    counter = iter(range(1000))
    book = ShadowBook(
        config=config,
        quote_provider=quotes,
        database=lambda: database,
        id_factory=lambda: f"shadow-{next(counter)}",
        pool_evidence=lambda address: {"slot": "1"},
        reserve_mark=lambda position, at: None,
        on_hold=held.add,
        on_release=held.discard,
        metrics=BotMetrics(),
    )
    clock = [NOW]

    async def no_sleep(seconds: float) -> None:
        clock[0] += timedelta(seconds=seconds)

    book.set_clock(lambda: clock[0], no_sleep)
    return book


def _entry(**overrides: Any) -> ShadowEntry:
    values: dict[str, Any] = {
        "candidate_id": "candidate",
        "mint": "TOKEN",
        "pool_address": "POOL",
        "size_usd": Decimal("10"),
        "block_reason": "CONSECUTIVE_LOSS_PAUSE",
        "tokens_per_usd": Decimal("10"),
    }
    values.update(overrides)
    return ShadowEntry(**values)


def _no_features(pool: str, at: datetime) -> None:
    return None


def _no_dev_sale(position: PositionRecord, at: datetime) -> bool:
    return False


@pytest.mark.asyncio
async def test_shadow_trade_takes_the_refused_entry_and_exits_by_the_same_rules(
    tmp_path: Path,
) -> None:
    database = await _database(tmp_path)
    quotes = _Quotes()
    held: set[str] = set()
    config = _config()
    book = _book(config, quotes, database, held)

    position = await book.open(_entry())
    assert position is not None
    # 100 tokens at 50 bps adverse, cost includes the network fee.
    assert position.entry_token_amount == Decimal("99.5")
    assert position.entry_cost_usd == Decimal("10.01")
    assert held == {"POOL"}

    quotes.usd_per_token = Decimal("0.08")  # -20 % -> stop loss
    decisions = await book.evaluate_exits(
        NOW + timedelta(seconds=30),
        policy=ExitPolicy(),
        features=_no_features,
        dev_sold=_no_dev_sale,
    )
    assert decisions[0].reason.value == "STOP_LOSS"
    assert book.open_positions == []
    assert held == set()

    async with database.sessions() as session:
        row = (await session.scalars(select(ShadowPositionRow))).one()
        fills = list((await session.scalars(select(ShadowFillRow))).all())
    assert (row.status, row.exit_reason, row.block_reason) == (
        "CLOSED",
        "STOP_LOSS",
        "CONSECUTIVE_LOSS_PAUSE",
    )
    assert len(fills) == 1
    expected_pnl = Decimal("99.5") * Decimal("0.08") * Decimal("0.995") - Decimal("0.01") - Decimal("10.01")
    assert Decimal(row.realized_pnl_usd).quantize(Decimal("0.000001")) == expected_pnl.quantize(
        Decimal("0.000001")
    )
    await database.close()


@pytest.mark.asyncio
async def test_shadow_entry_that_slips_fails_like_a_real_swap(tmp_path: Path) -> None:
    database = await _database(tmp_path)
    quotes = _Quotes()
    quotes.tokens_per_usd = Decimal("9")
    held: set[str] = set()
    book = _book(_config(), quotes, database, held)

    assert await book.open(_entry()) is None
    assert held == set()
    async with database.sessions() as session:
        row = (await session.scalars(select(ShadowPositionRow))).one()
    assert (row.status, row.exit_reason) == ("FAILED", "SLIPPAGE_EXCEEDED")
    assert Decimal(row.realized_pnl_usd).quantize(Decimal("0.01")) == Decimal("-0.01")
    await database.close()


@pytest.mark.asyncio
async def test_open_shadow_positions_survive_a_restart(tmp_path: Path) -> None:
    database = await _database(tmp_path)
    config = _config()
    held: set[str] = set()
    first = _book(config, _Quotes(), database, held)
    await first.open(_entry())

    restarted_held: set[str] = set()
    restarted = _book(config, _Quotes(), database, restarted_held)
    pools = await restarted.restore()

    assert pools == ["POOL"]
    assert restarted_held == {"POOL"}
    assert [position.token_mint for position in restarted.open_positions] == ["TOKEN"]
    assert restarted.holds("TOKEN")
    assert await restarted.open(_entry()) is None  # one shadow per mint
    await database.close()


def _pool_event(event_type: ChainEventType, at: datetime, **payload: Any) -> EventEnvelope:
    return EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=Protocol.PUMPSWAP,
        event_type=event_type,
        slot=1,
        signature=f"{event_type.value}-{at.timestamp()}",
        instruction_index=1,
        block_time=at,
        observed_at=at,
        mint="TOKEN",
        pool_address="POOL",
        payload=payload,
    )


@pytest.mark.asyncio
async def test_held_pools_keep_their_activity_after_the_candidate_is_rejected(
    tmp_path: Path,
) -> None:
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
    await pipeline.process_event(
        _pool_event(
            ChainEventType.POOL_CREATED,
            NOW,
            base_mint="TOKEN",
            quote_mint=WSOL_MINT,
            base_mint_decimals=6,
            quote_mint_decimals=9,
            pool_base_amount=1_000_000_000_000,
            pool_quote_amount=300_000_000_000,
        )
    )
    candidate_id = next(iter(pipeline.candidates))
    pipeline.candidates[candidate_id] = pipeline.candidates[candidate_id].model_copy(
        update={"state": CandidateState.REJECTED}
    )
    swap = _pool_event(
        ChainEventType.SWAP_BUY,
        NOW + timedelta(seconds=90),
        pool_base_token_reserves=990_000_000_000,
        pool_quote_token_reserves=303_000_000_000,
    )

    assert pipeline._admit_for_ingest(swap) is False
    pipeline.hold_pool("POOL")
    assert pipeline._admit_for_ingest(swap) is True
    assert pipeline._event_state_filter_reason(swap) is None
    await pipeline.evaluate_candidates(NOW + timedelta(hours=3))
    assert pipeline.pools.pool("POOL") is not None  # held pools are never forgotten
    pipeline.release_pool("POOL")
    assert pipeline._admit_for_ingest(swap) is False


def _protocol() -> StatisticalProtocol:
    return StatisticalProtocol(
        schema_version=4,
        revision="a" * 40,
        strategy_version_id="strategy-v1",
        config_hash="b" * 64,
        frozen_at=datetime(2026, 5, 31, tzinfo=timezone.utc),
        collection_started_at=datetime(2026, 6, 1, tzinfo=timezone.utc),
        oos_started_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        collection_ended_at=datetime(2026, 7, 7, tzinfo=timezone.utc),
        minimum_negative_launches=300,
        minimum_oos_trades=100,
        maximum_equity_mark_gap_seconds=3600,
        daily_operational_cost_usd=Decimal("0.10"),
        negative_launch_definition="distinct_rejected_pumpswap_pool",
        signal_sample_definition="account_and_risk_blocked_shadow_trades",
        risk_unit_pct=Decimal("0.15"),
    )


def test_shadow_trades_complete_the_signal_sample_and_its_r_criteria() -> None:
    from tests.test_acceptance import _passing_inputs

    protocol = _protocol()
    inputs = _passing_inputs(protocol)
    account_oos = [
        trade for trade in inputs.closed_trades if trade.entry_time >= protocol.oos_started_at
    ][:60]
    account_in_sample = [
        trade for trade in inputs.closed_trades if trade.entry_time < protocol.oos_started_at
    ][:100]
    shadows = [
        replace(trade, position_id=f"shadow-{index}", shadow=True)
        for index, trade in enumerate(
            [trade for trade in inputs.closed_trades if trade.entry_time >= protocol.oos_started_at][:90]
        )
    ]
    sha = hashlib.sha256(b"protocol").hexdigest()

    without_shadows = evaluate_statistical_stage(
        inputs=replace(inputs, closed_trades=[*account_in_sample, *account_oos]),
        protocol=protocol,
        protocol_sha256=sha,
    )
    with_shadows = evaluate_statistical_stage(
        inputs=replace(
            inputs,
            closed_trades=[*account_in_sample, *account_oos],
            shadow_trades=shadows,
        ),
        protocol=protocol,
        protocol_sha256=sha,
    )

    def failed(report: Any) -> set[str]:
        return {item.name for item in report.criteria if not item.passed}

    assert "minimum_oos_trades" in failed(without_shadows)
    assert with_shadows.metrics.signal_oos_trade_count == 150
    assert with_shadows.metrics.shadow_trade_count == 90
    assert with_shadows.metrics.oos_trade_count == 60  # account economics stay account-only
    assert "minimum_oos_trades" not in failed(with_shadows)
    assert with_shadows.metrics.signal_oos_mean_r is not None
    assert with_shadows.metrics.signal_oos_mean_r > 0
    assert {"signal_positive_expectancy", "signal_expectancy_confidence"}.isdisjoint(
        failed(with_shadows)
    )

    losing = [replace(trade, pnl_usd=Decimal("-1")) for trade in shadows]
    losing_report = evaluate_statistical_stage(
        inputs=replace(
            inputs,
            closed_trades=[*account_in_sample, *account_oos],
            shadow_trades=losing,
        ),
        protocol=protocol,
        protocol_sha256=sha,
    )
    assert "signal_expectancy_confidence" in failed(losing_report)


@pytest.mark.asyncio
async def test_statistical_loader_reads_closed_failed_and_open_shadows(tmp_path: Path) -> None:
    protocol = _protocol()
    database = await _database(tmp_path)
    opened = protocol.oos_started_at + timedelta(hours=1)
    for position_id, status in (("closed", "CLOSED"), ("failed", "FAILED"), ("open", "OPEN")):
        await database.record_shadow_entry(
            position_id=position_id,
            candidate_id="candidate",
            mint="TOKEN",
            pool_address="POOL",
            notional_usd=Decimal("10"),
            block_reason="MAX_TRADES_PER_DAY",
            status="FAILED" if status == "FAILED" else "OPEN",
            opened_at=opened,
            token_amount=Decimal("100"),
            network_fee_usd=Decimal("0.01"),
            adverse_fill_bps=50,
            strategy_version_id="strategy-v1",
            config_hash=protocol.config_hash,
            evidence={},
        )
    closed = PositionRecord(
        position_id="closed",
        token_mint="TOKEN",
        open_fill_id="x",
        entry_token_amount=Decimal("100"),
        entry_cost_usd=Decimal("10.01"),
        open_ratio=Decimal("1"),
        opened_at=opened,
        locked_usd=Decimal("10.01"),
        remaining_token_amount=Decimal("0"),
        remaining_cost_usd=Decimal("0"),
        realized_pnl_usd=Decimal("3"),
    )
    await database.record_shadow_exit(
        position=closed,
        fill_id="fill",
        token_amount=Decimal("100"),
        gross_usd=Decimal("13.02"),
        network_fee_usd=Decimal("0.01"),
        realized_pnl_usd=Decimal("3"),
        exit_reason="TP1",
        closed=True,
        filled_at=opened + timedelta(minutes=5),
        evidence={},
    )

    inputs = await load_statistical_stage_data(database, protocol)

    assert [trade.position_id for trade in inputs.shadow_trades] == ["closed"]
    shadow = inputs.shadow_trades[0]
    assert isinstance(shadow, ClosedTrade) and shadow.shadow
    assert shadow.cost_usd.quantize(Decimal("0.01")) == Decimal("10.01")
    assert inputs.censored_shadow_position_count == 1
    await database.close()


@pytest.mark.asyncio
async def test_risk_blocked_entry_becomes_a_shadow_trade_at_fresh_day_size(
    tmp_path: Path,
) -> None:
    from sniper_bot.runtime import SniperRuntime
    from sniper_bot.security import RejectReason
    from tests.test_provider_budget import _context, _score

    runtime = SniperRuntime(_config(), data_dir=tmp_path)
    runtime.database = None
    runtime.pipeline.database = None
    quotes = _Quotes()
    runtime.quote_provider = quotes  # type: ignore[assignment]
    assert runtime.shadow is not None
    runtime.shadow.quote_provider = quotes
    runtime.shadow._delay_seconds = 0
    runtime.ledger.set_pause_until(datetime.now(tz=timezone.utc) + timedelta(minutes=30))
    candidate = Candidate(
        candidate_id="candidate", mint="TOKEN", pool_address="POOL",
        detected_at=NOW, updated_at=NOW, strategy_version="s", config_hash="h",
        state=CandidateState.ENTRY_PENDING,
    )
    now = datetime.now(tz=timezone.utc)
    security = _context(now, now)
    snapshot = runtime.pipeline.features.snapshot("POOL", now)

    reason = await runtime._open_candidate(candidate, snapshot, _score("95"), security)

    assert reason == RejectReason.RISK_MANAGER_BLOCKED
    assert runtime.ledger.open_positions == []
    shadows = runtime.shadow.open_positions
    assert [position.candidate_id for position in shadows] == ["candidate"]
    assert shadows[0].entry_cost_usd > Decimal("10")
    assert runtime.pipeline._held_pools == {"POOL"}
