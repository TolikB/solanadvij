from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select

from scripts.fill_sensitivity import (
    ClosedTrade,
    ExitFill,
    report,
    scenario_adverse,
    scenario_no_delay,
    scenario_size,
)
from scripts.mark_divergence import decide
from sniper_bot.broker import FillReference, PaperBroker
from sniper_bot.candidates import Candidate
from sniper_bot.config import AppConfig
from sniper_bot.database import Database
from sniper_bot.db_models import PaperAccountRow, PaperFillRow, PaperOrderRow, RiskEventRow
from sniper_bot.errors import EntrySlippageExceededError
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.jupiter import JupiterQuoteProvider
from sniper_bot.ledger import PaperLedger
from sniper_bot.metrics import BotMetrics
from sniper_bot.models import QuoteResponse
from sniper_bot.registry import (
    USDC_MINT,
    WSOL_MINT,
    PoolRecord,
    PoolState,
    QuoteAssetPrice,
    TokenRecord,
    reserve_sell_value_usd,
)
from sniper_bot.risk import RiskManager
from sniper_bot.runtime import MARK_COMPARISON_LOG, SniperRuntime

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
    def __init__(self, tokens_per_usd: Decimal, usd_per_token: Decimal) -> None:
        self.tokens_per_usd = tokens_per_usd
        self.usd_per_token = usd_per_token
        self.network_fee_usd = Decimal("0.2")

    async def get_buy_quote(
        self, quote_token: str, token: str, usdc_amount: Decimal
    ) -> QuoteResponse:
        out = usdc_amount * self.tokens_per_usd
        return QuoteResponse(
            token_in=quote_token,
            token_out=token,
            in_amount=usdc_amount,
            out_amount=out,
            in_amount_usd=usdc_amount,
            out_amount_usd=usdc_amount,
            estimated_network_fee_usd=self.network_fee_usd,
        )

    async def get_sell_quote(
        self, token: str, quote_token: str, token_amount: Decimal
    ) -> QuoteResponse:
        usd = token_amount * self.usd_per_token
        return QuoteResponse(
            token_in=token,
            token_out=quote_token,
            in_amount=token_amount,
            out_amount=usd,
            in_amount_usd=usd,
            out_amount_usd=usd,
        )

    async def get_sell_quote_mark_to_market(
        self, token: str, quote_token: str, token_amount: Decimal
    ) -> QuoteResponse:
        return await self.get_sell_quote(token, quote_token, token_amount)


def _broker(
    tmp_path: Path,
    quotes: _Quotes,
    *,
    database: Database | None = None,
    sleeps: list[float] | None = None,
    pool_states: list[dict[str, str]] | None = None,
) -> tuple[PaperBroker, PaperLedger]:
    config = _config()
    ledger = PaperLedger(
        storage_path=tmp_path / "ledger.json",
        starting_equity_usd=Decimal("500"),
        strategy_version="strategy-v1",
        config_hash="hash",
    )
    evidence = iter(pool_states or [])
    broker = PaperBroker(
        quotes,  # type: ignore[arg-type]
        ledger,
        RiskManager(config.risk, ledger),
        database=database,
        strategy_version="strategy-v1" if database is not None else "",
        config_hash="hash",
        max_entry_slippage_bps=300,
        pool_evidence=(lambda address: next(evidence, None)) if pool_states else None,
        metrics=BotMetrics(),
    )
    recorded = sleeps if sleeps is not None else []

    async def fake_sleep(seconds: float) -> None:
        recorded.append(seconds)

    broker.set_clock(lambda: NOW, fake_sleep)
    return broker, ledger


@pytest.mark.asyncio
async def test_exits_wait_the_same_execution_delay_as_entries(tmp_path: Path) -> None:
    sleeps: list[float] = []
    broker, ledger = _broker(tmp_path, _Quotes(Decimal("10"), Decimal("0.1")), sleeps=sleeps)

    result = await broker.open("TOKEN", Decimal("10"))
    await broker.close("TOKEN", result.token_amount / 2, exit_reason="TP1")

    assert sleeps == [1.2, 1.2]


@pytest.mark.asyncio
async def test_entry_within_tolerance_fills_and_records_slippage(tmp_path: Path) -> None:
    broker, ledger = _broker(tmp_path, _Quotes(Decimal("9.8"), Decimal("0.1")))
    await broker.open(
        "TOKEN",
        Decimal("10"),
        reference=FillReference(tokens_per_usd=Decimal("10"), quoted_at=NOW),
    )
    assert len(ledger.open_positions) == 1


@pytest.mark.asyncio
async def test_entry_that_slips_past_tolerance_books_only_the_fee(tmp_path: Path) -> None:
    broker, ledger = _broker(tmp_path, _Quotes(Decimal("9"), Decimal("0.1")))
    equity_before = ledger.state.equity_usd

    with pytest.raises(EntrySlippageExceededError, match="1000.00 bps"):
        await broker.open(
            "TOKEN",
            Decimal("10"),
            reference=FillReference(tokens_per_usd=Decimal("10"), quoted_at=NOW),
        )

    assert ledger.open_positions == []
    assert ledger.state.equity_usd == equity_before - Decimal("0.2")
    assert ledger.daily_pnl(ledger.current_date_key(NOW)) == Decimal("-0.2")
    assert ledger.reconcile()["is_reconciled"] is True


async def _paper_database(tmp_path: Path) -> Database:
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'paper.db'}")
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
    await database.initialize_paper_account(
        account_id="paper-main", starting_equity=Decimal("500"), now=NOW
    )
    return database


@pytest.mark.asyncio
async def test_failed_entry_is_a_rejected_order_that_pays_the_fee(tmp_path: Path) -> None:
    database = await _paper_database(tmp_path)
    broker, _ = _broker(tmp_path, _Quotes(Decimal("9"), Decimal("0.1")), database=database)

    with pytest.raises(EntrySlippageExceededError):
        await broker.open(
            "TOKEN",
            Decimal("10"),
            order_id="entry:candidate",
            candidate_id="candidate",
            pool_address="POOL",
            reference=FillReference(tokens_per_usd=Decimal("10"), quoted_at=NOW),
        )

    async with database.sessions() as session:
        order = (await session.scalars(select(PaperOrderRow))).one()
        account = await session.get(PaperAccountRow, "paper-main")
        event = (
            await session.scalars(
                select(RiskEventRow).where(RiskEventRow.event_type == "ENTRY_FILL_FAILED")
            )
        ).one()
        fills = list((await session.scalars(select(PaperFillRow))).all())
    assert (order.status, order.reject_reason, order.position_id) == (
        "REJECTED",
        "SLIPPAGE_EXCEEDED",
        None,
    )
    assert order.quote_request_id is not None
    assert account is not None
    assert Decimal(account.equity).quantize(Decimal("0.000001")) == Decimal("499.8")
    assert event.details_json["slippage_bps"] == "1000.00"
    assert fills == []
    await database.close()


@pytest.mark.asyncio
async def test_fills_record_the_pool_state_around_the_delay(tmp_path: Path) -> None:
    database = await _paper_database(tmp_path)
    pools = [
        {"slot": "10", "base_reserves": "1000", "quote_reserves": "100"},
        {"slot": "13", "base_reserves": "990", "quote_reserves": "101"},
        {"slot": "20", "base_reserves": "900", "quote_reserves": "110"},
        {"slot": "23", "base_reserves": "905", "quote_reserves": "109"},
    ]
    broker, _ = _broker(
        tmp_path, _Quotes(Decimal("9.9"), Decimal("0.1")), database=database, pool_states=pools
    )
    result = await broker.open(
        "TOKEN",
        Decimal("10"),
        order_id="entry:candidate",
        candidate_id="candidate",
        pool_address="POOL",
        reference=FillReference(tokens_per_usd=Decimal("10"), quoted_at=NOW),
    )
    # Half, so SQLite's float storage of the token amount cannot undercut it.
    await broker.close("TOKEN", result.token_amount / 2, exit_reason="TP1")

    async with database.sessions() as session:
        fills = {
            fill.side: fill.evidence_json
            for fill in (await session.scalars(select(PaperFillRow))).all()
        }
    assert fills["BUY"]["decision_pool"]["slot"] == "10"
    assert fills["BUY"]["fill_pool"]["slot"] == "13"
    assert fills["BUY"]["slippage_bps"] == "100.00"
    assert fills["BUY"]["execution_delay_ms"] == 1200
    assert fills["SELL"]["decision_pool"]["slot"] == "20"
    assert fills["SELL"]["fill_pool"]["slot"] == "23"
    assert fills["SELL"]["quote"]["out_amount"]
    await database.close()


def test_quotes_without_a_network_fee_pay_the_configured_floor() -> None:
    metrics = BotMetrics()
    provider = JupiterQuoteProvider(
        "key", metrics=metrics, minimum_network_fee_lamports=5000
    )
    provider._last_sol_usd_price = Decimal("200")
    now = datetime.now(tz=timezone.utc)
    quote = provider._build_quote(
        "A", "B", {"inAmount": "1", "outAmount": "2"},
        requested_at=now, received_at=now, latency_ms=1,
    )
    reported = provider._build_quote(
        "A", "B", {"inAmount": "1", "outAmount": "2", "prioritizationFeeLamports": 100000},
        requested_at=now, received_at=now, latency_ms=1,
    )
    assert quote.estimated_network_fee_usd == Decimal("0.001")
    assert reported.estimated_network_fee_usd == Decimal("0.02")
    assert metrics.jupiter_quotes_without_network_fee._value.get() == 1


def _pool_state(base: str, quote: str, fee_bps: str | None = "25") -> PoolState:
    return PoolState(
        pool_address="POOL",
        base_mint="TOKEN",
        quote_mint=WSOL_MINT,
        effective_base_reserves=Decimal(base),
        effective_quote_reserves=Decimal(quote),
        last_update_time=NOW,
        quote_price_updated_at=NOW,
        swap_fee_bps=Decimal(fee_bps) if fee_bps is not None else None,
    )


def _pool_record() -> PoolRecord:
    return PoolRecord(
        pool_address="POOL", base_mint="TOKEN", quote_mint=WSOL_MINT,
        creation_signature="pool", creation_slot=1, creation_time=NOW,
        base_decimals=6, quote_decimals=9, updated_at=NOW,
    )


def test_reserve_value_is_the_constant_product_sell_after_fees() -> None:
    value = reserve_sell_value_usd(
        _pool_state("1000000000000", "300000000000"),
        _pool_record(),
        Decimal("1000000000"),
        quote_price_usd=Decimal("150"),
    )
    expected = (
        Decimal("300000000000") * Decimal("1000000000") / Decimal("1001000000000")
        * Decimal("0.9975") / Decimal("1000000000") * Decimal("150")
    )
    assert value == expected
    assert reserve_sell_value_usd(
        _pool_state("0", "1"), _pool_record(), Decimal("1"), quote_price_usd=Decimal("150")
    ) is None


def _pool_event(event_type: ChainEventType, at: datetime, **payload: Any) -> EventEnvelope:
    return EventEnvelope(
        source=EventSource.HELIUS_WSS,
        protocol=Protocol.PUMPSWAP,
        event_type=event_type,
        slot=int(payload.pop("slot", 1)),
        signature=f"{event_type.value}-{at.timestamp()}",
        instruction_index=1,
        block_time=at,
        observed_at=at,
        mint="TOKEN",
        pool_address="POOL",
        payload=payload,
    )


async def _runtime_with_position(tmp_path: Path, mark_source: str) -> tuple[SniperRuntime, Any]:
    runtime = SniperRuntime(_config(exits={"mark_source": mark_source}), data_dir=tmp_path)
    runtime.database = None
    runtime.pipeline.database = None
    start = datetime.now(tz=timezone.utc)
    runtime.pipeline.pools.set_quote_price(
        QuoteAssetPrice(mint=WSOL_MINT, price_usd=Decimal("150"), observed_at=start)
    )
    await runtime.pipeline.process_event(
        _pool_event(
            ChainEventType.POOL_CREATED,
            start,
            base_mint="TOKEN",
            quote_mint=WSOL_MINT,
            base_mint_decimals=6,
            quote_mint_decimals=9,
            pool_base_amount=1_000_000_000_000,
            pool_quote_amount=300_000_000_000,
        )
    )

    class Quotes(_Quotes):
        mark_calls = 0

        async def get_sell_quote_mark_to_market(
            self, token: str, quote_token: str, token_amount: Decimal
        ) -> QuoteResponse:
            Quotes.mark_calls += 1
            return await super().get_sell_quote_mark_to_market(token, quote_token, token_amount)

    # 300 SOL at $150 against 1e12 raw tokens: $4.5e-8 per raw unit.
    quotes = Quotes(Decimal("22222222"), Decimal("0.000000045"))
    runtime.quote_provider = quotes  # type: ignore[assignment]
    assert runtime.broker is not None
    runtime.broker._quote_provider = quotes  # type: ignore[assignment]
    runtime.broker._execution_delay_seconds = 0
    await runtime.broker.open("TOKEN", Decimal("10"), pool_address="POOL")
    return runtime, Quotes


@pytest.mark.asyncio
async def test_reserve_marks_value_positions_without_provider_calls(tmp_path: Path) -> None:
    runtime, quotes = await _runtime_with_position(tmp_path, "reserves")
    await runtime.evaluate_and_close_exits(now=datetime.now(tz=timezone.utc))
    assert quotes.mark_calls == 0
    position = runtime.ledger.open_positions[0]
    assert position.last_executable_value_usd is not None
    assert position.last_executable_value_usd > 0


@pytest.mark.asyncio
async def test_jupiter_marks_log_the_reserve_comparison(tmp_path: Path) -> None:
    runtime, quotes = await _runtime_with_position(tmp_path, "jupiter")
    await runtime.evaluate_and_close_exits(now=datetime.now(tz=timezone.utc))
    assert quotes.mark_calls == 1
    lines = (tmp_path / MARK_COMPARISON_LOG).read_text(encoding="utf-8").splitlines()
    record = json.loads(lines[0])
    assert record["pool_address"] == "POOL"
    assert Decimal(record["jupiter_usd"]) > 0 and Decimal(record["reserve_usd"]) > 0


def _trade(**overrides: Any) -> ClosedTrade:
    values: dict[str, Any] = {
        "position_id": "p",
        "notional_usd": Decimal("10"),
        "entry_fee_usd": Decimal("0.1"),
        "adverse_bps": 50,
        "entry_evidence": {
            "decision_pool": {"base_reserves": "1000", "quote_reserves": "100"},
            "fill_pool": {
                "base_reserves": "1000",
                "quote_reserves": "110",
                "quote_decimals": "0",
                "quote_price_usd": "1",
            },
        },
        "exits": [
            ExitFill(
                gross_usd=Decimal("12"),
                network_fee_usd=Decimal("0.1"),
                evidence={
                    "decision_pool": {"base_reserves": "1000", "quote_reserves": "130"},
                    "fill_pool": {
                        "base_reserves": "1000",
                        "quote_reserves": "120",
                        "quote_decimals": "0",
                        "quote_price_usd": "1",
                    },
                },
            )
        ],
    }
    values.update(overrides)
    return ClosedTrade(**values)


def test_sensitivity_scenarios_reprice_recorded_fills() -> None:
    trade = _trade()
    recorded = trade.pnl([Decimal("12")])
    assert recorded == Decimal("1.8")
    assert scenario_adverse(trade, 50) == recorded
    assert scenario_adverse(trade, 0) > recorded > scenario_adverse(trade, 200)
    # Entry price rose 10 % and exit price fell from 0.13 to 0.12 during the delays.
    no_delay = scenario_no_delay(trade)
    assert no_delay is not None
    expected = Decimal("12") * Decimal("1.1") * Decimal("130") / Decimal("120") - Decimal("0.2") - Decimal("10")
    assert no_delay == expected
    doubled = scenario_size(trade, Decimal("2"))
    assert doubled is not None and doubled < 2 * recorded
    summary = report([trade, _trade(entry_evidence=None, exits=[ExitFill(Decimal("9"), Decimal("0.1"), None)])])
    assert summary["closed_trades"] == 2
    assert summary["no_delay"]["trades_without_evidence"] == 1


def test_mark_divergence_rule() -> None:
    tight = [Decimal("20")] * 600
    assert decide(tight)["decision"] == "reserves"
    assert decide(tight[:100])["decision"] == "jupiter"
    wide = [Decimal("20")] * 500 + [Decimal("900")] * 100
    assert decide(wide)["decision"] == "jupiter"
    assert decide([])["decision"] == "jupiter"
