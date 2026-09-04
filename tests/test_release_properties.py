from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from hypothesis import given, settings
from hypothesis import strategies as st

from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol
from sniper_bot.exit_engine import ExitPolicy, evaluate_exit
from sniper_bot.jupiter import JupiterQuoteProvider
from sniper_bot.ledger import PaperLedger
from sniper_bot.models import PositionRecord, PositionStatus
from sniper_bot.reports import ReportBuilder


def _position() -> PositionRecord:
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    return PositionRecord(
        position_id="position",
        token_mint="TOKEN",
        open_fill_id="fill",
        entry_token_amount=Decimal("10"),
        entry_cost_usd=Decimal("100"),
        open_ratio=Decimal("10"),
        opened_at=now,
        locked_usd=Decimal("100"),
        status=PositionStatus.OPEN,
        remaining_token_amount=Decimal("10"),
        remaining_cost_usd=Decimal("100"),
    )


@given(size=st.integers(min_value=-500, max_value=500))
def test_property_partial_exit_fraction_is_bounded(size: int) -> None:
    position = _position()
    decision = evaluate_exit(
        position,
        executable_price_usd=Decimal("20"),
        now=position.opened_at,
        policy=ExitPolicy(
            tp1_return=Decimal("0"),
            tp1_size=Decimal(size) / 100,
        ),
    )

    assert Decimal("0") <= decision.close_fraction <= Decimal("1")


@given(
    entry=st.integers(min_value=1, max_value=400),
    mark=st.integers(min_value=0, max_value=1_000),
)
@settings(max_examples=40, deadline=None)
def test_property_paper_equity_remains_finite(entry: int, mark: int) -> None:
    with TemporaryDirectory() as directory:
        ledger = PaperLedger(
            storage_path=Path(directory) / "ledger.json",
            starting_equity_usd=Decimal("500"),
            strategy_version="test",
            config_hash="hash",
        )
        ledger.open_position(
            token_mint="TOKEN",
            usd_amount=Decimal(entry),
            token_amount=Decimal("10"),
            order_id="entry",
            quote_id="quote-entry",
        )
        ledger.mark_to_market({"TOKEN": Decimal(mark) / Decimal("10")})
        ledger.close_position(
            token_mint="TOKEN",
            usd_received=Decimal(mark),
            token_closed=Decimal("10"),
            order_id="exit",
            quote_id="quote-exit",
        )

        assert ledger.state.equity_usd.is_finite()
        assert ledger.state.realized_pnl_usd.is_finite()
        assert ledger.reconcile()["is_reconciled"] is True


@given(
    percentages=st.lists(
        st.integers(min_value=1, max_value=99),
        min_size=0,
        max_size=6,
    )
)
@settings(max_examples=40, deadline=None)
def test_property_total_closed_amount_equals_entry_without_overshoot(
    percentages: list[int],
) -> None:
    with TemporaryDirectory() as directory:
        ledger = PaperLedger(
            storage_path=Path(directory) / "ledger.json",
            starting_equity_usd=Decimal("500"),
            strategy_version="test",
            config_hash="hash",
        )
        position = ledger.open_position(
            token_mint="TOKEN",
            usd_amount=Decimal("100"),
            token_amount=Decimal("100"),
            order_id="entry",
            quote_id="quote-entry",
        )
        total_closed = Decimal("0")
        for index, percentage in enumerate(percentages):
            remaining = position.remaining_token_amount
            if remaining <= 0:
                break
            amount = remaining * Decimal(percentage) / Decimal("100")
            position = ledger.close_position(
                token_mint="TOKEN",
                usd_received=amount,
                token_closed=amount,
                order_id=f"partial-{index}",
                quote_id=f"quote-{index}",
            )
            total_closed += amount
            assert total_closed <= Decimal("100")
        remaining = position.remaining_token_amount
        if remaining > 0:
            position = ledger.close_position(
                token_mint="TOKEN",
                usd_received=remaining,
                token_closed=remaining,
                order_id="final",
                quote_id="quote-final",
            )
            total_closed += remaining

        assert total_closed == Decimal("100")
        assert position.remaining_token_amount == 0
        assert position.status == PositionStatus.CLOSED


@given(
    platform_fee=st.integers(min_value=-1_000_000, max_value=1_000_000),
    signature_fee=st.integers(min_value=-1_000_000, max_value=1_000_000),
)
def test_property_quote_fees_are_non_negative(
    platform_fee: int,
    signature_fee: int,
) -> None:
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    provider = JupiterQuoteProvider("key", quote_mint="USDC")
    provider.set_clock(lambda: now)
    quote = provider._build_quote(
        "USDC",
        "TOKEN",
        {
            "inAmount": "100",
            "outAmount": "99",
            "inAmountUsd": "1",
            "outAmountUsd": "0.99",
            "platformFee": {"usdValue": str(platform_fee)},
            "signatureFeeLamports": str(signature_fee),
        },
        requested_at=now,
        received_at=now,
        latency_ms=1,
    )

    assert quote.platform_fee_usd >= 0
    assert quote.estimated_network_fee_usd >= 0


@given(
    path=st.lists(
        st.integers(min_value=0, max_value=1_000_000),
        min_size=1,
        max_size=50,
    )
)
def test_property_drawdown_fraction_is_between_zero_and_one(
    path: list[int],
) -> None:
    drawdown = ReportBuilder._equity_path_drawdown_pct(
        [Decimal(value) for value in path]
    )

    assert Decimal("0") <= drawdown <= Decimal("1")


@given(
    signature=st.text(
        alphabet=st.characters(
            whitelist_categories=("Ll", "Lu", "Nd"),
            min_codepoint=48,
            max_codepoint=122,
        ),
        min_size=1,
        max_size=40,
    ),
    slot=st.integers(min_value=0, max_value=2**63 - 1),
    first_sequence=st.integers(min_value=1, max_value=2**31),
    second_sequence=st.integers(min_value=1, max_value=2**31),
)
def test_property_event_identity_is_idempotent_across_ingest_sequences(
    signature: str,
    slot: int,
    first_sequence: int,
    second_sequence: int,
) -> None:
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    base = {
        "source": EventSource.REPLAY,
        "protocol": Protocol.PUMPSWAP,
        "event_type": ChainEventType.SWAP_BUY,
        "slot": slot,
        "signature": signature,
        "instruction_index": 0,
        "block_time": now,
        "observed_at": now,
        "payload": {},
    }
    first = EventEnvelope(ingest_sequence=first_sequence, **base)
    second = EventEnvelope(ingest_sequence=second_sequence, **base)

    assert first.event_id == second.event_id