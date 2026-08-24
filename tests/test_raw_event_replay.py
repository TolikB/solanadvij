from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from sniper_bot.config import AppConfig
from sniper_bot.events import ChainEventType, EventEnvelope, EventSource, Protocol, RawEventRecorder
from sniper_bot.registry import USDC_MINT
from sniper_bot.replay import RawEventReplayRunner, ReplaySpeed
from sniper_bot.runtime import SniperRuntime


def _config(quote_journal: str) -> AppConfig:
    return AppConfig(
        **{
            "APP_MODE": "record", "HELIUS_API_KEY": "helius", "JUPITER_API_KEY": "jupiter",
            "POSTGRES_DSN": "postgresql://user:pass@localhost/db", "TELEGRAM_BOT_TOKEN": "tg",
            "TELEGRAM_ADMIN_CHAT_ID": 123, "STARTING_EQUITY_USD": Decimal("500"),
            "JUPITER_REPLAY_MODE": True, "JUPITER_QUOTE_JOURNAL_PATH": quote_journal,
            "REPLAY_SEED": 7,
        }
    )


@pytest.mark.asyncio
async def test_raw_event_replay_is_deterministic_and_offline(tmp_path, monkeypatch) -> None:
    archive = tmp_path / "archive"
    now = datetime(2026, 8, 18, tzinfo=timezone.utc)
    event = EventEnvelope(
        source=EventSource.REPLAY, protocol=Protocol.PUMPSWAP,
        event_type=ChainEventType.POOL_CREATED, slot=1, signature="sig",
        instruction_index=0, block_time=now, observed_at=now, mint="TOKEN",
        pool_address="POOL",
        payload={
            "base_mint": "TOKEN", "quote_mint": USDC_MINT,
            "base_mint_decimals": 6, "quote_mint_decimals": 6,
            "pool_base_amount": "100000000", "pool_quote_amount": "50000000000",
        },
    )
    await RawEventRecorder(archive).record(event)
    second_payload = event.model_dump(exclude={"event_id"})
    second_payload.update(
        {
            "slot": 2,
            "signature": "sig-2",
            "block_time": now + timedelta(seconds=10),
            "observed_at": now + timedelta(seconds=10),
        }
    )
    await RawEventRecorder(archive).record(EventEnvelope.model_validate(second_payload))

    def forbidden_client(*_args, **_kwargs):
        raise AssertionError("network must not be used in replay")

    monkeypatch.setattr("sniper_bot.solana_rpc.httpx.AsyncClient", forbidden_client)
    monkeypatch.setattr("sniper_bot.jupiter.httpx.AsyncClient", forbidden_client)
    config = _config(str(tmp_path / "quotes.ndjson"))
    one_x_sleeps: list[float] = []
    five_x_sleeps: list[float] = []

    async def one_x_sleep(seconds: float) -> None:
        one_x_sleeps.append(seconds)

    async def five_x_sleep(seconds: float) -> None:
        five_x_sleeps.append(seconds)

    first = await RawEventReplayRunner(
        SniperRuntime(config, data_dir=tmp_path / "one"), sleeper=one_x_sleep
    ).run(archive, speed=ReplaySpeed.ONE_X)
    second = await RawEventReplayRunner(
        SniperRuntime(config, data_dir=tmp_path / "two"), sleeper=five_x_sleep
    ).run(archive, speed=ReplaySpeed.FIVE_X)
    maximum = await RawEventReplayRunner(
        SniperRuntime(config, data_dir=tmp_path / "maximum")
    ).run(archive, speed=ReplaySpeed.MAXIMUM)

    assert first.input_hash == second.input_hash
    assert first.output_hash == second.output_hash
    assert first.output_hash == maximum.output_hash
    assert first.candidate_states == second.candidate_states
    assert one_x_sleeps == [0.0, 10.0]
    assert five_x_sleeps == [0.0, 2.0]
