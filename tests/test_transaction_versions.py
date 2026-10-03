from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from sniper_bot.external_journal import ExternalJournal
from sniper_bot.solana_rpc import SolanaRpcClient, SolanaRpcError, _RpcResponse


def _key(version: int) -> str:
    return ExternalJournal.key("solana_rpc", "getTransaction", ["SIGNATURE", {
        "commitment": "confirmed", "encoding": "jsonParsed",
        "maxSupportedTransactionVersion": version,
    }])


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["legacy", 0, 1])
async def test_transaction_read_requests_v1_and_accepts_supported_versions(
    monkeypatch: pytest.MonkeyPatch, version: object,
) -> None:
    client = SolanaRpcClient("https://rpc.invalid")
    payload = {"version": version, "slot": 5}

    async def post(method: str, body: dict[str, Any]) -> _RpcResponse:
        assert method == "getTransaction"
        assert body["params"][1]["encoding"] == "jsonParsed"
        assert type(body["params"][1]["maxSupportedTransactionVersion"]) is int
        assert body["params"][1]["maxSupportedTransactionVersion"] == 1
        return _RpcResponse(200, {"result": payload})

    monkeypatch.setattr(client, "_post_rpc", post)
    assert await client.get_transaction("SIGNATURE") == payload


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [True, False, None, 2, -1, "1", 1.0, [], {}])
async def test_transaction_read_rejects_future_or_invalid_version(
    monkeypatch: pytest.MonkeyPatch, version: object,
) -> None:
    client = SolanaRpcClient("https://rpc.invalid")

    async def post(*_args: Any) -> _RpcResponse:
        return _RpcResponse(200, {"result": {"version": version}})

    monkeypatch.setattr(client, "_post_rpc", post)
    with pytest.raises(SolanaRpcError, match="unsupported transaction version"):
        await client.get_transaction("SIGNATURE")


@pytest.mark.asyncio
async def test_legacy_transaction_journal_sequence_replays_without_network_or_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "external.ndjson"
    journal = ExternalJournal(path)
    expected = [None, {"version": "legacy", "slot": 4}, {"version": 0, "slot": 5}]
    for response in expected:
        journal.record(_key(0), response)
    original = path.read_bytes()
    client = SolanaRpcClient("https://rpc.invalid", replay_mode=True, journal=ExternalJournal(path))

    async def forbidden(*_args: Any) -> _RpcResponse:
        raise AssertionError("network forbidden in replay")

    monkeypatch.setattr(client, "_post_rpc", forbidden)
    for response in expected:
        assert await client.get_transaction("SIGNATURE") == response
    with pytest.raises(SolanaRpcError, match="replay data missing"):
        await client.get_transaction("SIGNATURE")
    assert path.read_bytes() == original


@pytest.mark.asyncio
async def test_current_journal_is_preferred_and_recorded_null_does_not_consume_legacy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = ExternalJournal(tmp_path / "external.ndjson")
    journal.record(_key(0), {"version": 0, "slot": 4})
    journal.record(_key(1), None)
    journal.record(_key(1), {"version": 1, "slot": 5})
    client = SolanaRpcClient("https://rpc.invalid", replay_mode=True, journal=journal)

    async def forbidden(*_args: Any) -> _RpcResponse:
        raise AssertionError("network forbidden in replay")

    monkeypatch.setattr(client, "_post_rpc", forbidden)
    assert await client.get_transaction("SIGNATURE") is None
    assert await client.get_transaction("SIGNATURE") == {"version": 1, "slot": 5}
    assert await client.get_transaction("SIGNATURE") == {"version": 0, "slot": 4}


@pytest.mark.asyncio
async def test_live_read_does_not_use_legacy_journal_and_records_v1_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = ExternalJournal(tmp_path / "external.ndjson")
    journal.record(_key(0), {"version": 0, "slot": 4})
    client = SolanaRpcClient("https://rpc.invalid", journal=journal, record_responses=True)

    async def post(*_args: Any) -> _RpcResponse:
        return _RpcResponse(200, {"result": {"version": 1, "slot": 5}})

    monkeypatch.setattr(client, "_post_rpc", post)
    assert await client.get_transaction("SIGNATURE") == {"version": 1, "slot": 5}
    assert journal.get(_key(0))["response"] == {"version": 0, "slot": 4}
    assert journal.get(_key(1))["response"] == {"version": 1, "slot": 5}
