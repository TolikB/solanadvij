from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from sniper_bot.telegram import (
    NoopTelegramNotifier,
    TelegramMessage,
    TelegramNotifier,
)


class _Response:
    def __init__(self, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> object:
        if isinstance(self._payload, BaseException):
            raise self._payload
        return self._payload


def _client_factory(
    *,
    post_response: _Response | None = None,
    updates: list[_Response] | None = None,
    calls: list[dict[str, object]] | None = None,
) -> Callable[..., object]:
    pending = list(updates or [])
    recorded = calls if calls is not None else []

    class _Client:
        async def __aenter__(self) -> "_Client":
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def post(
            self,
            url: str,
            *,
            json: dict[str, object],
        ) -> _Response:
            recorded.append({"method": "POST", "url": url, "json": json})
            assert post_response is not None
            return post_response

        async def get(
            self,
            url: str,
            *,
            params: dict[str, object],
        ) -> _Response:
            recorded.append({"method": "GET", "url": url, "params": params})
            if pending:
                return pending.pop(0)
            await asyncio.Future()
            raise AssertionError("unreachable")

    return lambda *_args, **_kwargs: _Client()


@pytest.mark.asyncio
async def test_noop_notifier_has_zero_side_effects() -> None:
    notifier = NoopTelegramNotifier()

    await notifier.start(start_polling=True, command_handler=None)
    await notifier.send("ignored")
    await notifier.send_to(1, "ignored")
    await notifier.stop()


@pytest.mark.asyncio
async def test_send_immediate_enforces_allowlist_and_http_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        "sniper_bot.telegram.httpx.AsyncClient",
        _client_factory(
            post_response=_Response(
                200,
                {"result": {"message_id": 42}},
            ),
            calls=calls,
        ),
    )
    notifier = TelegramNotifier("token", 123)

    assert await notifier.send_immediate("ready") == "42"
    with pytest.raises(PermissionError, match="allowlisted"):
        await notifier.send_immediate("blocked", chat_id=999)
    assert calls[0]["json"] == {"chat_id": 123, "text": "ready"}


@pytest.mark.asyncio
async def test_send_immediate_rejects_http_failure_and_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    notifier = TelegramNotifier("token", 123)
    monkeypatch.setattr(
        "sniper_bot.telegram.httpx.AsyncClient",
        _client_factory(post_response=_Response(500, {})),
    )
    with pytest.raises(RuntimeError, match="status 500"):
        await notifier.send_immediate("fail")

    monkeypatch.setattr(
        "sniper_bot.telegram.httpx.AsyncClient",
        _client_factory(post_response=_Response(200, ValueError("json"))),
    )
    assert await notifier.send_immediate("no-id") == ""


@pytest.mark.asyncio
async def test_enqueue_deduplicates_and_filters_chat() -> None:
    notifier = TelegramNotifier(
        "token",
        123,
        allowlist=[456],
        allow_duplicate_seconds=60,
    )

    await notifier.send("same")
    await notifier.send("same")
    await notifier.send_to(999, "blocked")
    await notifier.send_to(456, "allowed")

    first = notifier._queue.get_nowait()
    second = notifier._queue.get_nowait()
    assert isinstance(first, TelegramMessage)
    assert isinstance(second, TelegramMessage)
    assert (first.chat_id, first.text) == (123, "same")
    assert (second.chat_id, second.text) == (456, "allowed")
    assert notifier._queue.empty()


@pytest.mark.asyncio
async def test_polling_dispatches_only_authorized_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _Response(
        200,
        {
            "result": [
                {
                    "update_id": 1,
                    "message": {
                        "chat": {"id": 999},
                        "from": {"id": 7},
                        "text": "/today",
                    },
                },
                {
                    "update_id": 2,
                    "message": {
                        "chat": {"id": 123},
                        "from": {"id": 7},
                        "text": "not-a-command",
                    },
                },
                {
                    "update_id": 3,
                    "edited_message": {
                        "chat": {"id": 123},
                        "from": {"id": 7},
                        "text": "/day@paper 2026-09-01",
                    },
                },
            ]
        },
    )
    monkeypatch.setattr(
        "sniper_bot.telegram.httpx.AsyncClient",
        _client_factory(updates=[response]),
    )
    notifier = TelegramNotifier(
        "token",
        123,
        user_allowlist=[7],
    )
    handled = asyncio.Event()
    commands: list[tuple[int, str, list[str]]] = []

    async def handler(
        chat_id: int,
        command: str,
        args: list[str],
    ) -> None:
        commands.append((chat_id, command, args))
        handled.set()

    notifier._command_handler = handler
    task = asyncio.create_task(notifier._poll_commands())
    await asyncio.wait_for(handled.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert commands == [(123, "day", ["2026-09-01"])]


@pytest.mark.asyncio
async def test_worker_retries_are_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    notifier = TelegramNotifier("token", 123)
    attempts = 0
    exhausted = asyncio.Event()

    async def fail_send(
        _text: str,
        *,
        chat_id: int | None = None,
    ) -> str:
        nonlocal attempts
        del chat_id
        attempts += 1
        if attempts == 10:
            exhausted.set()
        raise RuntimeError("offline")

    async def no_delay(_seconds: float) -> None:
        return None

    monkeypatch.setattr(notifier, "send_immediate", fail_send)
    monkeypatch.setattr("sniper_bot.telegram.asyncio.sleep", no_delay)
    worker = asyncio.create_task(notifier._worker())
    await notifier._queue.put(TelegramMessage("message", 123))
    await asyncio.wait_for(exhausted.wait(), timeout=1)
    await notifier._queue.put(None)
    await worker

    assert attempts == 10