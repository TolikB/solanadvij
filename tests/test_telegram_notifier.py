from __future__ import annotations

import asyncio

import pytest

from sniper_bot.telegram import TelegramNotifier


@pytest.mark.asyncio
async def test_telegram_worker_sends_message_via_httpx(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    class _Response:
        status_code = 200
        text = "ok"

    class _AsyncClient:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.calls = calls

        async def __aenter__(self) -> "_AsyncClient":
            return self

        async def __aexit__(self, *_args: object) -> None:  # noqa: ARG001
            return None

        async def post(self, url: str, *, json: dict[str, object]) -> _Response:
            self.calls.append({"url": url, "json": json})
            return _Response()

    monkeypatch.setattr("sniper_bot.telegram.httpx.AsyncClient", _AsyncClient)

    notifier = TelegramNotifier(bot_token="test-token", admin_chat_id=123)
    task = asyncio.create_task(notifier._worker())
    await notifier._enqueue_message(123, "hello")
    await notifier._queue.put(None)

    await task

    assert calls == [{"url": "https://api.telegram.org/bottest-token/sendMessage", "json": {"chat_id": 123, "text": "hello"}}]
