"""Telegram notifier with outbox and optional long-polling command intake."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Optional

import httpx


@dataclass
class TelegramMessage:
    text: str
    chat_id: int
    message_id: str | None = None
    attempts: int = 0


class NoopTelegramNotifier:
    """Notifier implementation that drops messages and starts/stops without network calls."""

    async def start(
        self,
        *,
        start_polling: bool = False,
        command_handler: Optional[Callable[[int, str, list[str]], Awaitable[None]]] = None,
    ) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, text: str) -> None:
        return None

    async def send_to(self, chat_id: int, text: str) -> None:
        return None


class TelegramNotifier:
    def __init__(
        self,
        bot_token: str,
        admin_chat_id: int,
        *,
        allowlist: list[int] | None = None,
        user_allowlist: list[int] | None = None,
        allow_duplicate_seconds: int = 60,
    ) -> None:
        self._token = bot_token
        self._chat_id = admin_chat_id
        self._allowlist = set(allowlist or [])
        self._allowlist.add(admin_chat_id)
        self._user_allowlist = set(user_allowlist or [])
        self._allow_duplicate_seconds = allow_duplicate_seconds
        self._dedupe: dict[str, datetime] = {}
        self._queue: asyncio.Queue[TelegramMessage | None] = asyncio.Queue()
        self._worker_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._command_handler: Callable[[int, str, list[str]], Awaitable[None]] | None = None

    async def start(self, *, start_polling: bool = False, command_handler: Optional[Callable[[int, str, list[str]], Awaitable[None]]] = None) -> None:
        self._command_handler = command_handler
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker())
        if start_polling and self._poll_task is None and self._command_handler is not None:
            self._poll_task = asyncio.create_task(self._poll_commands())

    async def stop(self) -> None:
        if self._worker_task:
            await self._queue.put(None)
            await self._worker_task
            self._worker_task = None
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

    async def send(self, text: str) -> None:
        await self._enqueue_message(self._chat_id, text)

    async def send_to(self, chat_id: int, text: str) -> None:
        if not self.allow_chat(chat_id):
            return
        await self._enqueue_message(chat_id, text)

    async def send_immediate(self, text: str, *, chat_id: int | None = None) -> str:
        target = self._chat_id if chat_id is None else chat_id
        if not self.allow_chat(target):
            raise PermissionError("Telegram chat is not allowlisted")
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(
                f"https://api.telegram.org/bot{self._token}/sendMessage",
                json={"chat_id": target, "text": text},
            )
        if response.status_code < 200 or response.status_code >= 300:
            raise RuntimeError(f"telegram send failed with status {response.status_code}")
        try:
            payload = response.json()
        except (AttributeError, ValueError):
            payload = {}
        message_id = (
            (payload.get("result") or {}).get("message_id")
            if isinstance(payload, dict)
            else None
        )
        return str(message_id) if message_id is not None else ""

    async def _enqueue_message(self, chat_id: int, text: str) -> None:
        key = self._dedupe_key(chat_id=chat_id, text=text)
        now = datetime.now(tz=timezone.utc)
        self._cleanup_dedupe(now)
        previous = self._dedupe.get(key)
        if previous is not None and (now - previous).total_seconds() < self._allow_duplicate_seconds:
            return
        self._dedupe[key] = now
        await self._queue.put(TelegramMessage(text=text, chat_id=chat_id, message_id=key))

    def _cleanup_dedupe(self, now: datetime) -> None:
        if not self._dedupe:
            return
        cutoff = now - timedelta(seconds=self._allow_duplicate_seconds)
        expired = [key for key, created_at in self._dedupe.items() if created_at < cutoff]
        for key in expired:
            self._dedupe.pop(key, None)

    def allow_chat(self, chat_id: int) -> bool:
        return chat_id in self._allowlist

    def _dedupe_key(self, *, chat_id: int, text: str) -> str:
        return hashlib.sha256(f"{chat_id}:{text}".encode("utf-8")).hexdigest()

    async def _poll_commands(self) -> None:
        async with httpx.AsyncClient(timeout=35.0) as client:
            offset = 0
            while True:
                try:
                    response = await client.get(
                        f"https://api.telegram.org/bot{self._token}/getUpdates",
                        params={"offset": offset, "timeout": 25},
                    )
                    if response.status_code != 200:
                        await asyncio.sleep(2)
                        continue
                    payload = response.json()
                    for update in payload.get("result", []):
                        offset = int(update.get("update_id", offset)) + 1
                        message = update.get("message") or update.get("edited_message") or {}
                        chat = message.get("chat") or {}
                        chat_id = chat.get("id")
                        sender = message.get("from") or {}
                        user_id = sender.get("id")
                        allowed_user = not self._user_allowlist or (
                            user_id is not None and int(user_id) in self._user_allowlist
                        )
                        if chat_id is None or not self.allow_chat(int(chat_id)) or not allowed_user:
                            logging.getLogger(__name__).warning(
                                "unauthorized Telegram command ignored"
                            )
                            continue
                        text = (message.get("text") or "").strip()
                        if not text.startswith("/"):
                            continue
                        parts = text.split()
                        cmd = parts[0].lstrip("/").split("@")[0]
                        args = parts[1:]
                        if self._command_handler is not None:
                            await self._command_handler(int(chat_id), cmd, args)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logging.getLogger(__name__).warning(
                        "Telegram polling failed error_type=%s", type(exc).__name__
                    )
                    await asyncio.sleep(2)
                    continue

    async def _worker(self) -> None:
        while True:
            message = await self._queue.get()
            if message is None:
                return
            try:
                await self.send_immediate(message.text, chat_id=message.chat_id)
            except Exception as exc:
                message.attempts += 1
                logging.getLogger(__name__).warning(
                    "Telegram queued delivery failed attempt=%s error_type=%s",
                    message.attempts,
                    type(exc).__name__,
                )
                if message.attempts < 10:
                    await self._queue.put(message)
                    await asyncio.sleep(min(30, 2 ** min(message.attempts, 5)))
                else:
                    logging.getLogger(__name__).error(
                        "Telegram queued delivery exhausted retries"
                    )
