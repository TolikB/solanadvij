"""Durable Telegram outbox delivery with leases and bounded retries."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from .database import Database
from .metrics import BotMetrics

logger = logging.getLogger(__name__)

PROACTIVE_TELEGRAM_EVENT_TYPES = frozenset(
    {"system_start", "system_stop", "daily_report", "daily_report_unavailable"}
)


class TelegramOutboxWorker:
    def __init__(
        self,
        database: Database,
        notifier: Any,
        *,
        metrics: BotMetrics,
        poll_seconds: float = 1.0,
        allowed_event_types: frozenset[str] = PROACTIVE_TELEGRAM_EVENT_TYPES,
    ) -> None:
        self.database = database
        self.notifier = notifier
        self.metrics = metrics
        self.poll_seconds = poll_seconds
        self.allowed_event_types = allowed_event_types
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="telegram-outbox")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def drain(self, *, timeout_seconds: float = 5.0) -> bool:
        """Wait for deliverable messages without treating terminal ambiguity as pending."""
        if timeout_seconds <= 0:
            raise ValueError("outbox drain timeout must be positive")

        async def wait_until_idle() -> None:
            while await self.database.deliverable_outbox_count() > 0:
                if self._task is None or self._task.done():
                    await self.deliver_once()
                else:
                    await asyncio.sleep(min(self.poll_seconds, 0.1))

        try:
            await asyncio.wait_for(wait_until_idle(), timeout=timeout_seconds)
        except TimeoutError:
            return False
        except Exception:
            logger.exception("telegram outbox drain failed")
            return False
        return True

    async def deliver_once(self) -> int:
        delivered = 0
        for event in await self.database.pending_outbox(limit=100):
            claim_token = event.claim_token
            if claim_token is None:
                raise RuntimeError("claimed outbox event has no fencing token")
            if event.event_type not in self.allowed_event_types:
                await self.database.mark_outbox_dead(
                    event.id, claim_token, "TELEGRAM_POLICY_BLOCKED"
                )
                self.metrics.telegram_messages.labels(status="suppressed").inc()
                continue
            payload = event.payload_json
            text = str(payload.get("text") or "").strip()
            chat_id = payload.get("chat_id")
            if not text:
                await self.database.mark_outbox_dead(
                    event.id, claim_token, "INVALID_EMPTY_MESSAGE"
                )
                self.metrics.telegram_messages.labels(status="invalid").inc()
                continue
            try:
                if hasattr(self.notifier, "send_immediate"):
                    message_id = await self.notifier.send_immediate(
                        text, chat_id=int(chat_id) if chat_id is not None else None
                    )
                elif chat_id is None:
                    await self.notifier.send(text)
                    message_id = None
                else:
                    await self.notifier.send_to(int(chat_id), text)
                    message_id = None
            except (PermissionError, ValueError) as exc:
                error_code = type(exc).__name__.upper()
                await self.database.mark_outbox_dead(event.id, claim_token, error_code)
                logger.warning(
                    "telegram outbox event permanently rejected error_type=%s",
                    type(exc).__name__,
                )
                self.metrics.telegram_messages.labels(status="failed").inc()
                continue
            except RuntimeError as exc:
                error_code = type(exc).__name__.upper()
                await self.database.mark_outbox_failed(event.id, claim_token, error_code)
                logger.warning(
                    "telegram outbox send rejected error_type=%s", type(exc).__name__
                )
                self.metrics.telegram_messages.labels(status="failed").inc()
                continue
            except Exception as exc:
                error_code = type(exc).__name__.upper()
                await self.database.mark_outbox_uncertain(
                    event.id, claim_token, error_code
                )
                logger.error(
                    "telegram outbox delivery outcome is uncertain error_type=%s",
                    type(exc).__name__,
                )
                self.metrics.telegram_messages.labels(status="failed").inc()
                continue
            await self.database.mark_outbox_delivered(
                event.id, claim_token, message_id
            )
            self.metrics.telegram_messages.labels(status="delivered").inc()
            delivered += 1
        return delivered

    async def _run(self) -> None:
        while True:
            try:
                delivered = await self.deliver_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("telegram outbox polling failed")
                delivered = 0
            if delivered == 0:
                await asyncio.sleep(self.poll_seconds)
