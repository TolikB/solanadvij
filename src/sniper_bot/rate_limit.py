"""Provider rate limiting that serves urgent requests first."""

from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from collections.abc import Awaitable, Callable
from enum import IntEnum


class QuotePriority(IntEnum):
    """Lower values are served first when requests queue for the rate limit."""

    # Selling or marking an open position decides a realized loss.
    EXIT = 0
    # A confirmed entry signal waits for its fill quote.
    ENTRY = 1
    # The SOL/USD reference price keeps every pool's USD values fresh.
    REFERENCE = 2
    # Round trips that feed security checks and scores of candidates.
    SECURITY = 3


class PriorityRateLimiter:
    """Grant one request per interval, the most urgent waiter first.

    A plain lock serves waiters in arrival order, so a burst of candidate
    security quotes could hold a stop-loss sell behind it for seconds. Here
    every waiter queues with a priority and the next free slot goes to the
    most urgent one (first come, first served within a priority).
    """

    def __init__(
        self,
        interval_seconds: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if interval_seconds < 0:
            raise ValueError("interval_seconds must be non-negative")
        self._interval = interval_seconds
        self._clock = clock
        self._sleep = sleep
        self._next_slot_at = float("-inf")
        self._waiters: list[tuple[int, int, asyncio.Future[None]]] = []
        self._sequence = itertools.count()
        self._dispatcher: asyncio.Task[None] | None = None

    @property
    def queued(self) -> int:
        return sum(1 for _, _, future in self._waiters if not future.done())

    async def acquire(self, priority: int = QuotePriority.ENTRY) -> None:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        heapq.heappush(self._waiters, (int(priority), next(self._sequence), future))
        if self._dispatcher is None or self._dispatcher.done():
            self._dispatcher = loop.create_task(
                self._dispatch(), name="provider-rate-limit"
            )
        try:
            await future
        except asyncio.CancelledError:
            future.cancel()
            raise

    async def _dispatch(self) -> None:
        while self._waiters:
            delay = self._next_slot_at - self._clock()
            if delay > 0:
                # Requests that arrive meanwhile compete for this slot too.
                await self._sleep(delay)
            while self._waiters:
                _, _, future = heapq.heappop(self._waiters)
                if future.done():
                    continue
                future.set_result(None)
                self._next_slot_at = self._clock() + self._interval
                break
