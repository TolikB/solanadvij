"""Read-only Solana JSON-RPC adapter used for recovery and token inspection."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable, Iterable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx

from .external_journal import ExternalJournal
from .security import HolderBalance, MintInfo

_READ_METHODS = frozenset(
    {
        "getAccountInfo",
        "getBlockTime",
        "getMultipleAccounts",
        "getSignaturesForAddress",
        "getSlot",
        "getTokenAccounts",
        "getTokenLargestAccounts",
        "getTokenAccountsByOwner",
        "getTransaction",
    }
)


class SolanaRpcError(RuntimeError):
    pass


class _RpcResponse:
    def __init__(self, status_code: int, payload: Any) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> Any:
        return self._payload


class SolanaRpcClient:
    def __init__(
        self,
        endpoint: str,
        *,
        timeout_seconds: float = 5.0,
        max_retries: int = 2,
        replay_mode: bool = False,
        journal: ExternalJournal | None = None,
        record_responses: bool = False,
        recorder: Callable[..., Awaitable[str]] | None = None,
        transaction_batch_size: int = 20,
        transaction_batch_window_seconds: float = 0.05,
        rpc_requests_per_second: float = 4.0,
    ) -> None:
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._request_id = 0
        self.replay_mode = replay_mode
        self.journal = journal
        self.record_responses = record_responses
        self.recorder = recorder
        if transaction_batch_size <= 0:
            raise ValueError("transaction_batch_size must be greater than zero")
        if transaction_batch_window_seconds <= 0:
            raise ValueError(
                "transaction_batch_window_seconds must be greater than zero"
            )
        if not math.isfinite(rpc_requests_per_second) or rpc_requests_per_second <= 0:
            raise ValueError("rpc_requests_per_second must be greater than zero")
        self.transaction_batch_size = transaction_batch_size
        self.transaction_batch_window_seconds = transaction_batch_window_seconds
        self.rpc_requests_per_second = rpc_requests_per_second
        self._transaction_batch_lock = asyncio.Lock()
        self._transaction_batch_pending: list[
            tuple[dict[str, Any], asyncio.Future[_RpcResponse]]
        ] = []
        self._transaction_batch_flush_task: asyncio.Task[None] | None = None
        self._rpc_rate_limit_lock = asyncio.Lock()
        self._next_rpc_send_at = 0.0
        self._monotonic: Callable[[], float] = time.monotonic
        self._sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
        self._clock: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc)

    def set_clock(self, clock: Callable[[], datetime]) -> None:
        self._clock = clock

    async def get_transaction(self, signature: str) -> dict[str, Any] | None:
        result = await self._call(
            "getTransaction",
            [
                signature,
                {
                    "commitment": "confirmed",
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0,
                },
            ],
        )
        return result if isinstance(result, dict) else None

    async def get_signatures_for_address(
        self,
        address: str,
        *,
        until: str | None = None,
        before: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        options: dict[str, Any] = {"commitment": "confirmed", "limit": min(limit, 1000)}
        if until:
            options["until"] = until
        if before:
            options["before"] = before
        result = await self._call("getSignaturesForAddress", [address, options])
        return list(result) if isinstance(result, list) else []

    async def get_mint_info(self, mint: str) -> MintInfo:
        result = await self._call(
            "getAccountInfo", [mint, {"encoding": "jsonParsed", "commitment": "confirmed"}]
        )
        value = (result or {}).get("value") if isinstance(result, dict) else None
        if not isinstance(value, dict):
            raise SolanaRpcError("mint account is unavailable")
        parsed = ((value.get("data") or {}).get("parsed") or {}).get("info") or {}
        if not isinstance(parsed, dict):
            raise SolanaRpcError("mint account did not return parsed SPL data")
        return MintInfo(
            mint=mint,
            token_program=str(value.get("owner") or ""),
            decimals=int(parsed.get("decimals", -1)),
            total_supply_raw=Decimal(str(parsed.get("supply", "0"))),
            mint_authority=_optional_text(parsed.get("mintAuthority")),
            freeze_authority=_optional_text(parsed.get("freezeAuthority")),
            observed_at=self._clock(),
        )

    async def get_owner_token_balance(self, owner: str, mint: str) -> Decimal:
        result = await self._call(
            "getTokenAccountsByOwner",
            [
                owner,
                {"mint": mint},
                {"encoding": "jsonParsed", "commitment": "confirmed"},
            ],
        )
        items = (result or {}).get("value") if isinstance(result, dict) else None
        if not isinstance(items, list):
            raise SolanaRpcError("owner token accounts are unavailable")
        total = Decimal("0")
        for item in items:
            account = item.get("account") if isinstance(item, dict) else None
            info = (((account or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
            token_amount = info.get("tokenAmount") if isinstance(info, dict) else None
            if isinstance(token_amount, dict):
                total += Decimal(str(token_amount.get("amount") or "0"))
        return total

    async def get_largest_holders(self, mint: str) -> list[HolderBalance]:
        largest = await self._call(
            "getTokenLargestAccounts", [mint, {"commitment": "confirmed"}]
        )
        items = (largest or {}).get("value") if isinstance(largest, dict) else None
        if not isinstance(items, list):
            raise SolanaRpcError("largest token accounts are unavailable")
        addresses = [str(item.get("address")) for item in items if item.get("address")]
        accounts = await self._multiple_accounts(addresses)
        holders: list[HolderBalance] = []
        for item, account in zip(items, accounts, strict=True):
            parsed = (((account or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
            holders.append(
                HolderBalance(
                    token_account=str(item.get("address")),
                    owner=_optional_text(parsed.get("owner")),
                    amount_raw=Decimal(str(item.get("amount", "0"))),
                )
            )
        return holders

    async def get_all_holders(
        self,
        mint: str,
        *,
        expected_supply_raw: Decimal,
        maximum_index_slot_lag: int = 20,
    ) -> list[HolderBalance]:
        """Return every non-zero token account through Helius DAS pagination.

        Concentration checks must not use Solana's top-20 largest-account RPC. The
        DAS index is accepted only when every page is structurally complete and
        its oldest reported index slot is close to the confirmed chain slot.
        """

        cursor: str | None = None
        seen_cursors: set[str] = set()
        seen_accounts: set[str] = set()
        holders: list[HolderBalance] = []
        indexed_slots: list[int] = []
        while True:
            params: dict[str, Any] = {
                "mint": mint,
                "limit": 1000,
                "options": {"showZeroBalance": False},
            }
            if cursor is not None:
                params["cursor"] = cursor
            result = await self._call("getTokenAccounts", params)
            if not isinstance(result, dict):
                raise SolanaRpcError("token-account index response is unavailable")
            accounts = result.get("token_accounts")
            indexed_slot = result.get("last_indexed_slot")
            if not isinstance(accounts, list) or not isinstance(indexed_slot, int):
                raise SolanaRpcError("token-account index response is incomplete")
            indexed_slots.append(indexed_slot)
            for account in accounts:
                if not isinstance(account, dict):
                    raise SolanaRpcError("token-account index item is invalid")
                address = str(account.get("address") or "").strip()
                owner = str(account.get("owner") or "").strip()
                if not address or not owner or address in seen_accounts:
                    raise SolanaRpcError("token-account index item is incomplete or duplicated")
                seen_accounts.add(address)
                holders.append(
                    HolderBalance(
                        token_account=address,
                        owner=owner,
                        amount_raw=Decimal(str(account.get("amount") or "0")),
                    )
                )
            next_cursor = result.get("cursor")
            if next_cursor is None or str(next_cursor).strip() == "":
                break
            cursor = str(next_cursor)
            if cursor in seen_cursors:
                raise SolanaRpcError("token-account index cursor did not advance")
            seen_cursors.add(cursor)

        current_slot = await self._call("getSlot", [{"commitment": "confirmed"}])
        if not isinstance(current_slot, int) or not indexed_slots:
            raise SolanaRpcError("confirmed slot is unavailable for holder freshness check")
        if current_slot - min(indexed_slots) > maximum_index_slot_lag:
            raise SolanaRpcError("token-account index is too stale for concentration checks")
        indexed_supply = sum((holder.amount_raw for holder in holders), Decimal("0"))
        if indexed_supply != expected_supply_raw:
            raise SolanaRpcError(
                "token-account index supply does not match the mint total supply"
            )
        return holders

    async def _multiple_accounts(self, addresses: list[str]) -> list[dict[str, Any] | None]:
        if not addresses:
            return []
        result = await self._call(
            "getMultipleAccounts",
            [addresses, {"encoding": "jsonParsed", "commitment": "confirmed"}],
        )
        values = (result or {}).get("value") if isinstance(result, dict) else None
        if not isinstance(values, list) or len(values) != len(addresses):
            raise SolanaRpcError("token-account owner response is incomplete")
        return values

    async def _post_rpc(
        self,
        method: str,
        body: dict[str, Any],
    ) -> httpx.Response | _RpcResponse:
        if method == "getTransaction":
            return await self._enqueue_transaction_batch(body)
        await self._acquire_rpc_capacity(1)
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            return await client.post(self.endpoint, json=body)

    async def _acquire_rpc_capacity(self, request_count: int) -> None:
        """Pace sustained RPC load, counting each JSON-RPC batch item."""
        if request_count <= 0:
            raise ValueError("request_count must be greater than zero")
        async with self._rpc_rate_limit_lock:
            await self._wait_for_rpc_capacity()
            self._reserve_rpc_capacity(request_count)

    async def _acquire_transaction_batch_capacity(
        self,
        batch: list[tuple[dict[str, Any], asyncio.Future[_RpcResponse]]],
    ) -> list[tuple[dict[str, Any], asyncio.Future[_RpcResponse]]]:
        async with self._rpc_rate_limit_lock:
            active = [item for item in batch if not item[1].done()]
            if not active:
                return []
            await self._wait_for_rpc_capacity()
            active = [item for item in active if not item[1].done()]
            if not active:
                return []
            self._reserve_rpc_capacity(len(active))
            return active

    async def _wait_for_rpc_capacity(self) -> None:
        delay = max(0.0, self._next_rpc_send_at - self._monotonic())
        if delay > 0:
            await self._sleep(delay)

    def _reserve_rpc_capacity(self, request_count: int) -> None:
        now = self._monotonic()
        self._next_rpc_send_at = (
            max(now, self._next_rpc_send_at)
            + request_count / self.rpc_requests_per_second
        )

    async def _enqueue_transaction_batch(
        self,
        body: dict[str, Any],
    ) -> _RpcResponse:
        future: asyncio.Future[_RpcResponse] = (
            asyncio.get_running_loop().create_future()
        )
        async with self._transaction_batch_lock:
            self._transaction_batch_pending.append((body, future))
            if self._transaction_batch_flush_task is None:
                self._start_transaction_batch_worker()
        try:
            return await future
        except asyncio.CancelledError:
            future.cancel()
            raise

    def _start_transaction_batch_worker(self) -> None:
        task = asyncio.create_task(
            self._transaction_batch_worker(),
            name="solana-transaction-rpc-batcher",
        )
        self._transaction_batch_flush_task = task
        task.add_done_callback(self._transaction_batch_worker_done)

    def _transaction_batch_worker_done(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled():
            task.exception()
            return
        loop = task.get_loop()
        if loop.is_closed():
            return
        loop.create_task(
            self._settle_prestart_cancelled_batch_worker(task),
            name="solana-transaction-rpc-batcher-cleanup",
        )

    async def _settle_prestart_cancelled_batch_worker(
        self,
        task: asyncio.Task[None],
    ) -> None:
        async with self._transaction_batch_lock:
            if self._transaction_batch_flush_task is not task:
                return
            pending = self._transaction_batch_pending
            self._transaction_batch_pending = []
            self._transaction_batch_flush_task = None
        for _body, future in pending:
            future.cancel()

    async def _transaction_batch_worker(self) -> None:
        current_task = asyncio.current_task()
        cancelled = False
        pending_to_cancel: list[
            tuple[dict[str, Any], asyncio.Future[_RpcResponse]]
        ] = []
        try:
            await asyncio.sleep(self.transaction_batch_window_seconds)
            while True:
                async with self._transaction_batch_lock:
                    if not self._transaction_batch_pending:
                        return
                    candidates = self._transaction_batch_pending[
                        : self.transaction_batch_size
                    ]
                    del self._transaction_batch_pending[
                        : self.transaction_batch_size
                    ]
                batch = [
                    (body, future)
                    for body, future in candidates
                    if not future.done()
                ]
                if batch:
                    await self._send_transaction_batch(batch)
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            async with self._transaction_batch_lock:
                if cancelled:
                    pending_to_cancel = self._transaction_batch_pending
                    self._transaction_batch_pending = []
                if self._transaction_batch_flush_task is current_task:
                    self._transaction_batch_flush_task = None
                if (
                    not cancelled
                    and self._transaction_batch_pending
                    and self._transaction_batch_flush_task is None
                ):
                    self._start_transaction_batch_worker()
            for _body, future in pending_to_cancel:
                future.cancel()

    async def _send_transaction_batch(
        self,
        batch: list[tuple[dict[str, Any], asyncio.Future[_RpcResponse]]],
    ) -> None:
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                batch = await self._acquire_transaction_batch_capacity(batch)
                if not batch:
                    return
                batch = [
                    (body, future)
                    for body, future in batch
                    if not future.done()
                ]
                if not batch:
                    return
                response = await client.post(
                    self.endpoint,
                    json=[body for body, _future in batch],
                )
        except asyncio.CancelledError:
            for _body, future in batch:
                future.cancel()
            raise
        except Exception as exc:
            for _body, future in batch:
                if not future.done():
                    future.set_exception(exc)
            return

        if response.status_code < 200 or response.status_code >= 300:
            for _body, future in batch:
                if not future.done():
                    future.set_result(_RpcResponse(response.status_code, {}))
            return

        try:
            payload = response.json()
        except Exception as exc:
            for _body, future in batch:
                if not future.done():
                    future.set_exception(exc)
            return

        expected_ids = {body["id"] for body, _future in batch}
        rows_by_id: dict[int, dict[str, Any]] = {}
        invalid_batch = not isinstance(payload, list)
        if isinstance(payload, list):
            for row in payload:
                if not isinstance(row, dict):
                    invalid_batch = True
                    continue
                response_id = row.get("id")
                if (
                    type(response_id) is not int
                    or response_id not in expected_ids
                    or response_id in rows_by_id
                ):
                    invalid_batch = True
                    continue
                rows_by_id[response_id] = row
            if set(rows_by_id) != expected_ids:
                invalid_batch = True

        shared_error = (
            payload
            if isinstance(payload, dict) and isinstance(payload.get("error"), dict)
            else None
        )
        invalid_response = {
            "error": {
                "code": "BATCH_INVALID",
                "message": "Solana RPC batch response IDs are invalid or incomplete",
            }
        }
        for body, future in batch:
            if future.done():
                continue
            if shared_error is not None:
                row = shared_error
            elif invalid_batch:
                row = invalid_response
            else:
                row = rows_by_id[body["id"]]
            future.set_result(_RpcResponse(response.status_code, row))

    async def _call(
        self, method: str, params: Iterable[Any] | dict[str, Any]
    ) -> Any:
        if method not in _READ_METHODS:
            raise SolanaRpcError(f"RPC method is not permitted in paper release: {method}")
        params_payload: list[Any] | dict[str, Any]
        params_payload = dict(params) if isinstance(params, dict) else list(params)
        journal_key = ExternalJournal.key("solana_rpc", method, params_payload)
        stored = self.journal.get(journal_key) if self.journal else None
        if stored is not None:
            return stored.get("response")
        if self.replay_mode:
            raise SolanaRpcError(f"replay data missing for Solana RPC method {method}")
        self._request_id += 1
        body = {
            "jsonrpc": "2.0",
            "id": self._request_id,
            "method": method,
            "params": params_payload,
        }
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            requested_at = datetime.now(tz=timezone.utc)
            started = __import__("time").perf_counter()
            try:
                response = await self._post_rpc(method, body)
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt < self.max_retries:
                        await asyncio.sleep(0.25 * (2**attempt))
                        continue
                if response.status_code < 200 or response.status_code >= 300:
                    await self._record_call(
                        method, params_payload, None, requested_at,
                        int((__import__("time").perf_counter() - started) * 1000),
                        response.status_code, f"HTTP_{response.status_code}",
                    )
                    raise SolanaRpcError(
                        f"Solana RPC {method} rejected status={response.status_code}"
                    )
                payload = response.json()
                if payload.get("error"):
                    error = payload["error"]
                    code = error.get("code") if isinstance(error, dict) else "unknown"
                    await self._record_call(
                        method, params_payload, payload, requested_at,
                        int((__import__("time").perf_counter() - started) * 1000),
                        response.status_code, f"RPC_{code}",
                    )
                    raise SolanaRpcError(f"Solana RPC {method} failed with code {code}")
                result = payload.get("result")
                if self.journal is not None and self.record_responses:
                    self.journal.record(journal_key, result)
                await self._record_call(
                    method, params_payload, {"result": result}, requested_at,
                    int((__import__("time").perf_counter() - started) * 1000),
                    response.status_code, None,
                )
                return result
            except (httpx.TimeoutException, httpx.NetworkError, httpx.DecodingError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    await asyncio.sleep(0.25 * (2**attempt))
                    continue
                break
        raise SolanaRpcError(f"Solana RPC {method} unavailable") from last_error

    async def _record_call(
        self,
        method: str,
        params: list[Any] | dict[str, Any],
        response: dict[str, Any] | None,
        requested_at: datetime,
        latency_ms: int,
        status: int,
        error_code: str | None,
    ) -> None:
        if self.recorder is None:
            return
        await self.recorder(
            provider="solana_rpc",
            endpoint=method,
            request_json={"params": params},
            response_json=response,
            requested_at=requested_at,
            received_at=datetime.now(tz=timezone.utc),
            latency_ms=latency_ms,
            http_status=status,
            error_code=error_code,
        )


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
