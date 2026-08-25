"""High-level orchestrator helpers for paper/replay execution."""

from __future__ import annotations

from decimal import Decimal

from .errors import ExecutionBlockedError
from .exit_engine import ExitPolicy
from .runtime import SniperRuntime


class PaperService:
    """Small orchestrator wrapper to isolate paper actions from API/loop callers."""

    def __init__(self, runtime: SniperRuntime) -> None:
        self._runtime = runtime

    @property
    def runtime(self) -> SniperRuntime:
        return self._runtime

    async def open_position(
        self,
        token_mint: str,
        usdc_amount: Decimal,
        *,
        order_id: str | None = None,
    ) -> str:
        if self._runtime.broker is None:
            raise RuntimeError("broker is disabled in record mode")
        try:
            result = await self._runtime.broker.open(token_mint, usdc_amount, order_id=order_id)
        except ExecutionBlockedError as exc:
            await self._notify_trade_alert(
                f"trade alert blocked: open token={token_mint} usdc={usdc_amount} reason={exc}"
            )
            await self._notify_risk_alert(
                f"risk alert: blocked open token={token_mint} usdc={usdc_amount} reason={exc}"
            )
            raise
        await self._notify_trade_alert(
            f"trade alert: open token={token_mint} usdc={usdc_amount} fill_id={result.fill_id}"
        )
        return result.fill_id

    async def close_position(
        self,
        token_mint: str,
        token_amount: Decimal,
        *,
        order_id: str | None = None,
        exit_reason: str | None = None,
    ) -> str:
        if self._runtime.broker is None:
            raise RuntimeError("broker is disabled in record mode")
        try:
            result = await self._runtime.broker.close(
                token_mint,
                token_amount,
                order_id=order_id,
                exit_reason=exit_reason,
            )
        except Exception as exc:
            await self._notify_system_alert(
                f"system alert: close failed token={token_mint} amount={token_amount} error={exc}"
            )
            raise
        await self._notify_trade_alert(
            "trade alert: close token={token_mint} amount={amount} close_fill_id={fill_id} exit_reason={reason}".format(
                token_mint=token_mint,
                amount=token_amount,
                fill_id=result.fill_id,
                reason=exit_reason or "manual",
            )
        )
        return result.fill_id

    async def close_half(
        self,
        token_mint: str,
        *,
        order_id: str | None = None,
    ) -> str:
        if self._runtime.broker is None:
            raise RuntimeError("broker is disabled in record mode")
        try:
            result = await self._runtime.broker.close_half(token_mint, order_id=order_id)
        except Exception as exc:
            await self._notify_system_alert(
                f"system alert: close_half failed token={token_mint} error={exc}"
            )
            raise
        await self._notify_trade_alert(
            f"trade alert: close_half token={token_mint} fill_id={result.fill_id}"
        )
        return result.fill_id

    async def process_exits(
        self,
        *,
        policy: ExitPolicy | None = None,
        max_positions: int | None = None,
    ) -> int:
        if self._runtime.broker is None:
            raise RuntimeError("broker is disabled in record mode")
        decisions = await self._runtime.evaluate_and_close_exits(policy=policy, max_positions=max_positions)
        return sum(1 for decision in decisions if decision.should_exit)

    async def _notify_trade_alert(self, message: str) -> None:
        return

    async def _notify_risk_alert(self, message: str) -> None:
        return

    async def _notify_system_alert(self, message: str) -> None:
        return
