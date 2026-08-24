"""Event-time wallet profiling, relation evidence, and developer history."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import combinations
from statistics import median
from typing import Any

from pydantic import BaseModel

from .clustering import RelationEvidence, WalletRelation, build_clusters, score_relation
from .events import ChainEventType, EventEnvelope


class WalletProfile(BaseModel):
    wallet_address: str
    first_seen_at: datetime
    initial_funder: str | None = None
    funding_signature: str | None = None
    known_creator: bool = False
    tokens_created_total: int = 0
    tokens_created_7d: int = 0
    tokens_created_30d: int = 0
    tokens_reaching_pumpswap: int = 0
    tokens_reaching_2x_executable: int = 0
    tokens_with_liquidity_rug: int = 0
    tokens_with_dev_dump_5m: int = 0
    median_peak_return: Decimal = Decimal("0")
    median_token_lifetime_seconds: Decimal = Decimal("0")
    median_dev_sell_delay_seconds: Decimal = Decimal("0")
    last_token_created_at: datetime | None = None
    known_funding_cluster: str | None = None
    tokens_traded: int = 0
    profile_updated_at: datetime


class TokenOutcome(BaseModel):
    mint: str
    creator: str
    created_at: datetime
    reached_pumpswap: bool = False
    peak_return: Decimal = Decimal("0")
    liquidity_rug: bool = False
    dev_sell_delay_seconds: Decimal | None = None
    lifetime_seconds: Decimal | None = None


class WalletAnalyzer:
    """Maintains only observations available by each requested event time."""

    def __init__(self) -> None:
        self._profiles: dict[str, WalletProfile] = {}
        self._baselines: dict[str, WalletProfile] = {}
        self._outcomes: dict[str, TokenOutcome] = {}
        self._relations: dict[tuple[str, str], WalletRelation] = {}
        self._buyers: dict[str, list[tuple[datetime, str, Decimal, int, str | None]]] = defaultdict(list)
        self._seen_event_ids: set[str] = set()

    def restore(
        self, profiles: list[WalletProfile], relations: list[WalletRelation]
    ) -> None:
        for profile in profiles:
            self._profiles[profile.wallet_address] = profile
            self._baselines[profile.wallet_address] = profile
        for relation in relations:
            self._relations[(relation.wallet_a, relation.wallet_b)] = relation

    def observe(self, event: EventEnvelope) -> tuple[list[WalletProfile], list[WalletRelation]]:
        if event.event_id in self._seen_event_ids:
            return [], []
        self._seen_event_ids.add(event.event_id)
        payload = event.payload
        wallet = _text(payload.get("user") or payload.get("creator"))
        creator = _text(payload.get("creator"))
        changed: list[WalletProfile] = []
        if wallet:
            changed.append(
                self._touch_profile(
                    wallet, event.observed_at,
                    initial_funder=_text(payload.get("initial_funder") or payload.get("funder")),
                    funding_signature=_text(payload.get("funding_signature")),
                    known_creator=event.event_type == ChainEventType.TOKEN_CREATED,
                )
            )
        if event.event_type == ChainEventType.TOKEN_CREATED and event.mint and creator:
            self._outcomes[event.mint] = TokenOutcome(
                mint=event.mint, creator=creator, created_at=event.block_time
            )
            changed.append(self._rebuild_developer_profile(creator, event.block_time))
        outcome = self._outcomes.get(event.mint or "")
        if outcome is not None:
            update: dict[str, Any] = {}
            if event.event_type in {ChainEventType.MIGRATION, ChainEventType.POOL_CREATED}:
                update["reached_pumpswap"] = True
            if event.event_type == ChainEventType.SWAP_SELL and wallet == outcome.creator:
                update["dev_sell_delay_seconds"] = max(
                    Decimal("0"), Decimal(str((event.block_time - outcome.created_at).total_seconds()))
                )
            peak = _decimal(payload.get("return_since_pool_creation"))
            if peak is not None and peak > outcome.peak_return:
                update["peak_return"] = peak
            liquidity_change = _decimal(payload.get("liquidity_change_30s"))
            if liquidity_change is not None and liquidity_change <= Decimal("-0.8"):
                update["liquidity_rug"] = True
            if update:
                self._outcomes[event.mint or ""] = outcome.model_copy(update=update)
                changed.append(self._rebuild_developer_profile(outcome.creator, event.block_time))
        if event.event_type == ChainEventType.SWAP_BUY and event.mint and wallet:
            amount = _decimal(
                payload.get("quote_amount_in") or payload.get("base_amount_out") or payload.get("token_amount")
            ) or Decimal("0")
            self._buyers[event.mint].append(
                (event.block_time, wallet, amount, event.slot, _text(payload.get("bundle_id")))
            )
        relations = self._recompute_relations(event.mint, event.block_time) if event.mint else []
        return list({item.wallet_address: item for item in changed}.values()), relations

    def profile(self, wallet: str | None, *, at: datetime | None = None) -> WalletProfile | None:
        if not wallet or wallet not in self._profiles:
            return None
        return self._rebuild_developer_profile(wallet, at or datetime.now(tz=timezone.utc))

    def first_buyers(self, mint: str, *, at: datetime, limit: int = 20) -> set[str]:
        ordered = sorted(
            (item for item in self._buyers.get(mint, []) if item[0] <= at),
            key=lambda item: (item[0], item[1]),
        )
        result: list[str] = []
        for _, wallet, _, _, _ in ordered:
            if wallet not in result:
                result.append(wallet)
        return set(result[:limit])

    def cluster_for(self, wallet: str | None, scope: set[str]) -> set[str]:
        if not wallet:
            return set()
        wallets = set(scope) | {wallet}
        relations = [
            relation for relation in self._relations.values()
            if relation.wallet_a in wallets and relation.wallet_b in wallets
        ]
        for cluster in build_clusters(wallets, relations):
            if wallet in cluster.wallets:
                return set(cluster.wallets)
        return {wallet}

    def largest_related_cluster(
        self, scope: set[str], *, excluded: set[str] | None = None
    ) -> set[str]:
        excluded = excluded or set()
        relations = [
            relation for relation in self._relations.values()
            if relation.wallet_a in scope and relation.wallet_b in scope
        ]
        clusters = [
            set(cluster.wallets) - excluded for cluster in build_clusters(scope, relations)
        ]
        clusters = [cluster for cluster in clusters if len(cluster) > 1]
        return max(clusters, key=lambda item: (len(item), sorted(item)), default=set())

    def is_same_funder_cluster(self, mint: str, wallet: str) -> bool:
        profile = self._profiles.get(wallet)
        if profile is None or not profile.initial_funder:
            return False
        buyers = {item[1] for item in self._buyers.get(mint, [])}
        return sum(
            1 for buyer in buyers
            if self._profiles.get(buyer)
            and self._profiles[buyer].initial_funder == profile.initial_funder
        ) >= 2

    def _touch_profile(
        self, wallet: str, observed_at: datetime, *, initial_funder: str | None,
        funding_signature: str | None, known_creator: bool,
    ) -> WalletProfile:
        current = self._profiles.get(wallet)
        if current is None:
            current = WalletProfile(
                wallet_address=wallet, first_seen_at=observed_at,
                initial_funder=initial_funder, funding_signature=funding_signature,
                known_creator=known_creator, profile_updated_at=observed_at,
            )
        else:
            current = current.model_copy(update={
                "initial_funder": current.initial_funder or initial_funder,
                "funding_signature": current.funding_signature or funding_signature,
                "known_creator": current.known_creator or known_creator,
                "profile_updated_at": max(current.profile_updated_at, observed_at),
            })
        self._profiles[wallet] = current
        return current

    def _rebuild_developer_profile(self, wallet: str, at: datetime) -> WalletProfile:
        profile = self._profiles.get(wallet) or self._touch_profile(
            wallet, at, initial_funder=None, funding_signature=None, known_creator=True
        )
        baseline = self._baselines.get(wallet)
        outcomes = [
            item for item in self._outcomes.values()
            if item.creator == wallet
            and item.created_at <= at
            and (baseline is None or item.created_at > baseline.profile_updated_at)
        ]
        base_total = baseline.tokens_created_total if baseline else 0
        base_7d = baseline.tokens_created_7d if baseline else 0
        base_30d = baseline.tokens_created_30d if baseline else 0
        base_pumpswap = baseline.tokens_reaching_pumpswap if baseline else 0
        base_success = baseline.tokens_reaching_2x_executable if baseline else 0
        base_rugs = baseline.tokens_with_liquidity_rug if baseline else 0
        base_dumps = baseline.tokens_with_dev_dump_5m if baseline else 0
        profile = profile.model_copy(update={
            "known_creator": bool(outcomes) or profile.known_creator,
            "tokens_created_total": base_total + len(outcomes),
            "tokens_created_7d": base_7d + sum(item.created_at >= at - timedelta(days=7) for item in outcomes),
            "tokens_created_30d": base_30d + sum(item.created_at >= at - timedelta(days=30) for item in outcomes),
            "tokens_reaching_pumpswap": base_pumpswap + sum(item.reached_pumpswap for item in outcomes),
            "tokens_reaching_2x_executable": base_success + sum(item.peak_return >= Decimal("1") for item in outcomes),
            "tokens_with_liquidity_rug": base_rugs + sum(item.liquidity_rug for item in outcomes),
            "tokens_with_dev_dump_5m": base_dumps + sum(
                item.dev_sell_delay_seconds is not None and item.dev_sell_delay_seconds <= Decimal("300")
                for item in outcomes
            ),
            "median_peak_return": _median(
                ([baseline.median_peak_return] if baseline and base_total else [])
                + [item.peak_return for item in outcomes]
            ),
            "median_token_lifetime_seconds": _median(
                ([baseline.median_token_lifetime_seconds] if baseline and base_total else [])
                + [item.lifetime_seconds for item in outcomes if item.lifetime_seconds is not None]
            ),
            "median_dev_sell_delay_seconds": _median(
                ([baseline.median_dev_sell_delay_seconds] if baseline and base_total else [])
                + [item.dev_sell_delay_seconds for item in outcomes if item.dev_sell_delay_seconds is not None]
            ),
            "last_token_created_at": max(
                [item.created_at for item in outcomes]
                + ([baseline.last_token_created_at] if baseline and baseline.last_token_created_at else []),
                default=None,
            ),
            "profile_updated_at": at,
        })
        self._profiles[wallet] = profile
        return profile

    def _recompute_relations(self, mint: str | None, at: datetime) -> list[WalletRelation]:
        if not mint:
            return []
        buys = [item for item in self._buyers.get(mint, []) if item[0] <= at]
        first: list[str] = []
        volume: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        for _, wallet, amount, _, _ in buys:
            if wallet not in first:
                first.append(wallet)
            volume[wallet] += amount
        top_volume = [item[0] for item in sorted(volume.items(), key=lambda item: (-item[1], item[0]))[:20]]
        scope = set(first[:20]) | set(top_volume)
        by_wallet = {wallet: [item for item in buys if item[1] == wallet] for wallet in scope}
        relations: list[WalletRelation] = []
        for left, right in combinations(sorted(scope), 2):
            left_profile, right_profile = self._profiles.get(left), self._profiles.get(right)
            left_buys, right_buys = by_wallet[left], by_wallet[right]
            relation = score_relation(left, right, RelationEvidence(
                same_initial_funder=bool(
                    left_profile and right_profile and left_profile.initial_funder
                    and left_profile.initial_funder == right_profile.initial_funder
                ),
                same_slot_buying=bool({item[3] for item in left_buys} & {item[3] for item in right_buys}),
                same_transaction_bundle_heuristic=bool(
                    {item[4] for item in left_buys if item[4]} & {item[4] for item in right_buys if item[4]}
                ),
                identical_trade_amount_pattern=bool(
                    {item[2] for item in left_buys if item[2] > 0}
                    & {item[2] for item in right_buys if item[2] > 0}
                ),
            ))
            if relation.evidence:
                self._relations[(relation.wallet_a, relation.wallet_b)] = relation
                relations.append(relation)
        return relations


def _median(values: list[Decimal]) -> Decimal:
    return Decimal(str(median(values))) if values else Decimal("0")


def _text(value: object) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None
