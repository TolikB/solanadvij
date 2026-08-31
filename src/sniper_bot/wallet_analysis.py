"""Event-time wallet profiling, relation evidence, and developer history."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
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


@dataclass(slots=True)
class _BuyerEvidence:
    total_amount: Decimal = Decimal("0")
    slots: set[int] = field(default_factory=set)
    bundles: set[str] = field(default_factory=set)
    amounts: set[Decimal] = field(default_factory=set)

class WalletAnalyzer:
    """Maintains only observations available by each requested event time."""

    def __init__(self) -> None:
        self._profiles: dict[str, WalletProfile] = {}
        self._baselines: dict[str, WalletProfile] = {}
        self._outcomes: dict[str, TokenOutcome] = {}
        self._relations: dict[tuple[str, str], WalletRelation] = {}
        self._buyers: dict[
            str,
            list[tuple[datetime, str, Decimal, int, str | None]],
        ] = defaultdict(list)
        self._buyer_first_order: dict[str, list[str]] = defaultdict(list)
        self._buyer_first_members: dict[str, set[str]] = defaultdict(set)
        self._buyer_top_volume: dict[str, list[str]] = defaultdict(list)
        self._buyer_evidence: dict[str, dict[str, _BuyerEvidence]] = defaultdict(dict)
        self._buyer_last_order_key: dict[str, tuple[datetime, str]] = {}
        self._seen_event_ids: set[str] = set()

    def restore(
        self, profiles: list[WalletProfile], relations: list[WalletRelation]
    ) -> None:
        for profile in profiles:
            self._profiles[profile.wallet_address] = profile
            self._baselines[profile.wallet_address] = profile
        for relation in relations:
            self._relations[(relation.wallet_a, relation.wallet_b)] = relation

    def observe(
        self,
        event: EventEnvelope,
    ) -> tuple[list[WalletProfile], list[WalletRelation]]:
        if event.event_id in self._seen_event_ids:
            return [], []
        self._seen_event_ids.add(event.event_id)
        payload = event.payload
        wallet = _text(payload.get("user") or payload.get("creator"))
        creator = _text(payload.get("creator"))
        changed: list[WalletProfile] = []
        relation_wallets: set[str] = set()
        relation_state: tuple[dict[str, _BuyerEvidence], set[str]] | None = None
        if wallet:
            previous = self._profiles.get(wallet)
            profile = self._touch_profile(
                wallet,
                event.observed_at,
                initial_funder=_text(
                    payload.get("initial_funder") or payload.get("funder")
                ),
                funding_signature=_text(payload.get("funding_signature")),
                known_creator=event.event_type == ChainEventType.TOKEN_CREATED,
            )
            if profile != previous:
                changed.append(profile)
            if (
                profile.initial_funder
                and (previous is None or previous.initial_funder != profile.initial_funder)
            ):
                relation_wallets.add(wallet)
        if event.event_type == ChainEventType.TOKEN_CREATED and event.mint and creator:
            self._outcomes[event.mint] = TokenOutcome(
                mint=event.mint,
                creator=creator,
                created_at=event.block_time,
            )
            changed.append(
                self._rebuild_developer_profile(creator, event.block_time)
            )
        outcome = self._outcomes.get(event.mint or "")
        if outcome is not None:
            update: dict[str, Any] = {}
            if event.event_type in {
                ChainEventType.MIGRATION,
                ChainEventType.POOL_CREATED,
            }:
                update["reached_pumpswap"] = True
            if event.event_type == ChainEventType.SWAP_SELL and wallet == outcome.creator:
                update["dev_sell_delay_seconds"] = max(
                    Decimal("0"),
                    Decimal(
                        str(
                            (
                                event.block_time - outcome.created_at
                            ).total_seconds()
                        )
                    ),
                )
            peak = _decimal(payload.get("return_since_pool_creation"))
            if peak is not None and peak > outcome.peak_return:
                update["peak_return"] = peak
            liquidity_change = _decimal(payload.get("liquidity_change_30s"))
            if (
                liquidity_change is not None
                and liquidity_change <= Decimal("-0.8")
            ):
                update["liquidity_rug"] = True
            if update:
                self._outcomes[event.mint or ""] = outcome.model_copy(
                    update=update
                )
                changed.append(
                    self._rebuild_developer_profile(
                        outcome.creator,
                        event.block_time,
                    )
                )
        if (
            event.event_type == ChainEventType.SWAP_BUY
            and event.mint
            and wallet
        ):
            amount = _decimal(
                payload.get("quote_amount_in")
                or payload.get("base_amount_out")
                or payload.get("token_amount")
            ) or Decimal("0")
            bundle = _text(payload.get("bundle_id"))
            relation_state = self._observe_buyer(
                event.mint,
                event.block_time,
                wallet,
                amount,
                event.slot,
                bundle,
            )
            relation_wallets.add(wallet)
        relations = (
            self._recompute_relations(
                relation_wallets,
                evidence_by_wallet=relation_state[0],
                scope=relation_state[1],
            )
            if event.mint and relation_wallets and relation_state is not None
            else []
        )
        return (
            list({item.wallet_address: item for item in changed}.values()),
            relations,
        )
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
        self,
        wallet: str,
        observed_at: datetime,
        *,
        initial_funder: str | None,
        funding_signature: str | None,
        known_creator: bool,
    ) -> WalletProfile:
        current = self._profiles.get(wallet)
        if current is None:
            current = WalletProfile(
                wallet_address=wallet,
                first_seen_at=observed_at,
                initial_funder=initial_funder,
                funding_signature=funding_signature,
                known_creator=known_creator,
                profile_updated_at=observed_at,
            )
            self._profiles[wallet] = current
            return current
        update: dict[str, Any] = {}
        if current.initial_funder is None and initial_funder is not None:
            update["initial_funder"] = initial_funder
        if current.funding_signature is None and funding_signature is not None:
            update["funding_signature"] = funding_signature
        if known_creator and not current.known_creator:
            update["known_creator"] = True
        if update:
            update["profile_updated_at"] = max(
                current.profile_updated_at,
                observed_at,
            )
            current = current.model_copy(update=update)
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

    def _observe_buyer(
        self,
        mint: str,
        at: datetime,
        wallet: str,
        amount: Decimal,
        slot: int,
        bundle: str | None,
    ) -> tuple[dict[str, _BuyerEvidence], set[str]]:
        buyers = self._buyers[mint]
        order_key = (at, wallet)
        previous_order_key = self._buyer_last_order_key.get(mint)
        out_of_order = (
            previous_order_key is not None and order_key < previous_order_key
        )
        buyers.append((at, wallet, amount, slot, bundle))
        if out_of_order:
            buyers.sort(key=lambda item: (item[0], item[1]))
            historical_state = self._historical_buyer_state(mint, at)
            self._rebuild_buyer_state(mint)
            return historical_state

        self._buyer_last_order_key[mint] = order_key
        evidence = self._buyer_evidence[mint].setdefault(
            wallet,
            _BuyerEvidence(),
        )
        self._accumulate_evidence(evidence, amount, slot, bundle)
        first_members = self._buyer_first_members[mint]
        if wallet not in first_members and len(first_members) < 20:
            first_members.add(wallet)
            self._buyer_first_order[mint].append(wallet)
        self._update_top_volume(mint, wallet)
        return self._buyer_evidence[mint], self._relation_scope(mint)

    @staticmethod
    def _accumulate_evidence(
        evidence: _BuyerEvidence,
        amount: Decimal,
        slot: int,
        bundle: str | None,
    ) -> None:
        evidence.total_amount += amount
        evidence.slots.add(slot)
        if bundle:
            evidence.bundles.add(bundle)
        if amount > 0:
            evidence.amounts.add(amount)

    def _relation_scope(self, mint: str) -> set[str]:
        return set(self._buyer_first_order[mint]) | set(
            self._buyer_top_volume[mint]
        )

    def _update_top_volume(self, mint: str, wallet: str) -> None:
        evidence_by_wallet = self._buyer_evidence[mint]
        top_volume = self._buyer_top_volume[mint]
        if wallet in top_volume:
            top_volume.remove(wallet)
        candidate_key = (-evidence_by_wallet[wallet].total_amount, wallet)
        if (
            len(top_volume) < 20
            or candidate_key
            < (
                -evidence_by_wallet[top_volume[-1]].total_amount,
                top_volume[-1],
            )
        ):
            top_volume.append(wallet)
            top_volume.sort(
                key=lambda item: (
                    -evidence_by_wallet[item].total_amount,
                    item,
                )
            )
            del top_volume[20:]

    def _rebuild_buyer_state(self, mint: str) -> None:
        evidence_by_wallet: dict[str, _BuyerEvidence] = {}
        first_order: list[str] = []
        first_members: set[str] = set()
        for _, wallet, amount, slot, bundle in self._buyers[mint]:
            evidence = evidence_by_wallet.setdefault(wallet, _BuyerEvidence())
            self._accumulate_evidence(evidence, amount, slot, bundle)
            if wallet not in first_members and len(first_members) < 20:
                first_members.add(wallet)
                first_order.append(wallet)
        self._buyer_evidence[mint] = evidence_by_wallet
        self._buyer_first_order[mint] = first_order
        self._buyer_first_members[mint] = first_members
        self._buyer_top_volume[mint] = [
            wallet
            for wallet, _ in sorted(
                evidence_by_wallet.items(),
                key=lambda item: (-item[1].total_amount, item[0]),
            )[:20]
        ]
        if self._buyers[mint]:
            latest = self._buyers[mint][-1]
            self._buyer_last_order_key[mint] = (latest[0], latest[1])

    def _historical_buyer_state(
        self,
        mint: str,
        at: datetime,
    ) -> tuple[dict[str, _BuyerEvidence], set[str]]:
        evidence_by_wallet: dict[str, _BuyerEvidence] = {}
        first_order: list[str] = []
        first_members: set[str] = set()
        for observed_at, wallet, amount, slot, bundle in self._buyers[mint]:
            if observed_at > at:
                break
            evidence = evidence_by_wallet.setdefault(wallet, _BuyerEvidence())
            self._accumulate_evidence(evidence, amount, slot, bundle)
            if wallet not in first_members and len(first_members) < 20:
                first_members.add(wallet)
                first_order.append(wallet)
        top_volume = [
            wallet
            for wallet, _ in sorted(
                evidence_by_wallet.items(),
                key=lambda item: (-item[1].total_amount, item[0]),
            )[:20]
        ]
        return evidence_by_wallet, set(first_order) | set(top_volume)

    def _recompute_relations(
        self,
        affected_wallets: set[str],
        *,
        evidence_by_wallet: dict[str, _BuyerEvidence],
        scope: set[str],
    ) -> list[WalletRelation]:
        if not evidence_by_wallet:
            return []
        affected = scope & affected_wallets
        if not affected:
            return []
        relations: list[WalletRelation] = []
        evaluated: set[tuple[str, str]] = set()
        for wallet in sorted(affected):
            for other in sorted(scope - {wallet}):
                left, right = sorted((wallet, other))
                key = (left, right)
                if key in evaluated:
                    continue
                evaluated.add(key)
                left_profile = self._profiles.get(left)
                right_profile = self._profiles.get(right)
                left_evidence = evidence_by_wallet[left]
                right_evidence = evidence_by_wallet[right]
                relation = score_relation(
                    left,
                    right,
                    RelationEvidence(
                        same_initial_funder=bool(
                            left_profile
                            and right_profile
                            and left_profile.initial_funder
                            and left_profile.initial_funder
                            == right_profile.initial_funder
                        ),
                        same_slot_buying=bool(
                            left_evidence.slots & right_evidence.slots
                        ),
                        same_transaction_bundle_heuristic=bool(
                            left_evidence.bundles & right_evidence.bundles
                        ),
                        identical_trade_amount_pattern=bool(
                            left_evidence.amounts & right_evidence.amounts
                        ),
                    ),
                )
                if not relation.evidence:
                    continue
                if self._relations.get(key) == relation:
                    continue
                self._relations[key] = relation
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
