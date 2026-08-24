"""Explainable wallet-relation scoring and deterministic clustering."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field, model_validator


class RelationEvidence(BaseModel):
    same_initial_funder: bool = False
    direct_token_transfer: bool = False
    direct_sol_transfer: bool = False
    same_creation_transaction: bool = False
    same_slot_buying: bool = False
    same_transaction_bundle_heuristic: bool = False
    identical_trade_amount_pattern: bool = False
    common_withdrawal_destination: bool = False
    shared_dev_funding_source: bool = False


class WalletRelation(BaseModel):
    wallet_a: str
    wallet_b: str
    relation_score: Decimal = Field(ge=0, le=1)
    evidence: list[str]
    eligible_for_cluster: bool

    @model_validator(mode="after")
    def wallets_must_differ(self) -> "WalletRelation":
        if self.wallet_a == self.wallet_b:
            raise ValueError("wallet relation requires two distinct wallets")
        return self


class WalletCluster(BaseModel):
    cluster_id: str
    wallets: list[str]
    relation_explanations: list[WalletRelation]
    holdings_pct: Decimal = Decimal("0")
    buy_share_pct: Decimal = Decimal("0")


_STRONG = {
    "same_initial_funder": Decimal("0.78"),
    "direct_token_transfer": Decimal("0.78"),
    "direct_sol_transfer": Decimal("0.72"),
    "common_withdrawal_destination": Decimal("0.75"),
    "shared_dev_funding_source": Decimal("0.85"),
}
_MEDIUM = {
    "same_creation_transaction": Decimal("0.50"),
    "same_slot_buying": Decimal("0.46"),
    "same_transaction_bundle_heuristic": Decimal("0.55"),
    "identical_trade_amount_pattern": Decimal("0.46"),
}


def score_relation(wallet_a: str, wallet_b: str, evidence: RelationEvidence) -> WalletRelation:
    if wallet_a == wallet_b:
        raise ValueError("cannot score a wallet against itself")
    active = [name for name, enabled in evidence.model_dump().items() if enabled]
    strong_count = sum(1 for name in active if name in _STRONG)
    medium_count = sum(1 for name in active if name in _MEDIUM)
    weights = [_STRONG.get(name, _MEDIUM.get(name, Decimal("0"))) for name in active]
    complement = Decimal("1")
    for weight in weights:
        complement *= Decimal("1") - weight
    score = Decimal("1") - complement
    eligible = score >= Decimal("0.70") and (strong_count >= 1 or medium_count >= 2)
    return WalletRelation(
        wallet_a=min(wallet_a, wallet_b),
        wallet_b=max(wallet_a, wallet_b),
        relation_score=score.quantize(Decimal("0.0001")),
        evidence=active,
        eligible_for_cluster=eligible,
    )


def build_clusters(
    wallets: set[str],
    relations: list[WalletRelation],
    *,
    holdings: dict[str, Decimal] | None = None,
    buy_volumes: dict[str, Decimal] | None = None,
) -> list[WalletCluster]:
    parent = {wallet: wallet for wallet in wallets}

    def find(wallet: str) -> str:
        while parent[wallet] != wallet:
            parent[wallet] = parent[parent[wallet]]
            wallet = parent[wallet]
        return wallet

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root == right_root:
            return
        low, high = sorted((left_root, right_root))
        parent[high] = low

    for relation in relations:
        if relation.eligible_for_cluster:
            if relation.wallet_a not in parent or relation.wallet_b not in parent:
                raise ValueError("relation contains a wallet outside clustering scope")
            union(relation.wallet_a, relation.wallet_b)
    grouped: dict[str, list[str]] = {}
    for wallet in sorted(wallets):
        grouped.setdefault(find(wallet), []).append(wallet)
    total_holdings = sum((holdings or {}).values(), Decimal("0"))
    total_volume = sum((buy_volumes or {}).values(), Decimal("0"))
    result: list[WalletCluster] = []
    for root, members in sorted(grouped.items()):
        member_set = set(members)
        explanations = [
            relation
            for relation in relations
            if relation.wallet_a in member_set
            and relation.wallet_b in member_set
            and relation.eligible_for_cluster
        ]
        cluster_holding = sum(((holdings or {}).get(wallet, Decimal("0")) for wallet in members), Decimal("0"))
        cluster_volume = sum(((buy_volumes or {}).get(wallet, Decimal("0")) for wallet in members), Decimal("0"))
        result.append(
            WalletCluster(
                cluster_id=root,
                wallets=members,
                relation_explanations=explanations,
                holdings_pct=cluster_holding / total_holdings if total_holdings > 0 else Decimal("0"),
                buy_share_pct=cluster_volume / total_volume if total_volume > 0 else Decimal("0"),
            )
        )
    return result
