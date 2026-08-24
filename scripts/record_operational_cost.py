from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from sniper_bot.acceptance import MAX_ARTIFACT_BYTES, OperationalCostReceiptArtifact
from sniper_bot.database import Database


def _read_receipt(path: str) -> bytes:
    receipt_path = Path(path).resolve(strict=True)
    with receipt_path.open("rb") as receipt_file:
        content = receipt_file.read(MAX_ARTIFACT_BYTES + 1)
    if len(content) > MAX_ARTIFACT_BYTES:
        raise ValueError("operational-cost receipt exceeds the acceptance artifact limit")
    return content


async def run(args: argparse.Namespace) -> int:
    dsn = os.environ.get("POSTGRES_DSN", "").strip()
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is required")
    incurred_at = datetime.fromisoformat(args.incurred_at.replace("Z", "+00:00"))
    if incurred_at.tzinfo is None:
        raise ValueError("--incurred-at must be timezone-aware")
    receipt_bytes = await asyncio.to_thread(_read_receipt, args.source_receipt)
    receipt = OperationalCostReceiptArtifact.model_validate_json(receipt_bytes)
    amount = Decimal(args.amount_usd)
    if (
        receipt.account_id != args.account_id
        or receipt.category != args.category
        or receipt.amount_usd != amount
        or receipt.incurred_at != incurred_at.astimezone(timezone.utc)
    ):
        raise ValueError("operational-cost receipt content does not match CLI arguments")
    source_reference_sha256 = hashlib.sha256(receipt_bytes).hexdigest()
    database = Database(dsn)
    try:
        inserted = await database.record_operational_cost(
            cost_id=str(uuid4()),
            account_id=args.account_id,
            category=args.category,
            amount_usd=amount,
            incurred_at=incurred_at,
            source_reference_sha256=source_reference_sha256,
            recorded_at=datetime.now(tz=timezone.utc),
        )
    finally:
        await database.close()
    print("operational_cost_recorded=true" if inserted else "operational_cost_recorded=false")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Record one source-linked operational cost")
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--category", required=True)
    parser.add_argument("--amount-usd", required=True)
    parser.add_argument("--incurred-at", required=True)
    parser.add_argument("--source-receipt", required=True)
    raise SystemExit(asyncio.run(run(parser.parse_args())))


if __name__ == "__main__":
    main()
