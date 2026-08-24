from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
from pathlib import Path

from sniper_bot.acceptance import (
    MAX_ARTIFACT_BYTES,
    StatisticalProtocol,
    evaluate_statistical_stage,
    load_statistical_stage_data,
)
from sniper_bot.database import Database


def _write_report(output: Path, rendered: str) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered + "\n", encoding="utf-8")


def _read_bounded(path: Path) -> bytes:
    with path.open("rb") as stream:
        content = stream.read(MAX_ARTIFACT_BYTES + 1)
    if len(content) > MAX_ARTIFACT_BYTES:
        raise ValueError(f"input exceeds {MAX_ARTIFACT_BYTES} bytes: {path}")
    return content


async def run(args: argparse.Namespace) -> int:
    dsn = os.environ.get("POSTGRES_DSN", "").strip()
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is required")
    protocol_path = Path(args.protocol)
    protocol_bytes = await asyncio.to_thread(_read_bounded, protocol_path)
    protocol = StatisticalProtocol.model_validate_json(protocol_bytes)
    protocol_sha256 = hashlib.sha256(protocol_bytes).hexdigest()
    database = Database(dsn)
    try:
        inputs = await load_statistical_stage_data(database, protocol)
    finally:
        await database.close()
    report = evaluate_statistical_stage(inputs=inputs, protocol=protocol, protocol_sha256=protocol_sha256)
    rendered = report.model_dump_json(indent=2)
    if args.output is None:
        print(rendered)
    else:
        output = Path(args.output)
        await asyncio.to_thread(_write_report, output, rendered)
        print(f"statistical_report={output}")
    return 0 if report.passed else 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate the frozen pre-live statistical protocol")
    parser.add_argument("--protocol", required=True)
    parser.add_argument("--output")
    raise SystemExit(asyncio.run(run(parser.parse_args())))


if __name__ == "__main__":
    main()
