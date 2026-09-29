"""Summarise reserve-based versus Jupiter marks and apply the frozen rule.

While ``exits.mark_source`` is ``jupiter`` the bot also values every open
position from the tracked pool reserves and logs both marks to
``data/mark_comparisons.ndjson``. Decision rule, fixed before the soak
(docs/RUNBOOK.md): switch to ``reserves`` only with at least 500 pairs, a
median absolute divergence of at most 100 bps and a 95th percentile of at
most 300 bps; otherwise keep ``jupiter``.

    python scripts/mark_divergence.py data/mark_comparisons.ndjson
"""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path
from typing import Any

MINIMUM_PAIRS = 500
MAXIMUM_MEDIAN_BPS = Decimal("100")
MAXIMUM_P95_BPS = Decimal("300")


def _quantile(values: list[Decimal], fraction: float) -> Decimal:
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


def read_divergences(paths: Iterable[Path]) -> list[Decimal]:
    values: list[Decimal] = []
    for path in paths:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as stream:
            for line in stream:
                line = line.strip()
                if not line:
                    continue
                try:
                    values.append(abs(Decimal(str(json.loads(line)["divergence_bps"]))))
                except (KeyError, ValueError, ArithmeticError):
                    continue
    return values


def decide(values: list[Decimal]) -> dict[str, Any]:
    if not values:
        return {"pairs": 0, "decision": "jupiter", "reason": "no comparisons recorded"}
    median_bps = _quantile(values, 0.5)
    p95_bps = _quantile(values, 0.95)
    switch = (
        len(values) >= MINIMUM_PAIRS
        and median_bps <= MAXIMUM_MEDIAN_BPS
        and p95_bps <= MAXIMUM_P95_BPS
    )
    return {
        "pairs": len(values),
        "median_abs_bps": str(median_bps),
        "p95_abs_bps": str(p95_bps),
        "max_abs_bps": str(max(values)),
        "decision": "reserves" if switch else "jupiter",
        "rule": (
            f">= {MINIMUM_PAIRS} pairs, median <= {MAXIMUM_MEDIAN_BPS} bps, "
            f"p95 <= {MAXIMUM_P95_BPS} bps"
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "log",
        nargs="?",
        default="data/mark_comparisons.ndjson",
        help="comparison log; its rotated .1 file is read too",
    )
    args = parser.parse_args(argv)
    log = Path(args.log)
    result = decide(read_divergences([log.with_suffix(log.suffix + ".1"), log]))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
