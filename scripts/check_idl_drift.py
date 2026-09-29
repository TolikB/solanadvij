"""Compare the vendored Pump and PumpSwap IDLs with the published upstream ones.

Consumed events may only have grown by appended fields, which the decoders
accept; a changed discriminator, a changed or removed field, a removed event,
or a moved program would misread live data, so the release must stop. New
event types upstream are reported and are harmless: the decoders count and
ignore them.

    python scripts/check_idl_drift.py            # fetch pump-public-docs main
    python scripts/check_idl_drift.py --upstream-dir DIR

Exit status: 0 compatible, 1 incompatible, 2 upstream unavailable.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from sniper_bot.protocols import pump as pump_package
from sniper_bot.protocols import pumpswap as pumpswap_package
from sniper_bot.protocols.pump import PUMP_EVENT_NAMES
from sniper_bot.protocols.pumpswap import PUMPSWAP_EVENT_NAMES

UPSTREAM_URL = (
    "https://raw.githubusercontent.com/pump-fun/pump-public-docs/{ref}/idl/{name}.json"
)
PROTOCOLS = (
    ("pump", Path(str(pump_package.__file__)).with_name("idl.json"), "pump", PUMP_EVENT_NAMES),
    (
        "pumpswap",
        Path(str(pumpswap_package.__file__)).with_name("idl.json"),
        "pump_amm",
        PUMPSWAP_EVENT_NAMES,
    ),
)


def _resolved(type_spec: Any, types: dict[str, Any], seen: frozenset[str] = frozenset()) -> Any:
    """Expand defined types so a nested struct change counts as a field change."""
    if isinstance(type_spec, dict):
        if "defined" in type_spec:
            defined = type_spec["defined"]
            name = defined["name"] if isinstance(defined, dict) else str(defined)
            if name in seen or name not in types:
                return {"defined": name}
            return {"defined": name, "type": _resolved(types[name], types, seen | {name})}
        return {key: _resolved(value, types, seen) for key, value in sorted(type_spec.items())}
    if isinstance(type_spec, list):
        return [_resolved(item, types, seen) for item in type_spec]
    return type_spec


def _event_layout(idl: dict[str, Any], event_name: str) -> list[tuple[str, str]] | None:
    types = {item["name"]: item["type"] for item in idl.get("types", [])}
    definition = types.get(event_name)
    if not isinstance(definition, dict):
        return None
    return [
        (str(field["name"]), json.dumps(_resolved(field["type"], types), sort_keys=True))
        for field in definition.get("fields", [])
    ]


def compare(
    vendored: dict[str, Any], upstream: dict[str, Any], consumed: frozenset[str]
) -> dict[str, Any]:
    vendored_events = {item["name"]: list(item["discriminator"]) for item in vendored.get("events", [])}
    upstream_events = {item["name"]: list(item["discriminator"]) for item in upstream.get("events", [])}
    report: dict[str, Any] = {
        "address_matches": vendored.get("address") == upstream.get("address"),
        "added_event_types": sorted(set(upstream_events) - set(vendored_events)),
        "removed_event_types": sorted(set(vendored_events) - set(upstream_events)),
        "consumed_events": {},
    }
    compatible = bool(report["address_matches"])
    for event_name in sorted(consumed):
        if event_name not in upstream_events:
            report["consumed_events"][event_name] = {"status": "removed"}
            compatible = False
            continue
        if upstream_events[event_name] != vendored_events.get(event_name):
            report["consumed_events"][event_name] = {"status": "discriminator_changed"}
            compatible = False
            continue
        ours = _event_layout(vendored, event_name) or []
        theirs = _event_layout(upstream, event_name) or []
        if theirs == ours:
            report["consumed_events"][event_name] = {"status": "same"}
        elif theirs[: len(ours)] == ours:
            report["consumed_events"][event_name] = {
                "status": "appended",
                "fields": [name for name, _ in theirs[len(ours) :]],
            }
        else:
            first = next(
                (
                    index
                    for index, (mine, published) in enumerate(zip(ours, theirs, strict=False))
                    if mine != published
                ),
                min(len(ours), len(theirs)),
            )
            report["consumed_events"][event_name] = {
                "status": "changed",
                "first_difference": ours[first][0] if first < len(ours) else None,
            }
            compatible = False
    report["compatible"] = compatible
    return report


def _load_upstream(name: str, ref: str, directory: str | None) -> dict[str, Any]:
    if directory is not None:
        loaded: Any = json.loads((Path(directory) / f"{name}.json").read_text(encoding="utf-8"))
    else:
        url = UPSTREAM_URL.format(ref=ref, name=name)
        with urllib.request.urlopen(url, timeout=20) as response:  # noqa: S310
            loaded = json.loads(response.read())
    if not isinstance(loaded, dict):
        raise ValueError(f"upstream {name} IDL is not an object")
    return loaded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--upstream-ref", default="main")
    parser.add_argument("--upstream-dir")
    args = parser.parse_args(argv)
    result: dict[str, Any] = {"upstream_ref": args.upstream_ref, "protocols": {}}
    for protocol, vendored_path, upstream_name, consumed in PROTOCOLS:
        vendored = json.loads(vendored_path.read_text(encoding="utf-8"))
        try:
            upstream = _load_upstream(upstream_name, args.upstream_ref, args.upstream_dir)
        except (OSError, ValueError, urllib.error.URLError) as error:
            result["error"] = f"{protocol}: {type(error).__name__}: {error}"
            print(json.dumps(result, sort_keys=True))
            return 2
        result["protocols"][protocol] = compare(vendored, upstream, consumed)
    result["compatible"] = all(item["compatible"] for item in result["protocols"].values())
    print(json.dumps(result, sort_keys=True))
    return 0 if result["compatible"] else 1


if __name__ == "__main__":
    sys.exit(main())
