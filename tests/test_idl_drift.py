from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from scripts.check_idl_drift import PROTOCOLS, compare, main


def _vendored(protocol: str) -> dict[str, Any]:
    path = next(path for name, path, _, _ in PROTOCOLS if name == protocol)
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def _fields(idl: dict[str, Any], event: str) -> list[dict[str, Any]]:
    definition = next(item for item in idl["types"] if item["name"] == event)
    fields: list[dict[str, Any]] = definition["type"]["fields"]
    return fields


CONSUMED = frozenset({"CreatePoolEvent", "BuyEvent"})


def test_identical_and_appended_layouts_are_compatible() -> None:
    vendored = _vendored("pumpswap")
    upstream = copy.deepcopy(vendored)
    _fields(upstream, "BuyEvent").append({"name": "new_reward", "type": "u64"})
    upstream["events"].append({"name": "BrandNewEvent", "discriminator": [9] * 8})

    report = compare(vendored, upstream, CONSUMED)

    assert report["compatible"] is True
    assert report["consumed_events"]["CreatePoolEvent"] == {"status": "same"}
    assert report["consumed_events"]["BuyEvent"] == {
        "status": "appended",
        "fields": ["new_reward"],
    }
    assert report["added_event_types"] == ["BrandNewEvent"]


@pytest.mark.parametrize(
    ("mutation", "status"),
    [
        ("insert", "changed"),
        ("retype", "changed"),
        ("drop_last", "changed"),
        ("discriminator", "discriminator_changed"),
        ("remove_event", "removed"),
    ],
)
def test_incompatible_changes_to_consumed_events_fail(mutation: str, status: str) -> None:
    vendored = _vendored("pumpswap")
    upstream = copy.deepcopy(vendored)
    fields = _fields(upstream, "BuyEvent")
    if mutation == "insert":
        fields.insert(3, {"name": "inserted", "type": "u8"})
    elif mutation == "retype":
        fields[1]["type"] = "u32"
    elif mutation == "drop_last":
        fields.pop()
    elif mutation == "discriminator":
        event = next(item for item in upstream["events"] if item["name"] == "BuyEvent")
        event["discriminator"] = [0] * 8
    else:
        upstream["events"] = [item for item in upstream["events"] if item["name"] != "BuyEvent"]

    report = compare(vendored, upstream, CONSUMED)

    assert report["compatible"] is False
    assert report["consumed_events"]["BuyEvent"]["status"] == status


def test_moved_program_is_incompatible() -> None:
    vendored = _vendored("pump")
    upstream = copy.deepcopy(vendored)
    upstream["address"] = "11111111111111111111111111111111"

    assert compare(vendored, upstream, frozenset({"CompleteEvent"}))["compatible"] is False


def test_command_reads_a_local_upstream_copy_and_reports_missing_upstream(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    for protocol, upstream_name in (("pump", "pump"), ("pumpswap", "pump_amm")):
        (tmp_path / f"{upstream_name}.json").write_text(
            json.dumps(_vendored(protocol)), encoding="utf-8"
        )

    assert main(["--upstream-dir", str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out)["compatible"] is True

    (tmp_path / "pump_amm.json").unlink()
    assert main(["--upstream-dir", str(tmp_path)]) == 2
