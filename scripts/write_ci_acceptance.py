from __future__ import annotations

import argparse
import hashlib
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from xml.etree import ElementTree

from sniper_bot.acceptance import REQUIRED_CI_GATES, CiAcceptanceArtifact


def main() -> None:
    parser = argparse.ArgumentParser(description="Write CI evidence after all workflow gates pass")
    parser.add_argument("--junit", required=True)
    parser.add_argument("--coverage", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--gates-dir", required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("CI evidence may only be generated inside GitHub Actions")
    revision = os.environ["GITHUB_SHA"]
    run_url = f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    junit = ElementTree.parse(args.junit).getroot()
    suites = [junit] if junit.tag == "testsuite" else list(junit.findall("testsuite"))
    test_count = sum(int(suite.attrib.get("tests", "0")) for suite in suites)
    failures = sum(int(suite.attrib.get("failures", "0")) for suite in suites)
    errors = sum(int(suite.attrib.get("errors", "0")) for suite in suites)
    skipped = sum(int(suite.attrib.get("skipped", "0")) for suite in suites)
    coverage = ElementTree.parse(args.coverage).getroot()
    coverage_pct = Decimal(coverage.attrib["line-rate"]) * Decimal("100")
    if test_count <= 0 or failures != 0 or errors != 0 or skipped != 0:
        raise RuntimeError("JUnit evidence does not describe a passing test suite")
    gates_dir = Path(args.gates_dir)
    gate_receipts: dict[str, str] = {}
    for gate in sorted(REQUIRED_CI_GATES):
        content = (gates_dir / f"{gate}.ok").read_bytes()
        if content.decode("utf-8").strip() != f"{revision} {gate}":
            raise RuntimeError(f"CI gate receipt does not match the workflow revision: {gate}")
        gate_receipts[gate] = hashlib.sha256(content).hexdigest()
    artifact = CiAcceptanceArtifact(
        schema_version=2,
        revision=revision,
        generated_at=datetime.now(tz=timezone.utc),
        run_url=run_url,
        test_count=test_count,
        skipped_test_count=0,
        coverage_pct=coverage_pct,
        gate_receipt_sha256=gate_receipts,
        ruff_passed=True,
        strict_mypy_passed=True,
        tests_passed=True,
        deterministic_replay_passed=True,
        postgres_previous_migration_passed=True,
        sqlite_previous_migration_passed=True,
        data_integrity_passed=True,
        no_live_audit_passed=True,
        internal_latency_passed=True,
        compose_config_passed=True,
        docker_startup_smoke_passed=True,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(artifact.model_dump_json(indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
