from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from sniper_bot.acceptance import MAX_ARTIFACT_BYTES, verify_acceptance_evidence


def _read_bounded(path: Path) -> bytes:
    with path.open("rb") as stream:
        content = stream.read(MAX_ARTIFACT_BYTES + 1)
    if len(content) > MAX_ARTIFACT_BYTES:
        raise ValueError(f"input exceeds {MAX_ARTIFACT_BYTES} bytes: {path}")
    return content


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify the structure and consistency of an MVP evidence bundle")
    parser.add_argument("evidence_file")
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument("--expected-revision", required=True)
    parser.add_argument("--max-evidence-age-hours", required=True, type=int)
    parser.add_argument("--expected-ci-sha256", required=True)
    parser.add_argument("--expected-runtime-sha256", required=True)
    parser.add_argument("--expected-precommit-receipt-sha256", required=True)
    parser.add_argument("--expected-protocol-sha256", required=True)
    parser.add_argument("--expected-report-sha256", required=True)
    parser.add_argument("--trusted-protocol-published-at", required=True)
    args = parser.parse_args()
    document = json.loads(_read_bounded(Path(args.evidence_file)))
    manifest = verify_acceptance_evidence(
        document,
        artifact_root=Path(args.artifact_root),
        expected_revision=args.expected_revision,
        max_evidence_age_hours=args.max_evidence_age_hours,
        expected_ci_sha256=args.expected_ci_sha256,
        expected_runtime_sha256=args.expected_runtime_sha256,
        expected_precommit_receipt_sha256=args.expected_precommit_receipt_sha256,
        expected_protocol_sha256=args.expected_protocol_sha256,
        expected_report_sha256=args.expected_report_sha256,
        trusted_protocol_published_at=datetime.fromisoformat(
            args.trusted_protocol_published_at.replace("Z", "+00:00")
        ),
    )
    print(f"ACCEPTANCE BUNDLE STRUCTURE VERIFIED revision={manifest.revision}")
    print("External provenance and observation truth still require independent operator review.")


if __name__ == "__main__":
    main()
