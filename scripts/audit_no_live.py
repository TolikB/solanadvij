"""Fail CI if the first release gains a real-execution or key-loading surface."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "src" / "sniper_bot"
FORBIDDEN = {
    "/execute": "Jupiter execution endpoint",
    "sendTransaction": "Solana transaction submission",
    "sendRawTransaction": "Solana raw transaction submission",
    "HELIUS_SENDER": "Helius Sender integration",
    "PRIVATE_KEY": "private key configuration",
    "KEYPAIR": "transaction signer material",
}


def main() -> None:
    violations: list[str] = []
    for path in sorted(ROOT.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for needle, reason in FORBIDDEN.items():
            if needle.lower() in text.lower():
                violations.append(f"{path.relative_to(ROOT)}: {reason} ({needle})")
    if violations:
        raise SystemExit("LIVE EXECUTION AUDIT FAILED\n" + "\n".join(violations))
    print("LIVE EXECUTION AUDIT PASSED: no signing, key loading, or submission surface")


if __name__ == "__main__":
    main()
