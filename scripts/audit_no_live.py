"""Fail CI if the production image gains a signing or submission surface."""

from __future__ import annotations

import ast
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
SCAN_ROOTS = (PROJECT / "src", PROJECT / "scripts")
FORBIDDEN_STRINGS = {
    "/execute",
    "sendtransaction",
    "sendrawtransaction",
    "helius_sender",
    "private_key",
}
FORBIDDEN_IDENTIFIERS = {
    "keypair",
    "send_transaction",
    "send_raw_transaction",
    "sign_transaction",
}


def main() -> None:
    violations: list[str] = []
    for root in SCAN_ROOTS:
        for path in sorted(root.rglob("*.py")):
            if path.resolve() == Path(__file__).resolve():
                continue
            tree = ast.parse(
                path.read_text(encoding="utf-8"),
                filename=str(path),
            )
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Constant)
                    and isinstance(node.value, str)
                ):
                    value = node.value.lower()
                    for needle in FORBIDDEN_STRINGS:
                        if needle in value:
                            violations.append(
                                f"{path.relative_to(PROJECT)}:"
                                f"{node.lineno}: forbidden literal {needle}"
                            )
                if isinstance(node, (ast.Name, ast.Attribute)):
                    identifier = (
                        node.id
                        if isinstance(node, ast.Name)
                        else node.attr
                    )
                    if identifier.lower() in FORBIDDEN_IDENTIFIERS:
                        violations.append(
                            f"{path.relative_to(PROJECT)}:"
                            f"{node.lineno}: forbidden identifier "
                            f"{identifier}"
                        )
    if violations:
        raise SystemExit(
            "LIVE EXECUTION AUDIT FAILED\n"
            + "\n".join(sorted(set(violations)))
        )
    print(
        "LIVE EXECUTION AUDIT PASSED: "
        "no signing, key loading, or submission surface"
    )


if __name__ == "__main__":
    main()
