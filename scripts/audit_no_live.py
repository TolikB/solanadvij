"""Fail CI if the production image gains a signing or submission surface."""

from __future__ import annotations

import ast
import re
import tomllib
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
PYTHON_SCAN_ROOTS = (PROJECT / "src", PROJECT / "scripts")
TEXT_SCAN_FILES = (
    PROJECT / "Dockerfile",
    PROJECT / "docker-compose.yml",
    PROJECT / ".env.example",
)
TEXT_SCAN_ROOTS = (
    (PROJECT / "scripts", ("*.sh",)),
    (PROJECT / "configs", ("*.yaml", "*.yml")),
)
FORBIDDEN_STRINGS = {
    "/execute",
    "sendtransaction",
    "sendrawtransaction",
    "helius_sender",
    "private_key",
    "secret_key",
    "seed_phrase",
    "mnemonic",
}
FORBIDDEN_IDENTIFIERS = {
    "keypair",
    "send_transaction",
    "send_raw_transaction",
    "sign_transaction",
}
FORBIDDEN_CODE_COMPACT = {
    "sendtransaction",
    "sendrawtransaction",
    "signtransaction",
}
FORBIDDEN_DEPLOYMENT_COMPACT = FORBIDDEN_CODE_COMPACT | {
    "privatekey",
    "secretkey",
    "seedphrase",
    "walletkey",
}
SIGNER_CAPABLE_DEPENDENCIES = {
    "anchorpy",
    "eth-account",
    "solana",
    "solders",
    "web3",
}


def _compact(text: str) -> str:
    return "".join(character for character in text.casefold() if character.isalnum())


def _scan_compact_text(
    path: Path,
    text: str,
    violations: list[str],
    *,
    needles: set[str],
) -> None:
    compact = _compact(text)
    for needle in needles:
        if needle in compact:
            violations.append(
                f"{path.relative_to(PROJECT)}: forbidden compact token {needle}"
            )


def _production_dependency_names() -> set[str]:
    manifest = tomllib.loads((PROJECT / "pyproject.toml").read_text(encoding="utf-8"))
    requirements = manifest.get("project", {}).get("dependencies", [])
    names: set[str] = set()
    for requirement in requirements:
        name = re.split(r"[\s\[<>=!~;]", str(requirement), maxsplit=1)[0]
        names.add(name.casefold().replace("_", "-"))
    return names


def _iter_deployment_text_files() -> list[Path]:
    paths = [path for path in TEXT_SCAN_FILES if path.is_file()]
    for root, patterns in TEXT_SCAN_ROOTS:
        for pattern in patterns:
            paths.extend(root.rglob(pattern))
    return sorted(set(paths))


def main() -> None:
    violations: list[str] = []
    own_path = Path(__file__).resolve()
    for root in PYTHON_SCAN_ROOTS:
        for path in sorted(root.rglob("*.py")):
            if path.resolve() == own_path:
                continue
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
            for node in ast.walk(tree):
                imported_modules: list[str] = []
                if isinstance(node, ast.Import):
                    imported_modules = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported_modules = [node.module]
                for module in imported_modules:
                    dependency = module.split(".", maxsplit=1)[0].casefold()
                    if dependency in SIGNER_CAPABLE_DEPENDENCIES:
                        violations.append(
                            f"{path.relative_to(PROJECT)}:"
                            f"{node.lineno}: signer-capable import {module}"
                        )
                if (
                    isinstance(node, ast.Constant)
                    and isinstance(node.value, str)
                ):
                    value = node.value.casefold()
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
                    if identifier.casefold() in FORBIDDEN_IDENTIFIERS:
                        violations.append(
                            f"{path.relative_to(PROJECT)}:"
                            f"{node.lineno}: forbidden identifier "
                            f"{identifier}"
                        )
            _scan_compact_text(
                path,
                source,
                violations,
                needles=FORBIDDEN_CODE_COMPACT,
            )

    for path in _iter_deployment_text_files():
        _scan_compact_text(
            path,
            path.read_text(encoding="utf-8"),
            violations,
            needles=FORBIDDEN_DEPLOYMENT_COMPACT,
        )

    blocked_dependencies = (
        _production_dependency_names() & SIGNER_CAPABLE_DEPENDENCIES
    )
    for dependency in sorted(blocked_dependencies):
        violations.append(
            f"pyproject.toml: signer-capable production dependency {dependency}"
        )

    dockerfile = (PROJECT / "Dockerfile").read_text(encoding="utf-8")
    if dockerfile.count("uv sync --frozen --no-dev") < 2:
        violations.append(
            "Dockerfile: every dependency sync must explicitly exclude dev packages"
        )

    if violations:
        raise SystemExit(
            "LIVE EXECUTION AUDIT FAILED\n"
            + "\n".join(sorted(set(violations)))
        )
    print(
        "LIVE EXECUTION AUDIT PASSED: "
        "production source, deployment files, dependencies, and image policy "
        "contain no signing, key-loading, or transaction-submission surface"
    )


if __name__ == "__main__":
    main()
