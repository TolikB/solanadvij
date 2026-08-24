import asyncio
from uuid import UUID

import pytest

from scripts.verify_postgres_hardening import (
    _close_all,
    _probe_id,
    _probe_sha256,
    _validate_probe_target,
    _validate_race_results,
)


def test_probe_id_fits_database_identifier_column() -> None:
    probe_id = _probe_id()

    assert len(probe_id) == 36
    assert str(UUID(probe_id)) == probe_id


def test_probe_sha256_is_unique_valid_hex() -> None:
    first = _probe_sha256()
    second = _probe_sha256()

    assert len(first) == 64
    assert int(first, 16) >= 0
    assert first != second


@pytest.mark.parametrize(
    ("dsn", "github_actions", "confirmation", "message"),
    [
        ("", "true", "EPHEMERAL_CI_ONLY", "required"),
        ("postgresql://u@127.0.0.1/prod", "true", "EPHEMERAL_CI_ONLY", "ending in _ci"),
        ("postgresql://u@example.com/sniper_ci", "true", "EPHEMERAL_CI_ONLY", "local"),
        ("postgresql://u@127.0.0.1/sniper_ci", "false", "EPHEMERAL_CI_ONLY", "ephemeral"),
    ],
)
def test_probe_target_fails_closed(
    dsn: str,
    github_actions: str,
    confirmation: str,
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        _validate_probe_target(
            dsn,
            github_actions=github_actions,
            confirmation=confirmation,
        )


def test_probe_target_accepts_local_ci_database() -> None:
    _validate_probe_target(
        "postgresql://u@127.0.0.1/sniper_ci",
        github_actions="true",
        confirmation="EPHEMERAL_CI_ONLY",
    )


def test_race_result_accepts_both_serializable_outcomes() -> None:
    assert _validate_race_results("run-id", True) == "run-id"
    assert (
        _validate_race_results("run-id", RuntimeError("stop the bot before recording"))
        == "run-id"
    )


def test_race_result_rejects_unexpected_failure() -> None:
    with pytest.raises(RuntimeError, match="unexpected startup/cost race"):
        _validate_race_results("run-id", ValueError("bad receipt"))


@pytest.mark.asyncio
async def test_close_all_attempts_every_resource() -> None:
    calls: list[str] = []

    class Resource:
        def __init__(self, name: str, fail: bool) -> None:
            self.name = name
            self.fail = fail

        async def close(self) -> None:
            calls.append(self.name)
            if self.fail:
                raise RuntimeError(self.name)

    with pytest.raises(ExceptionGroup, match="failed to close"):
        await _close_all(Resource("competitor", True), Resource("primary", False))

    assert calls == ["competitor", "primary"]


@pytest.mark.asyncio
async def test_close_all_attempts_every_resource_after_cancellation() -> None:
    calls: list[str] = []

    class Resource:
        def __init__(self, name: str, cancel: bool) -> None:
            self.name = name
            self.cancel = cancel

        async def close(self) -> None:
            calls.append(self.name)
            if self.cancel:
                raise asyncio.CancelledError

    with pytest.raises(BaseExceptionGroup, match="failed to close"):
        await _close_all(Resource("competitor", True), Resource("primary", False))

    assert calls == ["competitor", "primary"]
