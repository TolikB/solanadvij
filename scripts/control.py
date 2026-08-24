"""Local-only pause/resume control through Unix signals."""

from __future__ import annotations

import argparse
import os
import signal
import sys
from pathlib import Path
from typing import cast

from sniper_bot.process_identity import (
    ProcessIdentity,
    read_linux_process_identity,
    read_process_identity_file,
)


class ControlError(RuntimeError):
    pass


def _open_pidfd(pid: int) -> int:
    opener = cast(object, getattr(os, "pidfd_open", None))
    if not callable(opener):
        raise ControlError("Linux pidfd support is required for safe process control")
    result = opener(pid, 0)
    if not isinstance(result, int):
        raise ControlError("pidfd_open returned an invalid descriptor")
    return result


def _send_pidfd_signal(pidfd: int, signal_number: int) -> None:
    sender = cast(object, getattr(signal, "pidfd_send_signal", None))
    if not callable(sender):
        raise ControlError("Linux pidfd signal support is required for safe process control")
    sender(pidfd, signal_number, None, 0)


def _read_expected_identity(pid_file: Path) -> ProcessIdentity:
    try:
        return read_process_identity_file(pid_file)
    except (OSError, ValueError) as exc:
        raise ControlError(f"invalid PID file {pid_file}: {exc}") from exc


def send_control_signal(pid_file: Path, action: str) -> None:
    if not sys.platform.startswith("linux"):
        raise ControlError("safe signal control is available only on Linux")
    if action not in {"pause", "resume"}:
        raise ControlError(f"unsupported control action: {action}")
    expected = _read_expected_identity(pid_file)
    try:
        observed_before = read_linux_process_identity(expected.pid)
    except (OSError, ValueError) as exc:
        raise ControlError("target process is not available") from exc
    if observed_before != expected:
        raise ControlError("PID file is stale; refusing to signal a reused PID")

    pidfd = _open_pidfd(expected.pid)
    try:
        try:
            observed_after = read_linux_process_identity(expected.pid)
        except (OSError, ValueError) as exc:
            raise ControlError("target process exited before identity confirmation") from exc
        if observed_after != expected:
            raise ControlError("process identity changed; refusing to send a signal")
        signal_name = "SIGUSR1" if action == "pause" else "SIGUSR2"
        signal_number = getattr(signal, signal_name, None)
        if not isinstance(signal_number, int):
            raise ControlError(f"{signal_name} is unavailable on this platform")
        _send_pidfd_signal(pidfd, signal_number)
    finally:
        os.close(pidfd)


def main() -> None:
    parser = argparse.ArgumentParser(description="Control a local sniper-bot process")
    parser.add_argument("action", choices=("pause", "resume"))
    parser.add_argument("--pid-file", default="data/sniper.pid")
    args = parser.parse_args()
    try:
        send_control_signal(Path(args.pid_file), str(args.action))
    except ControlError as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
