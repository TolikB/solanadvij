from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import control
from sniper_bot import process_identity
from sniper_bot.process_identity import (
    ProcessIdentity,
    parse_linux_proc_stat,
    parse_process_identity,
    read_process_identity_file,
    remove_own_process_identity_file,
    write_process_identity_file,
)


def test_process_identity_rejects_nonpositive_pid() -> None:
    with pytest.raises(ValueError, match="positive"):
        parse_process_identity("0 123")


def test_linux_stat_parser_handles_parentheses_in_command() -> None:
    stat = "123 (worker (alpha)) S " + " ".join(str(field) for field in range(4, 23))

    assert parse_linux_proc_stat(123, stat) == ProcessIdentity(pid=123, start_ticks=22)


def test_identity_file_round_trip_and_owned_removal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_file = tmp_path / "sniper.pid"
    pid = process_identity.os.getpid()
    expected = ProcessIdentity(pid=pid, start_ticks=456)
    monkeypatch.setattr(process_identity.sys, "platform", "linux")
    monkeypatch.setattr(
        process_identity,
        "read_linux_process_identity",
        lambda _pid: expected,
    )

    write_process_identity_file(pid_file)

    assert read_process_identity_file(pid_file) == expected
    assert list(tmp_path.glob(".sniper.pid.*.tmp")) == []
    assert remove_own_process_identity_file(pid_file) is True
    assert not pid_file.exists()


def test_owned_removal_preserves_successor_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_file = tmp_path / "sniper.pid"
    pid_file.write_text("999 111\n", encoding="ascii")
    monkeypatch.setattr(process_identity.sys, "platform", "linux")
    monkeypatch.setattr(
        process_identity,
        "read_linux_process_identity",
        lambda pid: ProcessIdentity(pid=pid, start_ticks=456),
    )

    assert remove_own_process_identity_file(pid_file) is False
    assert pid_file.exists()


def test_identity_reader_rejects_fifo_before_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_file = tmp_path / "sniper.pid"
    fifo_mode = process_identity.stat.S_IFIFO | 0o600
    monkeypatch.setattr(
        process_identity.os,
        "lstat",
        lambda _path: SimpleNamespace(
            st_mode=fifo_mode,
            st_dev=1,
            st_ino=2,
            st_file_attributes=0,
        ),
    )
    monkeypatch.setattr(
        process_identity.os,
        "open",
        lambda *_args: pytest.fail("FIFO must be rejected before open"),
    )

    with pytest.raises(ValueError, match="regular"):
        read_process_identity_file(pid_file)


def test_identity_reader_rejects_windows_reparse_point(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_file = tmp_path / "sniper.pid"
    reparse_flag = getattr(process_identity.stat, "FILE_ATTRIBUTE_REPARSE_POINT", 1024)
    monkeypatch.setattr(process_identity.stat, "FILE_ATTRIBUTE_REPARSE_POINT", reparse_flag)
    monkeypatch.setattr(
        process_identity.os,
        "lstat",
        lambda _path: SimpleNamespace(
            st_mode=process_identity.stat.S_IFREG | 0o600,
            st_dev=1,
            st_ino=2,
            st_file_attributes=reparse_flag,
        ),
    )

    with pytest.raises(ValueError, match="non-reparse"):
        read_process_identity_file(pid_file)


def test_non_linux_owned_removal_uses_bounded_reader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_file = tmp_path / "sniper.pid"
    pid_file.write_text("1" * 129, encoding="ascii")
    monkeypatch.setattr(process_identity.sys, "platform", "win32")

    with pytest.raises(ValueError, match="too large"):
        remove_own_process_identity_file(pid_file)

    assert pid_file.exists()


def test_control_rejects_non_linux_platform(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(control.sys, "platform", "win32")

    with pytest.raises(control.ControlError, match="only on Linux"):
        control.send_control_signal(tmp_path / "sniper.pid", "pause")


def test_control_rejects_stale_reused_pid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_file = tmp_path / "sniper.pid"
    pid_file.write_text("123 456\n", encoding="ascii")
    monkeypatch.setattr(control.sys, "platform", "linux")
    monkeypatch.setattr(
        control,
        "read_linux_process_identity",
        lambda _pid: ProcessIdentity(pid=123, start_ticks=999),
    )

    with pytest.raises(control.ControlError, match="stale"):
        control.send_control_signal(pid_file, "pause")


def test_control_uses_pidfd_and_rechecks_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_file = tmp_path / "sniper.pid"
    pid_file.write_text("123 456\n", encoding="ascii")
    expected = ProcessIdentity(pid=123, start_ticks=456)
    sent: list[tuple[int, int]] = []
    closed: list[int] = []
    monkeypatch.setattr(control.sys, "platform", "linux")
    monkeypatch.setattr(control, "read_linux_process_identity", lambda _pid: expected)
    monkeypatch.setattr(control, "_open_pidfd", lambda _pid: 77)
    monkeypatch.setattr(
        control,
        "_send_pidfd_signal",
        lambda pidfd, signal_number: sent.append((pidfd, signal_number)),
    )
    monkeypatch.setattr(control.os, "close", closed.append)
    monkeypatch.setattr(control.signal, "SIGUSR1", 10, raising=False)

    control.send_control_signal(pid_file, "pause")

    assert sent == [(77, 10)]
    assert closed == [77]
