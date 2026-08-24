from __future__ import annotations

import os
import stat
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    pid: int
    start_ticks: int


def parse_process_identity(value: str) -> ProcessIdentity:
    fields = value.split()
    if len(fields) != 2:
        raise ValueError("PID file must contain exactly PID and Linux start ticks")
    try:
        identity = ProcessIdentity(pid=int(fields[0]), start_ticks=int(fields[1]))
    except ValueError as exc:
        raise ValueError("PID file contains non-integer process identity fields") from exc
    if identity.pid <= 0:
        raise ValueError("PID must be positive")
    if identity.start_ticks < 0:
        raise ValueError("Linux process start ticks must not be negative")
    return identity


def parse_linux_proc_stat(pid: int, value: str) -> ProcessIdentity:
    if pid <= 0:
        raise ValueError("PID must be positive")
    command_end = value.rfind(")")
    if command_end < 0:
        raise ValueError("Linux process stat has no command terminator")
    fields_after_command = value[command_end + 1 :].split()
    start_time_index = 19  # Field 22, with this list beginning at field 3.
    if len(fields_after_command) <= start_time_index:
        raise ValueError("Linux process stat is missing start time")
    try:
        start_ticks = int(fields_after_command[start_time_index])
    except ValueError as exc:
        raise ValueError("Linux process stat start time is not an integer") from exc
    if start_ticks < 0:
        raise ValueError("Linux process start ticks must not be negative")
    return ProcessIdentity(pid=pid, start_ticks=start_ticks)


def read_linux_process_identity(pid: int) -> ProcessIdentity:
    stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    return parse_linux_proc_stat(pid, stat)


def _read_bounded_regular_ascii(path: Path) -> str:
    before = os.lstat(path)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    file_attributes = getattr(before, "st_file_attributes", 0)
    if not stat.S_ISREG(before.st_mode) or (
        isinstance(reparse_flag, int)
        and isinstance(file_attributes, int)
        and file_attributes & reparse_flag
    ):
        raise ValueError("PID path must be a regular non-reparse file")
    flags = os.O_RDONLY
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    if isinstance(no_follow, int):
        flags |= no_follow
    non_block = getattr(os, "O_NONBLOCK", 0)
    if isinstance(non_block, int):
        flags |= non_block
    descriptor = os.open(path, flags)
    try:
        after = os.fstat(descriptor)
        if not stat.S_ISREG(after.st_mode):
            raise ValueError("opened PID path is not a regular file")
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise ValueError("PID path changed while it was being opened")
        with os.fdopen(descriptor, mode="r", encoding="ascii") as handle:
            descriptor = -1
            payload = handle.read(129)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(payload) > 128:
        raise ValueError("PID file is too large")
    return payload


def read_process_identity_file(path: Path) -> ProcessIdentity:
    payload = _read_bounded_regular_ascii(path)
    return parse_process_identity(payload)


def write_process_identity_file(path: Path) -> None:
    pid = os.getpid()
    if sys.platform.startswith("linux"):
        identity = read_linux_process_identity(pid)
        payload = f"{identity.pid} {identity.start_ticks}\n"
    else:
        payload = f"{pid}\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, mode="w", encoding="ascii", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def remove_own_process_identity_file(path: Path) -> bool:
    try:
        if sys.platform.startswith("linux"):
            expected = read_linux_process_identity(os.getpid())
            if read_process_identity_file(path) != expected:
                return False
        else:
            payload = _read_bounded_regular_ascii(path).strip()
            if payload != str(os.getpid()):
                return False
        path.unlink()
    except FileNotFoundError:
        return False
    return True
