# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Process helpers, errors, logging and small parsing utilities."""

from __future__ import annotations

import datetime
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

VERBOSE = False

# Exit codes (documented in SKILL.md). argparse uses 2 for usage errors.
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_AUTH = 3
EXIT_CAPACITY = 4
EXIT_PRECONDITION = 5
EXIT_STILL_RUNNING = 75


class VsbenchError(Exception):
    """An expected failure: printed as `error: ...` (+ `hint: ...`)."""

    exit_code = EXIT_ERROR

    def __init__(self, message: str, hint: str | None = None) -> None:
        super().__init__(message)
        self.hint = hint


class PreconditionError(VsbenchError):
    """The cluster is not in a state where the command can run."""

    exit_code = EXIT_PRECONDITION


class StillRunning(VsbenchError):
    """A detached job outlived the foreground wait; it keeps running remotely."""

    exit_code = EXIT_STILL_RUNNING


def log(message: str) -> None:
    """Progress output goes to stderr so stdout stays machine-readable.

    Never raises: cleanup paths (rollback, lock release) log, and a closed
    pipe or hung-up terminal must not stop them from finishing.
    """
    try:
        sys.stderr.write(f"vsbench: {message}\n")
        sys.stderr.flush()
    except (OSError, ValueError):
        pass


def warn(message: str) -> None:
    log(f"warning: {message}")


def debug(message: str) -> None:
    if VERBOSE:
        log(f"debug: {message}")


def run(
    cmd: Sequence[Any],
    *,
    input: str | None = None,
    check: bool = True,
    capture: bool = True,
    timeout: float | None = None,
    env: Mapping[str, str] | None = None,
    cwd: str | Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a command and return the CompletedProcess with text output.

    With capture=False stdout/stderr are inherited (streamed to the terminal).
    Raises VsbenchError when check is set and the command fails.
    """
    args = [str(c) for c in cmd]
    debug("run: " + " ".join(args))
    try:
        proc = subprocess.run(
            args,
            input=input,
            text=True,
            capture_output=capture,
            timeout=timeout,
            env=dict(env) if env is not None else None,
            cwd=cwd,
        )
    except FileNotFoundError as err:
        raise VsbenchError(f"command not found: {args[0]}") from err
    except subprocess.TimeoutExpired as err:
        raise VsbenchError(f"command timed out after {timeout}s: {' '.join(args[:4])} ...") from err
    if check and proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip() if capture else ""
        message = f"command failed (exit {proc.returncode}): {' '.join(args[:6])}"
        raise VsbenchError(message + (f"\n{tail(detail, 30)}" if detail else ""))
    return proc


def tail(text: str, lines: int) -> str:
    return "\n".join(text.splitlines()[-lines:])


def require_tool(name: str, hint: str | None = None) -> str:
    path = shutil.which(name)
    if not path:
        raise VsbenchError(f"required tool not found on PATH: {name}", hint)
    return path


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def iso(moment: datetime.datetime) -> str:
    return moment.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(text: str) -> datetime.datetime:
    return datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)


_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*(s|m|h|d)$")
_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(text: str) -> int:
    """Parse `90s`, `10m`, `24h`, `2d` into seconds."""
    match = _DURATION_RE.match(text.strip())
    if not match:
        raise VsbenchError(f"invalid duration '{text}'", "use a number with s, m, h or d, e.g. 30m or 24h")
    return int(float(match.group(1)) * _DURATION_UNITS[match.group(2)])


def format_duration(seconds: float) -> str:
    total = int(seconds)
    if total < 0:
        return "-" + format_duration(-total)
    hours, rem = divmod(total, 3600)
    minutes = rem // 60
    if hours >= 48:
        return f"{hours // 24}d{hours % 24}h"
    return f"{hours}h{minutes:02d}m" if hours else f"{minutes}m"


def atomic_write(path: str | Path, text: str, mode: int = 0o644) -> None:
    """Write a file atomically (tmp file in the same dir + rename)."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.")
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(text)
        os.chmod(tmp, mode)
        os.replace(tmp, target)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def dump_json(data: Any) -> str:
    return json.dumps(data, indent=2, default=str)


def print_json(data: Any) -> None:
    sys.stdout.write(dump_json(data) + "\n")
