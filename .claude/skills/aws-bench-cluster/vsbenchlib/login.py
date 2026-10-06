# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""`vsbench login`: AWS credentials through gimme-aws-creds' Okta device flow.

The flow needs the user: it prints an okta.com/activate URL that they approve in a
browser (with MFA). This runs the tool without a terminal or a browser, relays that one
URL as soon as it appears, waits for the approval, and reports the new expiry. Nothing
else the tool prints is shown.
"""

from __future__ import annotations

import os
import re
import select
import shutil
import signal
import subprocess
import time
from collections.abc import Callable
from typing import Any

from . import awsapi, config
from .awsapi import AuthError
from .proc import VsbenchError, iso

URL_RE = re.compile(r"https://[A-Za-z0-9.-]+\.okta\.com/activate\?user_code=[A-Z0-9]+")
DEFAULT_TIMEOUT_S = 600
_SECRET_RE = re.compile(r"secret|token|key|password", re.IGNORECASE)
_RETRY_HINT = "run `vsbench login` again and approve the URL it prints within a few minutes"


def default_username() -> str | None:
    """$OKTA_USERNAME, else the git user.email of the current checkout."""
    if os.environ.get("OKTA_USERNAME"):
        return os.environ["OKTA_USERNAME"]
    done = subprocess.run(["git", "config", "user.email"], text=True, capture_output=True)
    email = done.stdout.strip() if done.returncode == 0 else ""
    return email or None


def login(
    username: str | None,
    timeout_s: int = DEFAULT_TIMEOUT_S,
    on_url: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Run the device flow; call `on_url` once with the activation URL; return the result.

    Raises AuthError (exit 3) when the flow is not approved in time or the tool fails.
    """
    tool = shutil.which("gimme-aws-creds")
    if not tool:
        raise VsbenchError("gimme-aws-creds is not installed", config.INSTALL_HINT)
    user = username or default_username()
    if not user:
        raise VsbenchError("no Okta username", "pass --username <e-mail> or set OKTA_USERNAME")
    argv = [tool, "--username", user, "--roles", config.GIMME_ROLE_ARN]
    env = {**os.environ, "BROWSER": "true"}  # webbrowser.open() then runs `true <url>`: no browser
    child = subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=False,
        env=env,
        start_new_session=True,
    )
    try:
        url, lines = _relay(child, timeout_s, on_url)
    except BaseException:
        _stop(child)
        raise
    if child.returncode != 0:
        raise AuthError(_failure(child.returncode, lines, url), None, _RETRY_HINT)
    profile = awsapi.default_profile()
    expiry = awsapi.credentials_expiry(profile)
    return {"username": user, "url": url, "profile": profile, "expires_at": iso(expiry) if expiry else None}


def _relay(
    child: subprocess.Popen[bytes], timeout_s: int, on_url: Callable[[str], None] | None
) -> tuple[str | None, list[str]]:
    """Read the tool's output until it exits; report the first activation URL; keep the lines."""
    assert child.stdout is not None
    deadline, buffer, lines, url = time.monotonic() + timeout_s, b"", [], None
    while True:
        left = deadline - time.monotonic()
        if left <= 0:
            _stop(child)
            raise AuthError(f"the Okta device flow was not approved within {timeout_s} s", None, _RETRY_HINT)
        ready, _, _ = select.select([child.stdout], [], [], min(left, 1.0))
        chunk = child.stdout.read1(65536) if ready else b""  # type: ignore[attr-defined]
        if ready and not chunk:
            break
        buffer += chunk
        *complete, buffer = buffer.split(b"\n")
        for raw in complete:
            line = raw.decode("utf-8", "replace").rstrip()
            lines.append(line)
            match = URL_RE.search(line)
            if match and url is None:
                url = match.group(0)
                if on_url is not None:
                    on_url(url)
    child.wait()
    return url, lines + ([buffer.decode("utf-8", "replace")] if buffer else [])


def _stop(child: subprocess.Popen[bytes]) -> None:
    if child.poll() is None:
        with _suppress_oserror():
            os.killpg(child.pid, signal.SIGTERM)
        with _suppress_oserror():
            child.wait(timeout=5)


class _suppress_oserror:
    def __enter__(self) -> None:
        return None

    def __exit__(self, kind: Any, value: Any, traceback: Any) -> bool:
        return isinstance(value, (OSError, subprocess.TimeoutExpired))


def _failure(code: int, lines: list[str], url: str | None) -> str:
    """One line about why the tool failed, never echoing anything that looks like a secret."""
    if any("Timeout waiting for device authorization" in line for line in lines):
        return "the Okta device flow was not approved in time" + (f" ({url})" if url else "")
    safe = [line for line in lines if line.strip() and not _SECRET_RE.search(line) and "Traceback" not in line]
    detail = safe[-1].strip()[:200] if safe else "no output"
    return f"gimme-aws-creds failed (exit {code}): {detail}"
