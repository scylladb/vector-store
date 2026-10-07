# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.login against a fake gimme-aws-creds on PATH."""

from __future__ import annotations

import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vsbenchlib import config, login  # noqa: E402
from vsbenchlib.awsapi import AuthError  # noqa: E402
from vsbenchlib.proc import VsbenchError  # noqa: E402

URL = "https://scylladb.okta.com/activate?user_code=ABCDWXYZ"
FAKE = """#!/usr/bin/env bash
# fake gimme-aws-creds: $FAKE_MODE = ok | timeout | fail
echo "The system web browser will open the following URL to begin Okta device authorization:"
echo "{url}"
echo ""
case "${{FAKE_MODE:-ok}}" in
  ok)
    sleep 0.3
    printf '[{profile}]\\naws_access_key_id = AKIAFAKE\\n' > "$AWS_SHARED_CREDENTIALS_FILE"
    printf 'aws_secret_access_key = fake\\n' >> "$AWS_SHARED_CREDENTIALS_FILE"
    printf 'x_security_token_expires = 2026-10-07T01:00:00+00:00\\n' >> "$AWS_SHARED_CREDENTIALS_FILE"
    echo "Saving arn:aws:iam::797456418907:role/DeveloperAccessRole as {profile}"
    echo "Written profile {profile} to $AWS_SHARED_CREDENTIALS_FILE"
    ;;
  timeout)
    sleep 30
    ;;
  fail)
    echo "Traceback (most recent call last):"
    echo "Exception: Timeout waiting for device authorization"
    exit 1
    ;;
esac
"""


class LoginTest(unittest.TestCase):
    profile = f"{config.EXPECTED_ACCOUNT}-/DeveloperAccessRole"

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="vsbench-login-"))
        self.addCleanup(lambda: os.system(f"rm -rf {self.tmp}"))
        script = self.tmp / "gimme-aws-creds"
        script.write_text(FAKE.format(url=URL, profile=self.profile))
        script.chmod(script.stat().st_mode | stat.S_IEXEC)
        env = {
            "PATH": f"{self.tmp}:{os.environ.get('PATH', '')}",
            "AWS_SHARED_CREDENTIALS_FILE": str(self.tmp / "credentials"),
            "OKTA_USERNAME": "first.last@scylladb.com",
            "FAKE_MODE": "ok",
        }
        patcher = mock.patch.dict(os.environ, env)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_relays_the_url_first_and_reports_the_expiry(self) -> None:
        seen: list[tuple[str, bool]] = []
        result = login.login(None, 30, on_url=lambda u: seen.append((u, (self.tmp / "credentials").exists())))
        self.assertEqual(seen, [(URL, False)])  # relayed before the tool finished
        self.assertEqual((result["username"], result["url"]), ("first.last@scylladb.com", URL))
        self.assertEqual((result["profile"], result["expires_at"]), (self.profile, "2026-10-07T01:00:00Z"))

    def test_unapproved_flow_times_out_with_exit_3(self) -> None:
        os.environ["FAKE_MODE"] = "timeout"
        with self.assertRaises(AuthError) as ctx:
            login.login("x@scylladb.com", 1)
        self.assertIn("not approved within 1 s", str(ctx.exception))
        self.assertIn("vsbench login", ctx.exception.hint or "")

    def test_tool_failure_is_exit_3_without_echoing_secrets(self) -> None:
        os.environ["FAKE_MODE"] = "fail"
        with self.assertRaises(AuthError) as ctx:
            login.login("x@scylladb.com", 30)
        self.assertIn("the Okta code expired before it was approved", str(ctx.exception))
        self.assertNotIn("Traceback", str(ctx.exception))

    def test_missing_tool_and_username(self) -> None:
        with mock.patch.dict(os.environ, {"PATH": str(self.tmp / "nowhere")}):
            with self.assertRaises(VsbenchError) as ctx:
                login.login("x@scylladb.com", 30)
            self.assertEqual(ctx.exception.hint, config.INSTALL_HINT)
        with (
            mock.patch.dict(os.environ, {"OKTA_USERNAME": ""}),
            mock.patch.object(login, "default_username", return_value=None),
        ):
            with self.assertRaises(VsbenchError) as ctx:
                login.login(None, 30)
            self.assertIn("--username", ctx.exception.hint or "")


class FailureTextTest(unittest.TestCase):
    def test_last_safe_line_is_used(self) -> None:
        lines = ["Using inherited config", "aws_secret_access_key = x", "Error: 400 Client Error: Bad Request"]
        self.assertEqual(
            login._failure(1, lines, None), "gimme-aws-creds failed (exit 1): Error: 400 Client Error: Bad Request"
        )
        self.assertEqual(login._failure(2, [], None), "gimme-aws-creds failed (exit 2): no output")


if __name__ == "__main__":
    unittest.main()
