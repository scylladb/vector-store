# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.awsapi: the `aws` CLI wrapper and its error classification."""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vsbenchlib import awsapi  # noqa: E402
from vsbenchlib.awsapi import AuthError, AwsError, CapacityError  # noqa: E402


class CallTest(unittest.TestCase):
    def call(self, returncode: int, stdout: str = "", stderr: str = "") -> tuple[object, mock.Mock]:
        done = subprocess.CompletedProcess([], returncode, stdout, stderr)
        with (
            mock.patch.object(awsapi.shutil, "which", return_value="/usr/bin/aws"),
            mock.patch.object(awsapi.subprocess, "run", return_value=done) as run,
        ):
            aws = awsapi.Aws("prof", "us-east-1")
            return aws.call("ec2", "terminate-instances", "--instance-ids", "i-1"), run

    def test_child_runs_in_its_own_session(self) -> None:
        # A second terminal Ctrl-C (or SIGHUP) must not kill a rollback's terminate-instances.
        result, run = self.call(0, '{"TerminatingInstances": []}')
        self.assertEqual(result, {"TerminatingInstances": []})
        self.assertIs(run.call_args.kwargs.get("start_new_session"), True)
        cmd = run.call_args.args[0]
        self.assertEqual(cmd[:7], ["aws", "--profile", "prof", "--region", "us-east-1", "--output", "json"])

    def test_errors_are_classified(self) -> None:
        cases = [
            ("An error occurred (InsufficientInstanceCapacity) when calling RunInstances", CapacityError),
            ("An error occurred (ExpiredToken) when calling X: expired", AuthError),
            ("An error occurred (InvalidGroup.NotFound) when calling X", AwsError),
        ]
        for stderr, kind in cases:
            with self.assertRaises(kind) as ctx:
                self.call(255, stderr=stderr)
            self.assertEqual(ctx.exception.code, awsapi.error_code(stderr))
        self.assertTrue(awsapi.is_not_found(AwsError("x", "InvalidGroup.NotFound")))


class DefaultProfileTest(unittest.TestCase):
    """gimme-aws-creds names the profile differently depending on `include_path`."""

    def credentials(self, text: str) -> str:
        import tempfile

        handle = tempfile.NamedTemporaryFile("w", suffix=".ini", delete=False)
        self.addCleanup(Path(handle.name).unlink)
        handle.write(text)
        handle.close()
        return handle.name

    def profile(self, text: str) -> str:
        with mock.patch.dict(awsapi.os.environ, {"AWS_SHARED_CREDENTIALS_FILE": self.credentials(text)}):
            return awsapi.default_profile()

    def test_picks_the_freshest_matching_profile(self) -> None:
        text = (
            "[797456418907-DeveloperAccessRole]\nx_security_token_expires = 2025-02-10T17:43:47+00:00\n"
            "[797456418907-/DeveloperAccessRole]\nx_security_token_expires = 2026-10-06T19:40:00+00:00\n"
            "[111111111111-/DeveloperAccessRole]\nx_security_token_expires = 2030-01-01T00:00:00+00:00\n"
            "[797456418907-/OtherRole]\nx_security_token_expires = 2030-01-01T00:00:00+00:00\n"
        )
        self.assertEqual(self.profile(text), "797456418907-/DeveloperAccessRole")

    def test_path_variants_match(self) -> None:
        text = "[797456418907-/team/DeveloperAccessRole]\nx_security_token_expires = 2026-10-06T19:40:00Z\n"
        self.assertEqual(self.profile(text), "797456418907-/team/DeveloperAccessRole")

    def test_falls_back_to_the_documented_name(self) -> None:
        self.assertEqual(self.profile("[other]\naws_access_key_id = x\n"), awsapi.config.DEFAULT_PROFILE)


if __name__ == "__main__":
    unittest.main()
