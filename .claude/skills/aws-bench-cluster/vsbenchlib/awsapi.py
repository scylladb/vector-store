# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Thin wrapper over the `aws` CLI v2 (always JSON output, explicit profile/region)."""

from __future__ import annotations

import configparser
import datetime
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import config
from .proc import EXIT_AUTH, EXIT_CAPACITY, VsbenchError, debug

_CRED_ERRORS = (
    "ExpiredToken",
    "InvalidClientTokenId",
    "Unable to locate credentials",
    "The config profile",
    "RequestExpired",
    "security token included in the request is expired",
    "AuthFailure",
)


@dataclass(frozen=True)
class Aws:
    profile: str
    region: str

    def call(self, service: str, operation: str, *args: Any, check: bool = True) -> Any:
        """Run `aws <service> <operation> args...` and return parsed JSON (or None)."""
        if not shutil.which("aws"):
            raise VsbenchError("the aws CLI is not installed", config.INSTALL_HINT)
        cmd = ["aws", "--profile", self.profile, "--region", self.region, "--output", "json"]
        cmd += [service, operation, *[str(a) for a in args]]
        debug("aws: " + " ".join(cmd[7:]))
        # A new session: a terminal's Ctrl-C/SIGHUP goes to vsbench only, whose signal guard decides, so
        # a second Ctrl-C cannot kill a rollback's terminate-instances. When the guard raises,
        # subprocess.run still kills the child, so normal calls stay interruptible.
        proc = subprocess.run(cmd, text=True, capture_output=True, start_new_session=True)
        if proc.returncode != 0:
            if not check:
                return None
            raise aws_error(proc.stderr.strip() or proc.stdout.strip(), f"{service} {operation}")
        out = proc.stdout.strip()
        return json.loads(out) if out else None


class AwsError(VsbenchError):
    def __init__(self, message: str, code: str | None, hint: str | None = None) -> None:
        super().__init__(message, hint)
        self.code = code


class AuthError(AwsError):
    exit_code = EXIT_AUTH


class CapacityError(AwsError):
    exit_code = EXIT_CAPACITY


def error_code(stderr: str) -> str | None:
    """Extract `Code` from `An error occurred (Code) when calling ...`."""
    marker = "An error occurred ("
    start = stderr.find(marker)
    if start < 0:
        return None
    end = stderr.find(")", start)
    return stderr[start + len(marker) : end] if end > 0 else None


def aws_error(stderr: str, what: str) -> AwsError:
    code = error_code(stderr)
    if any(needle in stderr for needle in _CRED_ERRORS):
        return AuthError(f"AWS credentials are missing or expired ({what})", code, config.LOGIN_HINT)
    if code in config.CAPACITY_ERRORS:
        return CapacityError(f"AWS has no capacity for {what}: {stderr.splitlines()[0]}", code)
    if code == "UnauthorizedOperation":
        first = stderr.splitlines()[0]
        return AwsError(f"AWS denied {what}: {first}", code, "the role lacks a permission; see reference.md#aws-access")
    return AwsError(f"aws {what} failed: {stderr}", code)


def is_capacity_error(err: Exception) -> bool:
    return isinstance(err, CapacityError)


def is_not_found(err: Exception) -> bool:
    """NotFound-style errors, treated as success by idempotent deletes."""
    return isinstance(err, AwsError) and bool(err.code) and ("NotFound" in err.code or err.code.endswith(".Unknown"))


def _credentials() -> configparser.RawConfigParser:
    path = Path(os.environ.get("AWS_SHARED_CREDENTIALS_FILE", Path.home() / ".aws" / "credentials"))
    parser = configparser.RawConfigParser()
    try:
        parser.read(path)
    except (configparser.Error, OSError):
        pass
    return parser


def _expiry(parser: configparser.RawConfigParser, profile: str) -> datetime.datetime | None:
    try:
        raw = parser.get(profile, "x_security_token_expires")
        moment = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (configparser.Error, ValueError):
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=datetime.timezone.utc)


def credentials_expiry(profile: str) -> datetime.datetime | None:
    """Offline expiry of a gimme-aws-creds profile (`x_security_token_expires`).

    Returns None when unknown (no file, no profile or no expiry recorded).
    """
    return _expiry(_credentials(), profile)


def default_profile() -> str:
    """The freshest gimme-aws-creds profile of the expected account and role.

    With `cred_profile = acc-role` gimme-aws-creds names the profile
    `<account>-<role>`, but `<account>-/<path>/<role>` when the Okta config has
    `include_path = True`, so the name differs between engineers. Pick the
    matching section with the latest expiry; fall back to config.DEFAULT_PROFILE.
    """
    role = config.GIMME_ROLE_ARN.rsplit("/", 1)[-1]
    pattern = re.compile(rf"^{re.escape(config.EXPECTED_ACCOUNT)}-(/.*/|/)?{re.escape(role)}$")
    parser = _credentials()
    oldest = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    candidates = [(_expiry(parser, s) or oldest, s) for s in parser.sections() if pattern.match(s)]
    return max(candidates)[1] if candidates else config.DEFAULT_PROFILE


@dataclass(frozen=True)
class Identity:
    account: str
    arn: str
    owner: str


def owner_from_arn(arn: str) -> str | None:
    """`arn:aws:sts::1:assumed-role/Role/first.last@scylladb.com` -> `first.last`."""
    if ":assumed-role/" not in arn:
        return None
    session = arn.rsplit("/", 1)[-1]
    return session.split("@", 1)[0].lower() or None


def identity(aws: Aws) -> Identity:
    data = aws.call("sts", "get-caller-identity")
    owner = owner_from_arn(data["Arn"])
    if not owner:
        raise VsbenchError(
            f"cannot derive the owner from the caller ARN {data['Arn']}",
            "use credentials from gimme-aws-creds (assumed role with your e-mail as session name)",
        )
    return Identity(account=data["Account"], arn=data["Arn"], owner=owner)


def tag_list(tags: dict[str, str]) -> list[dict[str, str]]:
    return [{"Key": k, "Value": v} for k, v in tags.items()]


def tags_of(resource: dict[str, Any]) -> dict[str, str]:
    return {t["Key"]: t["Value"] for t in resource.get("Tags", [])}
