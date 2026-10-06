# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""argparse `type=` callables of the vsbench command line (moved out of cli.py)."""

from __future__ import annotations

import argparse
import datetime

from . import proc, retro
from .proc import VsbenchError


def _duration(text: str) -> int:
    try:
        return proc.parse_duration(text)
    except VsbenchError as err:
        raise argparse.ArgumentTypeError(f"invalid duration '{text}' (use e.g. 30s, 10m, 2h)") from err


def _ttl(text: str) -> str:
    _duration(text)
    return text


def _positive(text: str) -> int:
    if not text.isdigit() or int(text) < 1:
        raise argparse.ArgumentTypeError(f"expected a positive integer, got '{text}'")
    return int(text)


def _count(text: str) -> int:
    if not text.isdigit():
        raise argparse.ArgumentTypeError(f"expected a non-negative integer, got '{text}'")
    return int(text)


def _int_list(text: str) -> list[int]:
    return [_positive(part.strip()) for part in text.split(",")]


def _billing_project(text: str) -> str:
    if not text.strip():
        raise argparse.ArgumentTypeError("the billing project must not be empty, e.g. 'Vector Search: Sharding'")
    return text.strip()


def _env_key(text: str) -> str:
    if not text or not (text[0].isalpha() or text[0] == "_") or not text.replace("_", "a").isalnum():
        raise argparse.ArgumentTypeError(f"invalid environment variable name '{text}'")
    return text


def _env_pair(text: str) -> tuple[str, str]:
    key, sep, value = text.partition("=")
    if not sep:
        raise argparse.ArgumentTypeError(f"expected K=V, got '{text}'")
    return _env_key(key), value


def _param(text: str) -> tuple[str, str]:
    key, sep, value = text.partition("=")
    if not sep or not key:
        raise argparse.ArgumentTypeError(f"expected k=v, got '{text}'")
    return key, value


def _since(text: str) -> datetime.datetime:
    try:
        return retro.parse_time(text)
    except VsbenchError as err:
        raise argparse.ArgumentTypeError(str(err)) from err
