# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Local state: paths, cluster state.json, command history."""

from __future__ import annotations

import contextlib
import copy
import fcntl
import json
import os
import re
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .proc import PreconditionError, VsbenchError, atomic_write

SCHEMA = 1
CLUSTER_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,19}$")
State = dict[str, Any]
Node = dict[str, Any]


def home() -> Path:
    return Path(os.environ.get("VSBENCH_HOME", Path.home() / ".local" / "state" / "vsbench"))


def validate_cluster_name(name: str) -> str:
    if not CLUSTER_NAME_RE.match(name):
        raise VsbenchError(
            f"invalid cluster name '{name}'", "use 1-20 chars of [a-z0-9-], starting with a letter or digit"
        )
    return name


@dataclass(frozen=True)
class ClusterPaths:
    root: Path

    @property
    def state_file(self) -> Path:
        return self.root / "state.json"

    @property
    def ssh_dir(self) -> Path:
        return self.root / "ssh"

    @property
    def ssh_key(self) -> Path:
        return self.ssh_dir / "id_ed25519"

    @property
    def ssh_config(self) -> Path:
        return self.ssh_dir / "config"

    @property
    def known_hosts(self) -> Path:
        return self.ssh_dir / "known_hosts"

    @property
    def results_dir(self) -> Path:
        return self.root / "results"

    @property
    def results_file(self) -> Path:
        return self.results_dir / "results.jsonl"

    @property
    def work_dir(self) -> Path:
        """Scratch files (rendered templates, tag files) kept for debugging."""
        return self.root / "work"


def paths(cluster: str) -> ClusterPaths:
    return ClusterPaths(home() / "clusters" / validate_cluster_name(cluster))


def builds_dir() -> Path:
    return home() / "builds"


def src_dir() -> Path:
    return home() / "src"


def load(cluster: str) -> State | None:
    state_file = paths(cluster).state_file
    if not state_file.exists():
        return None
    with open(state_file) as handle:
        state = json.load(handle)
    if state.get("schema") != SCHEMA:
        raise VsbenchError(f"unsupported state schema in {state_file}: {state.get('schema')}")
    return state


def require(cluster: str) -> State:
    state = load(cluster)
    if state is None or not state.get("nodes"):
        raise VsbenchError(f"cluster '{cluster}' has no local state", f"create it with: vsbench -c {cluster} up")
    if state.get("terminated_at"):
        raise VsbenchError(
            f"cluster '{cluster}' was torn down at {state['terminated_at']}",
            f"create a new one with: vsbench -c {cluster} up",
        )
    return state


def save(cluster: str, state: State) -> None:
    atomic_write(paths(cluster).state_file, json.dumps(state, indent=2) + "\n", mode=0o600)


STATE_LOCK_TIMEOUT_S = 30


@contextlib.contextmanager
def _state_write_lock(cluster: str) -> Iterator[None]:
    """Short blocking lock around one load-change-save cycle.

    Separate from cluster_lock: commands that run without the cluster lock
    (extend, refresh-ip, job cancel) still serialize their state writes with
    a long-running command such as `bench ab`, so neither loses the other's
    changes.
    """
    lock_file = paths(cluster).root / "state.lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_file, "a") as handle:
        deadline = time.monotonic() + STATE_LOCK_TIMEOUT_S
        while True:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as err:
                if time.monotonic() >= deadline:
                    raise PreconditionError(
                        f"state of cluster '{cluster}' stayed locked for {STATE_LOCK_TIMEOUT_S}s"
                    ) from err
                time.sleep(0.1)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def update(cluster: str, change: Callable[[State], State]) -> State:
    """Load, apply `change` to a deep copy, save and return the new state.

    `change` must be quick and local (no ssh/AWS calls): it runs under a lock
    that other vsbench processes wait for.
    """
    with _state_write_lock(cluster):
        current = load(cluster) or {"schema": SCHEMA, "cluster": cluster}
        updated = change(copy.deepcopy(current))
        save(cluster, updated)
    return updated


def with_deployed(state: State, component: str, info: dict[str, Any] | None) -> State:
    """Return a copy of `state` with state["deployed"][component] = info."""
    result = copy.deepcopy(state)
    result.setdefault("deployed", {})[component] = info
    return result


def built_index(state: State) -> dict[str, Any] | None:
    """The loaded index, only if its build finished and no load/index job owns it.

    `bench load`/`bench index` record the new index name before their job runs,
    so a failed, cancelled or still-running job leaves `load.index` naming an
    index that does not exist. Everything that waits for or searches an index
    must use this instead of reading `load.index` directly.
    """
    load = state.get("load") or {}
    if not load.get("index") or load.get("pending_job"):
        return None
    if not (load.get("phases") or {}).get("index"):
        return None
    return load


# --- Nodes ----------------------------------------------------------------


def nodes(state: State, role: str | None = None) -> list[Node]:
    selected = [n for n in state.get("nodes", []) if role is None or n["role"] == role]
    return sorted(selected, key=lambda n: (("scylla", "vs", "client").index(n["role"]), n["index"]))


def node(state: State, name: str) -> Node:
    for candidate in state.get("nodes", []):
        if candidate["name"] == name:
            return candidate
    known = ", ".join(n["name"] for n in nodes(state))
    raise VsbenchError(f"unknown node '{name}'", f"known nodes: {known}")


def client(state: State) -> Node:
    return node(state, "client")


def resolve_targets(state: State, spec: str) -> list[Node]:
    """`all`, a role (`scylla`, `vs`, `client`) or comma-separated node names."""
    if spec == "all":
        return nodes(state)
    if spec in ("scylla", "vs"):
        return nodes(state, spec)
    return [node(state, name) for name in spec.split(",") if name]


# --- Locking ----------------------------------------------------------------


@contextlib.contextmanager
def cluster_lock(cluster: str, command: str) -> Iterator[None]:
    """Exclusive per-cluster lock for mutating commands (fails fast if busy)."""
    lock_file = paths(cluster).root / "lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_file, "a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as err:
            handle.seek(0)
            holder = handle.read().strip() or "another vsbench command"
            raise PreconditionError(
                f"cluster '{cluster}' is busy: {holder}",
                "wait for it to finish (never run two mutating vsbench commands at once); "
                "status, extend, refresh-ip and job cancel work meanwhile",
            ) from err
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid {os.getpid()}: vsbench {command}\n")
        handle.flush()
        try:
            yield
        finally:
            handle.seek(0)
            handle.truncate()
            fcntl.flock(handle, fcntl.LOCK_UN)


# --- History ----------------------------------------------------------------
# One global append-only file (never purged) so retrospectives can see
# patterns across clusters and sessions.


def history_file() -> Path:
    return home() / "history.jsonl"


def append_history(entry: dict[str, Any]) -> None:
    target = history_file()
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "a") as handle:
        handle.write(json.dumps(entry) + "\n")


def read_history() -> list[dict[str, Any]]:
    target = history_file()
    if not target.exists():
        return []
    entries = []
    with open(target) as handle:
        for line in handle:
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def retrospectives_dir() -> Path:
    return home() / "retrospectives"
