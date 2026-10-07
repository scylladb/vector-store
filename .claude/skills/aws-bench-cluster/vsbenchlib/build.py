# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Vector Store builds: source specs, version stamping, the arm64 cross build and the local build cache.

Sources:
- `release:<X.Y.Z>` / `release:latest`: official binaries, nothing is built locally
  (the node extracts the binary from the Docker Hub image).
- `git:<ref>`: a branch, tag or commit of scylladb/vector-store, fetched from the
  upstream remote and exported with `git archive` into `state.src_dir()/<commit>`.
- `local` / `local:+<label>`: the working tree of this repository.
- `build:<build_id>`: an existing entry of the local build cache.

Builds run `cross/cross-build.sh` then the `cross/verify.sh` gate, and land in
`state.builds_dir()/<build_id>/{vector-store,vector-search-benchmark,build.json}`
where build_id = `<version, "+" -> "_">-<sha256(vector-store)[:8]>`.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

from . import config, proc, state
from .proc import PreconditionError, VsbenchError

SOURCE_KINDS = ("release", "git", "local", "build")
SOURCE_FORMS = {
    "release": "release:<X.Y.Z>, release:latest",
    "git": "git:<branch|tag|commit>",
    "local": "local, local:+<label>",
    "build": "build:<build_id>",
}
SEMVER_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$")
RELEASE_VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$")
# No leading '-' (option injection), no ':' (refspec), no whitespace.
GIT_REF_RE = re.compile(r"^[0-9A-Za-z_][0-9A-Za-z._/~^@{}-]*$")
FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
LABEL_RE = re.compile(r"^[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*$")
BUILD_ID_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._-]*$")
TOOLCHAIN_CHANNEL_RE = re.compile(r'^\s*channel\s*=\s*"([^"]+)"', re.MULTILINE)
CHANNEL_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z.-]*$")
# Only version tags count for `git describe` (the repo also has tags like `initial`).
VERSION_TAG_GLOB = "[0-9]*.[0-9]*.[0-9]*"
DIRTY_HASH_PATHSPEC = (".", ":(exclude).claude")
BINARIES = ("vector-store", "vector-search-benchmark")
TARGET_SUBDIR = "cross-aarch64"
RELEASE_PREFIX = "release-"

BUILD_TIMEOUT_S = 3600
VERIFY_TIMEOUT_S = 900
IMAGE_BUILD_TIMEOUT_S = 1800
FETCH_TIMEOUT_S = 300
GITHUB_TIMEOUT_S = 20
STOP_GRACE_S = 15
KEEP_LOGS = 50


@dataclass(frozen=True)
class SourceSpec:
    kind: str  # "release" | "git" | "local" | "build"
    ref: str  # version | git ref | label ("" for plain local) | build_id

    def __str__(self) -> str:
        if self.kind == "local":
            return f"local:+{self.ref}" if self.ref else "local"
        return f"{self.kind}:{self.ref}"


# --- Source specs -------------------------------------------------------------


def parse_source(text: str, allow: set[str]) -> SourceSpec:
    """Parse `release:<ver>|release:latest|git:<ref>|local[:+label]|build:<id>`."""
    raw = text.strip()
    kind, has_ref, ref = raw.partition(":")
    hint = "allowed: " + "; ".join(SOURCE_FORMS[k] for k in SOURCE_KINDS if k in allow)
    if kind not in SOURCE_KINDS:
        raise VsbenchError(f"invalid source '{text}'", hint)
    if kind not in allow:
        raise VsbenchError(f"source '{text}' is not allowed here", hint)
    if kind == "local":
        label = ref.removeprefix("+")
        if has_ref and not LABEL_RE.match(label):
            raise VsbenchError(f"invalid label in '{text}'", "labels are SemVer build metadata: [0-9A-Za-z-] and dots")
        return SourceSpec("local", label)
    if not ref:
        raise VsbenchError(f"source '{text}' needs a value after '{kind}:'", hint)
    if kind == "release" and ref != "latest" and not RELEASE_VERSION_RE.match(ref):
        raise VsbenchError(
            f"invalid release version '{ref}'", "use release:<X.Y.Z> (e.g. release:1.11.0) or release:latest"
        )
    if kind == "git" and not GIT_REF_RE.match(ref):
        raise VsbenchError(f"invalid git ref '{ref}'", "use a branch, tag or commit sha (no ':' or leading '-')")
    if kind == "build":
        _check_build_id(ref)
    return SourceSpec(kind, ref)


def _check_build_id(build_id: str) -> str:
    if not BUILD_ID_RE.match(build_id) or ".." in build_id:
        raise VsbenchError(f"invalid build id '{build_id}'", "list cached builds with: vsbench builds")
    return build_id


def make_build_id(version: str, vector_store_sha256: str) -> str:
    return f"{version.replace('+', '_')}-{vector_store_sha256[:8]}"


def check_semver(version: str) -> str:
    if not SEMVER_RE.match(version):
        raise VsbenchError(
            f"version '{version}' is not valid SemVer",
            "cargo needs X.Y.Z[-pre][+build] using only [0-9A-Za-z.-]",
        )
    return version


# --- Git ------------------------------------------------------------------------


def _git(repo: Path, *args: str, check: bool = True, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    return proc.run(["git", "-C", repo, *args], check=check, timeout=timeout, env=env)


def _git_bytes(repo: Path, *args: str) -> bytes:
    """Run git and return raw stdout (diffs may contain non-UTF-8 bytes)."""
    cmd = ["git", "-C", str(repo), *args]
    proc.debug("run: " + " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        detail = result.stderr.decode(errors="replace").strip()
        raise VsbenchError(f"command failed (exit {result.returncode}): {' '.join(cmd)}\n{proc.tail(detail, 10)}")
    return result.stdout


def _is_workspace(root: Path) -> bool:
    return (root / "Cargo.toml").is_file() and (root / "crates" / "vector-store").is_dir()


def repo_root() -> Path:
    """The vector-store checkout: git toplevel of the skill dir (else of the cwd)."""
    for start in (config.SKILL_DIR, Path.cwd()):
        result = proc.run(["git", "-C", start, "rev-parse", "--show-toplevel"], check=False)
        if result.returncode == 0 and _is_workspace(Path(result.stdout.strip())):
            return Path(result.stdout.strip())
    raise VsbenchError(
        "cannot find the vector-store repository",
        "run vsbench from a vector-store checkout that contains this skill in .claude/skills",
    )


def upstream_remote(repo: Path) -> str | None:
    """Name of the remote pointing at scylladb/vector-store (prefers `upstream`)."""
    result = _git(repo, "config", "--get-regexp", r"^remote\..*\.url$", check=False)
    matches = []
    for line in result.stdout.splitlines():
        key, _, url = line.partition(" ")
        if re.search(config.VS_UPSTREAM_REMOTE_RE, url.strip()):
            matches.append(key[len("remote.") : -len(".url")])
    if not matches:
        return None
    return "upstream" if "upstream" in matches else sorted(matches)[0]


def _local_commit(repo: Path, ref: str) -> str | None:
    result = _git(repo, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}", check=False)
    sha = result.stdout.strip()
    return sha if result.returncode == 0 and FULL_SHA_RE.match(sha) else None


def _fetch_sources(repo: Path) -> list[str]:
    remote = upstream_remote(repo)
    return [remote, config.VS_GIT_URL] if remote else [config.VS_GIT_URL]


def _fetch_commit(repo: Path, ref: str) -> str:
    """Fetch `ref` from upstream and return its commit; fall back to a local ref."""
    errors = []
    for source in _fetch_sources(repo):
        proc.log(f"fetching {ref} from {source}")
        try:
            result = _git(repo, "fetch", "--quiet", source, ref, check=False, timeout=FETCH_TIMEOUT_S)
        except VsbenchError as err:  # timeout
            errors.append(f"{source}: {err}")
            continue
        if result.returncode == 0:
            commit = _git(repo, "rev-parse", "--verify", "FETCH_HEAD^{commit}").stdout.strip()
            # Separate call: a clobbered local tag must not fail the ref fetch.
            tags = _git(repo, "fetch", "--quiet", "--tags", source, check=False, timeout=FETCH_TIMEOUT_S)
            if tags.returncode != 0:
                proc.warn(f"could not fetch tags from {source}; the version string may be off")
            return commit
        errors.append(f"{source}: {proc.tail((result.stderr or '').strip(), 3)}")
    local = _local_commit(repo, ref)
    if local:
        proc.warn(f"'{ref}' was not fetched from upstream; using the local ref ({local[:12]})")
        return local
    raise VsbenchError(
        f"cannot resolve git ref '{ref}':\n" + "\n".join(errors),
        "use a branch, tag or commit of scylladb/vector-store, or a ref of the local repository",
    )


def _tree_dirty(repo: Path) -> bool:
    """Tracked changes against HEAD outside .claude/ (the pathspec of dirty_hash): edits to the
    skill itself do not change what gets built."""
    return _git(repo, "diff", "--quiet", "HEAD", "--", *DIRTY_HASH_PATHSPEC, check=False).returncode != 0


def _describe(repo: Path, commit: str | None) -> str:
    """`git describe --tags` of `commit` (or of HEAD; local_version adds -dirty itself)."""
    args = ["describe", "--tags", "--match", VERSION_TAG_GLOB]
    args += [commit] if commit else []
    result = _git(repo, *args, check=False)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    sha = commit or _git(repo, "rev-parse", "HEAD").stdout.strip()
    fallback = f"0.0.0-g{sha[:7]}"
    proc.warn(f"no version tag is reachable from {sha[:12]}; using version {fallback}")
    return fallback


def resolve_git(spec: SourceSpec, repo: Path) -> tuple[str, str]:
    """(commit sha, version) of a `git:<ref>` source; fetches from upstream unless the sha is local."""
    commit = _local_commit(repo, spec.ref) if FULL_SHA_RE.match(spec.ref) else None
    if commit is None:
        commit = _fetch_commit(repo, spec.ref)
    return commit, check_semver(_describe(repo, commit))


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _untracked_digest(path: Path) -> bytes:
    if path.is_symlink():
        return b"link:" + os.fsencode(os.readlink(path))
    if path.is_file():
        return _file_sha256(path).encode()
    return b"other"


def dirty_hash(repo: Path) -> str:
    """8 hex chars identifying uncommitted changes: `git diff HEAD --binary` + untracked files.

    `.claude/` (agent settings, skills) is left out: it never affects the Rust
    build and changes all the time while an agent works.
    """
    digest = hashlib.sha256(_git_bytes(repo, "diff", "HEAD", "--binary", "--", *DIRTY_HASH_PATHSPEC))
    listing = _git_bytes(repo, "ls-files", "--others", "--exclude-standard", "-z", "--", *DIRTY_HASH_PATHSPEC)
    for name in sorted(n for n in listing.split(b"\0") if n):
        digest.update(b"\0" + name + b"\0" + _untracked_digest(repo / os.fsdecode(name)))
    return digest.hexdigest()[:8]


def local_version(repo: Path, label: str) -> tuple[str, str, bool]:
    """(version, commit, dirty) of the working tree.

    `git describe --tags`, `-dirty` when tracked files outside .claude/ differ from HEAD
    (+ `+<label>`); a dirty tree also gets build metadata `d<dirty_hash>` so different
    uncommitted states get different versions.
    """
    commit = _git(repo, "rev-parse", "HEAD").stdout.strip()
    version, dirty = _describe(repo, None), _tree_dirty(repo)
    if dirty:
        version += "-dirty"
    if label:
        version += f"+{label}"
    if dirty:
        version += ("." if "+" in version else "+") + "d" + dirty_hash(repo)
    return check_semver(version), commit, dirty


def export_tree(repo: Path, commit: str) -> Path:
    """`git archive <commit>` extracted into `src_dir()/<commit>` (reused when present)."""
    final = state.src_dir() / commit
    if (final / "Cargo.toml").is_file():
        return final
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.parent / f".tmp-{commit[:12]}-{uuid.uuid4().hex[:8]}"
    archive = tmp.with_suffix(".tar")
    try:
        tmp.mkdir()
        proc.log(f"exporting {commit[:12]} to {final}")
        _git(repo, "archive", "--format=tar", "-o", str(archive), commit)
        proc.run(["tar", "-x", "-f", archive, "-C", tmp])
        if final.exists():
            shutil.rmtree(final)
        os.rename(tmp, final)
    finally:
        archive.unlink(missing_ok=True)
        shutil.rmtree(tmp, ignore_errors=True)
    return final


# --- Releases -------------------------------------------------------------------


def _github_json(url: str) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "vsbench",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=GITHUB_TIMEOUT_S) as response:
            return json.load(response)
    except urllib.error.HTTPError as err:
        hint = "GitHub API rate limit? " if err.code in (403, 429) else ""
        raise VsbenchError(
            f"GitHub API request failed: HTTP {err.code} for {url}", hint + "pass an explicit release:<X.Y.Z>"
        ) from err
    except (OSError, ValueError) as err:
        raise VsbenchError(f"GitHub API request failed for {url}: {err}", "pass an explicit release:<X.Y.Z>") from err


def resolve_release(ref: str) -> str:
    """`latest` -> the newest GitHub release tag (never Docker Hub `:latest`, which is stale)."""
    if ref != "latest":
        if not RELEASE_VERSION_RE.match(ref):
            raise VsbenchError(f"invalid release version '{ref}'", "use release:<X.Y.Z> or release:latest")
        return ref
    data = _github_json(config.VS_RELEASES_API)
    tag = str(data.get("tag_name") or "") if isinstance(data, dict) else ""
    version = tag.removeprefix("v")
    if not RELEASE_VERSION_RE.match(version):
        raise VsbenchError(f"unexpected latest release tag '{tag}' from {config.VS_RELEASES_API}")
    proc.log(f"release:latest is {version}")
    return version


def release_build(version: str, source: str) -> dict[str, Any]:
    """build.json-like record of an official release (no local binaries)."""
    return {
        "build_id": f"{RELEASE_PREFIX}{version}",
        "kind": "release",
        "version": version,
        "source": source,
        "pin": f"release:{version}",
        "commit": None,
        "dirty": False,
        "rustc_channel": None,
        "built_at": None,
        "sha256": {},
    }


# --- Cross image and process helpers ------------------------------------------


def rust_channel(src: Path) -> str:
    path = src / "rust-toolchain.toml"
    try:
        match = TOOLCHAIN_CHANNEL_RE.search(path.read_text())
    except OSError as err:
        raise VsbenchError(f"cannot read {path}: {err}") from err
    if not match or not CHANNEL_RE.match(match.group(1)):
        raise VsbenchError(f"no valid toolchain channel in {path}")
    return match.group(1)


def cross_image_tag(src: Path) -> str:
    """`vs-bench-cross:<channel of src/rust-toolchain.toml>`; built from cross/Dockerfile if missing."""
    channel = rust_channel(src)
    tag = f"{config.CROSS_IMAGE_REPO}:{channel}"
    proc.require_tool("docker", "install Docker; builds run in a local amd64 container")
    inspect = proc.run(["docker", "image", "inspect", "--format", "{{.Id}}", tag], check=False)
    if inspect.returncode == 0:
        return tag
    if "no such" not in (inspect.stderr or "").lower():
        raise PreconditionError(
            f"docker is not usable: {proc.tail((inspect.stderr or '').strip(), 3)}",
            "start the Docker daemon and make sure this user may run docker",
        )
    proc.log(f"building the cross image {tag} (one-off, about 1 min)")
    cmd = ["docker", "build", "--build-arg", f"RUST_VERSION={channel}", "-t", tag, str(config.CROSS_DIR)]
    _run_logged(cmd, log_path=_new_log_path("image"), timeout_s=IMAGE_BUILD_TIMEOUT_S, what=f"docker build {tag}")
    return tag


def _new_log_path(kind: str) -> Path:
    logs = state.builds_dir() / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    for old in sorted(logs.glob("*.log"))[:-KEEP_LOGS]:
        old.unlink(missing_ok=True)
    stamp = proc.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return logs / f"{stamp}-{kind}-{uuid.uuid4().hex[:4]}.log"


def _stop(child: subprocess.Popen[str]) -> None:
    """SIGTERM the child's process group (docker forwards it to the container), then SIGKILL."""
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if child.poll() is not None:
            return
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(child.pid, sig)
        with contextlib.suppress(subprocess.TimeoutExpired):
            child.wait(timeout=STOP_GRACE_S)


def _pump(child: subprocess.Popen[str], out: TextIO) -> int:
    """Copy the child's output to `out` (and to stderr with -v) until it exits."""
    assert child.stdout is not None
    for line in child.stdout:
        out.write(line)
        if proc.VERBOSE:
            sys.stderr.write(line)
    return child.wait()


def _run_logged(
    cmd: list[str], *, log_path: Path, timeout_s: float, what: str, env: dict[str, str] | None = None
) -> None:
    """Run cmd with stdout+stderr appended to log_path; on failure raise with the log tail.

    The child gets its own process group so a timeout or Ctrl-C stops all of it.
    """
    proc.debug("run: " + " ".join(cmd))
    timed_out = threading.Event()
    with open(log_path, "a", encoding="utf-8", errors="replace") as out:
        out.write("$ " + " ".join(cmd) + "\n")
        out.flush()
        try:
            child = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                errors="replace",
                env=env,
                start_new_session=True,
            )
        except FileNotFoundError as err:
            raise VsbenchError(f"command not found: {cmd[0]}") from err

        def on_timeout() -> None:
            timed_out.set()
            _stop(child)

        timer = threading.Timer(timeout_s, on_timeout)
        timer.start()
        with child:
            try:
                code = _pump(child, out)
            except BaseException:
                _stop(child)
                raise
            finally:
                timer.cancel()
    if timed_out.is_set():
        raise VsbenchError(f"{what} timed out after {timeout_s:g}s", f"full log: {log_path}")
    if code != 0:
        text = log_path.read_text(errors="replace")
        raise VsbenchError(f"{what} failed (exit {code}):\n{proc.tail(text.rstrip(), 25)}", f"full log: {log_path}")


@contextlib.contextmanager
def _build_lock() -> Iterator[None]:
    """One cross build at a time per host (they share the cargo target dir)."""
    lock_file = state.builds_dir() / ".lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_file, "a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as err:
            handle.seek(0)
            holder = handle.read().strip() or "unknown process"
            raise PreconditionError(
                f"another vsbench build is running ({holder})", "wait for it to finish, then retry"
            ) from err
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid {os.getpid()}\n")
        handle.flush()
        try:
            yield
        finally:
            handle.seek(0)
            handle.truncate()
            fcntl.flock(handle, fcntl.LOCK_UN)


# --- Build cache --------------------------------------------------------------------


def build_dir(build_id: str) -> Path:
    return state.builds_dir() / _check_build_id(build_id)


def load_build(build_id: str) -> dict[str, Any]:
    """build.json of a cached build (`release-<ver>` ids are synthesized)."""
    if build_id.startswith(RELEASE_PREFIX) and RELEASE_VERSION_RE.match(build_id[len(RELEASE_PREFIX) :]):
        version = build_id[len(RELEASE_PREFIX) :]
        return release_build(version, f"release:{version}")
    directory = build_dir(build_id)
    meta = directory / "build.json"
    if not meta.is_file():
        raise VsbenchError(f"unknown build '{build_id}'", "list cached builds with: vsbench builds")
    try:
        info: dict[str, Any] = json.loads(meta.read_text())
    except (OSError, ValueError) as err:
        raise VsbenchError(f"cannot read {meta}: {err}", f"remove {directory} and rebuild") from err
    missing = [b for b in BINARIES if not (directory / b).is_file()]
    if missing:
        raise VsbenchError(f"build {build_id} is incomplete (missing {', '.join(missing)})", f"remove {directory}")
    return info


def cached_builds() -> list[dict[str, Any]]:
    """All cached builds, newest first."""
    root = state.builds_dir()
    if not root.is_dir():
        return []
    builds = []
    for meta in root.glob("*/build.json"):
        if meta.parent.name.startswith("."):
            continue
        try:
            builds.append(json.loads(meta.read_text()))
        except (OSError, ValueError) as err:
            proc.warn(f"skipping unreadable {meta}: {err}")
    return sorted(builds, key=lambda b: str(b.get("built_at") or ""), reverse=True)


def _cached_git_build(commit: str) -> dict[str, Any] | None:
    for info in cached_builds():
        if info.get("kind") != "git" or info.get("commit") != commit:
            continue
        directory = state.builds_dir() / str(info.get("build_id"))
        if all((directory / b).is_file() for b in BINARIES):
            return info
    return None


def _clean_tmp_dirs() -> None:
    for leftover in state.builds_dir().glob(".tmp-*"):
        shutil.rmtree(leftover, ignore_errors=True)


def _install(out: Path, info: dict[str, Any]) -> dict[str, Any]:
    """Move a verified output dir to builds_dir()/<build_id> (or reuse an identical build)."""
    build_id = make_build_id(info["version"], info["sha256"]["vector-store"])
    final = build_dir(build_id)
    if (final / "build.json").is_file() and all((final / b).is_file() for b in BINARIES):
        proc.log(f"identical build already cached: {build_id}")
        return {**load_build(build_id), "source": info["source"]}
    record = {"build_id": build_id, **info}
    if record["kind"] == "local":
        record["pin"] = f"build:{build_id}"
    proc.atomic_write(out / "build.json", proc.dump_json(record) + "\n")
    if final.exists():
        shutil.rmtree(final)
    os.rename(out, final)
    proc.log(f"build {build_id} ready in {final}")
    return record


def _cross_build(src: Path, version: str, meta: dict[str, Any], repo: Path, jobs: int) -> dict[str, Any]:
    """cross-build.sh + verify.sh into a temp dir, then install it under its build_id."""
    channel = rust_channel(src)
    image = cross_image_tag(src)
    target = repo / "target" / TARGET_SUBDIR
    env = {**os.environ, "CROSS_IMAGE": image, "MAX_GLIBC": config.MAX_GLIBC, "MAX_GLIBCXX": config.MAX_GLIBCXX}
    log_path = _new_log_path(meta["kind"])
    with _build_lock():
        _clean_tmp_dirs()
        out = state.builds_dir() / f".tmp-{uuid.uuid4().hex[:12]}"
        started = time.monotonic()
        try:
            proc.log(f"cross-building {meta['source']} as {version} ({image}, -j {jobs}); log: {log_path}")
            build_cmd = ["bash", str(config.CROSS_DIR / "cross-build.sh"), "-s", str(src), "-v", version]
            build_cmd += ["-j", str(jobs), "-t", str(target), "-o", str(out)]
            _run_logged(build_cmd, log_path=log_path, timeout_s=BUILD_TIMEOUT_S, what="cross build", env=env)
            proc.log("verifying the binaries (aarch64, GLIBC/GLIBCXX limits, --version under qemu, usearch SIMD)")
            verify_cmd = ["bash", str(config.CROSS_DIR / "verify.sh"), str(out), version, str(target)]
            _run_logged(verify_cmd, log_path=log_path, timeout_s=VERIFY_TIMEOUT_S, what="build verification", env=env)
            info = {
                **meta,
                "version": version,
                "rustc_channel": channel,
                "cross_image": image,
                "built_at": proc.iso(proc.utcnow()),
                "build_seconds": round(time.monotonic() - started, 1),
                "sha256": {b: _file_sha256(out / b) for b in BINARIES},
                "log": str(log_path),
            }
            return _install(out, info)
        finally:
            shutil.rmtree(out, ignore_errors=True)


def _build_git(spec: SourceSpec, repo: Path, jobs: int) -> dict[str, Any]:
    commit, version = resolve_git(spec, repo)
    cached = _cached_git_build(commit)
    if cached:
        proc.log(f"using cached build {cached['build_id']} of {commit[:12]}")
        return {**cached, "source": str(spec)}
    src = export_tree(repo, commit)
    meta = {"kind": "git", "source": str(spec), "pin": f"git:{commit}", "commit": commit, "dirty": False}
    return _cross_build(src, version, meta, repo, jobs)


def _build_local(spec: SourceSpec, repo: Path, jobs: int) -> dict[str, Any]:
    version, commit, dirty = local_version(repo, spec.ref)
    meta = {"kind": "local", "source": str(spec), "commit": commit, "dirty": dirty}
    return _cross_build(repo, version, meta, repo, jobs)


def build(spec: SourceSpec, jobs: int = 8) -> dict[str, Any]:
    """Resolve and (if needed) build `spec`; returns its build.json record.

    Record: {build_id, kind, version, source (as requested), pin (resolved spec to
    reproduce it: git:<sha> | release:<ver> | build:<id>), commit, dirty,
    rustc_channel, cross_image, built_at, build_seconds, sha256:{binary: hex}, log}.
    Release records have no binaries (sha256 {}).
    """
    if not 1 <= jobs <= 256:
        raise VsbenchError(f"invalid --jobs {jobs}", "use 1-256 (default 8)")
    if spec.kind == "release":
        return release_build(resolve_release(spec.ref), str(spec))
    if spec.kind == "build":
        return load_build(spec.ref)
    if spec.kind not in ("git", "local"):
        raise VsbenchError(f"unknown source kind '{spec.kind}'")
    repo = repo_root()
    if spec.kind == "git":
        return _build_git(spec, repo, jobs)
    return _build_local(spec, repo, jobs)
