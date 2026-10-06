# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.build and cross/*.sh.

No network and no docker: git runs on throwaway local repos (the "upstream"
remote is a bare repo on disk), the cross scripts are mocked in the Python
tests and run against stub `docker`/binutils/qemu commands in the shell tests.
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import tempfile
import unittest
import urllib.error
from pathlib import Path
from typing import Any
from unittest import mock

from vsbenchlib import build, config, proc, state
from vsbenchlib.proc import PreconditionError, VsbenchError

GIT_ENV = {
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_AUTHOR_NAME": "test",
    "GIT_AUTHOR_EMAIL": "test@example.com",
    "GIT_COMMITTER_NAME": "test",
    "GIT_COMMITTER_EMAIL": "test@example.com",
}
SHA_RE = r"[0-9a-f]{40}"


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def make_repo(path: Path, tag: str | None = "1.0.0") -> Path:
    """A minimal vector-store-like workspace with one commit (and an annotated tag)."""
    (path / "crates" / "vector-store").mkdir(parents=True)
    (path / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/*"]\n')
    (path / "Cargo.lock").write_text("# lock\n")
    (path / "rust-toolchain.toml").write_text('[toolchain]\nchannel = "1.97.1"\ncomponents = ["clippy"]\n')
    (path / "crates" / "vector-store" / "lib.rs").write_text("// v1\n")
    git(path, "init", "-q", "-b", "master")
    git(path, "add", "-A")
    git(path, "commit", "-q", "-m", "init")
    if tag:
        git(path, "tag", "-a", tag, "-m", tag)
    return path


def commit_file(repo: Path, name: str, text: str) -> str:
    (repo / name).write_text(text)
    git(repo, "add", name)
    git(repo, "commit", "-q", "-m", f"change {name}")
    return git(repo, "rev-parse", "HEAD")


class TempHomeTest(unittest.TestCase):
    """Isolated VSBENCH_HOME, hermetic git config, no network for VS_GIT_URL."""

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        env = {**GIT_ENV, "VSBENCH_HOME": str(self.tmp / "home")}
        for patcher in (
            mock.patch.dict(os.environ, env),
            mock.patch.object(config, "VS_GIT_URL", str(self.tmp / "missing" / "scylladb" / "vector-store.git")),
            mock.patch.object(proc, "log"),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)


# --- Source specs ---------------------------------------------------------------


class ParseSourceTest(unittest.TestCase):
    ALL = {"release", "git", "local", "build"}

    def test_valid_forms(self) -> None:
        cases = {
            "release:1.11.0": ("release", "1.11.0"),
            "release:latest": ("release", "latest"),
            "git:master": ("git", "master"),
            "git:pull/123/head": ("git", "pull/123/head"),
            "git:HEAD~3": ("git", "HEAD~3"),
            "local": ("local", ""),
            "local:+exp.fast-ivf": ("local", "exp.fast-ivf"),
            "local:exp": ("local", "exp"),
            "build:1.11.0-65-gc0d7bdb_exp-80c8b9f1": ("build", "1.11.0-65-gc0d7bdb_exp-80c8b9f1"),
        }
        for text, (kind, ref) in cases.items():
            with self.subTest(text=text):
                self.assertEqual(build.parse_source(text, self.ALL), build.SourceSpec(kind, ref))

    def test_str_round_trips(self) -> None:
        for text in ("release:1.11.0", "git:master", "local", "local:+exp", "build:x-1"):
            with self.subTest(text=text):
                self.assertEqual(str(build.parse_source(text, self.ALL)), text)

    def test_rejects_kind_not_allowed_and_lists_allowed_forms(self) -> None:
        with self.assertRaises(VsbenchError) as ctx:
            build.parse_source("release:1.11.0", {"local", "git"})
        self.assertIn("git:<branch|tag|commit>", ctx.exception.hint or "")
        self.assertIn("local:+<label>", ctx.exception.hint or "")
        self.assertNotIn("release", ctx.exception.hint or "")

    def test_rejects_invalid_values(self) -> None:
        bad = [
            "",
            "foo:bar",
            "local:+",
            "local:+a_b",
            "local:+a..b",
            "git:",
            "git:-upload-pack=x",
            "git:a:b",
            "git:a b",
            "release:1.2",
            "release:v1.2.3",
            "build:../x",
            "build:a..b",
        ]
        for text in bad:
            with self.subTest(text=text), self.assertRaises(VsbenchError):
                build.parse_source(text, self.ALL)

    def test_make_build_id_and_semver(self) -> None:
        self.assertEqual(
            build.make_build_id("1.11.0-65-gabc+exp.d1234abcd", "80c8b9f1ee12"), "1.11.0-65-gabc_exp.d1234abcd-80c8b9f1"
        )
        self.assertEqual(build.check_semver("1.11.0-65-gc0d7bdb-dirty+exp"), "1.11.0-65-gc0d7bdb-dirty+exp")
        for bad in ("1.11", "1.11.0-feat_x/y", "1.11.0+", "v1.0.0"):
            with self.subTest(version=bad), self.assertRaises(VsbenchError):
                build.check_semver(bad)


# --- Versions of the working tree -----------------------------------------------


class LocalVersionTest(TempHomeTest):
    def setUp(self) -> None:
        super().setUp()
        self.repo = make_repo(self.tmp / "repo")

    def test_clean_tree_on_tag(self) -> None:
        head = git(self.repo, "rev-parse", "HEAD")
        self.assertEqual(build.local_version(self.repo, ""), ("1.0.0", head, False))

    def test_commits_after_tag_and_label(self) -> None:
        head = commit_file(self.repo, "a.txt", "a")
        version, commit, dirty = build.local_version(self.repo, "exp")
        self.assertRegex(version, r"^1\.0\.0-1-g[0-9a-f]{7,}\+exp$")
        self.assertEqual((commit, dirty), (head, False))

    def test_tracked_change_is_dirty_with_hash(self) -> None:
        (self.repo / "crates" / "vector-store" / "lib.rs").write_text("// v2\n")
        version, _, dirty = build.local_version(self.repo, "")
        self.assertTrue(dirty)
        self.assertRegex(version, r"^1\.0\.0-dirty\+d[0-9a-f]{8}$")
        labelled, _, _ = build.local_version(self.repo, "exp")
        self.assertEqual(labelled, version.replace("+d", "+exp.d"))

    def test_dirty_hash_follows_untracked_content(self) -> None:
        (self.repo / "Cargo.toml").write_text("changed\n")
        new_file = self.repo / "crates" / "vector-store" / "new.rs"
        new_file.write_text("fn a() {}\n")
        first = build.dirty_hash(self.repo)
        new_file.write_text("fn b() {}\n")
        second = build.dirty_hash(self.repo)
        new_file.write_text("fn a() {}\n")
        self.assertNotEqual(first, second)
        self.assertEqual(build.dirty_hash(self.repo), first)

    def test_dirty_hash_ignores_agent_files(self) -> None:
        (self.repo / "Cargo.toml").write_text("changed\n")
        before = build.dirty_hash(self.repo)
        (self.repo / ".claude" / "skills").mkdir(parents=True)
        (self.repo / ".claude" / "skills" / "SKILL.md").write_text("x")
        self.assertEqual(build.dirty_hash(self.repo), before)

    def test_untracked_files_alone_are_not_dirty(self) -> None:
        (self.repo / "notes.txt").write_text("x")
        self.assertEqual(build.local_version(self.repo, "")[0::2], ("1.0.0", False))

    def test_ignores_non_version_tags(self) -> None:
        commit_file(self.repo, "a.txt", "a")
        git(self.repo, "tag", "initial")
        self.assertRegex(build.local_version(self.repo, "")[0], r"^1\.0\.0-1-g")

    def test_no_version_tag_falls_back(self) -> None:
        repo = make_repo(self.tmp / "untagged", tag=None)
        head = git(repo, "rev-parse", "HEAD")
        with mock.patch.object(proc, "warn") as warn:
            self.assertEqual(build.local_version(repo, "")[0], f"0.0.0-g{head[:7]}")
        warn.assert_called_once()

    def test_label_must_be_semver(self) -> None:
        with self.assertRaises(VsbenchError):
            build.local_version(self.repo, "a_b")


# --- Upstream remote, git refs, export --------------------------------------------


class UpstreamRemoteTest(TempHomeTest):
    def test_picks_the_scylladb_remote(self) -> None:
        repo = make_repo(self.tmp / "repo")
        cases = [
            (
                {
                    "origin": "git@github.com:swasik/vector-store.git",
                    "upstream": "git@github.com:scylladb/vector-store.git",
                },
                "upstream",
            ),
            ({"origin": "https://github.com/scylladb/vector-store"}, "origin"),
            (
                {"scy": "https://github.com/scylladb/vector-store.git/", "zz": "git@github.com:scylladb/vector-store"},
                "scy",
            ),
            ({"origin": "git@github.com:scylladb/vector-store-fork.git"}, None),
        ]
        for remotes, expected in cases:
            for name in git(repo, "remote").split():
                git(repo, "remote", "remove", name)
            for name, url in remotes.items():
                git(repo, "remote", "add", name, url)
            with self.subTest(remotes=remotes):
                self.assertEqual(build.upstream_remote(repo), expected)


class ResolveGitTest(TempHomeTest):
    """`upstream` is a bare repo whose path ends in scylladb/vector-store.git."""

    def setUp(self) -> None:
        super().setUp()
        self.dev = make_repo(self.tmp / "dev")
        self.bare = self.tmp / "remote" / "scylladb" / "vector-store.git"
        subprocess.run(["git", "clone", "-q", "--bare", str(self.dev), str(self.bare)], check=True)
        self.user = self.tmp / "user"
        subprocess.run(["git", "clone", "-q", "-o", "upstream", str(self.bare), str(self.user)], check=True)
        git(self.user, "remote", "add", "origin", "git@github.com:someone/vector-store.git")
        # Upstream moves on: a new tagged commit and one more commit, unknown to `user`.
        commit_file(self.dev, "b.txt", "b")
        git(self.dev, "tag", "-a", "1.1.0", "-m", "1.1.0")
        self.tagged = git(self.dev, "rev-parse", "HEAD")
        self.head = commit_file(self.dev, "c.txt", "c")
        git(self.dev, "push", "-q", str(self.bare), "master", "--tags")

    def test_fetches_branch_and_tags(self) -> None:
        commit, version = build.resolve_git(build.SourceSpec("git", "master"), self.user)
        self.assertEqual(commit, self.head)
        self.assertRegex(version, r"^1\.1\.0-1-g[0-9a-f]{7,}$")

    def test_fetches_tag(self) -> None:
        self.assertEqual(build.resolve_git(build.SourceSpec("git", "1.1.0"), self.user), (self.tagged, "1.1.0"))

    def test_local_full_sha_needs_no_fetch(self) -> None:
        first = git(self.user, "rev-parse", "HEAD")
        with mock.patch.object(build, "_fetch_commit", side_effect=AssertionError("fetched")):
            self.assertEqual(build.resolve_git(build.SourceSpec("git", first), self.user), (first, "1.0.0"))

    def test_falls_back_to_local_ref(self) -> None:
        git(self.user, "branch", "mybranch")
        with mock.patch.object(proc, "warn") as warn:
            commit, _ = build.resolve_git(build.SourceSpec("git", "mybranch"), self.user)
        self.assertEqual(commit, git(self.user, "rev-parse", "mybranch"))
        warn.assert_called_once()

    def test_unknown_ref_fails(self) -> None:
        with self.assertRaises(VsbenchError) as ctx:
            build.resolve_git(build.SourceSpec("git", "no-such-branch"), self.user)
        self.assertIn("no-such-branch", str(ctx.exception))

    def test_export_tree_is_the_commit_not_the_worktree(self) -> None:
        commit, _ = build.resolve_git(build.SourceSpec("git", "master"), self.user)
        (self.user / "Cargo.toml").write_text("local edit\n")
        src = build.export_tree(self.user, commit)
        self.assertEqual(src, state.src_dir() / commit)
        self.assertEqual((src / "c.txt").read_text(), "c")
        self.assertIn("[workspace]", (src / "Cargo.toml").read_text())
        self.assertEqual([p.name for p in state.src_dir().iterdir()], [commit])
        with mock.patch.object(build, "_git", side_effect=AssertionError("re-exported")):
            self.assertEqual(build.export_tree(self.user, commit), src)


# --- Releases ---------------------------------------------------------------------


def fake_response(payload: Any) -> mock.MagicMock:
    response = mock.MagicMock()
    response.__enter__.return_value = io.BytesIO(json.dumps(payload).encode())
    return response


class ReleaseTest(TempHomeTest):
    def test_explicit_version_needs_no_network(self) -> None:
        with mock.patch("urllib.request.urlopen", side_effect=AssertionError("network")):
            self.assertEqual(build.resolve_release("1.11.0"), "1.11.0")
            record = build.build(build.SourceSpec("release", "1.11.0"))
        self.assertEqual(record["build_id"], "release-1.11.0")
        self.assertEqual(record["sha256"], {})

    def test_latest_uses_github_api(self) -> None:
        with mock.patch("urllib.request.urlopen", return_value=fake_response({"tag_name": "1.11.0"})) as urlopen:
            record = build.build(build.parse_source("release:latest", {"release"}))
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, config.VS_RELEASES_API)
        self.assertEqual(request.get_header("User-agent"), "vsbench")
        self.assertEqual(
            {k: record[k] for k in ("build_id", "kind", "version", "source", "pin", "commit")},
            {
                "build_id": "release-1.11.0",
                "kind": "release",
                "version": "1.11.0",
                "source": "release:latest",
                "pin": "release:1.11.0",
                "commit": None,
            },
        )

    def test_latest_errors(self) -> None:
        rate_limited = urllib.error.HTTPError(config.VS_RELEASES_API, 403, "rate limited", {}, None)  # type: ignore[arg-type]
        self.addCleanup(rate_limited.close)
        cases = [
            (mock.patch("urllib.request.urlopen", side_effect=rate_limited), "rate limit"),
            (mock.patch("urllib.request.urlopen", side_effect=urllib.error.URLError("offline")), "release:<X.Y.Z>"),
            (mock.patch("urllib.request.urlopen", return_value=fake_response({"tag_name": "nightly"})), None),
            (mock.patch("urllib.request.urlopen", return_value=fake_response(["not", "a", "dict"])), None),
        ]
        for patcher, hint in cases:
            with self.subTest(hint=hint), patcher, self.assertRaises(VsbenchError) as ctx:
                build.resolve_release("latest")
            if hint:
                self.assertIn(hint, ctx.exception.hint or "")

    def test_load_build_synthesizes_release_ids(self) -> None:
        self.assertEqual(build.load_build("release-1.10.0")["pin"], "release:1.10.0")


# --- Build cache, cross image, process helpers --------------------------------------


class CacheTest(TempHomeTest):
    def write_build(self, build_id: str, built_at: str, binaries: bool = True) -> Path:
        directory = state.builds_dir() / build_id
        directory.mkdir(parents=True)
        (directory / "build.json").write_text(json.dumps({"build_id": build_id, "built_at": built_at}))
        for name in build.BINARIES if binaries else ():
            (directory / name).write_text(name)
        return directory

    def test_load_build_errors(self) -> None:
        self.write_build("partial-1", "2026-10-01T00:00:00Z", binaries=False)
        for build_id, message in (("nope-1", "unknown build"), ("partial-1", "incomplete"), ("../x", "invalid")):
            with self.subTest(build_id=build_id), self.assertRaises(VsbenchError) as ctx:
                build.load_build(build_id)
            self.assertIn(message, str(ctx.exception))

    def test_cached_builds_newest_first_skipping_garbage(self) -> None:
        self.assertEqual(build.cached_builds(), [])
        self.write_build("a-1", "2026-10-01T00:00:00Z")
        self.write_build("b-2", "2026-10-03T00:00:00Z")
        self.write_build(".tmp-x", "2026-10-09T00:00:00Z")
        (self.write_build("c-3", "x") / "build.json").write_text("{broken")
        with mock.patch.object(proc, "warn") as warn:
            ids = [b["build_id"] for b in build.cached_builds()]
        self.assertEqual(ids, ["b-2", "a-1"])
        warn.assert_called_once()

    def test_build_lock_is_exclusive(self) -> None:
        with build._build_lock(), self.assertRaises(PreconditionError), build._build_lock():
            pass
        with build._build_lock():  # released again
            pass


class CrossImageTest(TempHomeTest):
    def setUp(self) -> None:
        super().setUp()
        self.src = make_repo(self.tmp / "src")
        patcher = mock.patch.object(proc, "require_tool", return_value="/usr/bin/docker")
        patcher.start()
        self.addCleanup(patcher.stop)

    def inspect(self, code: int, stderr: str = "") -> mock._patch[Any]:
        result = subprocess.CompletedProcess([], code, stdout="", stderr=stderr)
        return mock.patch.object(proc, "run", return_value=result)

    def test_channel(self) -> None:
        self.assertEqual(build.rust_channel(self.src), "1.97.1")
        (self.src / "rust-toolchain.toml").write_text("[toolchain]\n")
        with self.assertRaises(VsbenchError):
            build.rust_channel(self.src)

    def test_existing_image_is_used(self) -> None:
        with self.inspect(0), mock.patch.object(build, "_run_logged") as run_logged:
            self.assertEqual(build.cross_image_tag(self.src), "vs-bench-cross:1.97.1")
        run_logged.assert_not_called()

    def test_missing_image_is_built(self) -> None:
        with (
            self.inspect(1, "Error response from daemon: No such image: x"),
            mock.patch.object(build, "_run_logged") as run_logged,
        ):
            build.cross_image_tag(self.src)
        cmd = run_logged.call_args.args[0]
        self.assertEqual(
            cmd,
            [
                "docker",
                "build",
                "--build-arg",
                "RUST_VERSION=1.97.1",
                "-t",
                "vs-bench-cross:1.97.1",
                str(config.CROSS_DIR),
            ],
        )

    def test_docker_daemon_down(self) -> None:
        with self.inspect(1, "Cannot connect to the Docker daemon"), self.assertRaises(PreconditionError):
            build.cross_image_tag(self.src)


class RunLoggedTest(TempHomeTest):
    def run_logged(self, script: str, timeout_s: float = 30) -> Path:
        log_path = self.tmp / "out.log"
        build._run_logged(["bash", "-c", script], log_path=log_path, timeout_s=timeout_s, what="step")
        return log_path

    def test_success_is_logged(self) -> None:
        self.assertIn("hello\n", self.run_logged("echo hello; echo oops >&2").read_text())

    def test_failure_shows_tail_and_log_path(self) -> None:
        with self.assertRaises(VsbenchError) as ctx:
            self.run_logged("echo first; echo boom >&2; exit 3")
        self.assertIn("step failed (exit 3)", str(ctx.exception))
        self.assertIn("boom", str(ctx.exception))
        self.assertIn(str(self.tmp / "out.log"), ctx.exception.hint or "")

    def test_timeout_kills_the_process_group(self) -> None:
        with mock.patch.object(build, "STOP_GRACE_S", 2), self.assertRaises(VsbenchError) as ctx:
            self.run_logged("sleep 30 & wait", timeout_s=0.5)
        self.assertIn("timed out", str(ctx.exception))

    def test_missing_command(self) -> None:
        with self.assertRaises(VsbenchError):
            build._run_logged(["/nonexistent/x"], log_path=self.tmp / "x.log", timeout_s=5, what="x")


# --- build() pipeline (scripts mocked) -------------------------------------------------


class BuildPipelineTest(TempHomeTest):
    def setUp(self) -> None:
        super().setUp()
        self.repo = make_repo(self.tmp / "repo")
        self.calls: list[tuple[list[str], dict[str, str]]] = []
        self.verify_fails = False
        for patcher in (
            mock.patch.object(build, "repo_root", return_value=self.repo),
            mock.patch.object(build, "cross_image_tag", return_value="vs-bench-cross:1.97.1"),
            mock.patch.object(build, "_run_logged", side_effect=self.fake_run_logged),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def fake_run_logged(self, cmd: list[str], *, log_path: Path, timeout_s: float, what: str, env: Any = None) -> None:
        self.calls.append((cmd, dict(env or {})))
        if cmd[1].endswith("cross-build.sh"):
            out, version = Path(cmd[cmd.index("-o") + 1]), cmd[cmd.index("-v") + 1]
            out.mkdir(parents=True, exist_ok=True)
            for name in build.BINARIES:
                (out / name).write_text(f"{name} {version}\n")
        elif self.verify_fails:
            raise VsbenchError("build verification failed (exit 1)")

    def build_dirs(self) -> list[str]:
        return sorted(p.name for p in state.builds_dir().iterdir() if p.is_dir() and p.name != "logs")

    def test_local_build_is_installed_and_reused(self) -> None:
        record = build.build(build.parse_source("local", {"local"}), jobs=4)
        sha = build._file_sha256(build.build_dir(record["build_id"]) / "vector-store")
        self.assertEqual(record["build_id"], f"1.0.0-{sha[:8]}")
        self.assertEqual(record["pin"], f"build:{record['build_id']}")
        self.assertEqual((record["kind"], record["version"], record["dirty"]), ("local", "1.0.0", False))
        self.assertEqual(record["commit"], git(self.repo, "rev-parse", "HEAD"))
        self.assertEqual(record["rustc_channel"], "1.97.1")
        self.assertEqual(build.load_build(record["build_id"]), record)
        self.assertEqual(self.build_dirs(), [record["build_id"]])
        again = build.build(build.parse_source("local", {"local"}))
        self.assertEqual(again, record)
        self.assertEqual(self.build_dirs(), [record["build_id"]])

    def test_script_arguments_and_env(self) -> None:
        build.build(build.SourceSpec("local", ""), jobs=4)
        (cross_cmd, env), (verify_cmd, _) = self.calls
        target = str(self.repo / "target" / "cross-aarch64")
        out = cross_cmd[cross_cmd.index("-o") + 1]
        self.assertEqual(cross_cmd[1], str(config.CROSS_DIR / "cross-build.sh"))
        self.assertEqual(cross_cmd[2:12], ["-s", str(self.repo), "-v", "1.0.0", "-j", "4", "-t", target, "-o", out])
        self.assertEqual(verify_cmd[1:], [str(config.CROSS_DIR / "verify.sh"), out, "1.0.0", target])
        self.assertTrue(Path(out).name.startswith(".tmp-"))
        self.assertEqual(env["CROSS_IMAGE"], "vs-bench-cross:1.97.1")
        self.assertEqual((env["MAX_GLIBC"], env["MAX_GLIBCXX"]), (config.MAX_GLIBC, config.MAX_GLIBCXX))

    def test_dirty_labelled_build(self) -> None:
        (self.repo / "Cargo.lock").write_text("# changed\n")
        record = build.build(build.parse_source("local:+exp", {"local"}))
        self.assertRegex(record["version"], r"^1\.0\.0-dirty\+exp\.d[0-9a-f]{8}$")
        self.assertRegex(record["build_id"], r"^1\.0\.0-dirty_exp\.d[0-9a-f]{8}-[0-9a-f]{8}$")
        self.assertTrue(record["dirty"])
        self.assertEqual(record["source"], "local:+exp")

    def test_failed_verification_leaves_nothing(self) -> None:
        self.verify_fails = True
        with self.assertRaises(VsbenchError):
            build.build(build.SourceSpec("local", ""))
        self.assertEqual(self.build_dirs(), [])
        self.assertEqual(list(state.builds_dir().glob(".tmp-*")), [])

    def test_git_build_is_cached_by_commit(self) -> None:
        sha = "a" * 40
        with (
            mock.patch.object(build, "resolve_git", return_value=(sha, "1.0.0-1-gaaaaaaa")),
            mock.patch.object(build, "export_tree", return_value=self.repo) as export,
        ):
            first = build.build(build.parse_source("git:master", {"git"}))
            second = build.build(build.parse_source(f"git:{sha}", {"git"}))
        self.assertEqual(len(self.calls), 2)  # one cross build + one verify
        export.assert_called_once_with(self.repo, sha)
        self.assertEqual((first["kind"], first["commit"], first["pin"]), ("git", sha, f"git:{sha}"))
        self.assertEqual(second["build_id"], first["build_id"])
        self.assertEqual((first["source"], second["source"]), ("git:master", f"git:{sha}"))

    def test_build_source_and_jobs_validation(self) -> None:
        record = build.build(build.SourceSpec("local", ""))
        self.assertEqual(build.build(build.SourceSpec("build", record["build_id"])), record)
        with self.assertRaises(VsbenchError):
            build.build(build.SourceSpec("local", ""), jobs=0)


# --- cross/*.sh against stub commands ----------------------------------------------------


def write_stub(directory: Path, name: str, body: str) -> None:
    path = directory / name
    path.write_text("#!/usr/bin/env bash\n" + body + "\n")
    path.chmod(0o755)


class CrossScriptsTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.stubs = self.tmp / "stubs"
        self.stubs.mkdir()
        self.env = {
            **os.environ,
            "PATH": f"{self.stubs}:{os.environ['PATH']}",
            "STUB_LOG": str(self.tmp / "docker.args"),
        }
        self.env.pop("CROSS_IMAGE", None)
        # docker: record args, copy the mounted temp Cargo.toml, exit $STUB_DOCKER_EXIT.
        write_stub(
            self.stubs,
            "docker",
            'printf "%s\\n" "$@" > "$STUB_LOG"\n'
            'for a in "$@"; do case $a in *:*/Cargo.toml) cp "${a%%:*}" "$STUB_LOG.toml" ;; esac; done\n'
            'exit "${STUB_DOCKER_EXIT:-0}"',
        )

    def run_script(
        self, name: str, *args: str, cwd: Path | None = None, **env: str
    ) -> subprocess.CompletedProcess[str]:
        cmd = ["bash", str(config.CROSS_DIR / name), *args]
        return subprocess.run(cmd, capture_output=True, text=True, env={**self.env, **env}, cwd=cwd, timeout=60)

    def workspace(self) -> tuple[Path, Path]:
        ws = self.tmp / "ws"
        ws.mkdir()
        (ws / "Cargo.toml").write_text('[workspace.package]\nversion = "0.0.0-dev"\n')
        (ws / "Cargo.lock").write_text("")
        (ws / "rust-toolchain.toml").write_text('[toolchain]\nchannel = "1.97.1"\n')
        release = self.tmp / "target" / "aarch64-unknown-linux-gnu" / "release"
        release.mkdir(parents=True)
        for name in build.BINARIES:
            (release / name).write_text(name)
        return ws, self.tmp / "target"

    def test_cross_build_rejects_non_semver_before_docker(self) -> None:
        ws, _ = self.workspace()
        result = self.run_script("cross-build.sh", "-s", str(ws), "-v", "1.0.0-feat_x/y")
        self.assertEqual(result.returncode, 1)
        self.assertIn("not valid SemVer", result.stderr)
        self.assertFalse(Path(self.env["STUB_LOG"]).exists())

    def test_cross_build_runs_two_cargo_builds_and_copies(self) -> None:
        ws, target = self.workspace()
        out = self.tmp / "out"
        result = self.run_script("cross-build.sh", "-s", str(ws), "-v", "1.2.3+exp", "-t", str(target), "-o", str(out))
        self.assertEqual(result.returncode, 0, result.stderr)
        args = Path(self.env["STUB_LOG"]).read_text()
        self.assertIn("--init", args.splitlines())
        self.assertIn("vs-bench-cross:1.97.1", args.splitlines())
        self.assertIn("-p vector-store --bin vector-store", args)
        self.assertIn("-p vector-search-benchmark --bin vector-search-benchmark", args)
        self.assertNotIn("target-cpu", args)
        self.assertIn('version = "1.2.3+exp"', Path(self.env["STUB_LOG"] + ".toml").read_text())
        self.assertEqual(sorted(p.name for p in out.iterdir()), sorted(["VERSION", *build.BINARIES]))
        self.assertEqual((out / "VERSION").read_text(), "1.2.3+exp\n")

    def inside_stubs(self) -> Path:
        write_stub(self.stubs, "file", 'echo "${STUB_FILE:-ELF 64-bit LSB pie executable, ARM aarch64, version 1}"')
        write_stub(
            self.stubs,
            "aarch64-linux-gnu-objdump",
            'echo "0 DF *UND* GLIBC_2.17 memcpy"; echo "0 DF *UND* GLIBC_${STUB_GLIBC:-2.34} f"\n'
            'echo "0 DF *UND* GLIBC_PRIVATE g"; echo "0 DF *UND* GLIBCXX_${STUB_GLIBCXX:-3.4.29} h"',
        )
        write_stub(
            self.stubs, "aarch64-linux-gnu-readelf", 'echo " (NEEDED) Shared library: [libc.so.6]"; echo "GCC: 12"'
        )
        write_stub(self.stubs, "aarch64-linux-gnu-nm", "echo 0 T simsimd_dot_f32_sve")
        write_stub(self.stubs, "qemu-aarch64", 'echo "$(basename "$3") ${STUB_VERSION:-1.2.3}"')
        bins = self.tmp / "bins"
        bins.mkdir()
        for name in build.BINARIES:
            (bins / name).write_text("elf")
            (bins / name).chmod(0o755)
        return bins

    def test_verify_inside_gate(self) -> None:
        bins = self.inside_stubs()
        limits = {"MAX_GLIBC": "2.34", "MAX_GLIBCXX": "3.4.29"}
        cases = [
            ({}, 0, "qemu --version: vector-store 1.2.3"),
            ({"STUB_GLIBC": "2.38"}, 1, "needs GLIBC_2.38"),
            ({"STUB_GLIBCXX": "3.4.30"}, 1, "needs GLIBCXX_3.4.30"),
            ({"STUB_VERSION": "1.2.4"}, 1, "expected 'vector-store 1.2.3'"),
            ({"STUB_FILE": "ELF 64-bit LSB pie executable, x86-64"}, 1, "not an ARM aarch64 binary"),
        ]
        for extra, code, needle in cases:
            with self.subTest(extra=extra):
                result = self.run_script("verify.sh", "--inside", "1.2.3", cwd=bins, **limits, **extra)
                self.assertEqual(result.returncode, code, result.stdout + result.stderr)
                self.assertIn(needle, result.stdout + result.stderr)
        (bins / "vector-search-benchmark").unlink()
        result = self.run_script("verify.sh", "--inside", "1.2.3", cwd=bins, **limits)
        self.assertEqual(result.returncode, 1)
        self.assertIn("vector-search-benchmark: missing", result.stderr)

    def test_verify_host_checks(self) -> None:
        bins = self.inside_stubs()
        _, target = self.workspace()
        usearch = target / "aarch64-unknown-linux-gnu" / "release" / "build" / "usearch-0123"
        usearch.mkdir(parents=True)
        (usearch / "output").write_text("cargo:rerun-if-changed=rust/lib.rs\n")
        args = (str(bins), "1.2.3", str(target))
        ok = self.run_script("verify.sh", *args, CROSS_IMAGE="vs-bench-cross:1.97.1")
        self.assertEqual(ok.returncode, 0, ok.stdout + ok.stderr)
        docker_args = Path(self.env["STUB_LOG"]).read_text().splitlines()
        self.assertEqual(docker_args[-4:], ["bash", "/verify.sh", "--inside", "1.2.3"])
        self.assertIn("MAX_GLIBC", docker_args)
        failed_docker = self.run_script("verify.sh", *args, CROSS_IMAGE="x", STUB_DOCKER_EXIT="1")
        self.assertEqual(failed_docker.returncode, 1)
        self.assertEqual(self.run_script("verify.sh", *args).returncode, 1)  # CROSS_IMAGE required
        (usearch / "output").write_text("cargo:warning=Failed to compile with all SIMD backends\n")
        simd = self.run_script("verify.sh", *args, CROSS_IMAGE="x")
        self.assertEqual(simd.returncode, 1)
        self.assertIn("usearch dropped SIMD backends", simd.stderr)
        wrong_target = self.run_script("verify.sh", str(bins), "1.2.3", str(self.tmp), CROSS_IMAGE="x")
        self.assertEqual(wrong_target.returncode, 1)


if __name__ == "__main__":
    unittest.main()
