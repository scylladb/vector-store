# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.cli: parsing, handler wiring, exit codes, history, signals, locking.

No network, AWS or ssh: modules behind the handlers are replaced in sys.modules.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime
import io
import os
import shutil
import signal
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

SKILL_DIR = Path(__file__).resolve().parent.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from vsbenchlib import awsapi, cli, config, proc, retro  # noqa: E402
from vsbenchlib import state as st  # noqa: E402
from vsbenchlib.proc import PreconditionError, StillRunning, VsbenchError  # noqa: E402

CLUSTER = "t1"
UTC = datetime.timezone.utc


def make_state(cluster: str = CLUSTER, expires_in_s: int = 86400) -> dict[str, Any]:
    now = proc.utcnow()

    def node(name: str, role: str, ip: str) -> dict[str, Any]:
        return {"name": name, "role": role, "index": 0, "instance_id": f"i-0{ip[-1]}", "public_ip": ip}

    return {
        "schema": st.SCHEMA,
        "cluster": cluster,
        "owner": "first.last",
        "profile": "state-profile",
        "region": "eu-west-1",
        "az": "eu-west-1a",
        "expires_at": proc.iso(now + datetime.timedelta(seconds=expires_in_s)),
        "nodes": [
            node("scylla-0", "scylla", "3.0.0.1"),
            node("vs-0", "vs", "3.0.0.2"),
            node("client", "client", "3.0.0.3"),
        ],
        "deployed": {"scylla": None, "vector_store": None, "bench": None, "monitoring": None},
        "jobs": {},
        "terminated_at": None,
    }


class CliCase(unittest.TestCase):
    """Isolated VSBENCH_HOME, no AWS credentials file, fake modules via sys.modules."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="vsbench-cli-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        env = {"VSBENCH_HOME": str(self.tmp / "home"), "AWS_SHARED_CREDENTIALS_FILE": str(self.tmp / "credentials")}
        patcher = mock.patch.dict(os.environ, env)
        patcher.start()
        self.addCleanup(patcher.stop)
        for name in ("VSBENCH_CLUSTER", "AWS_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION"):
            os.environ.pop(name, None)
        self.addCleanup(setattr, proc, "VERBOSE", False)
        rev = mock.patch.object(retro, "skill_rev", return_value="abc1234")
        rev.start()
        self.addCleanup(rev.stop)

    def run_cli(self, *argv: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def fake(self, name: str, **attrs: Any) -> mock.MagicMock:
        module = mock.MagicMock(name=name)
        for key, value in attrs.items():
            setattr(module, key, value)
        patcher = mock.patch.dict(sys.modules, {f"vsbenchlib.{name}": module})
        patcher.start()
        self.addCleanup(patcher.stop)
        return module

    def save_state(self, **changes: Any) -> dict[str, Any]:
        state = make_state() | changes
        st.save(CLUSTER, state)
        return state

    def write_credentials(self, expires_in_s: int, profile: str = config.DEFAULT_PROFILE) -> None:
        moment = (proc.utcnow() + datetime.timedelta(seconds=expires_in_s)).isoformat()
        (self.tmp / "credentials").write_text(f"[{profile}]\nx_security_token_expires = {moment}\n")


def leaves(parser: argparse.ArgumentParser, prefix: tuple[str, ...] = ()) -> dict[tuple[str, ...], Any]:
    """Every leaf subcommand parser, keyed by its words."""
    found: dict[tuple[str, ...], Any] = {}
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for name, sub in action.choices.items():
                found |= leaves(sub, (*prefix, name))
    if parser.get_default("func") is not None:
        found[prefix] = parser
    return found


# --- parsing -------------------------------------------------------------------------------
class ParseTest(unittest.TestCase):
    EVERY_COMMAND = [
        ["doctor", "--json"],
        ["login", "--username", "first.last@scylladb.com", "--timeout", "5m"],
        ["bench", "search", "cql", "--perf", "vs-0,scylla-0", "--duration", "30s"],
        ["up", "--scylla-nodes", "3", "--vs-nodes", "2", "--scylla-type", "i8g.4xlarge", "--vs-type", "r8g.8xlarge"],
        ["up", "--client-type", "r8g.xlarge", "--az", "us-east-1b", "--subnet-id", "subnet-1", "--ttl", "6h"],
        ["up", "--billing-project", "Vector Search: DiskANN", "--node-disk-gb", "80", "--client-disk-gb", "300"],
        ["up", "--dry-run", "--keep-on-failure"],
        ["down", "--yes", "--purge"],
        ["list", "--all-owners", "--json"],
        ["status", "--json", "--refresh"],
        ["extend", "--ttl", "12h"],
        ["extend", "--until", "2026-10-08T10:00:00Z", "--shorten"],
        ["refresh-ip"],
        ["build", "--source", "git:master", "--jobs", "4"],
        ["builds", "--nodes", "--json"],
        ["deploy", "scylla", "--image", "nightly", "--wipe", "--refresh"],
        ["deploy", "vs", "--source", "local:+x", "--env", "A=1", "--env", "B=x=y", "--unset", "C", "--timeout", "5m"],
        ["deploy", "bench", "--source", "git:abc", "--refresh"],
        ["deploy", "monitoring"],
        ["deploy", "all", "--force"],
        ["wait-serving", "--timeout", "2m"],
        ["ssh", "vs-0"],
        ["ssh", "vs-0", "--", "ls", "-la"],
        ["exec", "all", "--json", "--tail", "5", "--timeout", "1m", "--i-mean-it", "--", "a | b"],
        ["exec", "vs-0", "uptime"],
        ["push", "file.txt", "vs-0:/tmp/file.txt"],
        ["pull", "vs-0:/tmp/file.txt", "."],
        ["logs", "vs-0", "--service", "vector-store", "-n", "20", "--since", "10m"],
        ["prom", "query", "up", "--time", "-5m", "--raw"],
        ["prom", "range", "rate(x[1m])", "--start", "-1h", "--end", "now", "--step", "30s", "--raw"],
        ["prom", "range", "up", "--start", "now-1h", "--end", "now-5m"],
        ["prom", "api", "targets", "state=active"],
        ["dataset", "list", "--json"],
        ["dataset", "fetch", "cohere-1m", "--timeout", "20m"],
        ["dataset", "status"],
        ["bench", "load", "cohere-1m", "--index-options", "{'a': 1}", "--rf", "3", "--concurrency", "256"],
        ["bench", "load", "cohere-1m", "--local-index", "--resume", "--index-timeout", "1h", "--timeout", "8m"],
        ["bench", "index", "--index-options", "{'a': 1}", "--index-timeout", "1h", "--timeout", "8m"],
        ["bench", "search", "cql", "--concurrency", "16,64", "--duration", "30s", "--warmup", "0s", "--repeat", "3"],
        ["bench", "search", "http", "--bucket", "5", "--label", "x", "--limit", "100", "--timeout", "9m"],
        ["bench", "validate", "cql", "--bucket", "2"],
        ["bench", "ab", "--a", "release:latest", "--b", "local", "--kind", "http", "--concurrency", "64"],
        ["bench", "ab", "--a", "build:x", "--b", "git:y", "--repeat", "4", "--label", "y", "--duration", "2m"],
        ["bench", "ab", "--a", "x", "--b", "y", "--bucket", "0", "--timeout", "5m", "--", "--filter", "z"],
        ["bench", "rerun", "RUN", "--timeout", "1m"],
        ["bench", "raw", "--timeout", "5m", "--", "search-cql", "--help"],
        ["job", "list", "--json"],
        ["job", "status", "ID"],
        ["job", "logs", "ID", "--tail", "0"],
        ["job", "wait", "ID", "--timeout", "1m"],
        ["job", "cancel", "ID"],
        ["results", "--last", "5", "--kind", "search", "--series", "S", "--comparison", "C", "--json"],
        ["results", "--format", "md"],
        ["results", "compare", "A", "B", "--force", "--json", "--format", "md"],
        ["results", "--json", "--format", "md", "compare", "A"],
        ["collect"],
        ["note", "--kind", "workaround", "some", "text"],
        ["retro", "--since", "2026-10-06T08:00:00Z", "--json"],
        ["history", "--failed", "--last", "10", "--since", "-6h", "--json"],
        ["history", "--since", "6h"],
    ]

    def test_every_command_parses(self) -> None:
        for argv in self.EVERY_COMMAND:
            with self.subTest(argv=argv):
                args = cli.parse_args(argv)
                self.assertTrue(callable(cli.resolve_handler(args.func)))

    def test_every_leaf_has_a_handler_and_a_parse_case(self) -> None:
        found = leaves(cli.build_parser())
        covered = set()
        for argv in self.EVERY_COMMAND:
            grouped = argv[0] in retro.GROUP_COMMANDS and len(argv) > 1 and not argv[1].startswith("-")
            words = tuple(argv[:2]) if grouped else (argv[0],)
            covered.add(words if words in found else words[:1])
        for words, parser in found.items():
            with self.subTest(command=" ".join(words)):
                self.assertTrue(callable(cli.resolve_handler(parser.get_default("func"))))
                self.assertIn(words, covered)

    def test_values(self) -> None:
        args = cli.parse_args(["deploy", "vs", "--env", "A=1", "--env", "B=x=y", "--unset", "C", "--timeout", "5m"])
        self.assertEqual(
            (args.env, args.unset, args.timeout_s, args.source), ([("A", "1"), ("B", "x=y")], ["C"], 300, None)
        )
        args = cli.parse_args(["bench", "search", "cql", "--concurrency", "16,64", "--warmup", "0s", "--", "--x", "1"])
        self.assertEqual(
            (args.concurrency, args.warmup_s, args.duration_s, args.extra), ([16, 64], 0, 60, ["--x", "1"])
        )
        self.assertEqual(cli.parse_args(["extend", "--ttl", "12h"]).ttl, 43200)
        self.assertEqual(cli.parse_args(["prom", "api", "targets", "state=active"]).params, [("state", "active")])
        self.assertEqual(cli.parse_args(["up"]).ttl, config.DEFAULT_TTL)

    def test_dashdash_extra(self) -> None:
        args = cli.parse_args(["exec", "vs-0", "--json", "--", "curl -s x | grep y", "--tail"])
        self.assertEqual(
            (args.words, args.extra, args.json, args.tail), ([], ["curl -s x | grep y", "--tail"], True, 50)
        )
        args = cli.parse_args(["exec", "all", "uptime"])
        self.assertEqual((args.words, args.extra), (["uptime"], []))
        args = cli.parse_args(["ssh", "vs-0", "--", "sudo", "docker", "ps", "-a"])
        self.assertEqual(args.extra, ["sudo", "docker", "ps", "-a"])
        args = cli.parse_args(["bench", "raw", "--", "search-cql", "--duration", "10s"])
        self.assertEqual(args.extra, ["search-cql", "--duration", "10s"])
        # A command without extra arguments treats `--` the usual way (end of options).
        self.assertEqual(cli.parse_args(["note", "--", "-starts with a dash"]).text, ["-starts with a dash"])

    def test_global_options_anywhere(self) -> None:
        for argv in (["-c", "x1", "status"], ["status", "-c", "x1"], ["--cluster", "x1", "deploy", "vs"]):
            with self.subTest(argv=argv):
                self.assertEqual(cli.parse_args(argv).cluster, "x1")
        args = cli.parse_args(["--profile", "p", "status", "-v", "--region", "r"])
        self.assertEqual((args.profile, args.region, args.verbose), ("p", "r", True))
        self.assertEqual((cli.parse_args(["status"]).cluster, cli.parse_args(["status"]).verbose), (None, False))

    def test_usage_errors(self) -> None:
        bad = [
            [],
            ["bogus"],
            ["deploy"],
            ["extend"],
            ["extend", "--ttl", "1h", "--until", "2026-10-08T10:00:00Z"],
            ["up", "--ttl", "forever"],
            ["up", "--scylla-nodes", "0"],
            ["deploy", "vs", "--env", "NOEQUALS"],
            ["deploy", "vs", "--env", "1BAD=x"],
            ["bench", "raw"],
            ["bench", "raw", "--"],
            ["bench", "search"],
            ["bench", "search", "grpc"],
            ["bench", "search", "cql", "--concurrency", "64,x"],
            ["exec"],
            ["logs", "vs-0", "--service", "nope"],
            ["prom", "api", "targets", "novalue"],
            ["results", "compare"],
            ["history", "--since", "yesterday"],
        ]
        for argv in bad:
            with self.subTest(argv=argv), self.assertRaises(cli.UsageError) as caught:
                cli.parse_args(argv)
            self.assertEqual(caught.exception.exit_code, 2)

    def test_lock_flags(self) -> None:
        # extend, refresh-ip and job cancel run without the cluster lock (st.update serializes their
        # state writes), so the TTL can be moved while a long `bench ab` holds the lock. wait-serving
        # takes it: it records the pending index build in state and results.
        locked = {
            ("up",), ("down",), ("wait-serving",), ("collect",), ("dataset", "fetch"), ("job", "wait"),
            ("deploy", "scylla"), ("deploy", "vs"), ("deploy", "bench"), ("deploy", "monitoring"), ("deploy", "all"),
            ("bench", "load"), ("bench", "index"), ("bench", "search"), ("bench", "ab"), ("bench", "rerun"),
            ("bench", "raw"),
        }  # fmt: skip
        for words, parser in leaves(cli.build_parser()).items():
            with self.subTest(command=" ".join(words)):
                self.assertEqual(parser.get_default("lock"), words in locked)

    def test_history_flags(self) -> None:
        for words, parser in leaves(cli.build_parser()).items():
            with self.subTest(command=" ".join(words)):
                self.assertEqual(parser.get_default("history"), words[0] not in ("note", "retro", "history"))

    def test_relative_times_parse_on_every_python(self) -> None:
        # argparse before 3.14 took `-15m` for an option: "--start: expected one argument" (exit 2).
        args = cli.parse_args(["prom", "range", "up", "--start", "-15m", "--end", "-1m", "-v"])
        self.assertEqual((args.start, args.end, args.verbose), ("-15m", "-1m", True))
        self.assertEqual(cli.parse_args(["prom", "query", "up", "--time", "-90s"]).time, "-90s")
        for flag in ("retro", "history"):
            with self.subTest(command=flag):
                since = cli.parse_args([flag, "--since", "-6h"]).since
                self.assertAlmostEqual((proc.utcnow() - since).total_seconds(), 6 * 3600, delta=60)
        self.assertEqual(cli.parse_args(["prom", "range", "up"]).start, "now-15m")  # documented form, no leading -
        with self.assertRaises(cli.UsageError):
            cli.parse_args(["status", "--bogus"])  # a real unknown option is still an error

    def test_results_options_before_compare_survive(self) -> None:
        args = cli.parse_args(["results", "--json", "--format", "md", "compare", "X"])
        self.assertEqual((args.func, args.json, args.format, args.ids), ("cmd_results_compare", True, "md", ["X"]))
        args = cli.parse_args(["results", "compare", "X"])
        self.assertEqual((args.json, args.format), (False, "table"))
        args = cli.parse_args(["results", "--format", "md", "compare", "X", "--json"])
        self.assertEqual((args.json, args.format), (True, "md"))

    def test_no_subcommand_default_clobbers_a_parent_option(self) -> None:
        """argparse copies a subparser's defaults over the parent's values: an option defined on both
        levels needs default=SUPPRESS below, or `parent --opt sub` silently drops --opt."""

        def check(parser: argparse.ArgumentParser, path: tuple[str, ...]) -> None:
            own = {a.dest for a in parser._actions if a.option_strings and a.dest != "help"}
            for action in parser._actions:
                if not isinstance(action, argparse._SubParsersAction):
                    continue
                for name, sub in action.choices.items():
                    for child in sub._actions:
                        if child.dest in own and child.option_strings and child.default is not argparse.SUPPRESS:
                            self.fail(f"{' '.join((*path, name))}: {child.option_strings[-1]} clobbers the parent's")
                    check(sub, (*path, name))

        check(cli.build_parser(), ())

    def test_ab_takes_bucket_timeout_and_extra_args(self) -> None:
        args = cli.parse_args(["bench", "ab", "--a", "x", "--b", "y", "--bucket", "0", "--timeout", "5m", "--", "-f"])
        self.assertEqual((args.bucket, args.timeout_s, args.extra), (0, 300, ["-f"]))
        args = cli.parse_args(["bench", "ab", "--a", "x", "--b", "y"])
        self.assertEqual((args.bucket, args.timeout_s, args.extra, args.repeat), (None, None, [], 2))

    def test_billing_project_must_not_be_empty(self) -> None:
        for value in ("", "   "):
            with self.subTest(value=value), self.assertRaises(cli.UsageError):
                cli.parse_args(["up", "--billing-project", value])
        self.assertEqual(cli.parse_args(["up", "--billing-project", " Vector Search: DiskANN "]).billing_project,
                         "Vector Search: DiskANN")  # fmt: skip
        self.assertEqual(cli.parse_args(["up"]).billing_project, config.DEFAULT_BILLING_PROJECT)


class HelpTest(CliCase):
    def help_text(self, *words: str) -> str:
        code, out, _err = self.run_cli(*words, "--help")
        self.assertEqual(code, 0)
        return " ".join(out.split())

    def test_tuning_flags_show_their_defaults(self) -> None:
        text = self.help_text("bench", "search")
        for expected in ("(default: 64)", "(default: 9m)", "(default: 1m)", "(default: 30s)", "(default: 10)"):
            self.assertIn(expected, text)
        self.assertIn("runs per concurrency; 3+ for a noise estimate (default: 1)", text)
        text = self.help_text("bench", "load")
        self.assertIn("upload concurrency (parallel inserts) (default: 512)", text)
        self.assertIn("build limit (default: 2h)", text)
        self.assertIn("(default: 24h)", self.help_text("up"))
        self.assertIn("(default: now-15m)", self.help_text("prom", "range"))
        self.assertIn("(default: 2)", self.help_text("bench", "ab"))
        self.assertNotIn("-15m", self.help_text("prom", "query").replace("now-15m", ""))
        self.assertIn("e.g. 6h", self.help_text("history"))

    def test_every_option_with_a_default_has_help(self) -> None:
        for words, parser in leaves(cli.build_parser()).items():
            parser.format_help()  # %-formatting of every help string works
            for action in parser._actions:
                default = action.default
                unset = default is None or default is False or default is argparse.SUPPRESS or default == []
                if action.option_strings and not unset and action.dest != "help":
                    with self.subTest(command=" ".join(words), option=action.option_strings[-1]):
                        self.assertTrue(action.help, "an option with a default needs help text to show it")

    def test_duration_text(self) -> None:
        cases = {540: "9m", 7200: "2h", 30: "30s", 90: "90s", 0: "0s", 86400: "24h"}
        self.assertEqual({s: cli.duration_text(s) for s in cases}, cases)


# --- exit codes ---------------------------------------------------------------------------------
class ExitCodeTest(unittest.TestCase):
    def test_mapping(self) -> None:
        cases = [
            (VsbenchError("x"), 1),
            (cli.UsageError("x"), 2),
            (awsapi.AuthError("x", "ExpiredToken"), 3),
            (awsapi.CapacityError("x", "InsufficientInstanceCapacity"), 4),
            (PreconditionError("x"), 5),
            (StillRunning("x"), 75),
            (cli.Interrupted(signal.SIGTERM), 143),
            (cli.Interrupted(signal.SIGHUP), 129),
            (cli.Interrupted(signal.SIGINT), 130),
            (KeyboardInterrupt(), 130),
            (RuntimeError("bug"), 1),
            (SystemExit(0), 0),
            (SystemExit(None), 0),
            (SystemExit(3), 3),
            (BrokenPipeError(), 141),
        ]
        for exc, code in cases:
            with self.subTest(exc=type(exc).__name__):
                self.assertEqual(cli.exit_code_for(exc), code)


class MainTest(CliCase):
    def handler(self, side_effect: Any) -> mock.MagicMock:
        from vsbenchlib import cli_cluster

        patcher = mock.patch.object(cli_cluster, "cmd_refresh_ip", side_effect=side_effect)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def test_exit_codes_and_history(self) -> None:
        cases = [
            (VsbenchError("plain failure", "do x"), 1),
            (awsapi.AuthError("expired", "ExpiredToken", config.LOGIN_HINT), 3),
            (awsapi.CapacityError("no capacity", "InsufficientInstanceCapacity"), 4),
            (PreconditionError("busy", "wait"), 5),
            (StillRunning("still running", "vsbench job wait X"), 75),
            (RuntimeError("a bug"), 1),
        ]
        for exc, code in cases:
            with self.subTest(exc=type(exc).__name__):
                self.handler(exc)
                got, _out, err = self.run_cli("-c", CLUSTER, "refresh-ip")
                self.assertEqual(got, code)
                end = st.read_history()[-1]
                self.assertEqual((end["event"], end["exit"], end["argv"]), ("end", code, ["-c", CLUSTER, "refresh-ip"]))
                self.assertIn(str(exc), end["error"])
                self.assertEqual(end["hint"], getattr(exc, "hint", None))
                self.assertIn(f"error: {exc}" if isinstance(exc, VsbenchError) else "internal error", err)

    def test_start_and_end_lines(self) -> None:
        self.handler(lambda ctx, args: 0)
        code, _out, _err = self.run_cli("refresh-ip", "-c", CLUSTER)
        self.assertEqual(code, 0)
        start, end = st.read_history()
        self.assertEqual(start["id"], end["id"])
        self.assertEqual(
            (start["event"], start["cluster"], start["pid"], start["skill_rev"]),
            ("start", CLUSTER, os.getpid(), "abc1234"),
        )
        self.assertEqual((end["exit"], end["error"], end["hint"], end["skill_rev"]), (0, None, None, "abc1234"))
        self.assertIsInstance(end["duration_s"], float)
        rows = retro.command_rows(st.read_history())
        self.assertEqual([(r["status"], r["command"]) for r in rows], [("ok", "refresh-ip")])

    def test_usage_error_is_recorded(self) -> None:
        code, out, err = self.run_cli("-c", "c9", "deploy", "nope")
        self.assertEqual((code, out), (2, ""))
        self.assertIn("usage: vsbench deploy", err)
        start, end = st.read_history()
        self.assertEqual((start["id"], end["exit"], end["cluster"], end["hint"]), (end["id"], 2, "c9", None))
        self.assertIn("invalid choice", end["error"])

    def test_invalid_cluster_name_is_a_usage_error(self) -> None:
        code, _out, err = self.run_cli("-c", "Bad_Name", "status")
        self.assertEqual(code, 2)
        self.assertIn("invalid cluster name", err)
        with mock.patch.dict(os.environ, {"VSBENCH_CLUSTER": "UPPER"}):
            self.assertEqual(self.run_cli("status")[0], 2)

    def test_help_exits_zero_without_history(self) -> None:
        code, out, _err = self.run_cli("--help")
        self.assertEqual(code, 0)
        self.assertIn("usage: vsbench", out)
        self.assertEqual(st.read_history(), [])

    def test_meta_commands_write_no_start_end(self) -> None:
        self.assertEqual(self.run_cli("note", "--kind", "surprise", "a", "note")[0], 0)
        self.assertEqual(self.run_cli("history")[0], 0)
        self.assertEqual(self.run_cli("retro")[0], 0)
        self.assertEqual([e["event"] for e in st.read_history()], ["note"])

    def test_verbose_flag(self) -> None:
        seen = []
        self.handler(lambda ctx, args: seen.append(proc.VERBOSE) or 0)
        self.run_cli("refresh-ip", "-v")
        self.assertEqual(seen, [True])


class SignalTest(CliCase):
    def kill_self(self, signum: int) -> Any:
        def handler(ctx: cli.Context, args: argparse.Namespace) -> int:
            os.kill(os.getpid(), signum)
            time.sleep(5)  # interrupted by the signal handler
            raise AssertionError("not interrupted")

        return handler

    def test_signals_write_the_end_line(self) -> None:
        from vsbenchlib import cli_cluster

        before = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)}
        for signum, code in ((signal.SIGTERM, 143), (signal.SIGHUP, 129), (signal.SIGINT, 130)):
            with (
                self.subTest(signal=signum.name),
                mock.patch.object(cli_cluster, "cmd_refresh_ip", self.kill_self(signum)),
            ):
                started = time.monotonic()
                got, _out, err = self.run_cli("-c", CLUSTER, "refresh-ip")
                self.assertLess(time.monotonic() - started, 4)
                self.assertEqual(got, code)
                self.assertIn(f"interrupted by {signum.name}", err)
                end = st.read_history()[-1]
                self.assertEqual((end["event"], end["exit"]), ("end", code))
                self.assertEqual(end["error"], f"interrupted by {signum.name}")
        self.assertEqual({s: signal.getsignal(s) for s in before}, before)

    def test_lock_is_released_after_a_signal(self) -> None:
        fake = self.fake("deploy")
        fake.deploy_monitoring.side_effect = lambda cluster: os.kill(os.getpid(), signal.SIGTERM) or time.sleep(5)
        self.save_state()
        self.assertEqual(self.run_cli("-c", CLUSTER, "deploy", "monitoring")[0], 143)
        with st.cluster_lock(CLUSTER, "test"):  # would raise PreconditionError if still held
            pass

    def test_signal_outside_the_handler_is_deferred(self) -> None:
        guard = cli.SignalGuard()
        guard.handle(signal.SIGTERM, None)  # inactive: remembered, not raised
        self.assertEqual(guard.signum, signal.SIGTERM)
        guard = cli.SignalGuard(active=True)
        with self.assertRaises(cli.Interrupted):
            guard.handle(signal.SIGHUP, None)
        guard.handle(signal.SIGTERM, None)  # second signal while unwinding: not raised
        self.assertEqual(guard.signum, signal.SIGHUP)


class LockTest(CliCase):
    def test_mutating_command_fails_fast_when_busy(self) -> None:
        fake = self.fake("deploy")
        self.save_state()
        with st.cluster_lock(CLUSTER, "bench search cql"):
            code, _out, err = self.run_cli("-c", CLUSTER, "deploy", "monitoring")
        self.assertEqual(code, 5)
        self.assertIn("is busy", err)
        self.assertIn("status, extend, refresh-ip and job cancel work meanwhile", err)
        fake.deploy_monitoring.assert_not_called()
        self.assertEqual(st.read_history()[-1]["exit"], 5)

    def test_lock_records_the_command(self) -> None:
        seen = []

        def deploy_monitoring(cluster: str) -> dict[str, Any]:
            seen.append((st.paths(cluster).root / "lock").read_text())
            return make_state()

        self.fake("deploy", deploy_monitoring=deploy_monitoring)
        self.save_state()
        self.assertEqual(self.run_cli("-c", CLUSTER, "deploy", "monitoring")[0], 0)
        self.assertIn("vsbench -c t1 deploy monitoring", seen[0])

    def test_read_only_and_cancel_work_while_busy(self) -> None:
        remote = self.fake("remote")
        self.fake("deploy", vs_status=lambda c: {}, scylla_status=lambda c: {}, monitoring_status=lambda c: {})
        self.save_state()
        with st.cluster_lock(CLUSTER, "bench search cql"):
            self.assertEqual(self.run_cli("-c", CLUSTER, "status")[0], 0)
            self.assertEqual(self.run_cli("-c", CLUSTER, "job", "cancel", "J1")[0], 0)
        remote.job_cancel.assert_called_once_with(CLUSTER, "client", "J1")

    def test_extend_and_refresh_ip_work_while_ab_runs(self) -> None:
        state = self.save_state()
        provision = self.fake("provision", extend=mock.Mock(return_value=state))
        provision.refresh_ip.return_value = state | {"aws": {"operator_cidr": "1.2.3.4/32"}}
        with st.cluster_lock(CLUSTER, "bench ab --a release:latest --b local"):
            self.assertEqual(self.run_cli("-c", CLUSTER, "extend", "--ttl", "12h")[0], 0)
            self.assertEqual(self.run_cli("-c", CLUSTER, "refresh-ip")[0], 0)
        self.assertEqual(provision.extend.call_args.args[2], 12 * 3600)
        provision.refresh_ip.assert_called_once()

    def test_wait_serving_takes_the_lock(self) -> None:
        deploy = self.fake("deploy")
        self.save_state()
        with st.cluster_lock(CLUSTER, "bench ab --a x --b y"):
            code, _out, err = self.run_cli("-c", CLUSTER, "wait-serving")
        self.assertEqual(code, 5)
        self.assertIn("is busy", err)
        deploy.wait_serving.assert_not_called()


# --- warnings and context -----------------------------------------------------------------------
class WarningTest(CliCase):
    def test_ttl_warnings(self) -> None:
        now = proc.utcnow()
        soon, later, past = make_state(expires_in_s=1830), make_state(expires_in_s=7200), make_state(expires_in_s=-60)
        self.assertIn("expires in 30m", cli.expiry_warnings(soon, "p", now)[0])
        self.assertIn("extend --ttl", cli.expiry_warnings(soon, "p", now)[0])
        self.assertEqual(cli.expiry_warnings(later, "p", now), [])
        self.assertIn("passed its TTL", cli.expiry_warnings(past, "p", now)[0])
        self.assertEqual(cli.expiry_warnings(past | {"terminated_at": "2026-01-01T00:00:00Z"}, "p", now), [])
        self.assertEqual(cli.expiry_warnings(None, "p", now), [])

    def test_credential_warnings(self) -> None:
        now = proc.utcnow()
        self.write_credentials(600, "prof")
        self.assertIn("expire in 10m", cli.expiry_warnings(None, "prof", now)[0])
        self.write_credentials(3600, "prof")
        self.assertEqual(cli.expiry_warnings(None, "prof", now), [])
        self.write_credentials(-60, "prof")  # expired: the AWS commands report it themselves (exit 3)
        self.assertEqual(cli.expiry_warnings(None, "prof", now), [])

    def test_commands_print_warnings(self) -> None:
        self.fake("deploy", vs_status=lambda c: {}, scylla_status=lambda c: {}, monitoring_status=lambda c: {})
        st.save(CLUSTER, make_state(expires_in_s=1200))
        _code, _out, err = self.run_cli("-c", CLUSTER, "status")
        self.assertIn("warning: cluster 't1' expires in", err)
        _code, _out, err = self.run_cli("-c", CLUSTER, "history")
        self.assertNotIn("expires in", err)


class ContextTest(CliCase):
    def args(self, *argv: str) -> argparse.Namespace:
        return cli.parse_args(list(argv) or ["status"])

    def test_cluster_resolution(self) -> None:
        self.assertEqual(cli.cluster_name(self.args("status")), "default")
        with mock.patch.dict(os.environ, {"VSBENCH_CLUSTER": "envc"}):
            self.assertEqual(cli.cluster_name(self.args("status")), "envc")
            self.assertEqual(cli.cluster_name(self.args("-c", "cli1", "status")), "cli1")

    def test_profile_and_region_precedence(self) -> None:
        state = make_state()
        ctx = cli.make_context(self.args("--profile", "p", "--region", "r", "status"), CLUSTER, [], state)
        self.assertEqual((ctx.profile, ctx.region), ("p", "r"))
        with mock.patch.dict(os.environ, {"AWS_PROFILE": "envp", "AWS_REGION": "envr"}):
            ctx = cli.make_context(self.args("status"), CLUSTER, [], state)
            self.assertEqual((ctx.profile, ctx.region), ("state-profile", "eu-west-1"))
            ctx = cli.make_context(self.args("status"), CLUSTER, [], None)
            self.assertEqual((ctx.profile, ctx.region), ("envp", "envr"))
        with mock.patch.dict(os.environ, {"AWS_DEFAULT_REGION": "defr"}):
            self.assertEqual(cli.make_context(self.args("status"), CLUSTER, [], None).region, "defr")
        ctx = cli.make_context(self.args("status"), CLUSTER, [], None)
        self.assertEqual((ctx.profile, ctx.region), (config.DEFAULT_PROFILE, config.DEFAULT_REGION))
        self.assertEqual(ctx.aws(), awsapi.Aws(config.DEFAULT_PROFILE, config.DEFAULT_REGION))


class LoadModuleTest(unittest.TestCase):
    def test_missing_module_is_a_vsbench_error(self) -> None:
        with self.assertRaises(VsbenchError) as caught:
            cli.load_module("no_such_module_xyz")
        self.assertIn("cannot be loaded", str(caught.exception))

    def test_tree_rendering(self) -> None:
        doc = {"a": 1, "flat": {"x": 1, "y": None}, "rows": [{"n": "s0", "ok": True}], "deep": {"b": {"c": [1, 2]}}}
        text = "\n".join(cli.tree(doc, ""))
        self.assertIn("flat: x=1 y=-", text)
        self.assertIn("- n=s0 ok=yes", text)
        self.assertIn("    c: 1,2", text)
        self.assertEqual(cli.table([], ["a"]), "(none)")
        self.assertEqual(cli.table([{"a": 1.23456, "b": [1, 2]}], ["a", "b"]).splitlines()[1], "1.235  1,2")


if __name__ == "__main__":
    unittest.main()
