# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.retro: history pairing, filters, the digest, notes and skill_rev."""

from __future__ import annotations

import datetime
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

SKILL_DIR = Path(__file__).resolve().parent.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from vsbenchlib import proc, retro  # noqa: E402
from vsbenchlib import state as st  # noqa: E402
from vsbenchlib.proc import VsbenchError  # noqa: E402

UTC = datetime.timezone.utc
T0 = datetime.datetime(2026, 10, 6, 8, 0, 0, tzinfo=UTC)


def ts(minutes: float) -> str:
    return proc.iso(T0 + datetime.timedelta(minutes=minutes))


def command(ident: str, minutes: float, argv: list[str], exit_code: int | None, **end: Any) -> list[dict[str, Any]]:
    """A start line and (unless exit_code is None) its end line."""
    base = {"id": ident, "cluster": end.pop("cluster", "c1"), "argv": argv}
    lines = [base | {"ts": ts(minutes), "event": "start", "pid": end.pop("pid", None), "skill_rev": "abc1234"}]
    if exit_code is not None:
        fields = {"exit": exit_code, "duration_s": end.pop("duration_s", 1.0), "error": None, "hint": None} | end
        lines.append(base | {"ts": ts(minutes + 1), "event": "end", "skill_rev": "abc1234"} | fields)
    return lines


def note(minutes: float, kind: str, text: str, cluster: str = "c1") -> dict[str, Any]:
    return {"ts": ts(minutes), "event": "note", "cluster": cluster, "kind": kind, "text": text, "skill_rev": "x"}


class RetroCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="vsbench-retro-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        patcher = mock.patch.dict(os.environ, {"VSBENCH_HOME": str(self.tmp / "home")})
        patcher.start()
        self.addCleanup(patcher.stop)

    def write_history(self, *groups: list[dict[str, Any]] | dict[str, Any]) -> None:
        for group in groups:
            for entry in group if isinstance(group, list) else [group]:
                st.append_history(entry)

    def write_results(self, cluster: str, records: list[dict[str, Any]]) -> None:
        target = st.paths(cluster).results_file
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("".join(json.dumps(r) + "\n" for r in records))


class HistoryRowsTest(RetroCase):
    def test_pairing_and_statuses(self) -> None:
        self.write_history(
            command("a", 0, ["-c", "c1", "deploy", "vs", "--source", "local"], 0),
            command("b", 5, ["bench", "search", "cql"], 75),
            command("c", 10, ["up"], 143),
            command("d", 15, ["status"], 1, error="boom\nmore", hint="do x"),
            command("e", 20, ["bench", "load", "cohere-1m"], None),  # SIGKILL: no end line
            [{"id": "f", "ts": ts(30), "event": "end", "cluster": "c1", "argv": ["list"], "exit": 0, "duration_s": 2}],
            note(31, "surprise", "ignored by history_rows"),
        )
        rows = retro.history_rows(None, False, None)
        self.assertEqual([r["id"] for r in rows], ["a", "b", "c", "d", "e", "f"])
        self.assertEqual([r["status"] for r in rows], ["ok", "detached", "interrupted", "failed", "killed", "ok"])
        self.assertEqual([r["command"] for r in rows][:5], ["deploy vs", "bench search", "up", "status", "bench load"])
        self.assertEqual(rows[0]["text"], "deploy vs --source local")
        self.assertEqual((rows[3]["error"], rows[3]["hint"], rows[3]["exit"]), ("boom\nmore", "do x", 1))
        self.assertEqual((rows[4]["exit"], rows[4]["duration_s"], rows[4]["ended_at"]), (None, None, None))

    def test_running_when_the_process_is_alive(self) -> None:
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)", "vsbench"])
        self.addCleanup(child.wait)
        self.addCleanup(child.kill)
        self.write_history(command("a", 0, ["bench", "ab"], None, pid=child.pid))
        self.assertEqual(retro.history_rows(None, False, None)[0]["status"], "running")
        child.kill()
        child.wait()
        self.assertEqual(retro.history_rows(None, False, None)[0]["status"], "killed")

    def test_filters(self) -> None:
        self.write_history(
            command("a", 0, ["status"], 0),
            command("b", 60, ["status"], 1),
            command("c", 120, ["up"], None),
            command("d", 180, ["status"], 0),
        )
        since = T0 + datetime.timedelta(minutes=30)
        self.assertEqual([r["id"] for r in retro.history_rows(since, False, None)], ["b", "c", "d"])
        self.assertEqual([r["id"] for r in retro.history_rows(None, True, None)], ["b", "c"])
        self.assertEqual([r["id"] for r in retro.history_rows(None, False, 2)], ["c", "d"])
        self.assertEqual(retro.history_rows(None, False, None), retro.history_rows(None, False, 0))

    def test_malformed_lines_are_skipped(self) -> None:
        target = st.history_file()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text('not json\n{"event": "start"}\n' + json.dumps(command("a", 0, ["status"], 0)[0]) + "\n")
        self.assertEqual([r["id"] for r in retro.history_rows(None, False, None)], ["a"])


class ArgvTest(unittest.TestCase):
    def test_command_names(self) -> None:
        cases = [
            (["-c", "x", "deploy", "vs", "--source", "local"], "deploy vs", "deploy vs --source local"),
            (["--cluster=x", "-v", "status", "--json"], "status", "status --json"),
            (["exec", "vs-0", "--", "ls", "-c", "x"], "exec", "exec vs-0 -- ls -c x"),
            (["bench", "--profile", "p", "raw", "--", "x"], "bench raw", "bench raw -- x"),
            (["results", "--last", "5"], "results", "results --last 5"),
            ([], "?", ""),
        ]
        for argv, name, text in cases:
            with self.subTest(argv=argv):
                self.assertEqual((retro.command_name(argv), retro.command_text(argv)), (name, text))

    def test_parse_time(self) -> None:
        self.assertEqual(retro.parse_time("2026-10-06T08:00:00Z"), T0)
        self.assertEqual(retro.parse_time("2026-10-06T10:00:00+02:00"), T0)
        self.assertEqual(retro.parse_time("2026-10-06T08:00:00"), T0)
        self.assertEqual(retro.parse_time("2026-10-06"), T0 - datetime.timedelta(hours=8))
        ago = proc.utcnow() - retro.parse_time("-6h")
        self.assertAlmostEqual(ago.total_seconds(), 6 * 3600, delta=5)
        self.assertAlmostEqual((proc.utcnow() - retro.parse_time("90m")).total_seconds(), 5400, delta=5)
        with self.assertRaises(VsbenchError):
            retro.parse_time("yesterday")

    def test_parse_time_forms_without_a_leading_dash(self) -> None:
        # before Python 3.14, argparse reads `--since -6h` as two options: `6h` and `now-6h` must work
        for text in ("6h", "now-6h", "now - 6h", " 6 h "):
            with self.subTest(text=text):
                ago = proc.utcnow() - retro.parse_time(text)
                self.assertAlmostEqual(ago.total_seconds(), 6 * 3600, delta=5)
        self.assertAlmostEqual((proc.utcnow() - retro.parse_time("now")).total_seconds(), 0, delta=5)
        for bad in ("now-", "now-6x", "nowhere"):
            with self.subTest(bad=bad), self.assertRaises(VsbenchError):
                retro.parse_time(bad)
        with self.assertRaises(VsbenchError) as caught:
            retro.parse_time("yesterday")
        hint = caught.exception.hint or ""
        self.assertIn("6h", hint)
        self.assertNotRegex(hint, r"(^|[\s,(])-\d")

    def test_format_seconds(self) -> None:
        self.assertEqual((retro.format_seconds(3.25), retro.format_seconds(600)), ("3.2s", "10m"))


class DigestTest(RetroCase):
    def history(self) -> None:
        self.write_history(
            command("a1", 0, ["deploy", "vs", "--source", "local"], 1, error="job 20261006T080000Z-load-1a2b failed"),
            command("a2", 5, ["deploy", "vs", "--source", "local"], 1, error="job 20261006T080500Z-load-9f9f failed"),
            command("a3", 10, ["deploy", "vs", "--source", "local"], 0, duration_s=400.0),
            command("b1", 20, ["bench", "search", "cql"], 5, error="index not SERVING on vs-0", hint="wait-serving"),
            command("b2", 22, ["bench", "search", "cql"], 5, error="index not SERVING on vs-1", hint="other hint"),
            command("c1", 30, ["up"], None, duration_s=None),
            command("c2", 70, ["up"], 0, duration_s=900.0),  # 40 min after the kill: not a retry
            command("d1", 80, ["exec", "vs-0", "--", "curl -s 127.0.0.1:6080/api/v1/indexes"], 0),
            command("d2", 81, ["exec", "vs-0", "--", "curl -s 127.0.0.1:6080/api/v1/indexes"], 0),
            command("d3", 82, ["ssh", "client"], 255),
            command("d4", 83, ["bench", "raw", "--", "search-cql", "--help"], 0),
            command("z1", 90, ["status"], 0, cluster="c2"),
            note(40, "workaround", "restarted VS by hand"),
            note(-10, "idea", "before the window"),
        )

    def test_groups(self) -> None:
        self.history()
        d = retro.digest(T0)
        self.assertEqual(d["summary"]["commands"], 12)
        self.assertEqual(d["summary"]["by_status"], {"failed": 5, "killed": 1, "ok": 6})
        groups = [(g["count"], g["error"], g["hint"]) for g in d["failures"]]
        self.assertEqual(groups[0], (2, "job 20261006T080000Z-load-1a2b failed", None))
        self.assertIn((1, "index not SERVING on vs-0", "wait-serving"), groups)
        self.assertIn((1, "index not SERVING on vs-1", "other hint"), groups)  # different hint
        self.assertIn((1, "exit 255", None), groups)
        self.assertEqual(d["failures"][0]["commands"], ["deploy vs --source local"])
        self.assertEqual([k["text"] for k in d["killed"]], ["up"])
        retries = [(r["command"], r["failed_at"], r["retry_status"]) for r in d["retries"]]
        self.assertEqual(
            retries,
            [
                ("deploy vs --source local", ts(0), "failed"),
                ("deploy vs --source local", ts(5), "ok"),
                ("bench search cql", ts(20), "failed"),
            ],
        )
        raw = [(r["command"], r["count"]) for r in d["raw_commands"]]
        self.assertEqual(raw[0], ("exec vs-0 -- curl -s 127.0.0.1:6080/api/v1/indexes", 2))
        self.assertEqual({c for c, _ in raw[1:]}, {"ssh client", "bench raw -- search-cql --help"})
        self.assertEqual([s["text"] for s in d["slowest"][:2]], ["up", "deploy vs --source local"])
        self.assertEqual([n["text"] for n in d["notes"]], ["restarted VS by hand"])
        self.assertEqual(d["result_flags"]["clusters"], ["c1", "c2"])

    def test_result_flags_and_high_cv(self) -> None:
        def record(run: str, qps: float, flags: list[str], started: float = 1) -> dict[str, Any]:
            return {
                "run_id": run,
                "kind": "search-cql",
                "series_id": "s1",
                "started_at": ts(started),
                "params": {"concurrency": 64},
                "versions": {"vector_store": {"build_id": "b1"}},
                "client_metrics": {"qps": qps},
                "flags": flags,
            }

        self.write_history(command("a", 0, ["bench", "search", "cql"], 0))
        self.write_results(
            "c1",
            [
                record("r1", 1000, ["latency_floored", "client_saturated"]),
                record("r2", 1500, ["latency_floored"]),
                record("r3", 1100, []),
                record("old", 10, ["timeouts"], started=-60),
            ],
        )
        flags = retro.digest(T0)["result_flags"]
        self.assertEqual(flags["records"], 3)
        self.assertEqual(flags["flags"]["latency_floored"], {"count": 2, "runs": ["r1", "r2"]})
        self.assertEqual(flags["flags"]["client_saturated"]["count"], 1)
        self.assertNotIn("timeouts", flags["flags"])
        self.assertEqual(flags["flags"]["high_cv"]["count"], 1)
        self.assertTrue(flags["flags"]["high_cv"]["runs"][0].startswith("s1@64 (cv "))

    def test_result_flags_degrade_when_results_fail(self) -> None:
        self.write_history(command("a", 0, ["status"], 0))
        with mock.patch.object(retro, "_result_flags", side_effect=ImportError("results is broken")):
            d = retro.digest(T0)
        self.assertEqual(d["result_flags"]["error"], "ImportError: results is broken")
        self.assertIn("(results unreadable: ImportError: results is broken)", retro.format_digest(d))

    def test_previous_retrospectives(self) -> None:
        directory = st.retrospectives_dir()
        directory.mkdir(parents=True)
        (directory / "20261001T100000Z-c1.md").write_text(
            "# Retro\n\n## What went wrong\n- not a proposal\n\n## Proposals\n\n"
            "### Doc: explain wait-serving\nbody text\n- **Code**: add `vsbench indexes`\n  - nested detail\n"
            "1. third proposal\n\n## Appendix\n- after the section\n"
        )
        (directory / "20261002T100000Z-c1.md").write_text("**Proposals**\n- bold heading style\n\n## Next\n- no\n")
        (directory / "20261003T100000Z-c1.md").write_text("# Nothing here\n")
        retros = retro.digest(None)["retrospectives"]
        self.assertEqual([r["file"] for r in retros], sorted(p.name for p in directory.iterdir()))
        self.assertEqual(
            retros[0]["proposals"], ["Doc: explain wait-serving", "**Code**: add `vsbench indexes`", "third proposal"]
        )
        self.assertEqual(retros[1]["proposals"], ["bold heading style"])
        self.assertEqual(retros[2]["proposals"], [])
        self.assertEqual([r["has_proposals"] for r in retros], [True, True, False])

    def proposals_of(self, text: str) -> list[str] | None:
        path = self.tmp / "retro.md"
        path.write_text(text)
        return retro._proposals(path)

    def test_documented_format(self) -> None:
        # the exact layout SKILL.md step 6.2 tells the agent to write
        text = (
            "## What happened\n- goal: compare builds\n\n## What went wrong\n- **Proposals**-like bold, wrong section\n"
            "\n## What we learned\n- x\n\n## Proposals\n- **Doc**: explain wait-serving (evidence: history)\n"
            "  - detail, not a proposal\n- **Code**: add `vsbench indexes`\n"
        )
        self.assertEqual(
            self.proposals_of(text),
            ["**Doc**: explain wait-serving (evidence: history)", "**Code**: add `vsbench indexes`"],
        )

    def test_bold_list_item_title_with_nested_items(self) -> None:
        text = (
            "- **What happened**: x\n- **What went wrong**: y\n- **Proposals**:\n"
            "  1. **Doc**: fix A\n  2. **Code**: B\n     - detail\n- **What we learned**: z\n  - not a proposal\n"
        )
        self.assertEqual(self.proposals_of(text), ["**Doc**: fix A", "**Code**: B"])
        self.assertEqual(self.proposals_of("- **Proposals**.\n  - a\n- sibling\n"), ["a"])

    def test_bold_title_with_trailing_punctuation_or_text(self) -> None:
        text = "**Proposals**.\n- Doc: fix A\n- **Code**: B\n\n## Next\n- no\n"
        self.assertEqual(self.proposals_of(text), ["Doc: fix A", "**Code**: B"])
        self.assertEqual(self.proposals_of("**Proposals:**\n* star item\n"), ["star item"])
        self.assertEqual(self.proposals_of("**Proposals**: none this time\n"), ["none this time"])

    def test_empty_and_missing_sections(self) -> None:
        self.assertEqual(self.proposals_of("## Proposals\n\nnothing worth proposing\n"), [])
        self.assertIsNone(self.proposals_of("## Notes\n- **Earlier proposals**: none applied\n"))
        self.assertIsNone(self.proposals_of(""))
        directory = st.retrospectives_dir()
        directory.mkdir(parents=True)
        (directory / "a.md").write_text("## Proposals\n")
        (directory / "b.md").write_text("# Nothing\n")
        text = retro.format_digest(retro.digest(None))
        self.assertIn("  a.md\n    (Proposals section is empty)", text)
        self.assertIn("  b.md\n    (no Proposals section)", text)

    def test_format_digest(self) -> None:
        self.history()
        text = retro.format_digest(retro.digest(T0))
        for heading in (
            "failures, grouped by error:",
            "killed or interrupted",
            "retries (same command within 30 min of a failure):",
            "exec/ssh/bench raw calls (candidates for new subcommands):",
            "slowest commands:",
            "result-quality flags:",
            "notes:",
            "previous retrospectives in",
        ):
            self.assertIn(heading, text)
        self.assertIn("2x job 20261006T080000Z-load-1a2b failed [exit 1]", text)
        self.assertIn("[workaround] c1: restarted VS by hand", text)
        self.assertIn("    15m  ok          up", text)
        empty = retro.format_digest(retro.digest(T0 + datetime.timedelta(days=1)))
        self.assertIn("commands: 0 (none)", empty)
        self.assertIn("  (none)", empty)


class NoteTest(RetroCase):
    def test_note(self) -> None:
        with mock.patch.object(retro, "skill_rev", return_value="abc1234-dirty"):
            retro.note("c1", "time-sink", "  waited 20 min for the index  ")
        entry = st.read_history()[-1]
        self.assertEqual(
            {k: entry[k] for k in ("event", "cluster", "kind", "text", "skill_rev")},
            {
                "event": "note",
                "cluster": "c1",
                "kind": "time-sink",
                "text": "waited 20 min for the index",
                "skill_rev": "abc1234-dirty",
            },
        )
        with self.assertRaises(VsbenchError):
            retro.note("c1", "rant", "x")
        with self.assertRaises(VsbenchError):
            retro.note("c1", "idea", "   ")


class SkillRevTest(unittest.TestCase):
    def setUp(self) -> None:
        retro.skill_rev.cache_clear()
        self.addCleanup(retro.skill_rev.cache_clear)

    def fake_git(self, log: str, status: str, code: int = 0) -> mock.MagicMock:
        def run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            out = log if "log" in cmd else status
            return subprocess.CompletedProcess(cmd, code, out, "")

        patcher = mock.patch.object(proc, "run", side_effect=run)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def test_clean_dirty_untracked_and_failures(self) -> None:
        cases = [
            (("abc1234\n", ""), "abc1234"),
            (("abc1234\n", " M SKILL.md\n"), "abc1234-dirty"),
            (("", "?? .\n"), "untracked-dirty"),
        ]
        for (log, status), expected in cases:
            with self.subTest(expected=expected):
                retro.skill_rev.cache_clear()
                self.fake_git(log, status)
                self.assertEqual(retro.skill_rev(), expected)
        retro.skill_rev.cache_clear()
        self.fake_git("", "", code=128)
        self.assertIsNone(retro.skill_rev())
        retro.skill_rev.cache_clear()
        with mock.patch.object(proc, "run", side_effect=VsbenchError("command not found: git")):
            self.assertIsNone(retro.skill_rev())

    def test_cached_per_process(self) -> None:
        fake = self.fake_git("abc1234\n", "")
        retro.skill_rev()
        retro.skill_rev()
        self.assertEqual(fake.call_count, 2)  # one log + one status, once


if __name__ == "__main__":
    unittest.main()
