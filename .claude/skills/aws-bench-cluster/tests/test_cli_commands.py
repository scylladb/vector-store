# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for the vsbench command handlers (vsbenchlib/cli_cluster.py, cli_bench.py).

The modules behind the handlers (remote, deploy, bench, provision, prom, build) are
replaced in sys.modules; nothing touches AWS, ssh or the network.
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import io
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

from vsbenchlib import cli, cli_bench, cli_cluster, config, proc, remote, retro  # noqa: E402
from vsbenchlib import state as st  # noqa: E402
from vsbenchlib.proc import PreconditionError, VsbenchError  # noqa: E402

CLUSTER = "t1"
Result = subprocess.CompletedProcess


def make_state(expires_in_s: int = 86400, **changes: Any) -> dict[str, Any]:
    now = proc.utcnow()

    def node(name: str, role: str, ip: str) -> dict[str, Any]:
        return {"name": name, "role": role, "index": 0, "instance_id": f"i-0{ip[-1]}", "instance_type": "r8g.large"}

    state = {
        "schema": st.SCHEMA,
        "cluster": CLUSTER,
        "owner": "first.last",
        "profile": "prof",
        "region": "us-east-1",
        "az": "us-east-1b",
        "expires_at": proc.iso(now + datetime.timedelta(seconds=expires_in_s)),
        "nodes": [node("scylla-0", "scylla", "1"), node("vs-0", "vs", "2"), node("client", "client", "3")],
        "deployed": {"scylla": {"version": "2026.2.0", "image": "img@sha256:1"}, "vector_store": None},
        "pins": {"scylla_image": "img@sha256:1"},
        "load": None,
        "jobs": {},
        "terminated_at": None,
    }
    return state | changes


def ok(stdout: str = "", stderr: str = "", code: int = 0) -> Result[str]:
    return Result(["ssh"], code, stdout, stderr)


class Case(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="vsbench-cmd-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        env = {"VSBENCH_HOME": str(self.tmp / "home"), "AWS_SHARED_CREDENTIALS_FILE": str(self.tmp / "credentials")}
        patcher = mock.patch.dict(os.environ, env)
        patcher.start()
        self.addCleanup(patcher.stop)
        os.environ.pop("VSBENCH_CLUSTER", None)
        self.addCleanup(setattr, proc, "VERBOSE", False)
        rev = mock.patch.object(retro, "skill_rev", return_value="abc1234")
        rev.start()
        self.addCleanup(rev.stop)
        self.state = make_state()
        st.save(CLUSTER, self.state)

    def run_cli(self, *argv: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(["-c", CLUSTER, *argv])
        return code, out.getvalue(), err.getvalue()

    def fake(self, name: str, **attrs: Any) -> mock.MagicMock:
        module = mock.MagicMock(name=name)
        for key, value in attrs.items():
            setattr(module, key, value)
        patcher = mock.patch.dict(sys.modules, {f"vsbenchlib.{name}": module})
        patcher.start()
        self.addCleanup(patcher.stop)
        return module

    def fake_remote(self, **attrs: Any) -> mock.MagicMock:
        defaults = {"is_dangerous": remote.is_dangerous, "JobNotFound": remote.JobNotFound}
        return self.fake("remote", **(defaults | attrs))


# --- exec / ssh -------------------------------------------------------------------------------
class ExecTest(Case):
    def test_dangerous_commands_are_refused(self) -> None:
        fake = self.fake_remote()
        for argv in (
            ["exec", "all", "--", "sudo shutdown -h now"],
            ["exec", "vs-0", "sudo", "poweroff"],
            ["exec", "scylla", "--", "sudo systemctl poweroff"],
            ["ssh", "vs-0", "--", "sudo", "halt"],
        ):
            with self.subTest(argv=argv):
                code, _out, err = self.run_cli(*argv)
                self.assertEqual(code, 5)
                self.assertIn("refusing to run: ", err)
                self.assertIn("powers the node off", err)
                self.assertIn("--i-mean-it", err)
        fake.run_many.assert_not_called()
        fake.interactive.assert_not_called()

    def test_ttl_tampering_is_refused_and_grep_patterns_are_not(self) -> None:
        fake = self.fake_remote(run_many=mock.Mock(return_value={"vs-0": ok()}))
        code, _out, err = self.run_cli("exec", "all", "--", "sudo systemctl stop vsbench-ttl.timer")
        self.assertEqual(code, 5)
        self.assertIn("disarms the node's TTL", err)
        self.assertIn("vsbench extend", err)
        self.assertEqual(self.run_cli("exec", "vs-0", "--", "journalctl -b | grep -iE 'error|shutdown'")[0], 0)
        self.assertEqual(
            [c.args[2] for c in fake.run_many.call_args_list], ["journalctl -b | grep -iE 'error|shutdown'"]
        )

    def test_i_mean_it_and_reboot_are_allowed(self) -> None:
        fake = self.fake_remote(run_many=mock.Mock(return_value={"vs-0": ok()}), interactive=mock.Mock(return_value=0))
        self.assertEqual(self.run_cli("exec", "vs-0", "--i-mean-it", "--", "sudo shutdown -h now")[0], 0)
        self.assertEqual(self.run_cli("exec", "vs-0", "--", "sudo reboot")[0], 0)
        self.assertEqual(self.run_cli("ssh", "vs-0", "--i-mean-it", "--", "sudo", "halt")[0], 0)
        commands = [c.args[2] for c in fake.run_many.call_args_list]
        self.assertEqual(commands, ["sudo shutdown -h now", "sudo reboot"])

    def test_targets_command_and_output(self) -> None:
        results = {"scylla-0": ok("a\n"), "vs-0": ok("b\n", "warn\n")}
        fake = self.fake_remote(run_many=mock.Mock(return_value=results))
        code, out, _err = self.run_cli("exec", "all", "--timeout", "1m", "--", "curl -s x | grep y", "&&", "true")
        self.assertEqual(code, 0)
        fake.run_many.assert_called_once_with(
            CLUSTER, ["scylla-0", "vs-0", "client"], "curl -s x | grep y && true", check=False, timeout=60
        )
        self.assertIn("=== scylla-0 (exit 0)\na", out)
        self.assertIn("=== vs-0 (exit 0)\nb\n--- stderr\nwarn", out)

    def test_single_node_output_and_failure(self) -> None:
        self.fake_remote(run_many=mock.Mock(return_value={"vs-0": ok("out\n", "err\n", code=3)}))
        code, out, err = self.run_cli("exec", "vs-0", "false")
        self.assertEqual((code, out), (1, "out\n"))
        self.assertIn("err\n", err)
        self.assertIn("the command failed on vs-0 (exit 3)", err)

    def test_tail_and_json(self) -> None:
        text = "".join(f"line {i}\n" for i in range(100))
        self.fake_remote(run_many=mock.Mock(return_value={"client": ok(text)}))
        code, out, _err = self.run_cli("exec", "client", "--tail", "3", "--json", "--", "seq 100")
        doc = json.loads(out)
        self.assertEqual(code, 0)
        self.assertEqual(doc["client"]["stdout"], "line 97\nline 98\nline 99")
        self.assertEqual((doc["client"]["exit"], doc["client"]["omitted_lines"]), (0, 97))
        _code, out, _err = self.run_cli("exec", "client", "--tail", "2", "--", "seq 100")
        self.assertTrue(out.startswith("[... 98 earlier lines omitted"))

    def test_tail_text_caps_bytes(self) -> None:
        text = "\n".join("x" * 1000 for _ in range(50))
        capped, omitted = cli_cluster.tail_text(text, 50)
        self.assertLessEqual(len(capped.encode()), cli_cluster.MAX_OUTPUT_BYTES)
        self.assertGreater(omitted, 0)
        self.assertEqual(cli_cluster.tail_text(text, 0), (text, 0))

    def test_unknown_target_and_missing_command(self) -> None:
        self.fake_remote()
        code, _out, err = self.run_cli("exec", "vs-9", "--", "true")
        self.assertEqual(code, 1)
        self.assertIn("unknown node 'vs-9'", err)
        self.assertEqual(self.run_cli("exec", "vs-0", "--")[0], 2)

    def test_ssh_passes_the_command_and_exit_code(self) -> None:
        fake = self.fake_remote(interactive=mock.Mock(return_value=7))
        self.assertEqual(self.run_cli("ssh", "vs-0", "--", "ls", "-la")[0], 7)
        fake.interactive.assert_called_once_with(CLUSTER, "vs-0", ["ls", "-la"])
        self.assertEqual(self.run_cli("ssh", "vs-0")[0], 7)
        self.assertEqual(fake.interactive.call_args.args[2], None)


# --- status -----------------------------------------------------------------------------------
class StatusTest(Case):
    def fake_deploy(self) -> mock.MagicMock:
        def monitoring(cluster: str) -> dict[str, Any]:
            raise PreconditionError("monitoring is not deployed", "run: vsbench deploy monitoring")

        return self.fake(
            "deploy",
            scylla_status=lambda c: {"ok": True, "un": 1, "nodes": [{"node": "scylla-0", "status": "UN"}]},
            vs_status=lambda c: {"vs-0": {"status": "SERVING", "indexes": [{"index": "i1", "status": "SERVING"}]}},
            monitoring_status=monitoring,
        )

    def test_status_json(self) -> None:
        self.fake_deploy()
        code, out, _err = self.run_cli("status", "--json")
        doc = json.loads(out)
        self.assertEqual(code, 0)
        cfg = st.paths(CLUSTER).ssh_config
        # -S none: not through the ControlMaster, whose forwards die with its ControlPersist.
        options = "-S none -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -N"
        forwards = "-L 13000:127.0.0.1:3000 -L 19090:127.0.0.1:9090"
        self.assertEqual(doc["tunnel"], f"ssh -F {cfg} {options} {forwards} client")
        self.assertEqual(doc["live"]["vector_store"]["vs-0"]["status"], "SERVING")
        self.assertEqual(
            doc["live"]["monitoring"], {"error": "monitoring is not deployed", "hint": "run: vsbench deploy monitoring"}
        )
        self.assertFalse(doc["overdue"])
        self.assertGreater(doc["expires_in_s"], 86000)
        self.assertEqual([n["name"] for n in doc["nodes"]], ["scylla-0", "vs-0", "client"])

    def test_status_text_and_overdue(self) -> None:
        self.fake_deploy()
        st.save(CLUSTER, make_state(expires_in_s=-3600))
        code, out, _err = self.run_cli("status")
        self.assertEqual(code, 0)
        self.assertIn("OVERDUE", out)
        self.assertIn("grafana/prometheus tunnel (blocks until Ctrl-C; run it in your own terminal): ssh -F", out)
        self.assertIn("- node=scylla-0 status=UN", out)
        doc = cli_cluster.status_doc(CLUSTER, make_state(expires_in_s=-600), {}, proc.utcnow())
        self.assertFalse(doc["overdue"])  # within the 15 min grace

    def test_status_without_deploy_module(self) -> None:
        with mock.patch.object(cli_cluster, "load_module", side_effect=VsbenchError("cannot be loaded")):
            live = cli_cluster._live_status(CLUSTER)
        self.assertEqual(live["scylla"], {"error": "cannot be loaded"})

    def test_status_refresh_uses_aws(self) -> None:
        self.fake_deploy()
        provision = self.fake("provision", refresh_status=mock.Mock(return_value=self.state))
        self.assertEqual(self.run_cli("status", "--refresh", "--profile", "p2")[0], 0)
        cluster, aws = provision.refresh_status.call_args.args
        self.assertEqual((cluster, aws.profile, aws.region), (CLUSTER, "p2", "us-east-1"))


# --- lifecycle --------------------------------------------------------------------------------
class LifecycleTest(Case):
    def test_up_builds_options(self) -> None:
        from vsbenchlib import provision as real

        plan = {"dry_run": True, "nodes": [{}, {}, {}], "ttl_seconds": 3600, "cost_per_hour": 2.1}
        fake = self.fake("provision", UpOptions=real.UpOptions, up=mock.Mock(return_value=plan | {"az_candidates": []}))
        code, out, _err = self.run_cli("up", "--dry-run", "--vs-nodes", "2", "--ttl", "1h", "--az", "us-east-1a")
        self.assertEqual(code, 0)
        opts = fake.up.call_args.args[2]
        self.assertEqual(
            (opts.vs_nodes, opts.ttl, opts.az, opts.dry_run, opts.scylla_type),
            (2, "1h", "us-east-1a", True, "i8g.2xlarge"),
        )
        self.assertIn("dry run, nothing created: 3 nodes", out)
        self.assertIn("$2.10/h", out)
        self.assertIn("AWS: profile prof, account ?, region us-east-1", out)  # the cluster state's profile
        self.assertNotIn("budget check incomplete", out)
        resume = {"would_adopt": ["i-1"], "kept_expires_at": "2026-10-07T00:00:00Z", "resume_note": "keeps it"}
        incomplete = {"cost_per_hour": None, "unpriced_types": ["x9.huge"], "other_clusters_cost_per_hour": 3.5}
        fake.up.return_value = plan | {"az_candidates": []} | resume | incomplete
        code, out, _err = self.run_cli("up", "--dry-run")
        for text in (
            "cost: ?/h",
            "no price for x9.huge: budget check incomplete",
            "other running clusters here: $3.50",
        ):
            self.assertIn(text, out)
        self.assertIn("would adopt the instances of the interrupted up: i-1", out)
        self.assertIn("resume keeps the expiry 2026-10-07T00:00:00Z\n  keeps it", out)

    def test_up_marks_an_explicit_profile(self) -> None:
        @dataclasses.dataclass(frozen=True)
        class UpOptions:
            ttl: str = "24h"
            dry_run: bool = False
            explicit_profile: bool = False

        plan = {"dry_run": True, "nodes": [], "ttl_seconds": 3600, "az_candidates": [], "account": "123"}
        fake = self.fake("provision", UpOptions=UpOptions, up=mock.Mock(return_value=plan | {"profile": "p2"}))
        code, out, _err = self.run_cli("up", "--dry-run", "--profile", "p2")
        self.assertEqual((code, fake.up.call_args.args[2]), (0, UpOptions(dry_run=True, explicit_profile=True)))
        self.assertIn("AWS: profile p2, account 123, region us-east-1", out)
        with mock.patch.dict(os.environ, {"AWS_PROFILE": "from-env"}):
            st.paths(CLUSTER).state_file.unlink()
            self.assertEqual(self.run_cli("up", "--dry-run")[0], 0)
        self.assertEqual(fake.up.call_args.args[1].profile, "from-env")
        self.assertFalse(fake.up.call_args.args[2].explicit_profile)  # $AWS_PROFILE is not explicit

    def test_extend_without_credentials_skips_aws(self) -> None:
        from vsbenchlib import provision as real

        fake = self.fake("provision", parse_until=real.parse_until, extend=mock.Mock(return_value=self.state))
        moment = (proc.utcnow() - datetime.timedelta(hours=1)).isoformat()
        (self.tmp / "credentials").write_text(f"[prof]\nx_security_token_expires = {moment}\n")
        self.assertEqual(self.run_cli("extend", "--until", "2030-01-01T00:00:00Z")[0], 0)
        args = fake.extend.call_args.args
        self.assertEqual((args[1], args[2], args[3].year, args[4]), (None, None, 2030, False))
        self.assertEqual(self.run_cli("extend", "--ttl", "2h", "--shorten")[0], 0)
        self.assertEqual(fake.extend.call_args.args[2:], (7200, None, True))

    def test_list_and_down(self) -> None:
        rows = [{"cluster": "a", "owner": "x", "nodes": 3, "states": {"running": 3}, "types": ["r8g.large"]}]
        rows[0] |= {"cost_per_hour": 1.5, "expires_in_s": -7200, "overdue": True, "tag_stale": False}
        rows.append({"cluster": "b", "owner": "x", "nodes": 1, "expires_in_s": 3600, "tag_stale": True})
        down = {"terminated": ["i-1"], "security_groups": ["sg-1"], "key_pair": "k", "purged": False}
        fake = self.fake("provision", list_clusters=mock.Mock(return_value=rows), down=mock.Mock(return_value=down))
        with mock.patch("vsbenchlib.awsapi.identity", return_value=mock.Mock(owner="x")):
            code, out, _err = self.run_cli("list")
        self.assertEqual(code, 0)
        self.assertIn("running:3", out)
        self.assertIn("regions scanned: us-east-1\n", out)
        self.assertRegex(out, r"expires_in\s+tag stale\s+OVERDUE\n")
        self.assertRegex(out, r"\na .*-2h00m\s+no\s+yes\nb .*1h00m\s+yes\s+-\n")  # b: the local expiry is newer
        self.assertEqual(fake.list_clusters.call_args.args[1], "x")
        self.assertEqual(self.run_cli("list", "--all-owners", "--json")[0], 0)
        self.assertIsNone(fake.list_clusters.call_args.args[1])
        code, out, _err = self.run_cli("down", "--yes")
        self.assertEqual(fake.down.call_args.args[2:], (True, False))
        self.assertIn("terminated i-1", out)

    def test_list_scans_every_region_with_local_state(self) -> None:
        st.save("w2", make_state(cluster="w2", region="us-west-2"))
        st.save("old", make_state(cluster="old", region="eu-west-1", terminated_at="2026-01-01T00:00:00Z"))
        (st.home() / "clusters" / "broken").mkdir(parents=True)
        (st.home() / "clusters" / "broken" / "state.json").write_text("{not json")

        def list_clusters(aws: Any, owner: str | None) -> list[dict[str, Any]]:
            if aws.region == "eu-west-1":
                raise VsbenchError("AuthFailure: region disabled")
            row = {"cluster": f"c-{aws.region}", "owner": owner, "nodes": 1, "expires_in_s": -3600, "overdue": True}
            return [row] if aws.region == "us-west-2" else []

        fake = self.fake("provision", list_clusters=mock.Mock(side_effect=list_clusters))
        with mock.patch("vsbenchlib.awsapi.identity", return_value=mock.Mock(owner="x")):
            code, out, err = self.run_cli("list")
            self.assertEqual(code, 0)
            calls = [c.args[0] for c in fake.list_clusters.call_args_list]
            self.assertEqual(sorted(aws.region for aws in calls), ["eu-west-1", "us-east-1", "us-west-2"])
            self.assertEqual({aws.profile for aws in calls}, {"prof"})
            self.assertIn("regions scanned: us-east-1, us-west-2\n", out)
            self.assertRegex(out, r"c-us-west-2\s+us-west-2\s+x\s+1")
            self.assertIn("cannot list region eu-west-1: AuthFailure", err)
            _code, out, _err = self.run_cli("list", "--json")
            self.assertEqual([(r["cluster"], r["region"]) for r in json.loads(out)], [("c-us-west-2", "us-west-2")])
            fake.list_clusters.side_effect = VsbenchError("ExpiredToken")  # the current region must work
            self.assertEqual(self.run_cli("list")[0], 1)

    def test_doctor_exit_codes(self) -> None:
        checks = [{"check": "aws", "status": "ok", "detail": "/usr/bin/aws", "hint": None}]
        fake = self.fake("provision", doctor=mock.Mock(return_value={"ok": True, "checks": checks}))
        self.assertEqual(self.run_cli("doctor")[0], 0)
        fake.doctor.return_value = {"checks": checks + [{"check": "identity", "status": "fail", "detail": "x"}]}
        self.assertEqual(self.run_cli("doctor", "--json")[0], 3)
        fake.doctor.return_value = {"checks": checks + [{"check": "host", "status": "fail", "detail": "x"}]}
        self.assertEqual(self.run_cli("doctor")[0], 1)

    def test_login_prints_only_the_url_and_maps_auth_errors(self) -> None:
        from vsbenchlib.awsapi import AuthError

        def fake_login(username: str | None, timeout_s: int, on_url: Any = None) -> dict[str, Any]:
            on_url("https://scylladb.okta.com/activate?user_code=ABCD1234")
            return {"username": username, "url": "u", "profile": "p", "expires_at": "2026-10-07T01:00:00Z"}

        fake = self.fake("login", login=mock.Mock(side_effect=fake_login))
        code, out, err = self.run_cli("login", "--username", "a@scylladb.com", "--timeout", "5m")
        self.assertEqual((code, out), (0, "https://scylladb.okta.com/activate?user_code=ABCD1234\n"))
        self.assertIn("expire at 2026-10-07T01:00:00Z", err)
        self.assertEqual(fake.login.call_args.args[:2], ("a@scylladb.com", 300))
        fake.login.side_effect = AuthError("not approved", None, "again")
        self.assertEqual(self.run_cli("login")[0], proc.EXIT_AUTH)

    def test_build_and_builds(self) -> None:
        from vsbenchlib import build as real

        fake = self.fake(
            "build", parse_source=real.parse_source, build=mock.Mock(return_value={"build_id": "1.0.0-abcd"})
        )
        code, out, _err = self.run_cli("build", "--source", "git:main", "--jobs", "2")
        self.assertEqual((code, out), (0, "1.0.0-abcd\n"))
        self.assertEqual((str(fake.build.call_args.args[0]), fake.build.call_args.kwargs), ("git:main", {"jobs": 2}))
        self.assertEqual(self.run_cli("build", "--source", "release:latest")[0], 1)
        fake.cached_builds.return_value = [{"build_id": "b1", "kind": "local"}]
        deploy = self.fake("deploy", builds_on_nodes=mock.Mock(return_value={"vector_store": {}}))
        code, out, _err = self.run_cli("builds", "--nodes", "--json")
        self.assertEqual(
            json.loads(out), {"local": [{"build_id": "b1", "kind": "local"}], "nodes": {"vector_store": {}}}
        )
        deploy.builds_on_nodes.assert_called_once_with(CLUSTER)


# --- deploy / logs / prom / push-pull ------------------------------------------------------------
class DeployTest(Case):
    def test_dispatch(self) -> None:
        deployed = make_state(
            deployed={"vector_store": {"version": "1.2.0", "build_id": "1.2.0-abc", "source": "local"}}
        )
        fake = self.fake("deploy", **{f: mock.Mock(return_value=deployed) for f in ("deploy_vs", "deploy_all")})
        code, out, _err = self.run_cli("deploy", "vs", "--source", "local", "--env", "A=1", "--unset", "B", "--refresh")
        self.assertEqual(code, 0)
        fake.deploy_vs.assert_called_once_with(
            CLUSTER, "local", {"A": "1"}, ["B"], True, config.DEFAULT_FOREGROUND_SECONDS
        )
        self.assertEqual(out, "vector_store: version=1.2.0 build_id=1.2.0-abc source=local\n")
        self.run_cli("deploy", "scylla", "--wipe")
        fake.deploy_scylla.assert_called_once_with(CLUSTER, None, True, False)
        self.run_cli("deploy", "bench", "--source", "git:x")
        fake.deploy_bench.assert_called_once_with(CLUSTER, "git:x", False)
        code, out, _err = self.run_cli("deploy", "all", "--force")
        fake.deploy_all.assert_called_once_with(CLUSTER, True, config.DEFAULT_FOREGROUND_SECONDS)
        self.run_cli("deploy", "all", "--timeout", "20m")
        self.assertEqual(fake.deploy_all.call_args.args, (CLUSTER, False, 1200))  # the budget of the whole call
        self.assertIn("monitoring: not deployed", out)
        self.run_cli("wait-serving", "--timeout", "1m")
        fake.wait_serving.assert_called_once_with(CLUSTER, 60)


class NodeToolsTest(Case):
    def test_logs_commands(self) -> None:
        fake = self.fake_remote(run=mock.Mock(return_value=ok("log line\n")))
        code, out, _err = self.run_cli("logs", "vs-0", "-n", "5", "--since", "10m")
        self.assertEqual((code, out), (0, "log line\n"))
        command = fake.run.call_args.args[2]
        self.assertEqual(command, "sudo journalctl -u vector-store --no-pager -o short-iso-precise -n 5 --since=-600s")
        self.run_cli("logs", "scylla-0")
        self.assertEqual(fake.run.call_args.args[2], "sudo docker logs --timestamps --tail 100 scylla 2>&1")
        self.run_cli("logs", "client", "--since", "1h")
        self.assertIn(
            'for c in aprom agraf; do echo "=== $c"; sudo docker logs --timestamps --tail 100 --since=3600s',
            fake.run.call_args.args[2],
        )
        self.assertEqual(cli_cluster.logs_command("userdata", 7, None), "sudo tail -n 7 /var/log/vsbench-userdata.log")
        for service in cli.LOG_SERVICES:
            with self.subTest(service=service):
                check = subprocess.run(["bash", "-n", "-c", cli_cluster.logs_command(service, 10, 60)])
                self.assertEqual(check.returncode, 0)

    def test_push_and_pull(self) -> None:
        fake = self.fake_remote()
        local = self.tmp / "data.bin"
        local.write_text("x")
        local.chmod(0o640)
        self.assertEqual(self.run_cli("push", str(local), "vs-0:/tmp/")[0], 0)
        fake.upload.assert_called_once_with(CLUSTER, "vs-0", local, "/tmp/data.bin", mode="0640")
        self.assertEqual(self.run_cli("pull", "vs-0:/var/log/x.log", str(self.tmp))[0], 0)
        fake.download.assert_called_once_with(CLUSTER, "vs-0", "/var/log/x.log", self.tmp / "x.log")
        self.assertEqual(self.run_cli("push", str(local), "nocolon")[0], 2)
        self.assertEqual(self.run_cli("pull", "vs-7:/x", str(self.tmp))[0], 1)

    def test_prom(self) -> None:
        fake = self.fake("prom", query=mock.Mock(return_value=[{"metric": {}, "value": [1, "2"]}]))
        fake.format_vector.return_value = "{} => 2"
        code, out, _err = self.run_cli("prom", "query", "up", "--time", "-5m")
        self.assertEqual((code, out), (0, "{} => 2\n"))
        fake.query.assert_called_once_with(CLUSTER, "up", "-5m")
        self.run_cli("prom", "range", "up", "--step", "30s")
        fake.query_range.assert_called_once_with(CLUSTER, "up", "now-15m", "now", "30s")
        self.run_cli("prom", "range", "up", "--start", "-1h", "--end", "-5m")  # Python 3.10-3.13 too
        self.assertEqual(fake.query_range.call_args.args[2:4], ("-1h", "-5m"))
        fake.api.return_value = {"activeTargets": []}
        code, out, _err = self.run_cli("prom", "api", "targets", "state=active")
        self.assertEqual(json.loads(out), {"activeTargets": []})
        fake.api.assert_called_once_with(CLUSTER, "targets", [("state", "active")])


# --- bench / datasets / jobs ---------------------------------------------------------------------
class BenchTest(Case):
    def test_bench_options_against_bench_module(self) -> None:
        try:
            from vsbenchlib import bench
        except Exception as err:  # noqa: BLE001 -- bench.py is developed in parallel
            self.skipTest(f"vsbenchlib.bench does not import: {err}")
        for words in (
            ["load", "cohere-1m"],
            ["index"],
            ["search", "cql", "--bucket", "1"],
            ["validate", "http"],
            ["ab", "--a", "x", "--b", "y", "--bucket", "0", "--timeout", "5m"],
        ):
            with self.subTest(command=words[0]):
                extra = words[0] in cli_bench.EXTRA_ARGS_COMMANDS
                args = cli.parse_args(["bench", *words, "--", "--x"] if extra else ["bench", *words])
                values = {name: getattr(args, name) for name in cli_bench.OPTION_FIELDS[words[0]]}
                values |= {"extra_args": args.extra} if extra else {}
                with mock.patch.object(proc, "warn") as warn:
                    opts = cli_bench.bench_options(bench, words[0], values)
                warn.assert_not_called()
                self.assertTrue(dataclasses.is_dataclass(opts))
                if extra:
                    self.assertEqual(opts.extra_args, ("--x",))

    def test_ab_passes_bucket_timeout_and_extra_args(self) -> None:
        try:
            from vsbenchlib import bench as real
        except Exception as err:  # noqa: BLE001 -- bench.py is developed in parallel
            self.skipTest(f"vsbenchlib.bench does not import: {err}")
        fake = self.fake("bench", AbOptions=real.AbOptions, ab=mock.Mock(return_value={"comparison_id": "c1"}))
        argv = ["bench", "ab", "--a", "release:latest", "--b", "local", "--bucket", "0", "--timeout", "5m"]
        self.assertEqual(self.run_cli(*argv, "--", "--filter", "x")[0], 0)
        opts = fake.ab.call_args.args[1]
        self.assertEqual((opts.bucket, opts.timeout_s, opts.extra_args), (0, 300, ("--filter", "x")))
        self.assertEqual((opts.a, opts.b, opts.repeat), ("release:latest", "local", 2))
        self.assertEqual(self.run_cli("bench", "ab", "--a", "x", "--b", "y")[0], 0)
        opts = fake.ab.call_args.args[1]
        default = real.AbOptions(a="x", b="y")
        self.assertEqual((opts.bucket, opts.timeout_s, opts.extra_args), (None, default.timeout_s, ()))

    def test_bench_options_unset_flags_keep_the_dataclass_default(self) -> None:
        @dataclasses.dataclass(frozen=True)
        class AbOptions:
            a: str
            timeout_s: int = 123
            label: str | None = "keep"

        bench = mock.Mock(AbOptions=AbOptions)
        opts = cli_bench.bench_options(bench, "ab", {"a": "x", "timeout_s": None, "label": None})
        self.assertEqual(opts, AbOptions("x", 123, "keep"))
        self.assertEqual(cli_bench.bench_options(bench, "ab", {"a": None}), AbOptions(None))  # required: passed

    def test_bench_options_checks(self) -> None:
        @dataclasses.dataclass(frozen=True)
        class SearchOptions:
            kind: str
            concurrency: tuple[int, ...] = (64,)

        bench = mock.Mock(SearchOptions=SearchOptions)
        with mock.patch.object(proc, "warn") as warn:
            opts = cli_bench.bench_options(bench, "search", {"kind": "cql", "concurrency": [1, 2], "label": "x"})
        self.assertEqual(opts, SearchOptions("cql", (1, 2)))
        self.assertIn("no field 'label'", warn.call_args.args[0])
        with self.assertRaises(VsbenchError):
            cli_bench.bench_options(bench, "search", {"concurrency": [1]})
        with self.assertRaises(VsbenchError):
            cli_bench.bench_options(mock.Mock(SearchOptions=None), "search", {})

    def test_search_dispatch_and_summary(self) -> None:
        @dataclasses.dataclass(frozen=True)
        class SearchOptions:
            kind: str
            concurrency: tuple[int, ...]
            extra_args: tuple[str, ...] = ()
            label: str | None = None
            limit: int = 10
            duration_s: int = 60
            warmup_s: int = 30
            repeat: int = 1
            bucket: int | None = None
            timeout_s: int = 540

        fake = self.fake("bench", SearchOptions=SearchOptions, search=mock.Mock(return_value=[{"run_id": "r1"}]))
        fake.format_summary.return_value = "SUMMARY"
        code, out, _err = self.run_cli("bench", "search", "cql", "--concurrency", "8,16", "--label", "L", "--", "--z")
        self.assertEqual((code, out), (0, "SUMMARY\n"))
        cluster, opts = fake.search.call_args.args
        self.assertEqual((cluster, opts.concurrency, opts.extra_args, opts.label), (CLUSTER, (8, 16), ("--z",), "L"))
        fake.ab.return_value = {"comparison_id": "c1", "records": [{"run_id": "r2"}]}
        fake.AbOptions = dataclasses.make_dataclass("AbOptions", ["a", "b", ("kind", str, "cql")], frozen=True)
        code, out, _err = self.run_cli("bench", "ab", "--a", "x", "--b", "y")
        self.assertEqual(out, "comparison_id: c1\nSUMMARY\n")

    def test_raw_and_rerun(self) -> None:
        fake = self.fake("bench", raw=mock.Mock(return_value=0), rerun=mock.Mock(return_value=[]))
        self.assertEqual(self.run_cli("bench", "raw", "--timeout", "2m", "--", "search-cql", "--help")[0], 0)
        fake.raw.assert_called_once_with(CLUSTER, ["search-cql", "--help"], 120)
        fake.raw.return_value = 101
        code, _out, err = self.run_cli("bench", "raw", "--", "x")
        self.assertEqual(code, 1)
        self.assertIn("exited with 101", err)
        code, out, _err = self.run_cli("bench", "rerun", "R1")
        self.assertEqual((code, out), (0, "(no results)\n"))

    def test_datasets(self) -> None:
        catalog = {"cohere-1m": {"dir": "cohere_medium_1m", "rows": 1000000, "dim": 768, "default": True}}
        fake = self.fake("bench", catalog=mock.Mock(return_value=catalog), dataset_rows=None, dataset_status=None)
        code, out, _err = self.run_cli("dataset", "list")
        self.assertIn("cohere-1m", out)
        self.assertIn("default", out)
        self.fake_remote(run=mock.Mock(return_value=ok("cohere_medium_1m 1 3500000000\nother 0 10\njunk\n")))
        code, out, _err = self.run_cli("dataset", "status")
        self.assertEqual(code, 0)
        self.assertIn("cohere_medium_1m  cohere-1m  yes", out)
        fake.dataset_status = mock.Mock(
            return_value={"dir": "/d", "free_gb": 9.5, "datasets": [{"dataset": "x", "complete": True}]}
        )
        code, out, _err = self.run_cli("dataset", "status")
        self.assertIn("/d on the client, 9.5 GB free", out)
        self.assertEqual(subprocess.run(["bash", "-n", "-c", cli_bench.dataset_status_command()]).returncode, 0)


class JobTest(Case):
    def setUp(self) -> None:
        super().setUp()
        jobs = {"J1": {"kind": "search", "node": "client", "started_at": "2026-10-06T10:00:00Z", "status": "running"}}
        jobs["J0"] = {"kind": "load", "node": "client", "started_at": "2026-10-06T09:00:00Z", "status": "finalized"}
        st.save(CLUSTER, make_state(jobs=jobs))

    def test_list(self) -> None:
        progress = remote.JobProgress(new_text="", offset=0, exit_code=None, ended_at=None, running=True)
        fake = self.fake_remote(job_poll=mock.Mock(return_value=progress))
        code, out, _err = self.run_cli("job", "list", "--json")
        rows = json.loads(out)
        self.assertEqual([(r["id"], r.get("live")) for r in rows], [("J0", None), ("J1", "running")])
        self.assertEqual(fake.job_poll.call_args.args[:3], (CLUSTER, "client", "J1"))
        fake.job_poll.side_effect = remote.JobNotFound("gone")
        self.assertIn("not found", self.run_cli("job", "list")[1])

    def test_wait_known_and_unknown_jobs(self) -> None:
        bench = self.fake("bench", job_wait=mock.Mock(return_value=[]))
        fake = self.fake_remote(job_follow=mock.Mock(return_value=0))
        self.assertEqual(self.run_cli("job", "wait", "J1", "--timeout", "1m")[0], 0)
        bench.job_wait.assert_called_once_with(CLUSTER, "J1", 60)
        self.assertEqual(self.run_cli("job", "wait", "X9")[0], 0)
        self.assertEqual(
            fake.job_follow.call_args.args[:4], (CLUSTER, "client", "X9", config.DEFAULT_FOREGROUND_SECONDS)
        )
        fake.job_follow.return_value = 2
        self.assertEqual(self.run_cli("job", "wait", "X9")[0], 1)
        bench.job_wait.side_effect = proc.StillRunning("still running", "vsbench job wait J1")
        self.assertEqual(self.run_cli("job", "wait", "J1")[0], 75)

    def test_logs_status_cancel(self) -> None:
        progress = remote.JobProgress(new_text="", offset=0, exit_code=0, ended_at="t", running=False)
        fake = self.fake_remote(job_log=mock.Mock(return_value="a\nb\n"), job_poll=mock.Mock(return_value=progress))
        code, out, _err = self.run_cli("job", "logs", "J1", "--tail", "0")
        self.assertEqual((code, out), (0, "a\nb\n"))
        self.assertEqual(fake.job_log.call_args.args, (CLUSTER, "client", "J1", None))
        code, out, _err = self.run_cli("job", "status", "J1")
        self.assertIn("job J1: search on client", out)
        self.assertIn("now exited 0", out)
        self.assertEqual(self.run_cli("job", "cancel", "J1")[0], 0)
        fake.job_cancel.assert_called_once_with(CLUSTER, "client", "J1")


# --- results ------------------------------------------------------------------------------------
class ResultsTest(Case):
    def setUp(self) -> None:
        super().setUp()
        records = [
            {"run_id": "r1", "kind": "search-cql", "series_id": "s1", "comparison_id": "c1"},
            {"run_id": "r2", "kind": "search-http", "series_id": "s1", "comparison_id": "c1"},
            {"run_id": "r3", "kind": "load", "series_id": "s2"},
        ]
        target = st.paths(CLUSTER).results_file
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("".join(json.dumps(r) + "\n" for r in records))

    def test_filters(self) -> None:
        def ids(*argv: str) -> list[str]:
            return [r["run_id"] for r in json.loads(self.run_cli("results", "--json", *argv)[1])]

        self.assertEqual(ids(), ["r1", "r2", "r3"])
        self.assertEqual(ids("--kind", "search"), ["r1", "r2"])
        self.assertEqual(ids("--series", "s2"), ["r3"])
        self.assertEqual(ids("--comparison", "c1", "--last", "1"), ["r2"])
        code, out, _err = self.run_cli("results", "--format", "md")
        self.assertEqual(code, 0)
        self.assertTrue(out.startswith("| run_id |"))

    def test_select_and_compare(self) -> None:
        records = [json.loads(line) for line in st.paths(CLUSTER).results_file.read_text().splitlines()]
        self.assertEqual([r["run_id"] for r in cli_bench.select_records(records, ["c1", "r3"])], ["r1", "r2", "r3"])
        with self.assertRaises(VsbenchError):
            cli_bench.select_records(records, ["r1", "nope"])
        code, _out, err = self.run_cli("results", "compare", "nope")
        self.assertEqual(code, 1)
        self.assertIn("no results with the id nope", err)
        code, _out, err = self.run_cli("results", "compare", "s1")  # different kinds are fine
        self.assertEqual(code, 0)

    def test_compare_honours_options_given_before_it(self) -> None:
        results = self.fake("results", load_records=mock.Mock(return_value=[{"run_id": "r1"}]))
        results.compare.return_value = {"groups": [], "warnings": []}
        display = self.fake("results_format", COMPARE_COLUMNS=["a"])
        display.format_markdown.return_value = "| md |"
        code, out, _err = self.run_cli("results", "--json", "compare", "r1")
        self.assertEqual((code, json.loads(out)), (0, {"groups": [], "warnings": []}))
        code, out, _err = self.run_cli("results", "--format", "md", "compare", "r1")
        self.assertEqual((code, out), (0, "| md |\n"))

    def test_compare_rejects_list_filters(self) -> None:
        for argv in (["--kind", "search"], ["--last", "5"], ["--series", "s1"], ["--comparison", "c1"]):
            with self.subTest(argv=argv):
                code, _out, err = self.run_cli("results", *argv, "compare", "r1")
                self.assertEqual(code, 2)
                self.assertIn(f"{argv[0]} cannot be used with `results compare`", err)


# --- collect ---------------------------------------------------------------------------------
class CollectTest(Case):
    def test_collect(self) -> None:
        st.save(CLUSTER, make_state(deployed={"monitoring": {"version": "4.16.1"}}))
        prom = self.fake("prom", api=mock.Mock(return_value={"name": "20261006T120000Z-1a2b"}))

        def download(cluster: str, node: str, path: str, local: Path) -> None:
            local.write_text(f"{node}:{path}")

        failing = {"vs-0": ok(stderr="tar: boom", code=2)}
        many = {n: failing.get(n, ok()) for n in ("scylla-0", "vs-0", "client")}
        fake = self.fake_remote(run=mock.Mock(return_value=ok()), run_many=mock.Mock(return_value=many))
        fake.download.side_effect = download
        code, out, err = self.run_cli("collect")
        self.assertEqual(code, 1)
        prom.api.assert_called_once_with(CLUSTER, "admin/tsdb/snapshot", post=True, timeout=300)
        dest = next((st.paths(CLUSTER).results_dir / "artifacts").iterdir())
        saved = sorted(p.name for p in dest.iterdir())
        self.assertEqual(
            saved, ["client-logs.tar.gz", "prometheus-snapshot.tar.gz", "scylla-0-logs.tar.gz", "state.json"]
        )
        self.assertIn("vs-0 logs: exit 2: tar: boom", err)
        self.assertIn("prometheus-snapshot.tar.gz", out)
        remote_commands = [c.args[2] for c in fake.run.call_args_list]
        self.assertIn("snapshots/20261006T120000Z-1a2b", remote_commands[0])
        self.assertEqual(sum(cmd.startswith("rm -f -- /tmp/vsbench-collect-") for cmd in remote_commands), 3)

    def test_collect_without_monitoring_and_shell_syntax(self) -> None:
        many = {n: ok() for n in ("scylla-0", "vs-0", "client")}
        fake = self.fake_remote(run=mock.Mock(return_value=ok()), run_many=mock.Mock(return_value=many))
        prom = self.fake("prom")
        code, _out, err = self.run_cli("collect")
        self.assertEqual(code, 0)
        self.assertIn("no Prometheus snapshot", err)
        prom.api.assert_not_called()
        self.assertEqual(fake.download.call_count, 3)
        for command in (cli_bench.snapshot_command("snap-1", "/tmp/a.tgz"), cli_bench.node_logs_command("/tmp/b.tgz")):
            self.assertEqual(subprocess.run(["bash", "-n", "-c", command]).returncode, 0)

    def test_bad_snapshot_answer(self) -> None:
        st.save(CLUSTER, make_state(deployed={"monitoring": {"version": "4.16.1"}}))
        self.fake("prom", api=mock.Mock(return_value={"name": "../../etc"}))
        self.fake_remote(run_many=mock.Mock(return_value={}))
        code, _out, err = self.run_cli("collect")
        self.assertEqual(code, 1)
        self.assertIn("unexpected snapshot answer", err)


if __name__ == "__main__":
    unittest.main()


class ProvisionContractTest(unittest.TestCase):
    """The shapes provision/teardown return (see their tests) render as the CLI promises."""

    def test_resumed_dry_run_plan(self) -> None:
        plan = {
            "dry_run": True,
            "nodes": [{"name": "scylla-0"}, {"name": "vs-0"}, {"name": "client"}],
            "profile": "test-profile",
            "account": config.EXPECTED_ACCOUNT,
            "region": "us-east-1",
            "ttl_seconds": 72 * 3600,
            "cost_per_hour": 2.1,
            "unpriced_types": [],
            "other_clusters_cost_per_hour": 0,
            "az_candidates": [{"az": "us-east-1a"}],
            "would_adopt": ["i-0000"],
            "kept_expires_at": "2026-10-07T14:08:35Z",
            "resume_note": "the resumed up keeps the expiry ...; after up run: vsbench -c c1 extend --ttl 72h",
        }
        text = "\n".join(cli_cluster.dry_run_lines(cli.Context("c1", "ctx-profile", "eu-west-1", ()), plan))
        self.assertIn(f"AWS: profile test-profile, account {config.EXPECTED_ACCOUNT}, region us-east-1", text)
        self.assertIn("would adopt the instances of the interrupted up: i-0000", text)
        self.assertIn("resume keeps the expiry 2026-10-07T14:08:35Z", text)
        self.assertIn("extend --ttl 72h", text)
        self.assertRegex(text, r"cost: \$\d+\.\d\d/h")

    def test_list_shows_tag_stale_not_overdue(self) -> None:
        row = {
            "cluster": "c1",
            "owner": "szymon.wasik",
            "nodes": 3,
            "states": {"running": 3},
            "types": ["i8g.2xlarge", "r8g.2xlarge", "r8g.4xlarge"],
            "instance_ids": ["i-0000", "i-0001", "i-0002"],
            "cost_per_hour": 2.1,
            "expires_at": "2026-10-07T20:08:35Z",
            "expires_in_s": 107999,
            "overdue": False,
            "tag_stale": True,
        }
        listed = cli.table([cli_cluster._list_row(row)], cli_cluster.LIST_COLUMNS).splitlines()
        self.assertRegex(listed[0], r"tag stale\s+OVERDUE$")
        self.assertRegex(listed[1], r"\s(29|30)h\d\dm\s+yes\s+no$")
