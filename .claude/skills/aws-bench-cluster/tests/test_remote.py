# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.remote and node/job-run.sh.

No network and no ssh: `proc.run` is replaced by a fake that records calls and
either returns canned results or runs the remote shell string with local bash.
"""

from __future__ import annotations

import base64
import os
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

SKILL_DIR = Path(__file__).resolve().parent.parent
if str(SKILL_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_DIR))

from vsbenchlib import config, proc, remote  # noqa: E402
from vsbenchlib import state as st  # noqa: E402
from vsbenchlib.proc import StillRunning, VsbenchError  # noqa: E402

JOB_RUN = SKILL_DIR / "node" / "job-run.sh"
CLUSTER = "t1"
Result = subprocess.CompletedProcess


def make_state(cluster: str = CLUSTER) -> dict[str, Any]:
    def node(name: str, role: str, ip: str, iid: str) -> dict[str, Any]:
        return {"name": name, "role": role, "index": 0, "instance_id": iid, "public_ip": ip, "private_ip": "10.0.0.9"}

    return {
        "schema": st.SCHEMA,
        "cluster": cluster,
        "aws": {"operator_cidr": "1.2.3.4/32"},
        "nodes": [
            node("client", "client", "3.0.0.3", "i-0ccc"),
            node("scylla-0", "scylla", "3.0.0.1", "i-0aaa"),
            node("vs-0", "vs", "3.0.0.2", "i-0bbb"),
        ],
    }


def ok(stdout: str = "", stderr: str = "", code: int = 0) -> Result[str]:
    return Result(["ssh"], code, stdout, stderr)


def local_bash(remote_cmd: str, input: str | None = None, env: dict[str, str] | None = None) -> Result[str]:
    """What the node's login shell would do with the remote command string."""
    return subprocess.run(["bash", "-c", remote_cmd], input=input, text=True, capture_output=True, env=env)


class RemoteTestCase(unittest.TestCase):
    """Isolated VSBENCH_HOME with a 3-node cluster and a fake proc.run."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="vsbench-remote-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)
        env = mock.patch.dict(os.environ, {"VSBENCH_HOME": str(self.tmp / "home"), "XDG_RUNTIME_DIR": "/usr"})
        env.start()
        self.addCleanup(env.stop)
        self.state = make_state()
        st.save(CLUSTER, self.state)
        remote.write_ssh_config(CLUSTER, self.state)
        remote._ENSURED.clear()
        remote.current_public_ip.cache_clear()
        self.addCleanup(remote.current_public_ip.cache_clear)
        self.calls: list[tuple[list[str], dict[str, Any]]] = []
        self.responses: list[Any] = []
        patcher = mock.patch.object(proc, "run", side_effect=self._fake_run)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _fake_run(self, cmd: list[Any], **kw: Any) -> Result[str]:
        argv = [str(c) for c in cmd]
        self.calls.append((argv, kw))
        if not self.responses:
            return ok()
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        if callable(response):
            return response(argv, kw)
        return response

    def remote_cmds(self) -> list[str]:
        return [argv[-1] for argv, _ in self.calls if argv[0] == "ssh"]

    @staticmethod
    def execute_locally(env: dict[str, str] | None = None) -> Callable[[list[str], dict[str, Any]], Result[str]]:
        return lambda argv, kw: local_bash(argv[-1], kw.get("input"), env)


class SshConfigTest(RemoteTestCase):
    def test_host_blocks_use_instance_id_alias_and_literal_control_path(self) -> None:
        text = st.paths(CLUSTER).ssh_config.read_text()
        self.assertEqual(remote.config_hosts(text), ["scylla-0", "vs-0", "client"])
        block = text.split("Host vs-0\n", 1)[1].split("\n\n", 1)[0]
        self.assertIn("  HostName 3.0.0.2\n", block)
        self.assertIn("  HostKeyAlias i-0bbb\n", block)
        self.assertIn("  ControlPath /usr/vsb-%C\n", block)
        self.assertIn(f"  IdentityFile {st.paths(CLUSTER).ssh_key}\n", block)
        self.assertIn(f"  UserKnownHostsFile {st.paths(CLUSTER).known_hosts}\n", block)
        for option in (
            "User ubuntu",
            "IdentitiesOnly yes",
            "StrictHostKeyChecking accept-new",
            "ServerAliveInterval 30",
            "ServerAliveCountMax 4",
            "ConnectTimeout 10",
            "ControlMaster auto",
            "ControlPersist 10m",
            "LogLevel ERROR",
        ):
            self.assertIn(f"  {option}\n", block + "\n")
        self.assertNotIn("${", text)

    def test_node_without_public_ip_is_left_out(self) -> None:
        self.state["nodes"][2]["public_ip"] = None
        text = remote.ssh_config_text(self.state, st.paths(CLUSTER))
        self.assertNotIn("Host vs-0", text)
        self.assertIn("# vs-0: no public IP yet", text)

    def test_paths_are_escaped_and_quoted(self) -> None:
        paths = st.ClusterPaths(Path("/data/my dir/50%/c"))
        text = remote.ssh_config_text(self.state, paths)
        self.assertIn('  IdentityFile "/data/my dir/50%%/c/ssh/id_ed25519"\n', text)
        with self.assertRaises(VsbenchError):
            remote.ssh_config_text(self.state, st.ClusterPaths(Path("/data/${HOME}/c")))

    def test_invalid_state_values_are_rejected(self) -> None:
        self.state["nodes"][0]["public_ip"] = "3.0.0.1\n  ProxyCommand evil"
        with self.assertRaises(VsbenchError):
            remote.ssh_config_text(self.state, st.paths(CLUSTER))

    def test_control_dir_resolution(self) -> None:
        for value, expected in (("", "/tmp"), ("/nonexistent-xyz", "/tmp"), ("relative", "/tmp"), ("/usr/", "/usr")):
            with mock.patch.dict(os.environ, {"XDG_RUNTIME_DIR": value}):
                self.assertEqual(remote.control_dir(), expected, value)
        with mock.patch.dict(os.environ, {"XDG_RUNTIME_DIR": str(self.tmp / ("x" * 60))}):
            (self.tmp / ("x" * 60)).mkdir()
            self.assertEqual(remote.control_dir(), "/tmp")
        self.assertLess(len("/run/user/1000") + remote._CONTROL_EXTRA, remote.MAX_CONTROL_PATH)

    def test_config_file_permissions(self) -> None:
        paths = st.paths(CLUSTER)
        self.assertEqual(stat.S_IMODE(paths.ssh_config.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(paths.ssh_dir.stat().st_mode), 0o700)

    def test_ssh_base(self) -> None:
        cfg = str(st.paths(CLUSTER).ssh_config)
        self.assertEqual(remote.ssh_base(CLUSTER), ["ssh", "-F", cfg])
        self.assertEqual(
            remote.ssh_base(CLUSTER, multiplex=False),
            ["ssh", "-F", cfg, "-o", "ControlMaster=no", "-o", "ControlPath=none"],
        )

    def test_missing_or_stale_config_is_rewritten_from_state(self) -> None:
        cfg = st.paths(CLUSTER).ssh_config
        cfg.unlink()
        remote.run(CLUSTER, "vs-0", "true")
        self.assertIn("Host vs-0", cfg.read_text())
        cfg.write_text(cfg.read_text().replace("ControlPath /usr/", "ControlPath /gone-xyz/"))
        remote.run(CLUSTER, "vs-0", "true")
        self.assertIn("ControlPath /usr/vsb-%C", cfg.read_text())

    def test_unknown_node_fails_without_ssh(self) -> None:
        with self.assertRaises(VsbenchError) as ctx:
            remote.run(CLUSTER, "vs-7", "true")
        self.assertIn("status --refresh", ctx.exception.hint or "")
        with self.assertRaises(VsbenchError):
            remote.run(CLUSTER, "-oProxyCommand=x", "true")
        self.assertEqual(self.calls, [])


class RunTest(RemoteTestCase):
    def test_argv_shape(self) -> None:
        remote.run(CLUSTER, "scylla-0", "uptime", timeout=7)
        argv, kw = self.calls[0]
        cfg = str(st.paths(CLUSTER).ssh_config)
        self.assertEqual(argv, ["ssh", "-F", cfg, "-o", "BatchMode=yes", "-n", "--", "scylla-0", "uptime"])
        self.assertEqual(kw["timeout"], 7)
        self.assertFalse(kw["check"])

    def test_input_keeps_stdin_and_multiplex_off(self) -> None:
        remote.run(CLUSTER, "client", "cat", input="data", multiplex=False)
        argv, kw = self.calls[0]
        self.assertNotIn("-n", argv)
        self.assertIn("ControlPath=none", argv)
        self.assertEqual(kw["input"], "data")

    def test_wrap_command(self) -> None:
        self.assertEqual(remote.wrap_command("a | b"), "a | b")
        self.assertEqual(remote.wrap_command("a | b", sudo=True), "sudo -n bash -c 'a | b'")
        self.assertEqual(remote.wrap_command("x", env={"A": "1 2"}), "env A='1 2' bash -c x")
        with self.assertRaises(VsbenchError):
            remote.wrap_command("x", env={"A B": "1"})

    def test_env_values_survive_the_remote_shell(self) -> None:
        tricky = 'it\'s $HOME `id` "q" \\ ;|&\nline2 *'
        self.responses = [self.execute_locally()]
        result = remote.run(CLUSTER, "client", 'printf %s "$V" && echo -n " | piped" | cat', env={"V": tricky})
        self.assertEqual(result.stdout, tricky + " | piped")

    def test_remote_failure_raises_with_stderr(self) -> None:
        self.responses = [ok(stderr="boom\n", code=3)]
        with self.assertRaises(VsbenchError) as ctx:
            remote.run(CLUSTER, "vs-0", "false")
        self.assertIn("vs-0: remote command failed (exit 3)", str(ctx.exception))
        self.assertIn("boom", str(ctx.exception))

    def test_check_false_returns_result(self) -> None:
        self.responses = [ok(code=255, stderr="ssh: connect to host 3.0.0.2 port 22: Connection timed out")]
        result = remote.run(CLUSTER, "vs-0", "true", check=False)
        self.assertTrue(remote.is_ssh_failure(result))

    def test_ssh_timeout_with_changed_ip_suggests_refresh_ip(self) -> None:
        self.responses = [ok(code=255, stderr="ssh: connect to host 3.0.0.2 port 22: Connection timed out")]
        with mock.patch.object(remote, "current_public_ip", return_value="5.6.7.8"):
            with self.assertRaises(VsbenchError) as ctx:
                remote.run(CLUSTER, "vs-0", "true")
        self.assertIn("1.2.3.4/32 -> 5.6.7.8/32", ctx.exception.hint or "")
        self.assertIn(f"vsbench -c {CLUSTER} refresh-ip", ctx.exception.hint or "")

    def test_ssh_timeout_with_same_ip_gives_generic_hint(self) -> None:
        stderr = "Connection timed out during banner exchange"
        with mock.patch.object(remote, "current_public_ip", return_value="1.2.3.4"):
            hint = remote.ssh_failure_hint(CLUSTER, stderr)
        self.assertIn("status --refresh", hint or "")
        self.assertNotIn("changed", hint or "")

    def test_other_ssh_failures_get_hints(self) -> None:
        self.assertIn("rebooting", remote.ssh_failure_hint(CLUSTER, "port 22: Connection refused") or "")
        self.assertIn("key", remote.ssh_failure_hint(CLUSTER, "Permission denied (publickey).") or "")
        self.assertIsNone(remote.ssh_failure_hint(CLUSTER, "something else"))

    def test_checkip_failure_is_tolerated(self) -> None:
        with mock.patch.object(remote.urllib.request, "urlopen", side_effect=OSError("offline")):
            self.assertIsNone(remote.current_public_ip())

    def test_local_timeout_names_the_node(self) -> None:
        err = VsbenchError("command timed out")
        err.__cause__ = subprocess.TimeoutExpired(["ssh"], 5)
        self.responses = [err]
        with self.assertRaises(VsbenchError) as ctx:
            remote.run(CLUSTER, "client", "sleep 99", timeout=5)
        self.assertIn("client: remote command timed out after 5s", str(ctx.exception))


class RunScriptTest(RemoteTestCase):
    def setUp(self) -> None:
        super().setUp()
        scripts = self.tmp / "node"
        scripts.mkdir()
        (scripts / "probe.sh").write_text(
            '#!/usr/bin/env bash\nset -euo pipefail\n: "${FOO:?}"\nrest=$(cat)\necho "FOO=$FOO stdin=${#rest} $0"\n'
        )
        patcher = mock.patch.object(config, "NODE_SCRIPTS_DIR", scripts)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_script_goes_over_stdin_with_sudo_and_env(self) -> None:
        remote.run_script(CLUSTER, "vs-0", "probe.sh", {"FOO": "a b"})
        argv, kw = self.calls[0]
        self.assertEqual(argv[-1], "sudo -n env FOO='a b' bash -c " + shlex.quote('exec bash -c "$(cat)" probe.sh'))
        self.assertIn("rest=$(cat)", kw["input"])
        self.assertNotIn("-n", argv[:-1])

    def test_script_runs_fully_read_with_its_env(self) -> None:
        self.responses = [self.execute_locally()]
        result = remote.run_script(CLUSTER, "vs-0", "probe.sh", {"FOO": "a 'b'"}, sudo=False)
        self.assertEqual(result.stdout, "FOO=a 'b' stdin=0 probe.sh\n")

    def test_bad_script_names(self) -> None:
        for name in ("../x.sh", "missing.sh", ".hidden"):
            with self.assertRaises(VsbenchError):
                remote.run_script(CLUSTER, "vs-0", name, {})


class RunManyTest(RemoteTestCase):
    def test_results_in_input_order(self) -> None:
        self.responses = [lambda argv, kw: ok(stdout=argv[-2])] * 3
        results = remote.run_many(CLUSTER, ["vs-0", "client", "scylla-0"], "hostname", timeout=3)
        self.assertEqual(list(results), ["vs-0", "client", "scylla-0"])
        self.assertEqual({k: v.stdout for k, v in results.items()}, {n: n for n in results})
        self.assertEqual(remote.run_many(CLUSTER, [], "x"), {})

    def test_failures_are_collected(self) -> None:
        self.responses = [lambda argv, kw: ok(code=1 if argv[-2] == "vs-0" else 0, stderr="nope")] * 3
        with self.assertRaises(VsbenchError) as ctx:
            remote.run_many(CLUSTER, ["scylla-0", "vs-0", "client"], "x")
        self.assertIn("failed on 1 of 3 nodes", str(ctx.exception))
        self.assertIn("vs-0: vs-0: remote command failed (exit 1)", str(ctx.exception))

    def test_check_false_turns_errors_into_results(self) -> None:
        err = VsbenchError("timed out")
        err.__cause__ = subprocess.TimeoutExpired(["ssh"], 1)

        def respond(argv: list[str], kw: dict[str, Any]) -> Result[str]:
            if argv[-2] == "vs-0":
                raise err
            return ok(code=2)

        self.responses = [respond] * 2
        results = remote.run_many(CLUSTER, ["vs-0", "client"], "x", check=False, timeout=1)
        self.assertEqual(results["vs-0"].returncode, remote.SSH_FAILED)
        self.assertIn("timed out", results["vs-0"].stderr)
        self.assertEqual(results["client"].returncode, 2)


class FilesTest(RemoteTestCase):
    def test_upload_copies_to_staging_then_installs_with_checksum(self) -> None:
        local = self.tmp / "bin"
        local.write_bytes(b"binary")
        remote.upload(CLUSTER, "vs-0", local, "/opt/vector-store/builds/x/vector-store", sudo=True, mode="0750")
        scp_argv = self.calls[0][0]
        self.assertEqual(scp_argv[:3], ["scp", "-F", str(st.paths(CLUSTER).ssh_config)])
        self.assertEqual(scp_argv[-2], str(local.resolve()))
        staging = scp_argv[-1].split(":", 1)[1]
        self.assertTrue(scp_argv[-1].startswith("vs-0:/tmp/vsbench-upload-"))
        install = self.remote_cmds()[0]
        self.assertTrue(install.startswith("sudo -n bash -c "))
        self.assertIn(remote.sha256_file(local), install)
        self.assertIn(staging, install)
        self.assertIn("0750", install)

    def test_install_command_checks_before_replacing(self) -> None:
        payload = self.tmp / "payload"
        payload.write_bytes(b"new")
        dest = self.tmp / "deep" / "dir" / "target file"
        good = remote.install_command(str(payload), str(dest), "0751", remote.sha256_file(payload))
        self.assertEqual(local_bash(good).returncode, 0)
        self.assertEqual(dest.read_bytes(), b"new")
        self.assertEqual(stat.S_IMODE(dest.stat().st_mode), 0o751)
        self.assertFalse(payload.exists())
        payload.write_bytes(b"corrupt")
        bad = remote.install_command(str(payload), str(dest), "0755", "0" * 64)
        result = local_bash(bad)
        self.assertEqual(result.returncode, 3)
        self.assertIn("sha256 mismatch", result.stderr)
        self.assertEqual(dest.read_bytes(), b"new")
        self.assertEqual(sorted(p.name for p in dest.parent.iterdir()), ["target file"])

    def test_upload_validates_arguments(self) -> None:
        local = self.tmp / "f"
        local.write_text("x")
        for args in ((local, "/opt/dir/", "0755"), (local, "/opt/f", "rwx"), (self.tmp / "missing", "/opt/f", "0755")):
            with self.assertRaises(VsbenchError):
                remote.upload(CLUSTER, "vs-0", args[0], args[1], mode=args[2])
        self.assertEqual(self.calls, [])

    def test_download_is_atomic(self) -> None:
        target = self.tmp / "out" / "2026-10-06T10:00:00Z.log"

        def scp(argv: list[str], kw: dict[str, Any]) -> Result[str]:
            Path(argv[-1]).write_text("content")
            return ok()

        self.responses = [scp]
        remote.download(CLUSTER, "client", "/var/lib/vsbench/jobs/x/log", target)
        self.assertEqual(target.read_text(), "content")
        self.assertEqual(self.calls[0][0][-2], "client:/var/lib/vsbench/jobs/x/log")
        self.assertTrue(self.calls[0][0][-1].startswith("/"))
        self.assertEqual(list(target.parent.iterdir()), [target])

    def test_failed_download_leaves_nothing(self) -> None:
        target = self.tmp / "out" / "f"

        def scp(argv: list[str], kw: dict[str, Any]) -> Result[str]:
            Path(argv[-1]).write_text("partial")
            return ok(code=1, stderr="scp: /x: No such file or directory")

        self.responses = [scp]
        with self.assertRaises(VsbenchError):
            remote.download(CLUSTER, "client", "/x", target)
        self.assertEqual(list(target.parent.iterdir()), [])

    def test_ensure_script_uploads_only_when_needed(self) -> None:
        digest = remote.sha256_file(JOB_RUN)
        self.responses = [ok(stdout=digest + "\n")]
        path = remote.ensure_script(CLUSTER, "client", "job-run.sh")
        self.assertEqual(path, f"{config.NODE_SCRIPTS}/job-run.sh")
        self.assertEqual(len(self.calls), 1)
        remote.ensure_script(CLUSTER, "client", "job-run.sh")  # cached
        self.assertEqual(len(self.calls), 1)
        self.responses = [ok(stdout="\n")]
        remote.ensure_script(CLUSTER, "vs-0", "job-run.sh")
        self.assertEqual([argv[0] for argv, _ in self.calls[1:]], ["ssh", "scp", "ssh"])
        self.assertIn(digest, self.remote_cmds()[-1])
        self.assertTrue(self.remote_cmds()[-1].startswith("sudo -n "))


class ConnectivityTest(RemoteTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.clock = [0.0]
        fake_time = mock.Mock()
        fake_time.monotonic.side_effect = lambda: self.clock[0]
        fake_time.sleep.side_effect = lambda s: self.clock.__setitem__(0, self.clock[0] + s)
        patcher = mock.patch.object(remote, "time", fake_time)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_wait_ssh_retries_without_multiplexing(self) -> None:
        self.responses = [ok(code=255, stderr="Connection refused"), ok(code=255), ok()]
        remote.wait_ssh(CLUSTER, "vs-0", 60)
        self.assertEqual(len(self.calls), 3)
        for argv, _ in self.calls:
            self.assertIn("ControlMaster=no", argv)
            self.assertIn("ControlPath=none", argv)

    def test_wait_ssh_times_out(self) -> None:
        self.responses = [ok(code=255, stderr="Connection refused")] * 100
        with self.assertRaises(VsbenchError) as ctx:
            remote.wait_ssh(CLUSTER, "vs-0", 12)
        self.assertIn("ssh not reachable after 12s", str(ctx.exception))
        self.assertIn("rebooting", ctx.exception.hint or "")

    def test_reset_master_ignores_errors(self) -> None:
        self.responses = [ok(code=255, stderr="No such file"), VsbenchError("timeout")]
        remote.reset_master(CLUSTER, "vs-0")
        remote.reset_master(CLUSTER, "vs-0")
        self.assertEqual(self.calls[0][0][-4:], ["-O", "exit", "--", "vs-0"])

    def test_http_helpers(self) -> None:
        self.responses = [ok(stdout='{"version": "1.2.3"}'), ok(code=22, stderr="curl: (22) 404"), ok(stdout="<html>")]
        self.assertEqual(remote.http_json(CLUSTER, "vs-0", "http://127.0.0.1:6080/api/v1/info"), {"version": "1.2.3"})
        self.assertEqual(self.remote_cmds()[0], "curl -fsS -g --max-time 10 http://127.0.0.1:6080/api/v1/info")
        with self.assertRaisesRegex(VsbenchError, "curl exit 22"):
            remote.http_get(CLUSTER, "vs-0", "http://127.0.0.1:6080/x")
        with self.assertRaisesRegex(VsbenchError, "invalid JSON"):
            remote.http_json(CLUSTER, "vs-0", "http://127.0.0.1:6080/x")
        with self.assertRaises(VsbenchError):
            remote.http_get(CLUSTER, "vs-0", "file:///etc/passwd")

    def test_interactive_inherits_terminal(self) -> None:
        self.responses = [ok(code=4)]
        self.assertEqual(remote.interactive(CLUSTER, "client", ["ls", "-l"]), 4)
        argv, kw = self.calls[0]
        self.assertEqual(argv[-4:], ["--", "client", "ls", "-l"])
        self.assertFalse(kw["capture"])


def poll_output(active: str, present: bool, exit_code: str, data: bytes, size: int) -> str:
    fields = {
        "active": active,
        "present": int(present),
        "exit_code": exit_code,
        "ended_at": "2026-10-06T10:00:00Z" if exit_code else "",
        "size": size,
        "data": base64.b64encode(data).decode(),
    }
    return "".join(f"{key}={value}\n" for key, value in fields.items())


class JobsTest(RemoteTestCase):
    def test_new_job_id(self) -> None:
        job_id = remote.new_job_id("search")
        self.assertRegex(job_id, r"^\d{8}T\d{6}Z-search-[0-9a-f]{4}$")
        self.assertEqual(remote.job_unit(job_id), f"vsbench-job-{job_id}.service")
        with self.assertRaises(VsbenchError):
            remote.new_job_id("Bad Kind")

    def test_job_start_runs_wrapper_under_systemd_run(self) -> None:
        self.responses = [ok(stdout=remote.sha256_file(JOB_RUN))]
        remote.job_start(CLUSTER, "client", "J1", argv=["bench", "--x", "a b", "$HOME"], env={"RUST_LOG": "info x"})
        words = shlex.split(self.remote_cmds()[-1])
        self.assertEqual(words[:3], ["sudo", "-n", "systemd-run"])
        for word in (
            "--unit=vsbench-job-J1.service",
            "--uid=ubuntu",
            "--gid=ubuntu",
            "--collect",
            "--expand-environment=no",
            f"--property=WorkingDirectory={config.NODE_HOME}",
            "--property=LimitNOFILE=1048576",  # search-http: one socket per concurrent request
            "--setenv=RUST_LOG=info x",
        ):
            self.assertIn(word, words)
        runner = words.index(f"{config.NODE_SCRIPTS}/job-run.sh")
        self.assertEqual(words[runner + 1 :], ["J1", "--", "bench", "--x", "a b", "$HOME"])

    def test_job_start_with_script(self) -> None:
        self.responses = [ok(stdout=remote.sha256_file(JOB_RUN))]
        remote.job_start(CLUSTER, "client", "J2", script="/var/lib/vsbench/jobs/J2/steps.sh")
        self.assertTrue(self.remote_cmds()[-1].endswith("J2 --script /var/lib/vsbench/jobs/J2/steps.sh"))

    def test_job_start_argument_checks(self) -> None:
        for kwargs in ({}, {"argv": []}, {"argv": ["x"], "script": "/s"}):
            with self.assertRaises(VsbenchError):
                remote.job_start(CLUSTER, "client", "J3", **kwargs)
        with self.assertRaises(VsbenchError):
            remote.job_start(CLUSTER, "client", "bad id", argv=["x"])

    def test_parse_poll_running_keeps_partial_line(self) -> None:
        progress, present = remote.parse_poll(poll_output("active", True, "", b"a\nb\npart", 8), 0, 100)
        self.assertTrue(present)
        self.assertEqual((progress.new_text, progress.offset), ("a\nb\n", 4))
        self.assertTrue(progress.running)
        self.assertFalse(progress.pending or progress.done or progress.lost)

    def test_parse_poll_finished_delivers_everything(self) -> None:
        text = "x µs\nlast".encode()
        progress, _ = remote.parse_poll(poll_output("inactive", True, "3", text, 10 + len(text)), 10, 100)
        self.assertEqual((progress.new_text, progress.offset, progress.exit_code), ("x µs\nlast", 10 + len(text), 3))
        self.assertEqual(progress.ended_at, "2026-10-06T10:00:00Z")
        self.assertTrue(progress.done)

    def test_parse_poll_chunk_limit_and_long_lines(self) -> None:
        progress, _ = remote.parse_poll(poll_output("inactive", True, "0", b"ab\ncd", 50), 0, 5)
        self.assertEqual(
            (progress.new_text, progress.offset, progress.pending, progress.done), ("ab\n", 3, True, False)
        )
        progress, _ = remote.parse_poll(poll_output("active", True, "", b"abcde", 50), 0, 5)
        self.assertEqual((progress.new_text, progress.offset), ("abcde", 5))
        progress, _ = remote.parse_poll(poll_output("inactive", False, "", b"", 0), 0, 5)
        self.assertTrue(progress.lost)

    def test_poll_of_unknown_job(self) -> None:
        self.responses = [ok(stdout=poll_output("inactive", False, "", b"", 0))]
        with self.assertRaises(remote.JobNotFound):
            remote.job_poll(CLUSTER, "client", "J9", 0)

    def follow(self, polls: list[Any], timeout_s: int = 60) -> tuple[int, list[str]]:
        lines: list[str] = []
        clock = [0.0]
        fake_time = mock.Mock()
        fake_time.monotonic.side_effect = lambda: clock[0]
        fake_time.sleep.side_effect = lambda s: clock.__setitem__(0, clock[0] + s)
        with mock.patch.object(remote, "time", fake_time), mock.patch.object(remote, "job_poll", side_effect=polls):
            code = remote.job_follow(CLUSTER, "client", "J1", timeout_s, lines.append)
        return code, lines

    def test_follow_streams_lines_and_returns_exit_code(self) -> None:
        polls = [
            remote.JobProgress("a\n", 2, None, None, running=True),
            remote.JobProgress("b\nc\n", 6, 0, "t", running=False, pending=True),
            remote.JobProgress("d", 7, 0, "t", running=False),
        ]
        self.assertEqual(self.follow(polls), (0, ["a", "b", "c", "d"]))

    def test_follow_raises_still_running_after_timeout(self) -> None:
        polls = [remote.JobProgress("", 0, None, None, running=True)] * 100
        with self.assertRaises(StillRunning) as ctx:
            self.follow(polls, timeout_s=9)
        self.assertEqual(ctx.exception.exit_code, proc.EXIT_STILL_RUNNING)
        self.assertIn(f"vsbench -c {CLUSTER} job wait J1", ctx.exception.hint or "")

    def test_follow_reports_lost_jobs_seen_twice(self) -> None:
        lost = remote.JobProgress("", 2, None, None, running=False)
        with self.assertRaisesRegex(VsbenchError, "without recording an exit code"):
            self.follow([remote.JobProgress("x\n", 2, None, None, running=False), lost], timeout_s=0)
        recovered = remote.JobProgress("y\n", 4, 0, "t", running=False)
        self.assertEqual(self.follow([lost, recovered], timeout_s=0), (0, ["y"]))

    def test_follow_tolerates_transient_failures(self) -> None:
        polls = [VsbenchError("ssh failed"), VsbenchError("ssh failed"), remote.JobProgress("", 0, 1, "t")]
        with mock.patch.object(proc, "warn"):
            self.assertEqual(self.follow(polls), (1, []))
        with mock.patch.object(proc, "warn"), self.assertRaisesRegex(VsbenchError, "ssh failed"):
            self.follow([VsbenchError("ssh failed")] * 4)
        with self.assertRaises(remote.JobNotFound):
            self.follow([remote.JobNotFound("gone")])

    def test_cancel_outcomes(self) -> None:
        self.responses = [ok(stdout="result=active\n"), ok(stdout="result=inactive\n"), ok(stdout="result=not-found\n")]
        with mock.patch.object(proc, "log") as log:
            remote.job_cancel(CLUSTER, "client", "J1")
            remote.job_cancel(CLUSTER, "client", "J1")
        self.assertIn("stopped", log.call_args_list[0].args[0])
        self.assertIn("already ended", log.call_args_list[1].args[0])
        with self.assertRaises(remote.JobNotFound):
            remote.job_cancel(CLUSTER, "client", "J1")
        self.assertIn("sudo -n systemctl stop vsbench-job-J1.service", self.remote_cmds()[0])

    def test_job_log_decodes_bytes(self) -> None:
        self.responses = [ok(stdout=base64.b64encode(b"ok \xff\n").decode())]
        self.assertEqual(remote.job_log(CLUSTER, "client", "J1", 20), "ok \ufffd\n")
        self.assertIn("tail -n 20 -- /var/lib/vsbench/jobs/J1/log", self.remote_cmds()[0])
        self.assertEqual(remote.job_log(CLUSTER, "client", "J1", 0), "")


@unittest.skipUnless(shutil.which("stdbuf") and shutil.which("base64"), "needs coreutils stdbuf/base64")
class JobRunScriptTest(RemoteTestCase):
    """node/job-run.sh executed locally, read back through remote's poll/log/cancel."""

    def setUp(self) -> None:
        super().setUp()
        self.jobs = self.tmp / "jobs"
        self.env = dict(os.environ, VSBENCH_JOBS_DIR=str(self.jobs))
        patcher = mock.patch.object(config, "NODE_JOBS", str(self.jobs))
        patcher.start()
        self.addCleanup(patcher.stop)

    def job(self, job_id: str, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run([str(JOB_RUN), job_id, *args], env=self.env, text=True, capture_output=True, timeout=30)

    def read(self, job_id: str, name: str) -> str:
        return (self.jobs / job_id / name).read_text()

    def fake_bin(self, systemctl_state: str) -> dict[str, str]:
        """PATH with fake `sudo` (drops -n) and `systemctl` (is-active/stop)."""
        bin_dir = self.tmp / "bin"
        bin_dir.mkdir(exist_ok=True)
        (bin_dir / "sudo").write_text('#!/usr/bin/env bash\n[ "$1" = -n ] && shift\nexec "$@"\n')
        (bin_dir / "systemctl").write_text(
            f'#!/usr/bin/env bash\ncase "$1" in is-active) echo {systemctl_state};;\n'
            'stop) echo "Unit $2 not loaded." >&2; exit 5;; esac\n'
        )
        for path in bin_dir.iterdir():
            path.chmod(0o755)
        return dict(self.env, PATH=f"{bin_dir}:{os.environ['PATH']}")

    def test_success_failure_and_records(self) -> None:
        self.assertEqual(
            self.job("ok1", "--", "bash", "-c", 'echo "out NO_COLOR=$NO_COLOR"; echo err >&2').returncode, 0
        )
        self.assertEqual(self.read("ok1", "exit_code"), "0\n")
        self.assertRegex(self.read("ok1", "ended_at"), r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ\n$")
        self.assertRegex(self.read("ok1", "started_at"), r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ\n$")
        log = self.read("ok1", "log")
        self.assertIn("out NO_COLOR=1\nerr\n", log)
        self.assertIn("=== VSBENCH JOB END ok1 exit=0", log)
        self.assertEqual(self.job("bad1", "--", "bash", "-c", "exit 7").returncode, 7)
        self.assertEqual(self.read("bad1", "exit_code"), "7\n")
        self.assertEqual(sorted(p.name for p in (self.jobs / "bad1").iterdir() if ".tmp." in p.name), [])

    def test_script_mode(self) -> None:
        script = self.tmp / "steps.sh"
        script.write_text("echo step-one\nexit 4\n")
        self.assertEqual(self.job("s1", "--script", str(script)).returncode, 4)
        self.assertIn("step-one\n", self.read("s1", "log"))
        self.assertEqual(self.read("s1", "exit_code"), "4\n")

    def test_bad_arguments_are_recorded(self) -> None:
        self.assertEqual(self.job("a1").returncode, 2)
        self.assertEqual(self.read("a1", "exit_code"), "2\n")
        self.assertIn("nothing to run", self.read("a1", "log"))
        self.assertEqual(self.job("a2", "--script", "/nonexistent", "--", "x").returncode, 2)
        self.assertEqual(self.job("bad id", "--", "true").returncode, 2)
        self.assertFalse((self.jobs / "bad id").exists())

    def test_refuses_to_reuse_a_job_id(self) -> None:
        self.job("r1", "--", "true")
        result = self.job("r1", "--", "bash", "-c", "exit 9")
        self.assertEqual(result.returncode, 2)
        self.assertIn("already ran", result.stderr)
        self.assertEqual(self.read("r1", "exit_code"), "0\n")

    def test_sigterm_to_the_whole_group_records_143(self) -> None:
        child = subprocess.Popen(
            [str(JOB_RUN), "t1", "--", "bash", "-c", "echo started; sleep 30"],
            env=self.env,
            start_new_session=True,
        )
        self.addCleanup(lambda: child.poll() is None and os.killpg(child.pid, signal.SIGKILL))
        log = self.jobs / "t1" / "log"
        deadline = time.monotonic() + 10
        while not (log.exists() and "started" in log.read_text()):
            self.assertLess(time.monotonic(), deadline, "job did not start")
            time.sleep(0.05)
        os.killpg(child.pid, signal.SIGTERM)  # what `systemctl stop` does (KillMode=control-group)
        self.assertEqual(child.wait(timeout=10), 143)
        self.assertEqual(self.read("t1", "exit_code"), "143\n")
        self.assertIn("=== VSBENCH JOB SIGNAL TERM", self.read("t1", "log"))

    def test_poll_and_log_read_real_job_files(self) -> None:
        self.job("p1", "--", "bash", "-c", "printf 'line1\\nµs line2\\nno-newline'")
        self.responses = [self.execute_locally(self.fake_bin("inactive"))] * 3
        progress = remote.job_poll(CLUSTER, "client", "p1", 0)
        self.assertEqual(progress.exit_code, 0)
        self.assertTrue(progress.done)
        self.assertTrue(progress.new_text.startswith("=== VSBENCH JOB START p1"))
        self.assertIn("line1\nµs line2\nno-newline\n=== VSBENCH JOB END p1 exit=0", progress.new_text)
        self.assertEqual(progress.offset, (self.jobs / "p1" / "log").stat().st_size)
        again = remote.job_poll(CLUSTER, "client", "p1", progress.offset)
        self.assertEqual((again.new_text, again.offset), ("", progress.offset))
        self.assertIn("µs line2", remote.job_log(CLUSTER, "client", "p1", 3))

    def test_poll_of_running_job_and_cancel_fallback(self) -> None:
        job = self.jobs / "c1"
        job.mkdir(parents=True)
        (job / "log").write_text("one\ntwo")
        self.responses = [self.execute_locally(self.fake_bin("active"))]
        progress = remote.job_poll(CLUSTER, "client", "c1", 0)
        self.assertEqual((progress.new_text, progress.offset, progress.running), ("one\n", 4, True))
        self.responses = [self.execute_locally(self.fake_bin("active"))]
        with mock.patch.object(proc, "log"):
            remote.job_cancel(CLUSTER, "client", "c1")
        self.assertEqual(self.read("c1", "exit_code"), f"{remote.JOB_CANCELLED_EXIT}\n")
        self.assertIn("=== VSBENCH JOB CANCELLED", self.read("c1", "log"))
        self.responses = [self.execute_locally(self.fake_bin("inactive"))]
        with self.assertRaises(remote.JobNotFound):
            remote.job_cancel(CLUSTER, "client", "never-started")


class JobRunConstantsTest(unittest.TestCase):
    def test_default_jobs_dir_matches_config(self) -> None:
        self.assertIn(': "${VSBENCH_JOBS_DIR:=' + config.NODE_JOBS + '}"', JOB_RUN.read_text())
        self.assertTrue(os.access(JOB_RUN, os.X_OK))


if __name__ == "__main__":
    unittest.main()
