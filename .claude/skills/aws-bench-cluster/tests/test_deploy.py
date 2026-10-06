# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Tests for vsbenchlib.deploy (ssh, Docker Hub and builds are faked) and its node scripts
(functions of the static bash scripts are sourced and run locally, without root or docker)."""

from __future__ import annotations

import base64
import datetime
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vsbenchlib import bench, build, config, deploy, proc, remote, results  # noqa: E402
from vsbenchlib import state as st  # noqa: E402
from vsbenchlib.proc import PreconditionError, StillRunning, VsbenchError  # noqa: E402

CLUSTER = "t1"
NODE_DIR = Path(__file__).resolve().parent.parent / "node"
DIGEST = "sha256:" + "b5" * 32
NIGHTLY = f"scylladb/scylla-nightly@{DIGEST}"
VS_SHA, BENCH_SHA = "a1" * 32, "c3" * 32
SCYLLA_VERSION = "2026.4.0~dev-0.20261005.b027e4e98fff"
BUILD_ID = "1.12.0-dev_x-a1a1a1a1"
GIT_RECORD = {"build_id": BUILD_ID, "kind": "git", "version": "1.12.0-dev", "source": "git:master", "dirty": False}
GIT_RECORD |= {"pin": "git:" + "f" * 40, "commit": "f" * 40}
GIT_RECORD |= {"sha256": {"vector-store": VS_SHA, "vector-search-benchmark": BENCH_SHA}}
LOAD = {"dataset": "cohere-100k", "keyspace": "vsb_keyspace", "index": "vsb_idx_1", "rows": 1000, "rf": 1}
LOAD |= {"phases": {"table": "2026-10-06T10:00:00Z", "index": "2026-10-06T10:20:00Z"}, "pending_job": None}
UNBUILT = LOAD | {"phases": {"table": "2026-10-06T10:00:00Z", "index": None}}  # failed/cancelled build
MONITORING_OUT = "job=scylla 2 0 10s\njob=node_exporter 2 0 10s\ndashboards=master\ncpuset=7\nrestarted=1\n"
NODETOOL = """Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load       Tokens  Owns  Host ID                               Rack
UN  10.0.0.10   1.2 MB     256     ?     8d5ed9f4-7764-4dbd-bad8-43fddce94b7c  rack1
DN  10.0.0.11   ?          256     ?     1f3c2b9a-0000-4dbd-bad8-43fddce94b7d  rack2
"""


def cp(stdout: str = "", returncode: int = 0, stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(["ssh"], returncode, stdout, stderr)


def make_state() -> dict[str, Any]:
    def node(name: str, role: str, index: int, ip: str, itype: str) -> dict[str, Any]:
        base = {"name": name, "role": role, "index": index, "instance_id": f"i-0{index}{len(role)}"}
        return base | {"instance_type": itype, "private_ip": ip, "public_ip": "3.3.3." + ip.rsplit(".", 1)[1]}

    nodes = [
        node("scylla-0", "scylla", 0, "10.0.0.10", "i8g.2xlarge"),
        node("scylla-1", "scylla", 1, "10.0.0.11", "i8g.8xlarge"),
        node("vs-0", "vs", 0, "10.0.1.10", "r8g.4xlarge"),
        node("vs-1", "vs", 1, "10.0.1.11", "r8g.4xlarge"),
        node("client", "client", 0, "10.0.2.10", "r8g.2xlarge"),
    ]
    deployed = {"scylla": None, "vector_store": None, "bench": None, "monitoring": None}
    pins = {"scylla_image": None, "vs_source": None, "bench_source": None}
    base = {"schema": 1, "cluster": CLUSTER, "nodes": nodes, "deployed": deployed, "pins": pins, "load": None}
    return base | {"jobs": {}, "terminated_at": None, "region": "us-east-1", "az": "us-east-1b"}


def probe_text(
    build_id: str, status: str = "SERVING", count: int = 1000, pid: int = 4242, now: float = 1050.0, indexes: Any = None
) -> str:
    index = {"keyspace": "vsb_keyspace", "index": "vsb_idx_1", "status": status, "count": count, "build_progress": 50}
    info = {"engine": "usearch-2.22.0", "service": "vector-store", "version": "1.12.0-dev"}
    lines = [f"now={now}", "active=active", f"pid={pid}"]
    lines += [f"exe={config.NODE_VS_DIR}/builds/{build_id}/vector-store", f"info={json.dumps(info)}"]
    return "\n".join([*lines, f'status="{status}"', f"indexes={json.dumps([index] if indexes is None else indexes)}"])


def old_probe_text(build_id: str, status: str = "SERVING", count: int = 1000) -> str:
    """Vector Store < 1.11: /indexes entries have no status/count; /indexes/<ks>/<idx>/status has them."""
    text = probe_text(build_id, indexes=[{"keyspace": "vsb_keyspace", "index": "vsb_idx_1", "options": {}}])
    return text + f"\nindex_ref=vsb_keyspace/vsb_idx_1\nindex_status={json.dumps({'status': status, 'count': count})}"


class FakeRemote:
    """Answers the remote.* calls deploy makes; records run_script calls, commands and uploads."""

    def __init__(self) -> None:
        self.scripts: list[tuple[str, str, dict[str, str]]] = []
        self.commands: list[tuple[str, str]] = []
        self.uploads: list[tuple[str, str, str, bool]] = []
        self.active_jobs, self.journal, self.info_version = "", "1100.5", "1.12.0-dev"
        self.inspect: dict[str, str] = {}
        self.probes: dict[str, list[str]] = {}
        self.probe_commands: list[str] = []
        self.shas: dict[str, str] = {}
        self.extract_sha = {"vs-0": VS_SHA, "vs-1": VS_SHA}
        self.bench_version = "vector-search-benchmark 1.12.0-dev"
        self.on_script: Any = None  # called with the ACTION of every run_script call

    def run(self, cluster: str, node: str, command: str, **_: Any) -> subprocess.CompletedProcess[str]:
        self.commands.append((node, command))
        if command == deploy._ACTIVE_JOBS:
            return cp(self.active_jobs)
        if "nodetool status" in command:
            return cp(NODETOOL)
        if "scylla --version" in command:
            return cp(SCYLLA_VERSION + "\n")
        if "{{.Config.Image}}" in command:
            return cp(self.inspect.get(node, ""))
        if command.startswith(deploy._VS_PROBE):
            self.probe_commands.append(command)
            queue = self.probes.get(node) or [probe_text(BUILD_ID)]
            return cp(queue.pop(0) if len(queue) > 1 else queue[0])
        if "journalctl" in command:
            return cp(self.journal + "\n")
        if "--version" in command:
            return cp(self.bench_version + "\n")
        if command.startswith("ls -1"):
            return cp(f"1.11.0-aaaa\n{BUILD_ID}\ncurrent=builds/{BUILD_ID}\n")
        raise AssertionError(f"unexpected command on {node}: {command}")

    def run_many(self, cluster: str, nodes: list[str], command: str, **kw: Any) -> dict[str, Any]:
        return {name: self.run(cluster, name, command, **kw) for name in nodes}

    def run_script(self, cluster: str, node: str, script: str, env: dict[str, str], **_: Any) -> Any:
        self.scripts.append((node, script, dict(env)))
        if self.on_script:
            self.on_script(env.get("ACTION"))
        if script == deploy.VS_SCRIPT and env["ACTION"] == "extract":
            return cp(f"sha256={self.extract_sha[node]}\n")
        if script == deploy.VS_SCRIPT:
            info = json.dumps({"engine": "usearch-2.22.0", "version": self.info_version})
            return cp(f"info={info}\npid=5000\nold_pid=4000\nexe=/x\nrestarted_at=1000.0\n")
        if script == deploy.MONITORING_SCRIPT and env["ACTION"] == "check":
            return cp("ready=1\njob=scylla 2 0 10s\nproblem=vector_search http://10.0.1.10:6080/metrics: down x\n")
        return cp(MONITORING_OUT if script == deploy.MONITORING_SCRIPT else "container=abc\n")

    def remote_sha256(self, cluster: str, node: str, path: str, **_: Any) -> str | None:
        return self.shas.get(f"{node}:{path}")

    def upload(self, cluster: str, node: str, local: Path, path: str, **kw: Any) -> None:
        self.uploads.append((node, str(local), path, bool(kw.get("sudo"))))

    def actions(self, script: str) -> list[tuple[str, str]]:
        return [(node, env["ACTION"]) for node, name, env in self.scripts if name == script]


class DeployTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.fake = FakeRemote()
        self.hub: list[str] = []
        self.clock = [0.0]
        patches = [
            mock.patch.dict(os.environ, {"VSBENCH_HOME": self.tmp.name}),
            mock.patch.object(remote, "run", side_effect=self.fake.run),
            mock.patch.object(remote, "run_many", side_effect=self.fake.run_many),
            mock.patch.object(remote, "run_script", side_effect=self.fake.run_script),
            mock.patch.object(remote, "remote_sha256", side_effect=self.fake.remote_sha256),
            mock.patch.object(remote, "upload", side_effect=self.fake.upload),
            mock.patch.object(remote, "ensure_script", side_effect=lambda c, n, s: f"{config.NODE_SCRIPTS}/{s}"),
            mock.patch.object(deploy, "_fetch_json", side_effect=self.fetch),
            mock.patch.object(deploy, "_sleep", side_effect=self.tick),
            mock.patch.object(deploy, "_monotonic", side_effect=lambda: self.clock[0]),
            mock.patch.object(build, "build", return_value=dict(GIT_RECORD)),
            mock.patch.object(proc, "log"),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.warn = mock.patch.object(proc, "warn").start()
        self.addCleanup(mock.patch.stopall)
        st.save(CLUSTER, make_state())

    def tick(self, seconds: float) -> None:
        self.clock[0] += seconds

    def fetch(self, url: str) -> Any:
        self.hub.append(url)
        return {"digest": DIGEST, "images": [{"architecture": "amd64"}, {"architecture": "arm64"}]}

    def change_state(self, **values: Any) -> None:
        st.update(CLUSTER, lambda s: s | values)

    def deployed(self, **components: Any) -> None:
        st.update(CLUSTER, lambda s: s | {"deployed": {**s["deployed"], **components}})


class PureFunctionTest(unittest.TestCase):
    def test_env_file_text_quotes_and_sorts(self) -> None:
        text = deploy.env_file_text({"VECTOR_STORE_URI": "0.0.0.0:6080", "A_MAP": '{"a:1":"b:2"}', "X": "$HOME #x"})
        self.assertEqual(text, "A_MAP='{\"a:1\":\"b:2\"}'\nVECTOR_STORE_URI='0.0.0.0:6080'\nX='$HOME #x'\n")
        self.assertEqual(deploy.env_file_text({}), "")

    def test_env_file_text_rejects_quotes_newlines_and_bad_keys(self) -> None:
        for env in ({"A": "it's"}, {"A": "x\ny"}, {"A": "x\r"}, {"1A": "x"}, {"A-B": "x"}):
            with self.subTest(env=env), self.assertRaises(VsbenchError):
                deploy.env_file_text(env)

    def test_merge_env_unset_then_set_and_warns_on_unknown_unset(self) -> None:
        with mock.patch.object(proc, "warn") as warn:
            merged = deploy._merge_env({"RUST_LOG": "debug", "T": "1"}, {"T": "2", "N": "3"}, ["RUST_LOG", "NOPE"])
        self.assertEqual(merged, {"T": "2", "N": "3"})
        self.assertEqual(warn.call_count, 1)
        with self.assertRaises(VsbenchError):
            deploy._merge_env({}, {"A": "x'y"}, [])

    def test_pick_spec_explicit_pinned_refresh_and_default(self) -> None:
        pick, pin = deploy._pick_spec, "git:" + "f" * 40
        pins = {"vs_source": pin, "vs_source_spec": "git:master"}
        self.assertEqual(pick("local", pins, "vs_source", "git:main", False), ("local", "local"))
        self.assertEqual(pick(None, pins, "vs_source", "git:main", False), (pin, "git:master"))
        self.assertEqual(pick(None, pins, "vs_source", "git:main", True), ("git:master", "git:master"))
        self.assertEqual(pick(None, {}, "vs_source", "git:main", False), ("git:main", "git:main"))
        old = {"scylla_image": NIGHTLY}  # pinned before *_spec existed: refresh re-resolves the pin
        self.assertEqual(pick(None, old, "scylla_image", "nightly", True), (NIGHTLY, NIGHTLY))

    def test_parse_nodetool_status(self) -> None:
        rows = deploy.parse_nodetool_status(NODETOOL)
        got = [(r["state"], r["address"], r["rack"]) for r in rows]
        self.assertEqual(got, [("UN", "10.0.0.10", "rack1"), ("DN", "10.0.0.11", "rack2")])
        self.assertEqual(rows[0]["host_id"], "8d5ed9f4-7764-4dbd-bad8-43fddce94b7c")

    def test_parse_probe_reads_build_and_errors(self) -> None:
        probe = deploy._parse_probe(probe_text("b-1", status="BOOTSTRAPPING"))
        got = (probe["build_id"], probe["pid"], probe["status"], probe["now"], probe["info"]["engine"])
        self.assertEqual(got, ("b-1", 4242, "BOOTSTRAPPING", 1050.0, "usearch-2.22.0"))
        down = deploy._parse_probe("active=failed\npid=0\nstatus=curl: (7) Failed to connect\n")
        self.assertIsNone(down["build_id"])
        self.assertIn("Failed to connect", down["errors"]["status"])
        self.assertEqual(down["errors"]["info"], "no answer")

    def test_is_serving_requires_index_count(self) -> None:
        expect = {"keyspace": "vsb_keyspace", "index": "vsb_idx_1", "min_count": 990}
        self.assertTrue(deploy._is_serving(deploy._parse_probe(probe_text("b", count=995)), expect))
        self.assertFalse(deploy._is_serving(deploy._parse_probe(probe_text("b", count=10)), expect))
        self.assertFalse(deploy._is_serving(deploy._parse_probe(probe_text("b", status="BOOTSTRAPPING")), None))
        self.assertTrue(deploy._is_serving(deploy._parse_probe(probe_text("b", count=10)), {}))
        other = {"keyspace": "vsb_keyspace", "index": "other", "min_count": 0}
        self.assertFalse(deploy._is_serving(deploy._parse_probe(probe_text("b")), other))

    def test_scylla_env_uses_io_properties_only_for_known_types(self) -> None:
        state = make_state()
        env = deploy._scylla_env(state["nodes"][0], NIGHTLY, "10.0.0.10", "http://10.0.1.10:6080")
        self.assertEqual((env["RACK"], env["IO_READ_BW"], env["CLUSTER_NAME"]), ("rack1", "2133338752", "vsbench"))
        env = deploy._scylla_env(state["nodes"][1], NIGHTLY, "10.0.0.10", "")
        self.assertEqual((env["RACK"], env["IO_READ_BW"], env["IO_WRITE_IOPS"]), ("rack2", "", ""))


class ResolveImageTest(DeployTestCase):
    def test_nightly_uses_the_latest_endpoint(self) -> None:
        image, display = deploy.resolve_scylla_image("nightly")
        self.assertEqual((image, self.hub), (NIGHTLY, [config.DOCKER_HUB_NIGHTLY_LATEST]))
        self.assertIn("scylla-nightly:latest", display)

    def test_release_resolves_a_digest_and_checks_the_minimum(self) -> None:
        self.assertEqual(deploy.resolve_scylla_image("release:2026.3.2")[0], f"scylladb/scylla@{DIGEST}")
        self.assertTrue(self.hub[0].endswith("/namespaces/scylladb/repositories/scylla/tags/2026.3.2"))
        for bad in ("release:2025.1", "release:2025.3.4", "release:abc", "release:"):
            with self.subTest(bad=bad), self.assertRaises(VsbenchError):
                deploy.resolve_scylla_image(bad)
        deploy.resolve_scylla_image("release:latest")

    def test_digest_refs_need_no_network_and_tags_are_resolved(self) -> None:
        self.assertEqual((deploy.resolve_scylla_image(NIGHTLY)[0], self.hub), (NIGHTLY, []))
        tag = "2026.4.0-dev-0.20261006.bedcc695789c"
        self.assertEqual(deploy.resolve_scylla_image(f"scylladb/scylla-nightly:{tag}")[0], NIGHTLY)
        self.assertTrue(self.hub[0].endswith(f"/tags/{tag}"))

    def test_rejects_invalid_refs_and_images_without_arm64(self) -> None:
        for bad in ("ghcr.io/x/y:1", "scylla", "scylladb/scylla:bad tag", "Scylladb/x"):
            with self.subTest(bad=bad), self.assertRaises(VsbenchError):
                deploy.resolve_scylla_image(bad)
        amd64_only = {"digest": DIGEST, "images": [{"architecture": "amd64"}]}
        with mock.patch.object(deploy, "_fetch_json", return_value=amd64_only):
            with self.assertRaisesRegex(VsbenchError, "no linux/arm64"):
                deploy.resolve_scylla_image("nightly")


class BenchJobGuardTest(DeployTestCase):
    def test_refuses_while_a_bench_job_runs_but_not_for_fetch(self) -> None:
        self.fake.active_jobs = "vsbench-job-20261006T101530Z-search-1a2b.service\n"
        with self.assertRaisesRegex(PreconditionError, "20261006T101530Z-search-1a2b"):
            deploy.deploy_scylla(CLUSTER, None, False, False)
        self.assertEqual(self.fake.scripts, [])
        self.fake.active_jobs = "vsbench-job-20261006T101530Z-fetch-1a2b.service\n"
        deploy.require_no_bench_job(CLUSTER, st.require(CLUSTER))

    def test_warns_about_jobs_state_still_calls_running(self) -> None:
        current = st.require(CLUSTER) | {"jobs": {"j1": {"kind": "search", "status": "running"}}}
        deploy.require_no_bench_job(CLUSTER, current)
        self.assertIn("j1", self.warn.call_args[0][0])


class DeployScyllaTest(DeployTestCase):
    def test_first_deploy_pulls_wipes_then_starts_seed_first(self) -> None:
        state = deploy.deploy_scylla(CLUSTER, None, False, False)
        actions = self.fake.actions(deploy.SCYLLA_SCRIPT)
        self.assertEqual(sorted(actions[:2]), [("scylla-0", "pull"), ("scylla-1", "pull")])
        self.assertEqual(sorted(actions[2:4]), [("scylla-0", "wipe"), ("scylla-1", "wipe")])
        self.assertEqual(actions[4:], [("scylla-0", "start"), ("scylla-1", "start"), ("scylla-0", "wait-un")])
        starts = [env for _, name, env in self.fake.scripts if env["ACTION"] == "start"]
        self.assertEqual(
            [(e["SEED"], e["RACK"], e["IMAGE"]) for e in starts],
            [("10.0.0.10", "rack1", NIGHTLY), ("10.0.0.10", "rack2", NIGHTLY)],
        )
        self.assertEqual(starts[0]["VS_PRIMARY"], "http://10.0.1.10:6080,http://10.0.1.11:6080")
        self.assertEqual(self.fake.scripts[-1][2]["EXPECT_IPS"], "10.0.0.10,10.0.0.11")
        info = state["deployed"]["scylla"]
        self.assertEqual((info["image"], info["digest"], info["version"]), (NIGHTLY, DIGEST, SCYLLA_VERSION))
        self.assertEqual((state["pins"]["scylla_image"], state["pins"]["scylla_image_spec"]), (NIGHTLY, "nightly"))

    def test_unchanged_when_every_node_runs_the_pinned_image(self) -> None:
        deploy.deploy_scylla(CLUSTER, None, False, False)
        self.fake.scripts.clear()
        self.hub.clear()
        self.fake.inspect = {"scylla-0": f"{NIGHTLY} true", "scylla-1": f"{NIGHTLY} true"}
        deploy.deploy_scylla(CLUSTER, None, False, False)
        self.assertEqual((self.fake.scripts, self.hub), ([], []))  # pinned digest: no Docker Hub call
        self.fake.inspect["scylla-1"] = f"{NIGHTLY} false"
        deploy.deploy_scylla(CLUSTER, None, False, False)
        self.assertNotIn(("scylla-0", "wipe"), self.fake.actions(deploy.SCYLLA_SCRIPT))  # rolling restart

    def test_new_image_is_a_rolling_restart_and_keeps_the_load(self) -> None:
        deploy.deploy_scylla(CLUSTER, None, False, False)
        self.change_state(load=dict(LOAD))
        self.fake.scripts.clear()
        state = deploy.deploy_scylla(CLUSTER, "release:2026.3.2", False, False)
        actions = [action for _, action in self.fake.actions(deploy.SCYLLA_SCRIPT)]
        self.assertEqual(actions, ["pull", "pull", "start", "start", "wait-un"])
        self.assertEqual((state["load"], state["pins"]["scylla_image_spec"]), (LOAD, "release:2026.3.2"))

    def test_wipe_clears_the_load_and_updates_monitoring_targets(self) -> None:
        deploy.deploy_scylla(CLUSTER, None, False, False)
        self.change_state(load=dict(LOAD))
        self.deployed(monitoring={"version": "4.16.1"})
        state = deploy.deploy_scylla(CLUSTER, None, True, False)
        self.assertIsNone(state["load"])
        self.assertEqual(self.fake.actions(deploy.MONITORING_SCRIPT), [("client", "targets")])

    def test_scylla_status_maps_rows_to_nodes(self) -> None:
        self.assertFalse(deploy.scylla_status(CLUSTER)["deployed"])
        self.deployed(scylla={"image": NIGHTLY, "version": SCYLLA_VERSION})
        status = deploy.scylla_status(CLUSTER)
        self.assertEqual((status["un"], status["expected"], status["ok"]), (1, 2, False))
        self.assertEqual([row["node"] for row in status["nodes"]], ["scylla-0", "scylla-1"])


class DeployVsTest(DeployTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.deployed(scylla={"image": NIGHTLY, "version": SCYLLA_VERSION})

    def env_file(self, node: str) -> str:
        env = next(e for n, s, e in self.fake.scripts if n == node and e.get("ACTION") == "activate")
        return base64.b64decode(env["ENV_B64"]).decode()

    def test_uploads_activates_and_records_state(self) -> None:
        state = deploy.deploy_vs(CLUSTER, None, {"VECTOR_STORE_THREADS": "8"}, [], False, 60)
        self.assertEqual(sorted(u[0] for u in self.fake.uploads), ["vs-0", "vs-1"])
        self.assertTrue(all(u[3] and u[2].endswith(f"/builds/{BUILD_ID}/vector-store") for u in self.fake.uploads))
        self.assertIn("VECTOR_STORE_SCYLLADB_URI='10.0.0.10:9042'", self.env_file("vs-0"))
        self.assertIn("RUST_LOG='info'\nVECTOR_STORE_SCYLLADB_URI='10.0.0.11:9042'", self.env_file("vs-1"))
        self.assertIn("VECTOR_STORE_THREADS='8'", self.env_file("vs-0"))
        info, pins = state["deployed"]["vector_store"], state["pins"]
        self.assertEqual((info["build_id"], info["env"]), (BUILD_ID, {"VECTOR_STORE_THREADS": "8"}))
        self.assertEqual((info["engine"], info["pending_index_build"]), ("usearch-2.22.0", None))
        self.assertEqual(info["nodes"], {"vs-0": BUILD_ID, "vs-1": BUILD_ID})
        self.assertEqual((pins["vs_source"], pins["vs_source_spec"]), (GIT_RECORD["pin"], "git:master"))
        build.build.assert_called_once_with(build.SourceSpec("git", "master"))

    def test_skips_the_upload_when_the_node_has_the_binary(self) -> None:
        self.fake.shas = {f"vs-0:{config.NODE_VS_DIR}/builds/{BUILD_ID}/vector-store": VS_SHA}
        deploy.deploy_vs(CLUSTER, None, {}, [], False, 60)
        self.assertEqual([u[0] for u in self.fake.uploads], ["vs-1"])

    def test_unchanged_build_and_env_is_skipped(self) -> None:
        deploy.deploy_vs(CLUSTER, None, {}, [], False, 60)
        self.fake.scripts.clear()
        deploy.deploy_vs(CLUSTER, None, {}, [], False, 60)
        self.assertEqual(self.fake.scripts, [])
        deploy.deploy_vs(CLUSTER, None, {"RUST_LOG": "debug"}, [], False, 60)  # env change -> redeploy
        self.assertIn("RUST_LOG='debug'", self.env_file("vs-0"))

    def test_rejects_a_wrong_running_version(self) -> None:
        self.fake.info_version = "1.11.0"
        with self.assertRaisesRegex(VsbenchError, "expected 1.12.0-dev"):
            deploy.deploy_vs(CLUSTER, None, {}, [], False, 60)

    def test_release_is_extracted_on_the_nodes(self) -> None:
        record = {"build_id": "release-1.11.0", "kind": "release", "version": "1.11.0", "pin": "release:1.11.0"}
        build.build.return_value = record | {"source": "release:latest", "sha256": {}}
        self.fake.info_version = "1.11.0"
        self.fake.probes = {n: [probe_text("release-1.11.0")] for n in ("vs-0", "vs-1")}
        state = deploy.deploy_vs(CLUSTER, "release:latest", {}, [], False, 60)
        extracts = [e for _, s, e in self.fake.scripts if e["ACTION"] == "extract"]
        self.assertEqual([e["VS_IMAGE"] for e in extracts], ["scylladb/vector-store:1.11.0"] * 2)
        self.assertEqual(self.fake.uploads, [])
        got = (state["deployed"]["vector_store"]["sha256"], state["pins"]["vs_source"])
        self.assertEqual(got, (VS_SHA, "release:1.11.0"))
        self.fake.extract_sha["vs-1"] = "dd" * 32
        with self.assertRaisesRegex(VsbenchError, "different vector-store binaries"):
            deploy.deploy_vs(CLUSTER, "release:1.11.0", {"X": "1"}, [], False, 60)

    def test_requires_scylla(self) -> None:
        self.deployed(scylla=None)
        with self.assertRaises(PreconditionError):
            deploy.deploy_vs(CLUSTER, None, {}, [], False, 60)

    def test_records_the_index_rebuild_with_journal_times(self) -> None:
        self.change_state(load=dict(LOAD))
        self.deployed(vector_store={"build_id": "old-1", "env": {}})
        extra = {"comparison_id": "c1", "arm": "A"}
        with mock.patch.object(results, "record_index_build", return_value={"index_build": {"seconds": {}}}) as rec:
            state = deploy.deploy_vs(CLUSTER, None, {}, [], False, 60, record_extra=extra)
        info = rec.call_args[0][1]
        self.assertEqual(info["seconds"], {"vs-0": 100.5, "vs-1": 100.5})
        got = (info["build_id"], info["previous_build_id"], info["trigger"], info["comparison_id"], info["arm"])
        self.assertEqual(got, (BUILD_ID, "old-1", "deploy-vs", "c1", "A"))
        self.assertEqual(info["window"][0], datetime.datetime.fromtimestamp(1000.0, tz=datetime.timezone.utc))
        self.assertNotIn("approximate_nodes", info)
        self.assertIsNone(state["deployed"]["vector_store"]["pending_index_build"])

    def test_timeout_keeps_the_pending_record_for_wait_serving(self) -> None:
        self.change_state(load=dict(LOAD))
        building = probe_text(BUILD_ID, status="BOOTSTRAPPING", count=10)
        self.fake.probes = {n: [building, building, probe_text(BUILD_ID)] for n in ("vs-0", "vs-1")}
        with self.assertRaises(StillRunning):
            deploy.deploy_vs(CLUSTER, None, {}, [], False, 0)
        self.assertIsNotNone(st.require(CLUSTER)["deployed"]["vector_store"]["pending_index_build"])
        self.fake.journal = ""  # not in the journal: falls back to the poll time (approximate)
        with mock.patch.object(results, "record_index_build", return_value={"index_build": {}}) as rec:
            result = deploy.wait_serving(CLUSTER, 60)
        self.assertEqual(rec.call_args[0][1]["seconds"], {"vs-0": 50.0, "vs-1": 50.0})
        self.assertEqual(rec.call_args[0][1]["approximate_nodes"], ["vs-0", "vs-1"])
        self.assertEqual(result["nodes"]["vs-0"]["count"], 1000)
        self.assertIsNone(st.require(CLUSTER)["deployed"]["vector_store"]["pending_index_build"])
        with mock.patch.object(results, "record_index_build") as rec:
            deploy.wait_serving(CLUSTER, 60)
        rec.assert_not_called()


class WaitServingTest(DeployTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.deployed(vector_store={"build_id": "b", "env": {}})
        self.change_state(load=dict(LOAD))

    def test_still_running_after_the_timeout(self) -> None:
        self.fake.probes = {n: [probe_text("b", status="BOOTSTRAPPING", count=5)] for n in ("vs-0", "vs-1")}
        with self.assertRaises(StillRunning) as err:
            deploy.wait_serving(CLUSTER, 30)
        self.assertIn("wait-serving", err.exception.hint or "")
        self.assertGreaterEqual(self.clock[0], 30)

    def test_detects_a_crash_while_building(self) -> None:
        building = [probe_text("b", status="BOOTSTRAPPING", pid=1), probe_text("b", status="BOOTSTRAPPING", pid=2)]
        self.fake.probes = {"vs-0": building, "vs-1": [probe_text("b")]}
        with self.assertRaisesRegex(VsbenchError, "restarted while building"):
            deploy.wait_serving(CLUSTER, 600)

    def test_fails_when_the_service_stays_down(self) -> None:
        self.fake.probes = {"vs-0": ["active=failed\npid=0\n"], "vs-1": [probe_text("b")]}
        with self.assertRaisesRegex(VsbenchError, "vs-0: vector-store is failed"):
            deploy.wait_serving(CLUSTER, 600)

    def test_index_check_can_be_skipped(self) -> None:
        self.fake.probes = {n: [probe_text("b", count=1)] for n in ("vs-0", "vs-1")}
        self.assertTrue(deploy.wait_serving(CLUSTER, 0, expect_index={})["serving"])
        with self.assertRaises(StillRunning):
            deploy.wait_serving(CLUSTER, 0)

    def test_vs_status_and_builds_on_nodes(self) -> None:
        self.assertEqual(deploy.vs_status(CLUSTER)["vs-1"]["build_id"], BUILD_ID)
        builds = deploy.builds_on_nodes(CLUSTER)
        self.assertEqual(builds["vector_store"]["vs-0"]["current"], BUILD_ID)
        self.assertEqual(builds["bench"]["client"]["builds"], ["1.11.0-aaaa", BUILD_ID])


class UnbuiltIndexTest(DeployTestCase):
    """A failed, cancelled or unfinished load/index job leaves load.index naming an index that does not exist."""

    def setUp(self) -> None:
        super().setUp()
        self.deployed(scylla={"image": NIGHTLY, "version": SCYLLA_VERSION})
        self.change_state(load=dict(UNBUILT))
        self.fake.probes = {n: [probe_text(BUILD_ID, indexes=[])] for n in ("vs-0", "vs-1")}

    def test_wait_serving_waits_for_node_serving_only(self) -> None:
        self.deployed(vector_store={"build_id": BUILD_ID, "env": {}})
        for load in (UNBUILT, LOAD | {"pending_job": "20261006T101530Z-load-1a2b"}):
            with self.subTest(load=load):
                self.change_state(load=dict(load))
                result = deploy.wait_serving(CLUSTER, 30)
                self.assertEqual((result["serving"], result["nodes"]["vs-0"]["count"]), (True, None))
                self.assertLess(self.clock[0], 1)
        self.assertNotIn("/indexes/", "".join(self.fake.probe_commands))
        logged = " ".join(c[0][0] for c in proc.log.call_args_list)
        self.assertIn("index vsb_idx_1 is not built (its load/index job failed", logged)
        self.assertIn("bench load cohere-100k --resume", logged)
        self.assertIn("job 20261006T101530Z-load-1a2b is not recorded as finished", logged)
        self.assertIn("job wait 20261006T101530Z-load-1a2b", logged)

    def test_deploy_vs_records_no_index_build(self) -> None:
        self.deployed(vector_store={"build_id": "old-1", "env": {}})
        starting = probe_text(BUILD_ID, status="BOOTSTRAPPING", indexes=[])
        self.fake.probes = {n: [starting, probe_text(BUILD_ID, indexes=[])] for n in ("vs-0", "vs-1")}
        with self.assertRaises(StillRunning):
            deploy.deploy_vs(CLUSTER, None, {}, [], False, 0)
        self.assertIsNone(st.require(CLUSTER)["deployed"]["vector_store"]["pending_index_build"])
        with mock.patch.object(results, "record_index_build") as rec:
            deploy.wait_serving(CLUSTER, 30)
        rec.assert_not_called()

    def test_a_stale_marker_is_cleared_without_a_record(self) -> None:
        marker = {"build_id": BUILD_ID, "trigger": "deploy-vs", "restarted_at": {"vs-0": 1000.0}, "extra": {}}
        self.deployed(vector_store={"build_id": BUILD_ID, "env": {}, "pending_index_build": marker})
        with mock.patch.object(results, "record_index_build") as rec:
            self.assertIsNone(deploy.wait_serving(CLUSTER, 30)["index_build"])
        rec.assert_not_called()
        self.assertIsNone(st.require(CLUSTER)["deployed"]["vector_store"]["pending_index_build"])

    def test_a_missing_built_index_points_to_bench_index_not_to_waiting(self) -> None:
        self.deployed(vector_store={"build_id": BUILD_ID, "env": {}})
        self.change_state(load=dict(LOAD))
        with self.assertRaises(StillRunning) as err:
            deploy.wait_serving(CLUSTER, 0)
        self.assertIn("vs-0: SERVING, index vsb_idx_1 not listed", str(err.exception))
        self.assertIn("bench index", err.exception.hint or "")
        self.assertNotIn("keeps building", err.exception.hint or "")


class OldVectorStoreTest(DeployTestCase):
    """Vector Store < 1.11 reports an index's status and count only in /indexes/<ks>/<idx>/status."""

    def setUp(self) -> None:
        super().setUp()
        self.deployed(scylla={"image": NIGHTLY, "version": SCYLLA_VERSION})
        self.deployed(vector_store={"build_id": BUILD_ID, "env": {}})
        self.change_state(load=dict(LOAD))
        self.fake.probes = {n: [old_probe_text(BUILD_ID)] for n in ("vs-0", "vs-1")}

    def test_wait_serving_reads_the_per_index_status(self) -> None:
        result = deploy.wait_serving(CLUSTER, 0)
        self.assertEqual(result["nodes"]["vs-0"]["count"], 1000)
        self.assertIn("/api/v1/indexes/vsb_keyspace/vsb_idx_1/status", self.fake.probe_commands[0])
        self.fake.probes = {n: [old_probe_text(BUILD_ID, status="BOOTSTRAPPING", count=3)] for n in ("vs-0", "vs-1")}
        with self.assertRaises(StillRunning):
            deploy.wait_serving(CLUSTER, 0)

    def test_vs_status_satisfies_bench_check_serving(self) -> None:
        view = bench._vs_view(CLUSTER, st.require(CLUSTER))
        self.assertEqual(bench.check_serving(view, LOAD, CLUSTER)["engine"], "usearch-2.22.0")

    def test_the_list_entry_wins_when_it_has_a_status(self) -> None:
        text = probe_text(BUILD_ID, status="BOOTSTRAPPING", count=3)
        text += '\nindex_ref=vsb_keyspace/vsb_idx_1\nindex_status={"status": "SERVING", "count": 1000}'
        entry = deploy._parse_probe(text)["indexes"][0]
        self.assertEqual((entry["status"], entry["count"]), ("BOOTSTRAPPING", 3))

    def test_probe_command_only_queries_valid_index_names(self) -> None:
        self.assertEqual(deploy._vs_probe(None), deploy._VS_PROBE)
        self.assertEqual(deploy._vs_probe({"keyspace": "ks", "index": "a'b"}), deploy._VS_PROBE)
        probe = deploy._vs_probe({"keyspace": "ks", "index": "i_1"})
        self.assertTrue(probe.startswith(deploy._VS_PROBE + "; "))
        self.assertIn(f"{deploy._VS_URL}/indexes/ks/i_1/status 2>&1", probe)


class BudgetTest(DeployTestCase):
    def test_deploy_vs_timeout_covers_the_whole_command(self) -> None:
        self.deployed(scylla={"image": NIGHTLY, "version": SCYLLA_VERSION})
        self.change_state(load=dict(LOAD))
        self.fake.probes = {n: [probe_text(BUILD_ID, status="BOOTSTRAPPING", count=3)] for n in ("vs-0", "vs-1")}
        self.fake.on_script = lambda action: self.tick(100) if action == "activate" else None
        with self.assertRaises(StillRunning):
            deploy.deploy_vs(CLUSTER, None, {}, [], False, 300)
        self.assertGreaterEqual(self.clock[0], 300)
        self.assertLessEqual(self.clock[0], 300 + deploy.POLL_S)

    def test_deploy_all_gives_vector_store_what_is_left(self) -> None:
        timeouts: list[int] = []
        mock.patch.object(deploy, "deploy_scylla", side_effect=lambda *a, **k: self.tick(200)).start()
        mock.patch.object(deploy, "deploy_monitoring").start()
        mock.patch.object(deploy, "deploy_bench").start()
        mock.patch.object(deploy, "deploy_vs", side_effect=lambda *a, **k: timeouts.append(a[5])).start()
        deploy.deploy_all(CLUSTER, True, 540)
        deploy.deploy_all(CLUSTER, True)
        self.assertEqual(timeouts, [340, config.DEFAULT_FOREGROUND_SECONDS - 200])


class VsProbeShellTest(unittest.TestCase):
    """The probe command itself, run by bash with systemctl/readlink/curl stubbed."""

    def test_probe_prints_the_per_index_status(self) -> None:
        exe = f"{config.NODE_VS_DIR}/builds/b-1/vector-store"
        stubs = "systemctl() { case $1 in show) echo 42 ;; is-active) echo active ;; esac; }; "
        stubs += f"readlink() {{ echo {exe}; }}; curl() {{ case ${{@: -1}} in "
        stubs += """*/indexes/*/status) echo '{"status": "SERVING", "count": 7}' ;; */status) echo '"SERVING"' ;; """
        stubs += """*/indexes) echo '[{"keyspace": "ks", "index": "i_1"}]' ;; *) echo '{"version": "1"}' ;; esac; }; """
        expect = {"keyspace": "ks", "index": "i_1", "min_count": 7}
        result = bash(stubs + deploy._vs_probe(expect))
        self.assertEqual(result.returncode, 0, result.stderr)
        probe = deploy._parse_probe(result.stdout)
        self.assertEqual((probe["build_id"], probe["pid"], probe["errors"]), ("b-1", 42, {}))
        self.assertTrue(deploy._is_serving(probe, expect))
        self.assertFalse(deploy._is_serving(deploy._parse_probe(bash(stubs + deploy._VS_PROBE).stdout), expect))


class DeployBenchTest(DeployTestCase):
    def test_bench_upload_symlink_and_version(self) -> None:
        state = deploy.deploy_bench(CLUSTER, None, False)
        target = f"{config.NODE_BENCH_DIR}/builds/{BUILD_ID}/vector-search-benchmark"
        local = str(build.build_dir(BUILD_ID) / "vector-search-benchmark")
        self.assertEqual(self.fake.uploads, [("client", local, target, False)])
        swap = self.fake.commands[-1][1]
        self.assertIn(f"builds/{BUILD_ID}/vector-search-benchmark", swap)
        self.assertIn("mv -T", swap)
        got = (state["deployed"]["bench"]["build_id"], state["pins"]["bench_source_spec"])
        self.assertEqual(got, (BUILD_ID, "git:master"))
        self.fake.shas = {f"client:{config.NODE_BENCH_DIR}/vector-search-benchmark": BENCH_SHA}
        self.fake.uploads.clear()
        deploy.deploy_bench(CLUSTER, None, False)
        self.assertEqual(self.fake.uploads, [])

    def test_bench_checks_the_version_and_rejects_release_sources(self) -> None:
        self.fake.bench_version = "vector-search-benchmark 0.0.0-dev"
        with self.assertRaisesRegex(VsbenchError, "--version printed"):
            deploy.deploy_bench(CLUSTER, None, False)
        with self.assertRaises(VsbenchError):
            deploy.deploy_bench(CLUSTER, "release:1.11.0", False)


class DeployAllTest(DeployTestCase):
    def test_only_missing_components_unless_forced(self) -> None:
        order: list[str] = []
        for name in ("scylla", "monitoring", "bench", "vs"):
            fake = mock.patch.object(deploy, f"deploy_{name}", side_effect=lambda *a, n=name, **k: order.append(n))
            fake.start()
        self.deployed(scylla={"image": NIGHTLY}, bench={"build_id": "b"})
        deploy.deploy_all(CLUSTER, False)
        self.assertEqual(order, ["monitoring", "vs"])
        order.clear()
        deploy.deploy_all(CLUSTER, True)
        self.assertEqual(order, ["scylla", "monitoring", "bench", "vs"])


def bash(script: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", "-c", script, "test", *args], capture_output=True, text=True)


def run_file(path: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", path], capture_output=True, text=True, env={**os.environ, **env})


class ScyllaScriptTest(unittest.TestCase):
    SCRIPT = str(NODE_DIR / "scylla-start.sh")
    SETUP = f'source "$1"; IMAGE={NIGHTLY} IP=10.0.0.10 SEED=10.0.0.10 RACK=rack1 CLUSTER_NAME=vsbench DC=dc1; '
    STUB = 'source "$1"; IP=10.0.0.10 CQL_TIMEOUT_S=1 UN_TIMEOUT_S=1; sleep() { :; }; '

    def run_args(self, extra: str) -> list[str]:
        result = bash(self.SETUP + extra + ' build_run_args; printf "%s\\n" "${RUN_ARGS[@]}"', self.SCRIPT)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.splitlines()

    def test_production_flags_with_precomputed_io(self) -> None:
        args = self.run_args(
            "VS_PRIMARY=http://10.0.1.10:6080 IO_READ_BW=1 IO_READ_IOPS=2 IO_WRITE_BW=3 IO_WRITE_IOPS=4;"
        )
        joined = " ".join(args)
        flags = "--network host|--restart unless-stopped|--developer-mode 0|--overprovisioned 0|--io-setup 0"
        for flag in (flags + "|--rack rack1|--vector-store-primary-uri http://10.0.1.10:6080|--cap-add SYS_NICE").split(
            "|"
        ):
            self.assertIn(flag, joined)
        self.assertIn("/etc/scylla-bench/io_properties.yaml:/etc/scylla.d/io_properties.yaml:ro", args)
        self.assertNotIn("--cpuset", joined)
        no_exporter = "/dev/null:/etc/supervisord.conf.d/scylla-node-exporter.conf:ro"
        self.assertLess(args.index(no_exporter), args.index(NIGHTLY))  # docker options precede the image
        self.assertEqual(args[args.index(NIGHTLY) + 1 : args.index(NIGHTLY) + 3], ["--developer-mode", "0"])

    def test_iotune_without_io_values_and_no_vs_flag_without_primary(self) -> None:
        joined = " ".join(self.run_args(""))
        self.assertIn("--io-setup 1", joined)
        self.assertNotIn("io_properties", joined)
        self.assertNotIn("--vector-store-primary-uri", joined)

    def test_validation(self) -> None:
        self.assertEqual(bash(self.SETUP + "validate_start_env", self.SCRIPT).returncode, 0)
        for bad in ("IP=10.0.0", "IO_READ_BW=5", "VS_PRIMARY=10.0.1.10:6080", "RACK='rack 1'", "IMAGE='x y'"):
            with self.subTest(bad=bad):
                self.assertEqual(bash(self.SETUP + bad + "; validate_start_env", self.SCRIPT).returncode, 1)
        self.assertEqual(run_file(self.SCRIPT, {"ACTION": "bogus"}).returncode, 1)

    def test_wait_cql_fails_fast_when_the_container_dies(self) -> None:
        dead = bash(self.STUB + 'docker() { [[ $1 == inspect ]] && echo "exited 0"; }; wait_cql', self.SCRIPT)
        self.assertEqual(dead.returncode, 1)
        self.assertIn("'exited 0'", dead.stderr)
        alive = 'docker() { [[ $1 == inspect ]] && echo "running 0"; return 0; }; wait_cql'
        self.assertEqual(bash(self.STUB + alive, self.SCRIPT).returncode, 0)

    def test_wait_un_wants_exactly_the_expected_nodes_all_up(self) -> None:
        wait = 'docker() { printf "%s" "$NT"; }; EXPECT_IPS=$2 NT=$3; wait_un'
        all_up = NODETOOL.replace("DN", "UN")
        for expect, text, code in (
            ("10.0.0.10,10.0.0.11", all_up, 0),
            ("10.0.0.10", all_up, 1),
            ("10.0.0.10,10.0.0.11", NODETOOL, 1),
        ):
            with self.subTest(expect=expect, code=code):
                result = bash(self.STUB + wait, self.SCRIPT, expect, text)
                self.assertEqual(result.returncode, code, result.stderr)

    def test_node_states_parses_nodetool(self) -> None:
        result = bash('source "$1"; node_states <<<"$2"', self.SCRIPT, NODETOOL)
        self.assertEqual(result.stdout.splitlines(), ["DN 10.0.0.11", "UN 10.0.0.10"])


class VsScriptTest(unittest.TestCase):
    SCRIPT = str(NODE_DIR / "vs-activate.sh")
    STUBS = (
        'source "$1"; VS_DIR=$2 UNIT_FILE=$2/unit.service STABLE_S=0 START_TIMEOUT_S=1 INFO_TIMEOUT_S=1; '
        'sleep() { :; }; journalctl() { :; }; curl() { echo \'{"version": "1.12.0"}\'; }; '
        'install() { if [[ $1 == -d ]]; then mkdir -p "${@: -1}"; else cp "${@: -2:1}" "${@: -1}"; fi; }; '
        'systemctl() { case $1 in show) cat "$VS_DIR/pid" ;; restart) echo "$NEW" >"$VS_DIR/pid" ;; esac; }; '
        'readlink() { if [[ $2 == /proc/* ]]; then echo "$VS_DIR/builds/b-1/vector-store"; '
        'else command readlink "$@"; fi; }; '
    )

    def test_env_and_symlink_swap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script = (
                'source "$1"; VS_DIR=$2 BUILD_ID=b-2 ENV_B64=$3; mkdir -p "$VS_DIR/builds/b-1" "$VS_DIR/builds/b-2"; '
            )
            script += 'ln -s builds/b-1 "$VS_DIR/current"; write_env; swap_current; readlink "$VS_DIR/current"'
            result = bash(script, self.SCRIPT, tmp, base64.b64encode(b"RUST_LOG='info'\n").decode())
            self.assertEqual((result.returncode, result.stdout.strip()), (0, "builds/b-2"), result.stderr)
            self.assertEqual(Path(tmp, ".env").read_text(), "RUST_LOG='info'\n")
            self.assertEqual(sorted(os.listdir(tmp)), [".env", "builds", "current"])

    def test_activate_switches_and_waits_for_a_new_pid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "builds", "b-1").mkdir(parents=True)
            Path(tmp, "builds", "b-1", "vector-store").write_text("")
            os.chmod(Path(tmp, "builds", "b-1", "vector-store"), 0o755)
            Path(tmp, "pid").write_text("100\n")
            Path(tmp, "src.service").write_text("[Service]\n")
            run = 'BUILD_ID=b-1 ENV_B64=$(printf "A=1" | base64) UNIT_SRC=$2/src.service VS_PORT=6080 NEW=$3; activate'
            result = bash(self.STUBS + run, self.SCRIPT, tmp, "200")
            self.assertEqual(result.returncode, 0, result.stderr)
            out = deploy._kv(result.stdout)
            self.assertEqual((out["pid"], out["old_pid"], out["info"]), ("200", "100", '{"version": "1.12.0"}'))
            self.assertEqual(out["exe"], f"{tmp}/builds/b-1/vector-store")
            self.assertEqual((os.readlink(Path(tmp, "current")), Path(tmp, ".env").read_text()), ("builds/b-1", "A=1"))
            self.assertEqual(Path(tmp, "unit.service").read_text(), "[Service]\n")
            stuck = bash(self.STUBS + run, self.SCRIPT, tmp, "200")  # MainPID stays 200: never restarted
            self.assertEqual(stuck.returncode, 1)
            self.assertIn("did not start", stuck.stderr)

    def test_extract_release_verifies_version_and_is_idempotent(self) -> None:
        binary = "#!/bin/sh\\\\necho \\'vector-store 1.11.0\\'\\\\n"
        docker = f'docker() {{ case $1 in create) echo cid ;; cp) printf "{binary}" >"$3" ;; esac; }}; '
        run = 'source "$1"; VS_DIR=$2 BUILD_ID=release-1.11.0 VS_IMAGE=x/y:1 VERSION=$3; ' + docker
        run += "extract_release; extract_release"
        with tempfile.TemporaryDirectory() as tmp:
            result = bash(run, self.SCRIPT, tmp, "1.11.0")
            self.assertEqual(result.returncode, 0, result.stderr)
            shas = [line for line in result.stdout.splitlines() if line.startswith("sha256=")]
            self.assertEqual(len(shas), 2)
            self.assertEqual(len(set(shas)), 1)
            self.assertEqual(Path(tmp, "builds", "release-1.11.0", "sha256").read_text().strip(), shas[0][7:])
            self.assertIn("already present", result.stderr)
            wrong = bash(run.replace("release-1.11.0", "release-9"), self.SCRIPT, tmp, "9.9.9")
            self.assertEqual(wrong.returncode, 1)
            self.assertEqual(sorted(os.listdir(Path(tmp, "builds"))), ["release-1.11.0"])  # temp dir cleaned up

    def test_rejects_bad_build_ids_and_actions(self) -> None:
        for build_id in ("../x", "a..b", "-x", ""):
            with self.subTest(build_id=build_id):
                self.assertEqual(bash('source "$1"; BUILD_ID=$2; check_build_id', self.SCRIPT, build_id).returncode, 1)
        self.assertEqual(run_file(self.SCRIPT, {"ACTION": "bogus"}).returncode, 1)


if __name__ == "__main__":
    unittest.main()
