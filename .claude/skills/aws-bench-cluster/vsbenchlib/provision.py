# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Cluster creation on AWS: doctor and up, with rollback and resume (design §4).

down, list, status refresh, extend and refresh-ip live in teardown.py together with the EC2
helpers both modules use; they are re-exported here, so `provision.<name>` keeps working.
"""

from __future__ import annotations

import datetime
import json
import platform
import shlex
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from . import awsapi, config, proc, remote
from . import state as st
from .awsapi import Aws, AuthError, AwsError, CapacityError
from .proc import PreconditionError, VsbenchError, log, warn
from .state import State
from .teardown import (
    SSH_CMD_TIMEOUT_S,
    TTL_LIMITS,
    Item,
    _authorize,
    _delete_key_pair,
    _ec2,
    _filter,
    _groups,
    _instances,
    _key_pairs,
    _recorded_tokens,
    _node_entry,
    _seconds_left,
    _sync_ssh_ingress,
    estimate_cost,
    find_instances,
    operator_cidr,
    require_credentials,
    resource_name,
    retry_not_found,
    rollback,
    sync_expires_tag,
)

# Re-exported: these moved to teardown.py; cli and other callers use them as provision.<name>.
from .teardown import LISTED_STATES as LISTED_STATES
from .teardown import LIVE_STATES as LIVE_STATES
from .teardown import OVERDUE_GRACE_S as OVERDUE_GRACE_S
from .teardown import delete_security_group as delete_security_group
from .teardown import down as down
from .teardown import extend as extend
from .teardown import list_clusters as list_clusters
from .teardown import parse_until as parse_until
from .teardown import refresh_ip as refresh_ip
from .teardown import refresh_status as refresh_status
from .teardown import terminate_and_wait as terminate_and_wait

DateTime = datetime.datetime

BOOTSTRAP_TIMEOUT_S = 15 * 60
MARKER_POLL_S = 10
# A stalled bootstrap gives up while the credentials still last this long, so the rollback can run.
ROLLBACK_MARGIN_S = 5 * 60
RESUME_TTL_TOLERANCE_S = 15 * 60
BOOT_CAPACITY_CODES = ("Server.InsufficientInstanceCapacity",)
USERDATA_LIMIT_BYTES = 16 * 1024
CLIENT_TOKEN_MAX = 64
MIN_DISK_GB = 8
SCT_VPC_NAME = "SCT-2-vpc"
USERDATA_LOG = "/var/log/vsbench-userdata.log"
NE_SHA256 = config.NODE_EXPORTER_SHA256_ARM64
METADATA_OPTIONS = "HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled"
LOCAL_TOOLS = {"aws": "fail", "docker": "fail", "ssh": "fail", "ssh-keygen": "fail", "git": "fail"}
LOCAL_TOOLS |= {"gimme-aws-creds": "warn", "zstd": "warn"}
_CLOUD_INIT_FINISHED = ("done", "error", "degraded done", "degraded error")
_MARKER_CMD = (
    f"if [ -e {config.NODE_READY_MARKER} ]; then echo ready; elif [ -e {config.NODE_FAILED_MARKER} ]; "
    "then echo failed; else cloud-init status 2>/dev/null | sed -n 's/^status: //p'; fi"
)
# Indirections so tests can fake the clock without patching the time module.
_sleep = time.sleep
_monotonic = time.monotonic


@dataclass(frozen=True)
class UpOptions:  # `vsbench up` flags; `ttl` is the raw --ttl text (e.g. "24h")
    scylla_nodes: int = 1
    vs_nodes: int = 1
    scylla_type: str = config.DEFAULT_INSTANCE_TYPES["scylla"]
    vs_type: str = config.DEFAULT_INSTANCE_TYPES["vs"]
    client_type: str = config.DEFAULT_INSTANCE_TYPES["client"]
    az: str | None = None
    subnet_id: str | None = None
    ttl: str = config.DEFAULT_TTL
    billing_project: str = config.DEFAULT_BILLING_PROJECT
    node_disk_gb: int = config.DEFAULT_DISK_GB["scylla"]
    client_disk_gb: int = config.DEFAULT_DISK_GB["client"]
    dry_run: bool = False
    keep_on_failure: bool = False
    explicit_profile: bool = False  # --profile given: another account than EXPECTED_ACCOUNT is intended


@dataclass(frozen=True)
class Placement:
    az: str
    vpc_id: str
    subnet_id: str
    source: str  # "default-vpc" | "sct-vpc" | "subnet-id"


@dataclass
class _Launch:  # bookkeeping of one `up` invocation; drives the rollback
    cluster: str
    owner: str
    opts: UpOptions
    expires: DateTime
    ami_id: str = ""
    root_device: str = ""
    cidr: str = ""
    instance_ids: list[str] = field(default_factory=list)
    tokens: list[str] = field(default_factory=list)
    inflight: str | None = None  # client token of a run-instances whose outcome is unknown
    groups: dict[str, str] = field(default_factory=dict)  # vpc id -> security group id


def node_name(role: str, index: int) -> str:
    return "client" if role == "client" else f"{role}-{index}"


def node_plan(opts: UpOptions) -> list[Item]:
    """Nodes in launch order: scylla (the scarcest type) first, then vs, then the client."""
    if opts.scylla_nodes < 1 or opts.vs_nodes < 1:
        raise VsbenchError("--scylla-nodes and --vs-nodes must be at least 1")
    if min(opts.node_disk_gb, opts.client_disk_gb) < MIN_DISK_GB:
        raise VsbenchError(f"disk sizes must be at least {MIN_DISK_GB} GB")
    shape = [("scylla", opts.scylla_type, opts.scylla_nodes, opts.node_disk_gb)]
    shape += [("vs", opts.vs_type, opts.vs_nodes, opts.node_disk_gb)]
    shape += [("client", opts.client_type, 1, opts.client_disk_gb)]
    return [
        {"name": node_name(role, i), "role": role, "index": i, "instance_type": itype, "disk_gb": disk}
        for role, itype, count, disk in shape
        for i in range(count)
    ]


def price_split(types: list[str]) -> tuple[float, list[str]]:
    """($/h of the instances with a known price, the sorted types without one)."""
    known = round(sum(config.PRICES.get(t, 0.0) for t in types), 3)
    return known, sorted({t for t in types if t not in config.PRICES})


def _cost_text(nodes: list[Item]) -> str:
    cost = estimate_cost({n["name"]: (n["instance_type"], 1) for n in nodes})
    return "?" if cost is None else f"${cost:.2f}/h"


def userdata_for(role: str, node_name: str, cluster: str, expires_epoch: int) -> str:
    """Static node/userdata.sh with shell-quoted input assignments inserted after the shebang."""
    if role not in config.ROLES:
        raise VsbenchError(f"unknown role '{role}'")
    shebang, _, body = (config.NODE_SCRIPTS_DIR / "userdata.sh").read_text().partition("\n")
    values = {"ROLE": role, "NODE_NAME": node_name, "CLUSTER": cluster, "EXPIRES_AT_EPOCH": str(int(expires_epoch))}
    values |= {"NODE_EXPORTER_VERSION": config.NODE_EXPORTER_VERSION, "NODE_EXPORTER_SHA256": NE_SHA256}
    header = [f"{key}={shlex.quote(value)}" for key, value in values.items()]
    text = "\n".join([shebang, "# --- generated by vsbench ---", *header, "# --- end generated ---", body])
    if len(text.encode()) >= USERDATA_LIMIT_BYTES:
        raise VsbenchError(f"user-data is {len(text.encode())} bytes; EC2 allows < {USERDATA_LIMIT_BYTES}")
    return text


def base_tags(owner: str, cluster: str, billing_project: str) -> dict[str, str]:
    """Tags of every resource. Never `keep` (the SCT janitor may clean leftovers)."""
    tags = {"Owner": owner, "RunByUser": owner, "billing_project": billing_project}
    return tags | {"VsBenchCluster": cluster, "VsBenchOwner": owner, "ManagedBy": "vsbench"}


def instance_tag_specs(owner: str, cluster: str, billing_project: str, node: Item, expires: DateTime) -> list[Item]:
    """keep=alive only on the instance; its volume and ENI stay janitor-eligible if orphaned."""
    name = f"{resource_name(owner, cluster)}-{node['role']}-{node['index']}"
    common = base_tags(owner, cluster, billing_project) | {"Name": name, "ExpiresAt": proc.iso(expires)}
    common |= {"VsBenchRole": node["role"], "VsBenchIndex": str(node["index"])}
    specs = [("instance", common | {"keep": "alive"}), ("volume", common), ("network-interface", common)]
    return [{"ResourceType": kind, "Tags": awsapi.tag_list(tags)} for kind, tags in specs]


def _named_tag_spec(kind: str, launch: _Launch) -> str:
    tags = base_tags(launch.owner, launch.cluster, launch.opts.billing_project)
    tags |= {"Name": resource_name(launch.owner, launch.cluster)}
    return json.dumps([{"ResourceType": kind, "Tags": awsapi.tag_list(tags)}])


def check_instance_types(aws: Aws, plan: list[Item]) -> None:
    """arm64 only (the builds are aarch64); scylla needs local NVMe instance storage."""
    found = _ec2(aws, "describe-instance-types", "--instance-types", *sorted({n["instance_type"] for n in plan}))
    info = {t["InstanceType"]: t for t in found.get("InstanceTypes", [])}
    for item in plan:
        itype = item["instance_type"]
        if itype not in info:
            raise VsbenchError(f"unknown instance type {itype}")
        if "arm64" not in info[itype].get("ProcessorInfo", {}).get("SupportedArchitectures", []):
            raise PreconditionError(f"{itype} is not arm64", "use Graviton types (e.g. i8g, r8g)")
        if item["role"] == "scylla" and not info[itype].get("InstanceStorageSupported"):
            raise PreconditionError(f"{itype} has no instance storage", "scylla needs local NVMe (e.g. i8g)")


def _vpc_subnets(aws: Aws) -> dict[str, Placement]:
    """az -> placement: the default VPC's default-for-az subnet, else SCT-2-vpc's SCT-2-subnet-<az>."""
    found: dict[str, Placement] = {}
    vpc_filters = [("default-vpc", _filter("is-default", ["true"])), ("sct-vpc", _filter("tag:Name", [SCT_VPC_NAME]))]
    for source, vpc_filter in vpc_filters:
        for vpc in _ec2(aws, "describe-vpcs", "--filters", vpc_filter).get("Vpcs", [])[:1]:
            extra = [_filter("default-for-az", ["true"])] if source == "default-vpc" else []
            subnets = _ec2(aws, "describe-subnets", "--filters", _filter("vpc-id", [vpc["VpcId"]]), *extra)
            for sub in subnets.get("Subnets", []):
                az, name = sub["AvailabilityZone"], awsapi.tags_of(sub).get("Name")
                if az not in found and (source == "default-vpc" or name == f"SCT-2-subnet-{az}"):
                    found[az] = Placement(az, vpc["VpcId"], sub["SubnetId"], source)
    return found


def az_candidates(aws: Aws, types: list[str], az: str | None, subnet_id: str | None) -> list[Placement]:
    """AZs that offer every type and have a usable subnet, sorted; `subnet_id` pins its AZ."""
    types = sorted(set(types))
    args = ["--location-type", "availability-zone", "--filters", _filter("instance-type", types)]
    per_type: dict[str, set[str]] = {t: set() for t in types}
    for offer in _ec2(aws, "describe-instance-type-offerings", *args).get("InstanceTypeOfferings", []):
        per_type.setdefault(offer["InstanceType"], set()).add(offer["Location"])
    offered = set.intersection(*per_type.values())
    if subnet_id:
        sub = _ec2(aws, "describe-subnets", "--subnet-ids", subnet_id)["Subnets"][0]  # unknown id: AWS NotFound
        sub_az = sub["AvailabilityZone"]
        placements = {sub_az: Placement(sub_az, sub["VpcId"], subnet_id, "subnet-id")}
    else:
        placements = _vpc_subnets(aws)
    usable = sorted(a for a in placements if a in offered and az in (None, a))
    if not usable:
        where = f" (restricted to {az or subnet_id})" if az or subnet_id else ""
        hint = f"offered in: {', '.join(sorted(offered)) or 'none'}; try --az, --subnet-id or other instance types"
        raise PreconditionError(f"no AZ offers {', '.join(types)} with a usable subnet{where}", hint)
    return [placements[a] for a in usable]


def _require_ours(kind: str, resource: Item, launch: _Launch) -> None:
    """Never reuse or replace a same-named resource whose tags name another owner or cluster."""
    tags = awsapi.tags_of(resource)
    if (tags.get("VsBenchOwner"), tags.get("VsBenchCluster")) != (launch.owner, launch.cluster):
        theirs = f"VsBenchOwner={tags.get('VsBenchOwner')}, VsBenchCluster={tags.get('VsBenchCluster')}"
        name = resource_name(launch.owner, launch.cluster)
        raise PreconditionError(f"the {kind} {name} exists but is tagged {theirs}", "use another cluster name (-c)")


def ensure_key_pair(aws: Aws, launch: _Launch) -> str:
    """Import the local per-cluster ed25519 key (generated if missing); re-import when AWS has another key."""
    paths, name = st.paths(launch.cluster), resource_name(launch.owner, launch.cluster)
    key, pub = paths.ssh_key, paths.ssh_key.with_name(paths.ssh_key.name + ".pub")
    paths.ssh_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not key.exists():
        pub.unlink(missing_ok=True)
        proc.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", f"vsbench-{launch.cluster}", "-f", key])
    if not pub.exists():
        pub.write_text(proc.run(["ssh-keygen", "-y", "-f", key]).stdout)
    found = _key_pairs(aws, name, "--include-public-key")
    if found:
        _require_ours("key pair", found[0], launch)
    if found and (found[0].get("PublicKey") or "").split()[1:2] == pub.read_text().split()[1:2]:
        return name
    if found:
        log(f"key pair {name} does not match the local key; re-importing it")
        _delete_key_pair(aws, name)
    args = ["--key-name", name, "--public-key-material", f"fileb://{pub}"]
    _ec2(aws, "import-key-pair", *args, "--tag-specifications", _named_tag_spec("key-pair", launch))
    return name


def ensure_security_group(aws: Aws, launch: _Launch, vpc_id: str) -> str:
    """Per-cluster SG in `vpc_id` (reused): all traffic within the SG + tcp/22 from the operator /32."""
    if vpc_id in launch.groups:
        return launch.groups[vpc_id]
    name = resource_name(launch.owner, launch.cluster)
    found = _groups(aws, "--filters", _filter("group-name", [name]), _filter("vpc-id", [vpc_id]))
    if found:
        _require_ours("security group", found[0], launch)
    group = found[0] if found else {"IpPermissions": []}
    if not found:
        description = f"vsbench {launch.cluster} of {launch.owner}"
        args = ["--group-name", name, "--description", description, "--vpc-id", vpc_id]
        tags = _named_tag_spec("security-group", launch)
        group["GroupId"] = _ec2(aws, "create-security-group", *args, "--tag-specifications", tags)["GroupId"]
    group_id = group["GroupId"]
    if group_id not in [g.get("GroupId") for p in group["IpPermissions"] for g in p.get("UserIdGroupPairs", [])]:
        self_rule = [{"GroupId": group_id, "Description": "intra-cluster"}]
        _authorize(aws, group_id, {"IpProtocol": "-1", "UserIdGroupPairs": self_rule})
    _sync_ssh_ingress(aws, group, launch.cidr)
    launch.groups[vpc_id] = group_id
    return group_id


def _with_pending_launch(state: State, pending: Item) -> State:
    aws_info = state.get("aws") or {}
    tokens = [*(aws_info.get("client_tokens") or []), pending["client_token"]]
    return state | {"pending_launch": pending, "aws": aws_info | {"client_tokens": tokens}}


def _launch_node(aws: Aws, launch: _Launch, item: Item, placement: Placement) -> Item:
    """One run-instances call per node: pending_launch is persisted before it, the instance right after."""
    suffix = uuid.uuid4().hex[:8]  # client token: unique per attempt, at most 64 ASCII characters
    token = f"vsbench-{launch.owner}-{launch.cluster}-{item['name']}"[: CLIENT_TOKEN_MAX - 9] + f"-{suffix}"
    pending = {"role": item["role"], "node": item["name"], "az": placement.az, "client_token": token}
    st.update(launch.cluster, lambda s: _with_pending_launch(s, pending))
    launch.tokens.append(token)
    work, epoch = st.paths(launch.cluster).work_dir, int(launch.expires.timestamp())
    userdata, tags = work / f"userdata-{item['name']}.sh", work / f"tags-{item['name']}.json"
    proc.atomic_write(userdata, userdata_for(item["role"], item["name"], launch.cluster, epoch), 0o600)
    specs = instance_tag_specs(launch.owner, launch.cluster, launch.opts.billing_project, item, launch.expires)
    proc.atomic_write(tags, json.dumps(specs, indent=2), 0o600)
    nic = {"DeviceIndex": 0, "SubnetId": placement.subnet_id, "Groups": [launch.groups[placement.vpc_id]]}
    nic |= {"AssociatePublicIpAddress": True, "DeleteOnTermination": True}
    ebs = {"VolumeSize": item["disk_gb"], "VolumeType": "gp3", "DeleteOnTermination": True, "Encrypted": True}
    args = [
        *("--image-id", launch.ami_id, "--instance-type", item["instance_type"], "--count", "1"),
        *("--key-name", resource_name(launch.owner, launch.cluster), "--client-token", token),
        *("--network-interfaces", json.dumps([nic])),
        *("--block-device-mappings", json.dumps([{"DeviceName": launch.root_device, "Ebs": ebs}])),
        *("--metadata-options", METADATA_OPTIONS, "--instance-initiated-shutdown-behavior", "terminate"),
        *("--user-data", f"file://{userdata}", "--tag-specifications", f"file://{tags}"),
    ]
    log(f"launching {item['name']} ({item['instance_type']}) in {placement.az}")
    launch.inflight = token
    try:  # the same client token keeps the retry of a just-created SG/key "not found" idempotent
        instance = retry_not_found(lambda: _ec2(aws, "run-instances", *args))["Instances"][0]
    except AwsError as err:
        if err.code:  # EC2 answered with an error: nothing was launched
            launch.inflight = None
        raise
    launch.instance_ids.append(instance["InstanceId"])
    launch.inflight = None
    return _node_entry(item, instance)


def _wait_running(aws: Aws, ids: list[str]) -> None:
    """All instances must reach `running`; one terminated at boot for lack of capacity is a CapacityError."""
    try:
        _ec2(aws, "wait", "instance-running", "--instance-ids", *ids)
        return
    except AwsError as err:
        if err.code or isinstance(err, AuthError):  # an API error, not the waiter's terminal state
            raise
        failure = err
    found = _instances(_ec2(aws, "describe-instances", "--filters", _filter("instance-id", ids)))
    reasons = {i["InstanceId"]: i.get("StateReason") or {} for i in found if i["State"]["Name"] != "running"}
    for iid, reason in sorted(reasons.items()):
        if reason.get("Code") in BOOT_CAPACITY_CODES:
            raise CapacityError(f"{iid} was terminated at boot: {reason.get('Message')}", reason["Code"]) from failure
    detail = "; ".join(f"{iid}: {r.get('Code')} {r.get('Message')}" for iid, r in sorted(reasons.items()))
    raise VsbenchError(f"instances did not reach running: {detail or failure}") from failure


def _launch_in(aws: Aws, launch: _Launch, plan: list[Item], placement: Placement) -> None:
    group_id = ensure_security_group(aws, launch, placement.vpc_id)
    info = {"vpc_id": placement.vpc_id, "subnet_id": placement.subnet_id, "security_group_id": group_id}
    update = lambda s: s | {"az": placement.az, "aws": s.get("aws", {}) | info}  # noqa: E731
    nodes = st.update(launch.cluster, update)["nodes"]
    for item in [i for i in plan if i["name"] not in {n["name"] for n in nodes}]:
        nodes = [*nodes, _launch_node(aws, launch, item, placement)]
        st.update(launch.cluster, lambda s, done=nodes: s | {"nodes": done, "pending_launch": None})
    log(f"waiting for {len(launch.instance_ids)} instance(s) to be running")
    _wait_running(aws, list(launch.instance_ids))


def _launch_all(aws: Aws, launch: _Launch, plan: list[Item], candidates: list[Placement]) -> None:
    """A capacity error (at launch or at boot) rolls back and retries the whole cluster in the next AZ."""
    errors = []
    for placement in candidates:
        try:
            _launch_in(aws, launch, plan, placement)
            return
        except CapacityError as err:
            errors.append(f"{placement.az}: {err}")
            try:
                rollback(aws, launch)
            except AuthError:
                raise
            except VsbenchError as rollback_err:  # keep exit 4: capacity is the cause; up rolls back once more
                hint = f"run: vsbench -c {launch.cluster} down --yes"
                raise CapacityError(f"{errors[-1]}; the rollback failed: {rollback_err}", err.code, hint) from err
            warn(f"no capacity in {placement.az}; rolled back")
    hint = "retry later, or use other instance types or another region"
    raise CapacityError("no AZ had capacity:\n" + "\n".join(errors), "InsufficientInstanceCapacity", hint)


def _poll(cluster: str, name: str, command: str) -> subprocess.CompletedProcess[str]:
    """ssh without the ControlMaster (the docker group must not be cached before the bootstrap ends)."""
    try:
        return remote.run(cluster, name, command, check=False, timeout=SSH_CMD_TIMEOUT_S, multiplex=False)
    except VsbenchError as err:  # e.g. an ssh timeout: the caller retries or reports it
        return subprocess.CompletedProcess([], 255, "", str(err))


def _wait_markers(cluster: str, names: list[str], deadline: float, limit: str) -> None:
    """Wait for the ready/failed marker on every node (cloud-init's exit codes 0 and 2 are both fine)."""
    pending = list(names)
    while pending:
        for name in list(pending):
            out = _poll(cluster, name, _MARKER_CMD)
            status = out.stdout.strip() if out.returncode == 0 else ""
            if status == "ready":
                log(f"{name}: bootstrap done")
                pending.remove(name)
            elif status == "failed" or status in _CLOUD_INIT_FINISHED:  # cloud-init finished without "ready"
                out = _poll(cluster, name, f"sudo cat {config.NODE_FAILED_MARKER}; sudo tail -n 40 {USERDATA_LOG}")
                log(f"--- {name}: the failed marker and the tail of {USERDATA_LOG} ---\n{out.stdout.rstrip()}")
                raise VsbenchError(f"bootstrap of {name} failed ({status})", f"see {USERDATA_LOG} on {name}")
        if pending and _monotonic() >= deadline:
            hint = "up --keep-on-failure keeps failed nodes for debugging"
            raise VsbenchError(f"bootstrap timed out {limit} on {', '.join(pending)}", hint)
        if pending:
            _sleep(MARKER_POLL_S)


def _bootstrap_deadline(aws: Aws) -> tuple[float, str]:
    """BOOTSTRAP_TIMEOUT_S, capped so the rollback still has ROLLBACK_MARGIN_S of valid credentials."""
    deadline, limit = _monotonic() + BOOTSTRAP_TIMEOUT_S, f"after {proc.format_duration(BOOTSTRAP_TIMEOUT_S)}"
    expiry = awsapi.credentials_expiry(aws.profile)
    usable = None if expiry is None else _seconds_left(expiry) - ROLLBACK_MARGIN_S
    if usable is not None and usable <= 0:
        raise AuthError(
            f"AWS credentials of profile {aws.profile} expire too soon to finish up", None, config.LOGIN_HINT
        )
    if usable is not None and _monotonic() + usable < deadline:
        deadline, limit = _monotonic() + usable, "while the AWS credentials still allow a rollback (log in, re-run)"
    return deadline, limit


def _wait_bootstrap(aws: Aws, launch: _Launch) -> None:
    nodes = st.nodes(st.require(launch.cluster))
    ids = [n["instance_id"] for n in nodes]
    found = {i["InstanceId"]: i for i in _instances(_ec2(aws, "describe-instances", "--instance-ids", *ids))}
    nodes = [_node_entry(n, found.get(n["instance_id"], {"InstanceId": n["instance_id"]})) for n in nodes]
    current = st.update(launch.cluster, lambda s: s | {"nodes": nodes})
    if missing := [n["name"] for n in nodes if not n["public_ip"]]:
        raise VsbenchError(f"no public IP on {', '.join(missing)}", "the subnet must allow public IPv4 addresses")
    remote.write_ssh_config(launch.cluster, current)
    names, (deadline, limit) = [n["name"] for n in nodes], _bootstrap_deadline(aws)
    for name in names:
        remote.wait_ssh(launch.cluster, name, max(1, int(deadline - _monotonic())))
    log("waiting for the node bootstrap (user-data)")
    _wait_markers(launch.cluster, names, deadline, limit)
    for name in names:
        out = _poll(launch.cluster, name, f"systemctl is-active vsbench-ttl.timer && cat {config.NODE_EXPIRES_FILE}")
        if out.returncode != 0 or out.stdout.split() != ["active", str(int(launch.expires.timestamp()))]:
            raise VsbenchError(f"the TTL is not armed on {name}: {out.stdout.strip() or out.stderr.strip()}")
        remote.reset_master(launch.cluster, name)


def _may_resume(cluster: str, previous: State | None, live: list[Item]) -> bool:
    """Resume only an unfinished `up` of this local state, and only instances it provably launched."""
    if not live:
        return False
    if not previous or previous.get("created_at") or previous.get("terminated_at"):
        ids = ", ".join(i["InstanceId"] for i in live)
        raise PreconditionError(f"cluster '{cluster}' already has instances: {ids}", "see: vsbench status / down")
    known, tokens = {n["instance_id"] for n in st.nodes(previous)}, _recorded_tokens(previous)
    foreign = [i["InstanceId"] for i in live if i["InstanceId"] not in known and i.get("ClientToken") not in tokens]
    if foreign:
        raise PreconditionError(
            f"cluster '{cluster}' already has instances not launched by this local state: {', '.join(foreign)}",
            "possibly launched from another machine or VSBENCH_HOME: see vsbench list; ask the user before "
            f"removing them with: vsbench -c {cluster} down --yes",
        )
    return True


def _adopt(live: list[Item], plan: list[Item]) -> list[Item]:
    """Map the live instances of an interrupted `up` onto this plan (by VsBenchRole/VsBenchIndex tags)."""
    by_name, adopted = {item["name"]: item for item in plan}, {}
    hint = "re-run up with the same flags, or remove them with: vsbench down --yes"
    for inst in live:
        tags, state_name = awsapi.tags_of(inst), inst["State"]["Name"]
        name = node_name(tags.get("VsBenchRole", "?"), int(tags.get("VsBenchIndex") or 0))
        item = by_name.get(name)
        if item is None or item["instance_type"] != inst["InstanceType"] or name in adopted:
            raise PreconditionError(f"instance {inst['InstanceId']} ({name}) does not fit this plan", hint)
        if state_name not in ("pending", "running"):
            raise PreconditionError(f"instance {inst['InstanceId']} ({name}) is {state_name}", hint)
        adopted[name] = _node_entry(item, inst)
    return list(adopted.values())


def _check_account(aws: Aws, ident: awsapi.Identity, opts: UpOptions, previous: State | None) -> None:
    """The skill is pinned to rnd-core-lab; another account only with an explicit --profile, or for a
    cluster whose live local state records this account and profile (a resumed `up` without --profile:
    the cli then takes the profile from the state)."""
    recorded = (previous or {}).get("account"), (previous or {}).get("profile")
    if previous and not previous.get("terminated_at") and recorded == (ident.account, aws.profile):
        return
    if ident.account != config.EXPECTED_ACCOUNT and not opts.explicit_profile:
        raise PreconditionError(
            f"profile {aws.profile} is account {ident.account}, not {config.EXPECTED_ACCOUNT} (rnd-core-lab)",
            "unset AWS_PROFILE, or pass --profile explicitly to use another account",
        )


def _check_location(cluster: str, previous: State | None, aws: Aws, ident: awsapi.Identity) -> None:
    """Never overwrite the state of a cluster that may still have nodes in another region/profile/account."""
    if not previous or previous.get("terminated_at"):
        return
    if not (st.nodes(previous) or previous.get("pending_launch")):
        return
    where = (previous.get("region"), previous.get("profile"), previous.get("account"))
    if where != (aws.region, aws.profile, ident.account):
        raise PreconditionError(
            f"cluster '{cluster}' still has nodes in {where[0]} (profile {where[1]}, account {where[2]})",
            f"use another name (-c NAME), or remove it first: vsbench -c {cluster} down --yes",
        )


def _resume_ttl(cluster: str, previous: State, opts: UpOptions, ttl_s: int) -> tuple[int, str | None]:
    """A resumed up keeps the interrupted up's expiry: (seconds left, a warning when --ttl differs)."""
    kept = previous["expires_at"]
    left = int(_seconds_left(proc.parse_iso(kept)))
    if left < config.MIN_TTL_SECONDS:
        hint = f"remove it first: vsbench -c {cluster} down --yes"
        raise PreconditionError(f"the interrupted up of cluster '{cluster}' expires at {kept}, too soon", hint)
    if abs(left - ttl_s) <= RESUME_TTL_TOLERANCE_S:
        return left, None
    note = f"the resumed up keeps the expiry {kept} ({proc.format_duration(left)} left), not --ttl {opts.ttl}"
    note += f"; after up run: vsbench -c {cluster} extend --ttl {opts.ttl}"
    warn(note)
    return left, note


def _other_clusters_cost(aws: Aws, owner: str, cluster: str) -> float:
    """$/h of the owner's other billed vsbench instances in this region (best effort, known prices only)."""
    try:
        found = find_instances(aws, None, owner, ("pending", "running"))
    except VsbenchError as err:
        warn(f"cannot list your other clusters for the budget check: {err}")
        return 0.0
    return price_split([i["InstanceType"] for i in found if awsapi.tags_of(i).get("VsBenchCluster") != cluster])[0]


def _warn_budgets(known: float, unpriced: list[str], others: float) -> None:
    """finops budgets; with an unknown price the check covers only the known part (and says so)."""
    if unpriced:
        types = ", ".join(unpriced)
        warn(f"no price for {types}: the budget check is incomplete; the known part alone is ${known:.2f}/h")
    mine = known + others
    if mine > config.PERSONAL_BUDGET_PER_HOUR:
        extra = f" (incl. ${others:.2f}/h of your other running clusters)" if others else ""
        warn(f"${mine:.2f}/h{extra} exceeds the personal budget of ${config.PERSONAL_BUDGET_PER_HOUR:.0f}/h")
    if known > config.PROJECT_BUDGET_PER_HOUR:
        warn(f"${known:.2f}/h exceeds the project budget of ${config.PROJECT_BUDGET_PER_HOUR:.0f}/h")


def _log_plan(cluster: str, aws: Aws, ident: awsapi.Identity, opts: UpOptions, plan: list[Item], ttl_s: int) -> Item:
    """Log the plan and the budget warnings; returns the cost fields of the dry-run result."""
    known, unpriced = price_split([n["instance_type"] for n in plan])
    cost, others = None if unpriced else known, _other_clusters_cost(aws, ident.owner, cluster)
    log(f"plan for cluster '{cluster}' of {ident.owner}: account {ident.account}, profile {aws.profile}, {aws.region}")
    log(f"  ttl {proc.format_duration(ttl_s)}, billing project '{opts.billing_project}'")
    for role in config.ROLES:
        items = [i for i in plan if i["role"] == role]
        log(f"  {role:<6} x{len(items)} {items[0]['instance_type']} ({items[0]['disk_gb']} GB gp3 root)")
    total = "?" if cost is None else f"${cost * ttl_s / 3600:.2f}"
    log(f"  estimated cost {_cost_text(plan)} ({total} if kept for the whole TTL)")
    if opts.billing_project not in config.BILLING_PROJECTS:
        warn(f"billing project '{opts.billing_project}' is not a known finops project; check it or ask Cloud FinOps")
    _warn_budgets(known, unpriced, others)
    return {"cost_per_hour": cost, "unpriced_types": unpriced, "other_clusters_cost_per_hour": round(others, 3)}


def _start_state(cluster: str, aws: Aws, ident: awsapi.Identity, opts: UpOptions, ttl_s: int, resume: bool) -> State:
    previous = st.load(cluster)
    if resume and previous:
        log(f"resuming the interrupted up of cluster '{cluster}' in {previous.get('az')} (keeps its expiry)")
        return previous
    known_hosts = st.paths(cluster).known_hosts
    known_hosts.parent.mkdir(parents=True, exist_ok=True)
    known_hosts.write_text("")  # public IPs get recycled; host keys are per instance
    expires = proc.iso(proc.utcnow() + datetime.timedelta(seconds=ttl_s))
    state: State = {"schema": st.SCHEMA, "cluster": cluster, "owner": ident.owner, "account": ident.account}
    state |= {"profile": aws.profile, "region": aws.region, "expires_at": expires, "ttl_seconds": ttl_s}
    state |= dict.fromkeys(("az", "created_at", "pending_launch", "load", "terminated_at", "expires_tag_pending"))
    key_name = resource_name(ident.owner, cluster)
    state |= {"billing_project": opts.billing_project, "aws": {"key_name": key_name, "client_tokens": []}}
    state |= {"nodes": [], "jobs": {}, "deployed": dict.fromkeys(("scylla", "vector_store", "bench", "monitoring"))}
    state |= {"pins": dict.fromkeys(("scylla_image", "vs_source", "bench_source"))}
    return st.update(cluster, lambda _old: state)


def _provision(aws: Aws, launch: _Launch, plan: list[Item], candidates: list[Placement], adopted: list[Item]) -> State:
    """Launch + bootstrap; any failure (incl. Ctrl-C) terminates this invocation's nodes unless kept."""
    launch.instance_ids.extend(n["instance_id"] for n in adopted)
    try:
        st.update(launch.cluster, lambda s: s | {"nodes": adopted})
        launch.cidr = operator_cidr()
        ensure_key_pair(aws, launch)
        launch.ami_id = aws.call("ssm", "get-parameter", "--name", config.UBUNTU_AMI_SSM)["Parameter"]["Value"]
        image = _ec2(aws, "describe-images", "--image-ids", launch.ami_id)["Images"][0]
        launch.root_device = image.get("RootDeviceName") or "/dev/sda1"
        extra = {"operator_cidr": launch.cidr, "ami_id": launch.ami_id}
        st.update(launch.cluster, lambda s: s | {"aws": s.get("aws", {}) | extra})
        _launch_all(aws, launch, plan, candidates)
        _wait_bootstrap(aws, launch)
    except BaseException as err:
        if launch.opts.keep_on_failure:
            warn(f"up failed ({type(err).__name__}); keeping the nodes (--keep-on-failure); remove: vsbench down")
        else:
            try:
                rollback(aws, launch)
            except BaseException as rollback_err:  # never mask the original failure
                hint = getattr(rollback_err, "hint", None) or f"run: vsbench -c {launch.cluster} down --yes"
                warn(f"rollback failed: {rollback_err}; {hint}")
        raise
    now = proc.iso(proc.utcnow())
    return st.update(launch.cluster, lambda s: s | {"created_at": now, "pending_launch": None})


@dataclass(frozen=True)
class _Resume:  # what `up` found about an interrupted earlier `up` of this local state
    active: bool
    live: list[Item]
    adopted: list[Item]
    ttl_s: int  # the TTL the plan shows: --ttl, or what is left of the kept expiry
    note: str | None = None  # the warning when --ttl differs from the kept expiry
    kept_expires_at: str | None = None
    az: str | None = None


def _resume_info(
    cluster: str, aws: Aws, ident: awsapi.Identity, opts: UpOptions, plan: list[Item], ttl_s: int
) -> _Resume:
    previous = st.load(cluster)
    _check_location(cluster, previous, aws, ident)
    check_instance_types(aws, plan)
    live = find_instances(aws, cluster, ident.owner)
    if not _may_resume(cluster, previous, live) or previous is None:
        return _Resume(False, live, [], ttl_s)
    adopted = _adopt(live, plan)
    left, note = _resume_ttl(cluster, previous, opts, ttl_s)
    return _Resume(True, live, adopted, left, note, previous["expires_at"], previous.get("az"))


def _candidates(aws: Aws, plan: list[Item], opts: UpOptions, resume: _Resume) -> list[Placement]:
    candidates = az_candidates(aws, [i["instance_type"] for i in plan], opts.az, opts.subnet_id)
    if resume.active and resume.az:  # adopted nodes pin the AZ of the interrupted up
        candidates = [p for p in candidates if p.az == resume.az]
        if not candidates:
            raise PreconditionError(f"the interrupted up used {resume.az}", "re-run it with the same flags")
    log("AZ candidates: " + ", ".join(f"{p.az} ({p.subnet_id}, {p.source})" for p in candidates))
    if resume.adopted:
        verb = "would adopt" if opts.dry_run else "adopting"
        log(f"{verb} the instances of the interrupted up: {', '.join(n['instance_id'] for n in resume.adopted)}")
    return candidates


def _dry_run_result(
    cluster: str,
    aws: Aws,
    ident: awsapi.Identity,
    plan: list[Item],
    costs: Item,
    candidates: list[Placement],
    resume: _Resume,
) -> Item:
    info = {"dry_run": True, "cluster": cluster, "owner": ident.owner, "account": ident.account}
    info |= {"profile": aws.profile, "region": aws.region, "nodes": plan, "ttl_seconds": resume.ttl_s} | costs
    info |= {"az_candidates": [dict(vars(p)) for p in candidates]}
    info |= {"would_adopt": [n["instance_id"] for n in resume.adopted]}
    if resume.active:
        info |= {"kept_expires_at": resume.kept_expires_at, "resume_note": resume.note}
    return info


def up(cluster: str, aws: Aws, opts: UpOptions) -> State:
    """Create a cluster, or resume an interrupted `up` of this local state by adopting its instances."""
    require_credentials(aws)
    ident = awsapi.identity(aws)
    _check_account(aws, ident, opts, st.load(cluster))
    plan, ttl_s = node_plan(opts), proc.parse_duration(opts.ttl)
    if not config.MIN_TTL_SECONDS <= ttl_s <= config.MAX_TTL_SECONDS:
        raise VsbenchError(f"--ttl {opts.ttl} is outside {TTL_LIMITS}")
    resume = _resume_info(cluster, aws, ident, opts, plan, ttl_s)
    costs = _log_plan(cluster, aws, ident, opts, plan, resume.ttl_s)
    candidates = _candidates(aws, plan, opts, resume)
    if opts.dry_run:
        return _dry_run_result(cluster, aws, ident, plan, costs, candidates, resume)
    started = _start_state(cluster, aws, ident, opts, ttl_s, resume.active)
    if resume.active:
        sync_expires_tag(cluster, aws, resume.live)
    launch = _Launch(cluster, ident.owner, opts, proc.parse_iso(started["expires_at"]))
    final = _provision(aws, launch, plan, candidates, resume.adopted)
    for line in summary_lines(final):
        log(line)
    if resume.note:
        warn(resume.note)
    return final


def summary_lines(state: State) -> list[str]:
    """Human summary of a provisioned cluster (stderr after `up`; cli may reuse it)."""
    lines = [f"cluster '{state['cluster']}' in {state.get('az')}, expires at {state.get('expires_at')}:"]
    for n in st.nodes(state):
        lines.append(f"  {n['name']:<10} {n['instance_type']:<13} {n.get('public_ip')} ({n.get('private_ip')})")
    return [*lines, f"  estimated cost {_cost_text(st.nodes(state))}; next: vsbench -c {state['cluster']} deploy all"]


def _check(name: str, status: str, detail: str, hint: str | None = None) -> dict[str, Any]:
    return {"check": name, "status": status, "detail": detail, "hint": hint}


def doctor(aws: Aws) -> dict[str, Any]:
    """Local prerequisites, AWS identity and credential expiry; never raises for a failed check."""
    checks, ident = [], None
    for tool, missing in LOCAL_TOOLS.items():
        path, hint = shutil.which(tool), config.INSTALL_HINT if tool in ("aws", "gimme-aws-creds") else None
        checks.append(_check(tool, "ok" if path else missing, path or "not found", None if path else hint))
    checks.append(_check("python", "ok" if sys.version_info >= (3, 10) else "fail", platform.python_version()))
    host_ok = platform.system() == "Linux" and platform.machine() in ("x86_64", "AMD64")
    hint = None if host_ok else "the cross build needs an x86_64 Linux host"
    checks.append(_check("host", "ok" if host_ok else "fail", f"{platform.system()} {platform.machine()}", hint))
    if shutil.which("docker"):
        fmt = "{{.Repository}}:{{.Tag}}"
        try:
            out = proc.run(["docker", "images", "--format", fmt, config.CROSS_IMAGE_REPO], check=False, timeout=30)
        except VsbenchError:  # a hung daemon times out
            out = subprocess.CompletedProcess([], 1, "", "")
        images = ", ".join(out.stdout.split()) if out.returncode == 0 else ""
        detail = images or ("not built yet (vsbench build does)" if not out.returncode else "docker unreachable")
        checks.append(_check("cross-image", "ok" if images else "warn", detail))
    if not shutil.which("aws"):
        return {"ok": False, "checks": checks, "identity": None}
    expiry = awsapi.credentials_expiry(aws.profile)
    left = _seconds_left(expiry) if expiry else None
    if left is None:
        checks.append(_check("credentials", "warn", f"expiry of profile {aws.profile} unknown"))
    else:
        status = "fail" if left <= 0 else "warn" if left < config.MIN_CREDENTIALS_SECONDS else "ok"
        detail = "expired" if left <= 0 else f"expire in {proc.format_duration(left)}"
        checks.append(_check("credentials", status, detail, config.LOGIN_HINT if status != "ok" else None))
    try:
        who = awsapi.identity(aws)
        ident, good = dict(vars(who)), who.account == config.EXPECTED_ACCOUNT
        detail = f"{who.owner} in {who.account} (profile {aws.profile}, region {aws.region})"
        detail += "" if good else f"; expected {config.EXPECTED_ACCOUNT}: unset AWS_PROFILE or pass --profile"
        checks.append(_check("identity", "ok" if good else "warn", detail))
    except VsbenchError as err:
        checks.append(_check("identity", "fail", str(err).splitlines()[0], err.hint or config.LOGIN_HINT))
    return {"ok": all(c["status"] != "fail" for c in checks), "checks": checks, "identity": ident}
