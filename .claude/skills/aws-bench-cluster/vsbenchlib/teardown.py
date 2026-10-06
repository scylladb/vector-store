# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Day-2 operations on an existing cluster: down, list, status refresh, extend, refresh-ip (design §4).

Also holds what `up` (provision.py) shares with them: instance discovery, termination with a
tolerant wait, the rollback of a failed `up`, security-group and key-pair lookups, and the
ExpiresAt tag re-sync. provision.py re-exports the public names, so `provision.down` and friends
keep working.
"""

from __future__ import annotations

import collections
import datetime
import ipaddress
import json
import re
import shutil
import sys
import time
import urllib.request
from collections.abc import Callable
from typing import Any, Protocol, TypeVar

from . import awsapi, config, proc, remote
from . import state as st
from .awsapi import Aws, AuthError, AwsError
from .proc import PreconditionError, VsbenchError, log, warn
from .state import State

Item = dict[str, Any]  # a planned node or an EC2 API object
DateTime = datetime.datetime
T = TypeVar("T")

SSH_CMD_TIMEOUT_S = 60
SG_DELETE_TIMEOUT_S = 5 * 60
SG_RETRY_S = 10
EXTEND_MIN_AHEAD_S = 15 * 60
OVERDUE_GRACE_S = 15 * 60
TERMINATE_POLL_S = 10
TERMINATE_TIMEOUT_S = 10 * 60
# EC2 is eventually consistent: a resource created (or an instance id returned) seconds ago can still
# be reported as not found. Retry such errors briefly instead of failing (and rolling back) the command.
NOT_FOUND_RETRY_S = 3
NOT_FOUND_TRIES = 6
EVENTUALLY_CONSISTENT = ("InvalidGroup.NotFound", "InvalidKeyPair.NotFound")
INSTANCE_NOT_FOUND = "InvalidInstanceID.NotFound"
# An interrupted run-instances may have launched an instance that describe-instances does not show
# yet: the rollback polls its client token this often, this many times (about 45 s).
ROLLBACK_TOKEN_POLL_S = 5
ROLLBACK_TOKEN_TRIES = 9
CHECKIP_URL = "https://checkip.amazonaws.com"
LIVE_STATES = ("pending", "running", "stopping", "stopped")
LISTED_STATES = (*LIVE_STATES, "shutting-down")
TTL_LIMITS = f"[{proc.format_duration(config.MIN_TTL_SECONDS)}, {config.MAX_TTL_SECONDS // 86400}d]"
_UNTIL_TZ_RE = re.compile(r"(Z|[+-]\d{2}:\d{2})$")
# Indirections so tests can fake the clock without patching the time module.
_sleep = time.sleep
_monotonic = time.monotonic


# --- EC2 helpers shared with provision.up ---------------------------------------------
def resource_name(owner: str, cluster: str) -> str:
    """Name of the key pair and the security group; prefix of instance Name tags."""
    return f"vsbench-{owner}-{cluster}"


def _ec2(aws: Aws, operation: str, *args: Any) -> Any:
    return aws.call("ec2", operation, *args) or {}


def _filter(name: str, values: list[str] | tuple[str, ...]) -> str:
    return f"Name={name},Values={','.join(values)}"


def _owner_filters(owner: str, cluster: str) -> list[str]:
    return [_filter("tag:VsBenchCluster", [cluster]), _filter("tag:VsBenchOwner", [owner])]


def _instances(data: Any) -> list[Item]:
    return [i for r in data.get("Reservations", []) for i in r.get("Instances", [])]


def find_instances(
    aws: Aws, cluster: str | None, owner: str | None, states: tuple[str, ...] | None = LIVE_STATES
) -> list[Item]:
    """vsbench instances by tags (cluster/owner None = any; states None = all, incl. terminated)."""
    filters = [_filter("tag:VsBenchCluster", [cluster]) if cluster else _filter("tag-key", ["VsBenchCluster"])]
    filters += [_filter("tag:VsBenchOwner", [owner])] if owner else []
    filters += [_filter("instance-state-name", states)] if states else []
    return _instances(_ec2(aws, "describe-instances", "--filters", *filters))


def estimate_cost(types_counts: dict[str, tuple[str, int]]) -> float | None:
    """$/h for {role: (instance type, count)}; None when a price is unknown."""
    if any(count and itype not in config.PRICES for itype, count in types_counts.values()):
        return None
    return round(sum(config.PRICES.get(itype, 0.0) * count for itype, count in types_counts.values()), 3)


def _seconds_left(moment: DateTime) -> float:
    aware = moment if moment.tzinfo else moment.replace(tzinfo=datetime.timezone.utc)
    return (aware - proc.utcnow()).total_seconds()


def require_credentials(aws: Aws) -> None:
    """`up`/`down` need MIN_CREDENTIALS_SECONDS of credential lifetime (offline check)."""
    expiry = awsapi.credentials_expiry(aws.profile)
    left = _seconds_left(expiry) if expiry else None  # unknown: the sts identity call that follows decides
    if left is not None and left < config.MIN_CREDENTIALS_SECONDS:
        what = "expired" if left <= 0 else f"expire in {proc.format_duration(left)}"
        need = proc.format_duration(config.MIN_CREDENTIALS_SECONDS)
        raise AuthError(f"AWS credentials of profile {aws.profile} {what} (need >= {need})", None, config.LOGIN_HINT)


def operator_cidr() -> str:
    """The caller's public IPv4 as a /32 (the ssh ingress rule)."""
    try:
        with urllib.request.urlopen(CHECKIP_URL, timeout=10) as response:
            return f"{ipaddress.IPv4Address(response.read(64).decode().strip())}/32"
    except (OSError, ValueError) as err:
        raise VsbenchError(f"cannot determine your public IP via {CHECKIP_URL}: {err}") from err


def _node_entry(item: Item, instance: Item) -> Item:
    entry = {k: item[k] for k in ("name", "role", "index", "instance_type")} | {"instance_id": instance["InstanceId"]}
    return entry | {"private_ip": instance.get("PrivateIpAddress"), "public_ip": instance.get("PublicIpAddress")}


def retry_not_found(
    action: Callable[[], T], codes: tuple[str, ...] = EVENTUALLY_CONSISTENT, tries: int = NOT_FOUND_TRIES
) -> T:
    """Run `action`, retrying while EC2 still reports a just-created resource as not found."""
    attempt = 1
    while True:
        try:
            return action()
        except AwsError as err:
            if err.code not in codes or attempt >= tries:
                raise
            proc.debug(f"{err.code}; retrying in {NOT_FOUND_RETRY_S}s (eventual consistency)")
        attempt += 1
        _sleep(NOT_FOUND_RETRY_S)


def _groups(aws: Aws, *args: str) -> list[Item]:
    return _ec2(aws, "describe-security-groups", *args).get("SecurityGroups", [])


def _key_pairs(aws: Aws, name: str, *extra: str) -> list[Item]:
    return _ec2(aws, "describe-key-pairs", "--filters", _filter("key-name", [name]), *extra).get("KeyPairs", [])


def _delete_key_pair(aws: Aws, name: str) -> None:
    try:
        _ec2(aws, "delete-key-pair", "--key-name", name)
    except AwsError as err:
        if not awsapi.is_not_found(err):
            raise


def _authorize(aws: Aws, group_id: str, permission: Item) -> None:
    args = ["--group-id", group_id, "--ip-permissions", json.dumps([permission])]
    try:
        retry_not_found(lambda: _ec2(aws, "authorize-security-group-ingress", *args))
    except AwsError as err:
        if err.code != "InvalidPermission.Duplicate":
            raise


def _sync_ssh_ingress(aws: Aws, group: Item, cidr: str) -> None:
    """Allow tcp/22 from `cidr` only; revokes other (stale operator) addresses."""
    group_id = group["GroupId"]
    ssh_rules = [p for p in group.get("IpPermissions", []) if (p.get("IpProtocol"), p.get("FromPort")) == ("tcp", 22)]
    stale = [r["CidrIp"] for p in ssh_rules for r in p.get("IpRanges", []) if r.get("CidrIp") != cidr]
    ranges = [{"CidrIp": cidr, "Description": "ssh-from-operator"}]
    _authorize(aws, group_id, {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": ranges})
    for old in stale:
        log(f"revoking ssh from {old} on {group_id}")
        args = ["--group-id", group_id, "--protocol", "tcp", "--port", "22", "--cidr", old]
        _ec2(aws, "revoke-security-group-ingress", *args)


# --- termination --------------------------------------------------------------------------
def terminate(aws: Aws, ids: list[str]) -> None:
    """terminate-instances; retries ids EC2 does not know yet, then skips ids that stay unknown."""
    if not ids:
        return
    try:
        call = lambda: _ec2(aws, "terminate-instances", "--instance-ids", *ids)  # noqa: E731
        retry_not_found(call, (INSTANCE_NOT_FOUND,))
        return
    except AwsError as err:
        if err.code != INSTANCE_NOT_FOUND:
            raise
    for iid in ids:  # one unknown id fails the whole call: terminate the others one by one
        try:
            _ec2(aws, "terminate-instances", "--instance-ids", iid)
        except AwsError as err:
            if err.code != INSTANCE_NOT_FOUND:
                raise
            warn(f"{iid} is unknown to EC2; nothing to terminate")


def wait_terminated(aws: Aws, ids: list[str], timeout_s: int = TERMINATE_TIMEOUT_S) -> None:
    """Poll until every id is `terminated` or unknown to EC2.

    Replaces `aws ec2 wait instance-terminated`, whose waiter fails at once on a (possibly stale)
    `pending` or `stopping` read. An instance still pending/running/stopping/stopped gets
    terminate-instances again (idempotent); `shutting-down` only needs time.
    """
    pending = sorted(set(ids))
    for attempt in range(max(1, timeout_s // TERMINATE_POLL_S)):
        if attempt:
            _sleep(TERMINATE_POLL_S)
        found = _instances(_ec2(aws, "describe-instances", "--filters", _filter("instance-id", pending)))
        states = {i["InstanceId"]: i["State"]["Name"] for i in found}
        pending = [i for i in pending if states.get(i, "terminated") != "terminated"]
        if not pending:
            return
        terminate(aws, [i for i in pending if states[i] in LIVE_STATES])
    raise VsbenchError(
        f"instances not terminated after {proc.format_duration(timeout_s)}: {', '.join(pending)}",
        "check with: vsbench list; retry with: vsbench -c <cluster> down --yes",
    )


def terminate_and_wait(aws: Aws, ids: list[str], timeout_s: int = TERMINATE_TIMEOUT_S) -> None:
    """Terminate `ids` and wait (tolerantly) until EC2 reports them terminated."""
    if ids:
        terminate(aws, sorted(set(ids)))
        wait_terminated(aws, ids, timeout_s)


# --- rollback of a failed `up` ------------------------------------------------------------
class LaunchRecord(Protocol):
    """What rollback needs from provision's per-invocation bookkeeping (provision._Launch)."""

    cluster: str
    owner: str
    instance_ids: list[str]
    tokens: list[str]  # client tokens of this invocation's run-instances calls
    inflight: str | None  # client token of a run-instances whose outcome is unknown (interrupted)


def _recorded_tokens(state: State) -> set[str]:
    """Client tokens of every run-instances this local state made: the proof it launched an instance."""
    tokens = set((state.get("aws") or {}).get("client_tokens") or [])
    return tokens | {t for t in [(state.get("pending_launch") or {}).get("client_token")] if t}


def _by_tokens(aws: Aws, tokens: list[str]) -> list[Item]:
    return _instances(_ec2(aws, "describe-instances", "--filters", _filter("client-token", sorted(tokens))))


def _find_inflight(aws: Aws, token: str) -> list[Item] | None:
    """The instance(s) of an interrupted run-instances; None if EC2 never shows one while polling."""
    for attempt in range(ROLLBACK_TOKEN_TRIES):
        if attempt:
            _sleep(ROLLBACK_TOKEN_POLL_S)
        if found := _by_tokens(aws, [token]):
            return found
    return None


def _discover(aws: Aws, launch: LaunchRecord) -> tuple[set[str], bool]:
    """Live instances of this state's launches (tags + client tokens); False when one stays unresolved.

    Tagged instances count only with a client token this state recorded: a same-named cluster
    launched from another machine or VSBENCH_HOME is never terminated by this rollback.
    """
    try:
        ours = set(launch.tokens) | _recorded_tokens(st.load(launch.cluster) or {})
        found = [i for i in find_instances(aws, launch.cluster, launch.owner) if i.get("ClientToken") in ours]
        found += _by_tokens(aws, launch.tokens) if launch.tokens else []
        token, resolved = launch.inflight, True
        if token and token not in {i.get("ClientToken") for i in found}:
            inflight = _find_inflight(aws, token)
            found, resolved = found + (inflight or []), inflight is not None
    except Exception as err:  # never let a discovery problem look like "nothing to terminate"
        warn(f"rollback: instance discovery failed: {err}")
        return set(), False
    return {i["InstanceId"] for i in found if i["State"]["Name"] in LIVE_STATES}, resolved


def rollback(aws: Aws, launch: LaunchRecord) -> None:
    """Terminate what an `up` invocation launched or adopted, then wait for it.

    Known ids go first, before any discovery or logging; then instances found by tags and client
    tokens. pending_launch is cleared only when every launch is accounted for; otherwise it stays in
    state and a VsbenchError says that an instance may be orphaned.
    """
    known = sorted(set(launch.instance_ids))
    terminate(aws, known)
    discovered, resolved = _discover(aws, launch)
    terminate(aws, sorted(discovered - set(known)))
    ids = sorted(set(known) | discovered)
    if ids:
        log(f"rolling back: terminated {', '.join(ids)}; waiting for them")
        wait_terminated(aws, ids)
    cleared = {"nodes": []} | ({"pending_launch": None} if resolved else {})
    st.update(launch.cluster, lambda s: s | cleared)
    launch.instance_ids.clear()
    if not resolved:
        raise VsbenchError(
            f"possible orphan: an interrupted launch of cluster '{launch.cluster}' could not be found or ruled out",
            f"check: vsbench list; remove it with: vsbench -c {launch.cluster} down --yes",
        )
    launch.tokens.clear()
    launch.inflight = None


def delete_security_group(aws: Aws, group_id: str) -> None:
    """Retry DependencyViolation every SG_RETRY_S (up to SG_DELETE_TIMEOUT_S), deleting detached ENIs."""
    deadline = _monotonic() + SG_DELETE_TIMEOUT_S
    enis = [_filter("group-id", [group_id]), _filter("status", ["available"])]
    while True:
        try:
            _ec2(aws, "delete-security-group", "--group-id", group_id)
            return
        except AwsError as err:
            if awsapi.is_not_found(err):
                return
            if err.code != "DependencyViolation" or _monotonic() >= deadline:
                raise
        for eni in _ec2(aws, "describe-network-interfaces", "--filters", *enis).get("NetworkInterfaces", []):
            log(f"deleting the detached network interface {eni['NetworkInterfaceId']}")
            try:
                _ec2(aws, "delete-network-interface", "--network-interface-id", eni["NetworkInterfaceId"])
            except AwsError as eni_err:
                warn(f"cannot delete {eni['NetworkInterfaceId']}: {eni_err}")
        _sleep(SG_RETRY_S)


# --- ExpiresAt tag ------------------------------------------------------------------------
def sync_expires_tag(cluster: str, aws: Aws, found: list[Item] | None = None) -> None:
    """Re-apply the ExpiresAt tag from state.expires_at (best effort, warns on failure).

    Tags every node when an earlier `extend` could not (`expires_tag_pending`), and every live
    instance in `found` (describe-instances objects) whose tag differs from the state.
    """
    state = st.load(cluster)
    if not state or state.get("terminated_at") or not state.get("expires_at"):
        return
    expires, ours = state["expires_at"], {n["instance_id"] for n in st.nodes(state)}
    live = [i for i in found or [] if i["InstanceId"] in ours and i["State"]["Name"] in LIVE_STATES]
    ids = {i["InstanceId"] for i in live if awsapi.tags_of(i).get("ExpiresAt") != expires}
    if state.get("expires_tag_pending"):
        ids |= {i["InstanceId"] for i in live} if found is not None else ours
    if not ids:
        return
    try:
        _ec2(aws, "create-tags", "--resources", *sorted(ids), "--tags", f"Key=ExpiresAt,Value={expires}")
    except VsbenchError as err:
        warn(f"cannot re-sync the ExpiresAt tag of cluster '{cluster}': {err}")
        return
    log(f"re-synced the ExpiresAt tag of cluster '{cluster}' to {expires}")
    st.update(cluster, lambda s: s | {"expires_tag_pending": None})


# --- down ---------------------------------------------------------------------------------
def _confirm(cluster: str) -> None:
    if not sys.stdin.isatty():
        raise PreconditionError("refusing to delete without confirmation", "pass --yes after the user agreed")
    if input(f"delete cluster '{cluster}'? [y/N] ").strip().lower() not in ("y", "yes"):
        raise PreconditionError("teardown cancelled")


def down(cluster: str, aws: Aws, assume_yes: bool, purge: bool) -> dict[str, Any]:
    """Idempotent teardown by tags (works without local state and with zero instances).

    Security groups and the key pair are found by their VsBenchOwner/VsBenchCluster tags, never
    by name alone (another owner's `vsbench-<owner>-<cluster>` name can collide).
    """
    require_credentials(aws)
    ident, current = awsapi.identity(aws), st.load(cluster)
    owner = ident.owner
    name, by_tags = resource_name(owner, cluster), _owner_filters(owner, cluster)
    ids = sorted(i["InstanceId"] for i in find_instances(aws, cluster, owner, LISTED_STATES))
    groups = sorted({g["GroupId"] for g in _groups(aws, "--filters", *by_tags)})
    key = name if _key_pairs(aws, name, *by_tags) else None
    log(f"cluster '{cluster}' of {owner}: instances {ids}, security groups {groups}, key pair {key}")
    if (ids or groups or key) and not assume_yes:
        _confirm(cluster)
    if ids:
        log("terminating the instances and waiting for them")
        terminate_and_wait(aws, ids)
    for group_id in groups:
        delete_security_group(aws, group_id)
    if key:
        _delete_key_pair(aws, key)
    now = proc.iso(proc.utcnow())
    ours = _state_matches(current, aws, ident)
    if not ours:  # the local state describes a same-named cluster elsewhere (other region/account/owner)
        recorded = "/".join(str(current.get(k)) for k in ("region", "account", "owner"))  # type: ignore[union-attr]
        here = f"{aws.region}/{ident.account}/{ident.owner}"
        warn(f"the local state of cluster '{cluster}' is for {recorded}, not {here}; kept as it is")
    elif current is not None:
        st.update(cluster, lambda s: s | {"terminated_at": now, "pending_launch": None, "expires_tag_pending": None})
    if purge and ours:
        shutil.rmtree(st.paths(cluster).root, ignore_errors=True)
    result = {"cluster": cluster, "owner": owner, "terminated": ids, "security_groups": groups, "key_pair": key}
    return result | {"purged": purge and ours, "terminated_at": now}


def _state_matches(current: State | None, aws: Aws, ident: awsapi.Identity) -> bool:
    """The local state describes the cluster this call tears down: same region, account and owner.

    `down --profile <other-account>` on a same-named cluster finds nothing to terminate there; it
    must not mark or purge the state of the cluster that keeps running in the original account.
    A state without one of the fields is accepted.
    """
    if current is None:
        return True
    expected = {"region": aws.region, "account": ident.account, "owner": ident.owner}
    return all(current.get(key) in (None, value) for key, value in expected.items())


# --- list ---------------------------------------------------------------------------------
def _local_state(aws: Aws, owner: str, cluster: str, members: list[Item]) -> State | None:
    """This machine's live state of a listed cluster: same owner and region, sharing an instance."""
    try:
        state = st.load(cluster)
    except (VsbenchError, OSError, ValueError):  # e.g. another owner's cluster name is not a valid local name
        return None
    if not state or state.get("terminated_at") or (state.get("owner"), state.get("region")) != (owner, aws.region):
        return None
    ours = {n["instance_id"] for n in st.nodes(state)}
    return state if ours & {i["InstanceId"] for i in members} else None


def _left_s(expires: str | None) -> int | None:
    try:
        return int(_seconds_left(proc.parse_iso(expires))) if expires else None
    except ValueError:
        return None


def _list_row(aws: Aws, who: str, cluster: str, members: list[Item]) -> dict[str, Any]:
    billed = [i for i in members if i["State"]["Name"] in ("pending", "running")]
    expires = min((t for t in (awsapi.tags_of(i).get("ExpiresAt") for i in members) if t), default=None)
    local = _local_state(aws, who, cluster, members)
    local_expires = (local or {}).get("expires_at")
    tag_stale = bool(local_expires) and local_expires != expires  # e.g. an extend without credentials
    if local is not None:
        sync_expires_tag(cluster, aws, members)
    expires = local_expires if tag_stale else expires
    left = _left_s(expires)
    row = {"cluster": cluster, "owner": who, "nodes": len(members)}
    row |= {"states": dict(collections.Counter(i["State"]["Name"] for i in members))}
    row |= {"types": sorted({i["InstanceType"] for i in members})}
    row |= {"instance_ids": sorted(i["InstanceId"] for i in members)}
    row |= {"cost_per_hour": estimate_cost({i["InstanceId"]: (i["InstanceType"], 1) for i in billed})}
    overdue = bool(billed) and left is not None and left < -OVERDUE_GRACE_S
    return row | {"expires_at": expires, "expires_in_s": left, "overdue": overdue, "tag_stale": tag_stale}


def list_clusters(aws: Aws, owner: str | None) -> list[dict[str, Any]]:
    """Clusters by (owner, cluster) in aws.region; `overdue` = still billed 15 min past its expiry.

    The expiry is the instances' ExpiresAt tag, unless this machine's state of the cluster (same
    owner, region and instances) records another one: then that wins, `tag_stale` is set (e.g. an
    `extend` ran without credentials) and the tag is re-synced.
    """
    groups: dict[tuple[str, str], list[Item]] = {}
    for inst in find_instances(aws, None, owner, LISTED_STATES):
        tags = awsapi.tags_of(inst)
        groups.setdefault((tags.get("VsBenchOwner", "?"), tags.get("VsBenchCluster", "?")), []).append(inst)
    return [_list_row(aws, who, cluster, members) for (who, cluster), members in sorted(groups.items())]


# --- status refresh, extend, refresh-ip ------------------------------------------------------
def refresh_status(cluster: str, aws: Aws) -> State:
    """Re-read the cluster's instances by tags; update the IPs and each node's `aws_state`."""
    current = st.require(cluster)
    found = {i["InstanceId"]: i for i in find_instances(aws, cluster, current["owner"], None)}

    def refreshed(n: Item) -> Item:
        inst = found.get(n["instance_id"])
        fresh = {} if inst is None else _node_entry(n, inst)
        return n | fresh | {"aws_state": inst["State"]["Name"] if inst else "not-found"}

    now = proc.iso(proc.utcnow())
    updated = st.update(cluster, lambda s: s | {"nodes": [refreshed(n) for n in s["nodes"]], "refreshed_at": now})
    old_ips = {n["name"]: n.get("public_ip") for n in current["nodes"]}
    if any(n.get("public_ip") and n["public_ip"] != old_ips.get(n["name"]) for n in updated["nodes"]):
        remote.write_ssh_config(cluster, updated)
    sync_expires_tag(cluster, aws, list(found.values()))
    return st.load(cluster) or updated


def parse_until(text: str) -> DateTime:
    """Parse `--until`; it must end with `Z` or an explicit ±HH:MM offset."""
    value = text.strip()
    if not _UNTIL_TZ_RE.search(value):
        raise VsbenchError(f"--until {text} has no timezone", "use e.g. 2026-10-08T10:00:00Z or ...T12:00:00+02:00")
    try:
        return DateTime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError as err:
        raise VsbenchError(f"invalid --until {text}: {err}") from err


def _new_expiry(state: State, ttl_s: int | None, until: DateTime | None, shorten: bool) -> DateTime:
    """At least 15 min and at most MAX_TTL_SECONDS (7 d) from now; earlier than now only with --shorten."""
    if (ttl_s is None) == (until is None):
        raise VsbenchError("pass exactly one of --ttl or --until")
    if until is not None and until.tzinfo is None:
        raise VsbenchError("--until needs a timezone (Z or ±HH:MM)")
    target = (until or proc.utcnow() + datetime.timedelta(seconds=ttl_s or 0)).astimezone(datetime.timezone.utc)
    target = target.replace(microsecond=0)
    iso = proc.iso(target)
    if _seconds_left(target) < EXTEND_MIN_AHEAD_S:
        raise VsbenchError(f"the new expiry {iso} is less than 15 min from now")
    if _seconds_left(target) > config.MAX_TTL_SECONDS:  # keep-alive instances: the TTL is their only terminator
        limit = proc.format_duration(config.MAX_TTL_SECONDS)
        raise VsbenchError(f"the new expiry {iso} is more than {limit} from now", f"TTL limits are {TTL_LIMITS}")
    current = state.get("expires_at")
    if current and target < proc.parse_iso(current) and not shorten:
        raise VsbenchError(f"{iso} is before the current expiry {current}", "pass --shorten to shorten the TTL")
    return target


def _tag_expiry(aws: Aws | None, ids: list[str], target_iso: str) -> bool:
    """Set the ExpiresAt tag; False (warned) when it could not be set."""
    later = "the next list, status --refresh or refresh-ip with credentials re-syncs it"
    if aws is None:
        warn(f"ExpiresAt tags not updated (no AWS credentials); the on-node TTL is what counts; {later}")
        return False
    try:
        _ec2(aws, "create-tags", "--resources", *ids, "--tags", f"Key=ExpiresAt,Value={target_iso}")
        return True
    except VsbenchError as err:
        warn(f"the nodes were updated but not the ExpiresAt tag: {err} ({config.LOGIN_HINT}); {later}")
        return False


def extend(cluster: str, aws: Aws | None, ttl_seconds: int | None, until: DateTime | None, shorten: bool) -> State:
    """Move the TTL: nodes first (atomic write + read back), then the ExpiresAt tag (warn only), then state.

    A tag that could not be written is recorded as `expires_tag_pending` and re-synced later.
    """
    current = st.require(cluster)
    target = _new_expiry(current, ttl_seconds, until, shorten)
    target_iso, epoch, nodes = proc.iso(target), int(target.timestamp()), st.nodes(current)
    path = config.NODE_EXPIRES_FILE
    cmd = f"printf '%s\\n' {epoch} | sudo tee {path}.tmp >/dev/null && sudo mv {path}.tmp {path} && cat {path}"
    results = remote.run_many(cluster, [n["name"] for n in nodes], cmd, check=False, timeout=SSH_CMD_TIMEOUT_S)
    failed = [n["name"] for n in nodes if getattr(results.get(n["name"]), "stdout", "").strip() != str(epoch)]
    if failed:
        earliest = min(target_iso, current["expires_at"], key=proc.parse_iso)  # mixed nodes: the earlier one wins
        moved = {"expires_tag_pending": earliest} if earliest != current["expires_at"] else {}
        st.update(cluster, lambda s: s | {"expires_at": earliest} | moved)
        raise VsbenchError(f"the expiry was not updated on {', '.join(failed)}", "check ssh (refresh-ip?) and retry")
    tagged = _tag_expiry(aws, [n["instance_id"] for n in nodes], target_iso)
    log(f"cluster '{cluster}' now expires at {target_iso}")
    pending = None if tagged else target_iso
    return st.update(cluster, lambda s: s | {"expires_at": target_iso, "expires_tag_pending": pending})


def refresh_ip(cluster: str, aws: Aws) -> State:
    """Allow ssh from the current public IP only (after a VPN/WARP/roaming change)."""
    current = st.require(cluster)
    group_id, cidr = current["aws"]["security_group_id"], operator_cidr()
    found = _groups(aws, "--filters", _filter("group-id", [group_id]))  # a filter: a deleted SG is just absent
    if not found:
        raise VsbenchError(f"security group {group_id} not found", "the cluster may be gone: vsbench status --refresh")
    _sync_ssh_ingress(aws, found[0], cidr)
    log(f"ssh is now allowed from {cidr} on {group_id}")
    sync_expires_tag(cluster, aws)
    return st.update(cluster, lambda s: s | {"aws": s["aws"] | {"operator_cidr": cidr}})
