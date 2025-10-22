#!/usr/bin/env python3
# Plain Python 3, minimal imports, no type annotations.

import os
import sys
import json
import argparse
import random
import string

import yaml
import boto3
from botocore.exceptions import ClientError

VERBOSE = False
DRY_RUN = False


def log(msg):
    if VERBOSE:
        print("[INFO]", msg)


def dry(msg):
    if DRY_RUN:
        print("[DRY-RUN]", msg)


# =========================
# Library helpers (importable)
# =========================


def get_session(region):
    """
    Return a boto3.Session bound to the given region.
    Credentials are taken from the environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).
    """
    return boto3.Session(region_name=region)


def _resolve_vpc_id(ec2_client, explicit_vpc_id=None, subnet_id=None, sg_ids=None):
    """
    Try to determine the VPC context in this order:
      1) explicit_vpc_id
      2) VPC of the given subnet_id
      3) VPC of the first security-group id
    """
    if explicit_vpc_id:
        return explicit_vpc_id

    if subnet_id:
        try:
            resp = ec2_client.describe_subnets(SubnetIds=[subnet_id])
            subs = resp.get("Subnets", [])
            if subs:
                return subs[0].get("VpcId")
        except ClientError:
            pass

    if sg_ids:
        try:
            resp = ec2_client.describe_security_groups(GroupIds=[sg_ids[0]])
            sgs = resp.get("SecurityGroups", [])
            if sgs:
                return sgs[0].get("VpcId")
        except ClientError:
            pass

    return None


def _resolve_sg_ids(ec2_client, security_groups, vpc_id=None):
    """
    security_groups can be a list of group *names* or *IDs* (or a mix).
    If names are provided, we'll scope the lookup to vpc_id when given.
    Returns a list of Security Group IDs.
    """
    if not security_groups:
        return []

    ids = [g for g in security_groups if str(g).startswith("sg-")]
    names = [g for g in security_groups if not str(g).startswith("sg-")]

    if names:
        try:
            filters = [{"Name": "group-name", "Values": names}]
            if vpc_id:
                filters.append({"Name": "vpc-id", "Values": [vpc_id]})
            resp = ec2_client.describe_security_groups(Filters=filters)
            found = {
                sg["GroupName"]: sg["GroupId"] for sg in resp.get("SecurityGroups", [])
            }
            for n in names:
                if n not in found:
                    raise RuntimeError(
                        "Security group name '%s' not found in VPC %s"
                        % (n, vpc_id or "<any>")
                    )
                ids.append(found[n])
        except ClientError as e:
            raise RuntimeError("Failed to resolve security group names: %s" % e)
    return ids


def _resolve_subnet_id(
    ec2_client, az, subnet_id=None, subnets_map=None, use_default=False, vpc_id=None
):
    """
    Resolution order:
      1) explicit subnet_id (unless literal 'default')
      2) subnets_map[az] (unless literal 'default')
      3) default-for-az (scoped to vpc_id when given)
      4) ANY subnet in az (prefer same vpc_id if given)
    """
    if subnet_id and str(subnet_id).lower() != "default":
        return subnet_id

    if subnets_map and az in subnets_map:
        v = str(subnets_map[az]).lower()
        if v != "default":
            return subnets_map[az]
        # else fall through to default-for-az

    # try default-for-az
    try:
        filters = [
            {"Name": "availability-zone", "Values": [az]},
            {"Name": "default-for-az", "Values": ["true"]},
        ]
        if vpc_id:
            filters.append({"Name": "vpc-id", "Values": [vpc_id]})
        resp = ec2_client.describe_subnets(Filters=filters)
        subs = resp.get("Subnets", [])
        if subs:
            return subs[0]["SubnetId"]
    except ClientError as e:
        raise RuntimeError("Failed to query default subnets in %s: %s" % (az, e))

    # fallback: any subnet in the AZ (prefer same VPC)
    try:
        filters = [{"Name": "availability-zone", "Values": [az]}]
        if vpc_id:
            filters.append({"Name": "vpc-id", "Values": [vpc_id]})
        resp = ec2_client.describe_subnets(Filters=filters)
        subs = resp.get("Subnets", [])
        if subs:
            return subs[0]["SubnetId"]
    except ClientError as e:
        raise RuntimeError("Failed to query subnets in %s: %s" % (az, e))

    raise RuntimeError(
        "No subnet found in AZ %s. Provide general.subnets[%s] or subnet_id." % (az, az)
    )


def _validate_vpc_consistency(ec2_client, vpc_id, subnet_id, sg_ids):
    """
    Ensure subnet and SGs belong to the same VPC when vpc_id is known.
    """
    if not (vpc_id and subnet_id):
        return
    resp = ec2_client.describe_subnets(SubnetIds=[subnet_id])
    sub_vpc = resp["Subnets"][0]["VpcId"]
    if sub_vpc != vpc_id:
        raise RuntimeError("Subnet %s is in %s, not %s" % (subnet_id, sub_vpc, vpc_id))
    if sg_ids:
        resp = ec2_client.describe_security_groups(GroupIds=sg_ids)
        for sg in resp["SecurityGroups"]:
            sg_vpc = sg.get("VpcId")
            if sg_vpc and sg_vpc != vpc_id:
                raise RuntimeError(
                    "Security group %s is in %s, not %s"
                    % (sg["GroupId"], sg_vpc, vpc_id)
                )


def _normalize_tags(tags_or_labels, name=None, extras=None):
    """
    Accept dict or list of (k,v). Returns AWS TagSpecifications list for 'instance'.
    Adds Name tag if provided. Merges extras dict if provided.
    """
    tags = {}

    if isinstance(tags_or_labels, dict):
        tags.update(tags_or_labels)
    elif isinstance(tags_or_labels, list):
        for kv in tags_or_labels:
            if isinstance(kv, (list, tuple)) and len(kv) == 2:
                k, v = kv
                tags[str(k)] = str(v)

    if isinstance(extras, dict):
        tags.update(extras)

    if name:
        tags["Name"] = name

    if not tags:
        return None

    return [
        {
            "ResourceType": "instance",
            "Tags": [{"Key": str(k), "Value": str(v)} for k, v in tags.items()],
        }
    ]


def run_instance(session, cfg):
    """
    Launch a single EC2 instance and return enough info to use and delete it.

    Required keys in cfg:
      - instance_type
      - availability_zone
      - ami
      - key_name

    Typical keys:
      - security_groups           (list of names or IDs)  [ignored if 'network_interface' used]
      - vpc_id
      - subnet_id                 (or general.subnets[az] / 'default' / auto)
      - subnets_map               (dict AZ -> subnetId or 'default')
      - associate_public_ip       (bool)
      - role, cluster_id, name, tags/labels
      - iam_instance_profile_arn, user_data
      - volume_size_gb, volume_type, delete_on_termination, ebs_optimized
      - metadata_options, cpu_options, hibernation

    Advanced NI options (mutually exclusive with the auto NI):
      - network_interface: { network_interface_id: eni-... }  # attach existing ENI (device index 0)
      - network_interfaces: [ { device_index, subnet_id, groups, ... }, ... ]  # full NI spec

    Returns a dict with enough info to later terminate and to build cluster.yml.
    """
    role = cfg.get("role")
    az = cfg.get("availability_zone")
    ami = cfg["ami"]
    instance_type = cfg["instance_type"]
    cluster_id = cfg.get("cluster_id")
    name = cfg.get("name")

    dry(
        f"Would launch {role or 'instance'} in {az} "
        f"(AMI={ami}, type={instance_type}, name={name}, cluster={cluster_id})"
    )

    # DRY RUN short-circuit: do not call AWS at all.
    if DRY_RUN:
        return {
            "InstanceId": f"i-dryrun-{random.randint(10000,99999)}",
            "PrivateIpAddress": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
            "PublicIpAddress": None,
            "AvailabilityZone": az,
            "SubnetId": f"subnet-dryrun-{az}",
            "SecurityGroupIds": ["sg-dryrun"],
            "KeyName": cfg.get("key_name"),
            "State": "running",
            "Role": role,
            "ClusterId": cluster_id,
        }

    log(f"Launching {role or 'instance'} ({ami}, {instance_type}) in {az}")
    ec2_client = session.client("ec2")

    key_name = cfg["key_name"]
    # Inputs (may be None)
    sg_input = cfg.get("security_groups", [])
    explicit_vpc = cfg.get("vpc_id")
    subnet_id_input = cfg.get("subnet_id")

    # ENI paths (if provided, we bypass normal SG/Subnet resolution)
    eni_cfg = cfg.get("network_interface")  # single existing ENI
    enis_cfg = cfg.get("network_interfaces")  # full list of NI specs

    # If no ENI provided, resolve SGs/Subnet with VPC awareness
    sg_ids = None
    subnet_id = None
    vpc_id = None

    if not eni_cfg and not enis_cfg:
        # First pass: if we already know VPC explicitly, scope SG resolution with it
        sg_ids = _resolve_sg_ids(ec2_client, sg_input, vpc_id=explicit_vpc)
        # Determine VPC context (explicit -> subnet -> SG)
        vpc_id = _resolve_vpc_id(
            ec2_client,
            explicit_vpc_id=explicit_vpc,
            subnet_id=subnet_id_input,
            sg_ids=sg_ids,
        )
        # Final SG resolution once VPC is known (ensures name->id is correct)
        sg_ids = _resolve_sg_ids(ec2_client, sg_input, vpc_id=vpc_id)
        # Subnet selection
        subnet_id = _resolve_subnet_id(
            ec2_client,
            az,
            subnet_id=subnet_id_input,
            subnets_map=cfg.get("subnets_map"),
            use_default=bool(cfg.get("use_default_subnet", False)),
            vpc_id=vpc_id,
        )
        _validate_vpc_consistency(ec2_client, vpc_id, subnet_id, sg_ids)

    extras = {}
    if role:
        extras["role"] = role
    if cluster_id:
        extras["cluster_id"] = cluster_id

    tag_specs = _normalize_tags(
        cfg.get("tags") or cfg.get("labels"),
        name=cfg.get("name"),
        extras=extras,
    )

    kwargs = {
        "ImageId": ami,
        "InstanceType": instance_type,
        "MinCount": 1,
        "MaxCount": 1,
        "Placement": {"AvailabilityZone": az},
        "KeyName": key_name,
    }

    # Choose NI mode
    if eni_cfg:
        # Attach existing ENI
        kwargs["NetworkInterfaces"] = [
            {
                "DeviceIndex": 0,
                "NetworkInterfaceId": eni_cfg["network_interface_id"],
            }
        ]
    elif enis_cfg:
        # Full custom NI list
        ni_list = []
        for ni in enis_cfg:
            item = {"DeviceIndex": int(ni.get("device_index", 0))}
            if "network_interface_id" in ni:
                item["NetworkInterfaceId"] = ni["network_interface_id"]
            else:
                if "subnet_id" in ni:
                    item["SubnetId"] = ni["subnet_id"]
                if "groups" in ni:
                    # Resolve SG names within the provided/explicit VPC if any
                    item["Groups"] = _resolve_sg_ids(
                        ec2_client, ni["groups"], vpc_id=explicit_vpc
                    )
                if "associate_public_ip_address" in ni:
                    item["AssociatePublicIpAddress"] = bool(
                        ni["associate_public_ip_address"]
                    )
                if "private_ip_address" in ni:
                    item["PrivateIpAddress"] = ni["private_ip_address"]
                if "secondary_private_ip_address_count" in ni:
                    item["SecondaryPrivateIpAddressCount"] = int(
                        ni["secondary_private_ip_address_count"]
                    )
            ni_list.append(item)
        kwargs["NetworkInterfaces"] = ni_list
    else:
        # Standard single NI with resolved subnet + SGs
        kwargs["NetworkInterfaces"] = [
            {
                "DeviceIndex": 0,
                "SubnetId": subnet_id,
                "Groups": sg_ids,
                "AssociatePublicIpAddress": bool(cfg.get("associate_public_ip", True)),
            }
        ]

    if tag_specs:
        kwargs["TagSpecifications"] = tag_specs
    if cfg.get("iam_instance_profile_arn"):
        kwargs["IamInstanceProfile"] = {"Arn": cfg["iam_instance_profile_arn"]}
    if cfg.get("user_data"):
        kwargs["UserData"] = cfg["user_data"]
    if cfg.get("ebs_optimized") is not None:
        kwargs["EbsOptimized"] = bool(cfg["ebs_optimized"])
    if cfg.get("metadata_options"):
        kwargs["MetadataOptions"] = cfg["metadata_options"]
    if cfg.get("cpu_options"):
        kwargs["CpuOptions"] = cfg["cpu_options"]
    if cfg.get("hibernation") is not None:
        kwargs["HibernationOptions"] = {"Configured": bool(cfg["hibernation"])}

    if (
        cfg.get("volume_size_gb")
        or cfg.get("volume_type")
        or cfg.get("delete_on_termination") is not None
    ):
        bdm = [
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "DeleteOnTermination": bool(cfg.get("delete_on_termination", True)),
                },
            }
        ]
        if cfg.get("volume_size_gb"):
            bdm[0]["Ebs"]["VolumeSize"] = int(cfg["volume_size_gb"])
        if cfg.get("volume_type"):
            bdm[0]["Ebs"]["VolumeType"] = cfg["volume_type"]
        kwargs["BlockDeviceMappings"] = bdm

    try:
        resp = ec2_client.run_instances(**kwargs)
        inst = resp["Instances"][0]
        instance_id = inst["InstanceId"]
    except ClientError as e:
        raise RuntimeError("run_instances failed: %s" % e)

    waiter = ec2_client.get_waiter("instance_running")
    waiter.wait(InstanceIds=[instance_id])

    desc = ec2_client.describe_instances(InstanceIds=[instance_id])
    i = desc["Reservations"][0]["Instances"][0]

    info = {
        "InstanceId": i["InstanceId"],
        "PrivateIpAddress": i.get("PrivateIpAddress"),
        "PublicIpAddress": i.get("PublicIpAddress"),
        "AvailabilityZone": i["Placement"]["AvailabilityZone"],
        "SubnetId": i.get("SubnetId"),
        "SecurityGroupIds": [g["GroupId"] for g in i.get("SecurityGroups", [])],
        "KeyName": i.get("KeyName"),
        "State": i.get("State", {}).get("Name"),
        "Role": role,
        "ClusterId": cluster_id,
    }
    return info


def delete_instance(session, instance_id, wait=True):
    """
    Terminate a single instance by ID. If wait=True, block until terminated.
    """
    dry(f"Would terminate instance {instance_id}")
    if DRY_RUN:
        return
    log(f"Terminating instance {instance_id}")
    ec2_client = session.client("ec2")
    try:
        ec2_client.terminate_instances(InstanceIds=[instance_id])
    except ClientError as e:
        raise RuntimeError("terminate_instances failed: %s" % e)
    if wait:
        waiter = ec2_client.get_waiter("instance_terminated")
        waiter.wait(InstanceIds=[instance_id])


# =========================
# CLI utilities
# =========================

STATE_FILE = ".run_cluster_state.json"
CLUSTER_YML_OUT = "cluster.yml"


def _load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _save_state(cluster_id, instances):
    state = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            state = {}
    state[cluster_id] = instances
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _load_state(cluster_id):
    if not os.path.exists(STATE_FILE):
        return []
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        return []
    return state.get(cluster_id, [])


def _random_cluster_id():
    sfx = "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(6)
    )
    return "cluster-" + sfx


def _describe_by_cluster(ec2_client, cluster_id):
    filters = [
        {"Name": "tag:cluster_id", "Values": [cluster_id]},
        {
            "Name": "instance-state-name",
            "Values": ["pending", "running", "stopping", "stopped"],
        },
    ]
    resp = ec2_client.describe_instances(Filters=filters)
    out = []
    for r in resp.get("Reservations", []):
        for i in r.get("Instances", []):
            role = None
            for t in i.get("Tags", []):
                if t["Key"] == "role":
                    role = t["Value"]
            out.append(
                {
                    "InstanceId": i["InstanceId"],
                    "Role": role,
                    "PrivateIpAddress": i.get("PrivateIpAddress"),
                    "PublicIpAddress": i.get("PublicIpAddress"),
                    "AvailabilityZone": i["Placement"]["AvailabilityZone"],
                    "State": i.get("State", {}).get("Name"),
                }
            )
    return out


def _build_cluster_yml(instances):
    """
    Role-agnostic cluster.yml:

    cluster:
      - az: <az>
        <role1>:
          - { private: 10.0.1.10, public: 44.55.66.77 }
          - { private: 10.0.1.11, public: null }
        <role2>:
          - { private: 10.0.2.20, public: 3.4.5.6 }
        ...

    Notes:
    - Only 'running' instances are included.
    - Role comes from the 'role' tag.
    - Both private and public may be null if not assigned; we still emit both keys.
    """
    by_az = {}

    for it in instances:
        if it.get("State") != "running":
            continue

        az = it.get("AvailabilityZone")
        role = it.get("Role")
        priv = it.get("PrivateIpAddress")
        pub = it.get("PublicIpAddress")

        if not az or not role:
            continue

        by_az.setdefault(az, {}).setdefault(role, []).append(
            {"private": priv, "public": pub}
        )

    # Emit sorted by AZ and role for determinism
    cluster = []
    for az in sorted(by_az.keys()):
        entry = {"az": az}
        for role in sorted(by_az[az].keys()):
            # (Optional) stable order inside a role: sort by private IP then public
            items = by_az[az][role]
            items = sorted(
                items,
                key=lambda x: (
                    str(x.get("private")) if x.get("private") is not None else "",
                    str(x.get("public")) if x.get("public") is not None else "",
                ),
            )
            entry[role] = items
        cluster.append(entry)

    return {"cluster": cluster}


def _write_cluster_yml(struct, out_path=CLUSTER_YML_OUT):
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(struct, f, sort_keys=False)
    return out_path


# =========================
# Start / Kill / Show actions
# =========================


def action_start_from_yaml(cfg_path):
    cfg = _load_yaml(cfg_path)

    general = cfg.get("general", {}) or {}
    types = cfg.get("types", {}) or {}
    cluster_entries = cfg.get("cluster", []) or []

    region = general.get("region")
    if not region:
        raise SystemExit("general.region is required")

    session = get_session(region)

    cluster_id = general.get("cluster_id") or _random_cluster_id()
    key_name = general.get("key_name")
    # Allow SGs as IDs or names at the *general* level (can be overridden per type/entry)
    security_groups_general = general.get("security_groups", [])
    subnets_map = general.get("subnets", {})  # values may be subnet-ids or "default"
    use_default_subnet = bool(general.get("use_default_subnet", False))
    associate_public_ip_default = bool(general.get("associate_public_ip", True))
    vpc_id_general = general.get("vpc_id")

    if not key_name:
        raise SystemExit("general.key_name is required")
    if not types:
        raise SystemExit("types: {} is required (define per-role defaults)")
    if not cluster_entries:
        raise SystemExit("cluster: [] is required (list of {az, type, count})")

    launched = []

    # Optional keys allowed to propagate from type/entry
    OPTIONAL_KEYS = (
        "iam_instance_profile_arn",
        "user_data",
        "volume_size_gb",
        "volume_type",
        "delete_on_termination",
        "ebs_optimized",
        "metadata_options",
        "cpu_options",
        "hibernation",
        "tags",
        "labels",
        "associate_public_ip",
        "subnet_id",
        "vpc_id",
        "security_groups",
        "network_interface",
        "network_interfaces",
    )

    for entry in cluster_entries:
        role = entry.get("type")
        az = entry.get("az")
        count = int(entry.get("count", 1))

        if not role or not az:
            raise SystemExit("Each cluster item must have 'type' and 'az'")
        if role not in types:
            raise SystemExit(
                "Unknown type '%s' in cluster item; define it under types:" % role
            )

        spec = types[role]
        ami = spec.get("ami")
        instance_type = spec.get("instance_type")
        name_template = spec.get("name_template", f"{role}-{{az}}-{{index}}")

        if not ami or not instance_type:
            raise SystemExit(
                "Type '%s' missing 'ami' or 'instance_type' in types section" % role
            )

        # Merge optional params: general defaults (SGs/VPC) -> type -> entry
        merged = {
            "vpc_id": vpc_id_general,
            "security_groups": security_groups_general,
        }
        for k in OPTIONAL_KEYS:
            if k in general:
                merged[k] = general[k]
        for k in OPTIONAL_KEYS:
            if k in spec:
                merged[k] = spec[k]
        for k in OPTIONAL_KEYS:
            if k in entry:
                merged[k] = entry[k]

        # Final associate_public_ip
        associate_public_ip = merged.get(
            "associate_public_ip", associate_public_ip_default
        )

        for idx in range(1, count + 1):
            try:
                name = name_template.format(
                    az=az, index=idx, type=role, cluster_id=cluster_id
                )
            except KeyError as e:
                raise SystemExit(
                    "name_template for type '%s' references unknown key: %s" % (role, e)
                )

            base_cfg = {
                "instance_type": instance_type,
                "availability_zone": az,
                "ami": ami,
                "key_name": key_name,
                "role": role,
                "cluster_id": cluster_id,
                "subnets_map": subnets_map,
                "use_default_subnet": use_default_subnet,
                "associate_public_ip": associate_public_ip,
                "name": name,
            }

            # Bring merged optionals into base_cfg
            for k, v in merged.items():
                base_cfg[k] = v

            info = run_instance(session, base_cfg)
            launched.append(info)

    _save_state(cluster_id, launched)
    struct = _build_cluster_yml(launched)

    if DRY_RUN:
        dry("Would write cluster.yml with %d instances" % len(launched))
        print(yaml.safe_dump(struct, sort_keys=False))
        return

    outp = _write_cluster_yml(struct)
    print("Cluster ID:", cluster_id)
    print("Instances launched:", len(launched))
    print("cluster.yml written to:", outp)


def action_start_from_args(args):
    required = ["region", "type", "az", "ami", "instance_type", "key_name"]
    for r in required:
        if not getattr(args, r):
            raise SystemExit("Missing --%s" % r)

    session = get_session(args.region)
    cluster_id = args.cluster_id or _random_cluster_id()

    # SGs can arrive via --security-groups/--sg (names or IDs)
    security_groups = args.security_groups or []
    if args.sg:
        security_groups.extend(args.sg)
    if not security_groups and not args.network_interface_id and not args.eni_json:
        raise SystemExit("At least one security group (or an ENI) is required")

    count = int(args.count or 1)
    subnets_map = {}
    if args.subnet_map_json:
        try:
            subnets_map = json.loads(args.subnet_map_json)
        except Exception:
            raise SystemExit("--subnet-map-json must be JSON")

    # Optional ENI controls
    eni_cfg = None
    enis_cfg = None
    if args.network_interface_id:
        eni_cfg = {"network_interface_id": args.network_interface_id}
    if args.eni_json:
        try:
            enis_cfg = json.loads(args.eni_json)
            if not isinstance(enis_cfg, list):
                raise ValueError
        except Exception:
            raise SystemExit("--eni-json must be a JSON array of NI specs")

    base_cfg = {
        "instance_type": args.instance_type,
        "availability_zone": args.az,
        "ami": args.ami,
        "key_name": args.key_name,
        "role": args.type,
        "cluster_id": cluster_id,
        "subnets_map": subnets_map,  # may contain "default"
        "use_default_subnet": bool(args.use_default_subnet),
        "associate_public_ip": bool(args.public_ip),
        "vpc_id": args.vpc_id,
        "security_groups": security_groups,
    }

    if args.subnet_id:
        base_cfg["subnet_id"] = args.subnet_id
    if args.user_data_file:
        with open(args.user_data_file, "r", encoding="utf-8") as f:
            base_cfg["user_data"] = f.read()
    if args.iam_instance_profile_arn:
        base_cfg["iam_instance_profile_arn"] = args.iam_instance_profile_arn
    if args.volume_size_gb:
        base_cfg["volume_size_gb"] = int(args.volume_size_gb)
    if args.volume_type:
        base_cfg["volume_type"] = args.volume_type
    if eni_cfg:
        base_cfg["network_interface"] = eni_cfg
    if enis_cfg:
        base_cfg["network_interfaces"] = enis_cfg

    launched = []
    for i in range(count):
        base_cfg["name"] = "%s-%s-%d" % (args.type, args.az, i + 1)
        info = run_instance(session, base_cfg)
        launched.append(info)

    _save_state(cluster_id, launched)
    struct = _build_cluster_yml(launched)
    if DRY_RUN:
        dry("Would write cluster.yml with %d instances" % len(launched))
        print(yaml.safe_dump(struct, sort_keys=False))
        return
    outp = _write_cluster_yml(struct)
    print("Cluster ID:", cluster_id)
    print("Instances launched:", len(launched))
    print("cluster.yml written to:", outp)


def action_kill(args):
    if not args.region:
        raise SystemExit("--region is required")
    if not args.cluster_id:
        raise SystemExit("kill requires --cluster-id")
    dry(
        "Would describe or terminate instances tagged with cluster_id=%s"
        % args.cluster_id
    )
    if DRY_RUN:
        return
    session = get_session(args.region)
    ec2_client = session.client("ec2")

    instances = _describe_by_cluster(ec2_client, args.cluster_id)
    if not instances:
        print("No instances found for cluster_id:", args.cluster_id)
        return

    ids = [i["InstanceId"] for i in instances]
    print("Terminating %d instances..." % len(ids))
    for iid in ids:
        delete_instance(session, iid, wait=False)

    waiter = ec2_client.get_waiter("instance_terminated")
    waiter.wait(InstanceIds=ids)
    print("Terminated:", ids)


def action_show(args):
    if not args.region:
        raise SystemExit("--region is required")
    if not args.cluster_id:
        raise SystemExit("show requires --cluster-id")
    dry("Would describe instances tagged with cluster_id=%s" % args.cluster_id)
    if DRY_RUN:
        return
    session = get_session(args.region)
    ec2_client = session.client("ec2")
    instances = _describe_by_cluster(ec2_client, args.cluster_id)
    if not instances:
        print("No instances found for cluster_id:", args.cluster_id)
        return

    print(json.dumps(instances, indent=2))

    struct = _build_cluster_yml(instances)
    outp = _write_cluster_yml(struct)
    print("cluster.yml written to:", outp)


# =========================
# Main
# =========================


def main():
    global VERBOSE, DRY_RUN

    parser = argparse.ArgumentParser(
        description="Start/Kill/Show EC2 instances for a test cluster and emit cluster.yml"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Print detailed progress logs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without doing it",
    )

    sub = parser.add_subparsers(dest="action", required=True)

    # start
    p_start = sub.add_parser(
        "start", help="Start instances (from YAML config or CLI args)"
    )
    p_start.add_argument(
        "--config",
        help="Path to YAML config with 'general', 'types', 'cluster' sections",
    )

    # CLI quick start (single batch)
    p_start.add_argument("--region")
    p_start.add_argument(
        "--type", help="Role type (e.g., scylla|vector|client|monitor)"
    )
    p_start.add_argument("--az", help="Availability Zone (e.g., us-east-1a)")
    p_start.add_argument("--count", help="Number of instances to start", default="1")
    p_start.add_argument("--ami")
    p_start.add_argument("--instance-type")
    p_start.add_argument("--key-name")
    p_start.add_argument(
        "--security-groups", nargs="*", help="Security group names or IDs"
    )
    p_start.add_argument("--sg", nargs="*", help="Alias for --security-groups")
    p_start.add_argument("--subnet-id")
    p_start.add_argument(
        "--subnet-map-json", help='JSON mapping of AZ->subnetId or "default"'
    )
    p_start.add_argument("--user-data-file")
    p_start.add_argument("--iam-instance-profile-arn")
    p_start.add_argument("--volume-size-gb")
    p_start.add_argument("--volume-type")
    p_start.add_argument("--public-ip", action="store_true", default=True)
    p_start.add_argument("--no-public-ip", action="store_false", dest="public_ip")
    p_start.add_argument(
        "--cluster-id",
        help="Optional cluster_id to tag instances; auto-generated if omitted",
    )
    p_start.add_argument(
        "--use-default-subnet",
        action="store_true",
        help="Launch in the AZ's default subnet (ignores --subnet-id unless it's not 'default')",
    )
    p_start.add_argument(
        "--vpc-id", help="Scope SG name lookups and subnet validation to this VPC"
    )
    # ENI options
    p_start.add_argument(
        "--network-interface-id", help="Attach an existing ENI as device 0"
    )
    p_start.add_argument(
        "--eni-json", help="JSON array of full network interface specs"
    )

    # kill
    p_kill = sub.add_parser(
        "kill", help="Terminate all instances tagged with a cluster_id"
    )
    p_kill.add_argument("--region", required=True)
    p_kill.add_argument("--cluster-id", required=True)

    # show
    p_show = sub.add_parser(
        "show", help="Show instances for a cluster_id and write cluster.yml"
    )
    p_show.add_argument("--region", required=True)
    p_show.add_argument("--cluster-id", required=True)

    args = parser.parse_args()
    VERBOSE = args.verbose
    DRY_RUN = args.dry_run

    if args.action == "start":
        if args.config:
            action_start_from_yaml(args.config)
        else:
            action_start_from_args(args)

    elif args.action == "kill":
        action_kill(args)

    elif args.action == "show":
        action_show(args)


if __name__ == "__main__":
    main()
