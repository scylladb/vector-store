#!/usr/bin/env python3
# Plain Python 3, minimal imports, no type annotations.

import os
import sys
import argparse
import yaml
import shlex
import subprocess
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

dry_run = False
# ===== Stubs you asked for (no implementation) =====
SSH_KEY_PATH = "TEST_SSH_KEY_PATH"
verbose = False
def trace_verbose(*arg):
    if verbose:
        print(*arg)


def run(template_cmd, shell=False):
    cmd = template_cmd #.format(_HOME=HOME_DIR, _USER=USER, _COPY_OR_MOVE=COPY_OR_MOVE)
    trace_verbose(cmd)
    if dry_run:
        return
    if not shell:
        cmd = shlex.split(cmd)
    try:
        res = subprocess.check_output(cmd, shell=shell)
        return res
    except Exception as e:
        print("Error while running:")
        print(cmd,end =" ")
        print(e.output)
        raise
def sh_command_with_args(*cmnds):
    trace_verbose(cmnds)
    if dry_run:
        return
    print(cmnds)
    p = subprocess.Popen(cmnds, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    out, err = p.communicate()
    return out, err

def run_remote(ip, cmd):
    """Run a shell command on a remote node (via SSH)."""
    sh_command_with_args("./remote_run.sh", SSH_KEY_PATH, ip, cmd)

def upload(ip, files):
    """Upload a list of local files/paths to a remote node."""
    sh_command_with_args("./upload.sh", SSH_KEY_PATH, ip, files)
# ===================================================


# ---------- Helpers ----------
def here():
    return os.path.dirname(os.path.abspath(__file__))

def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def path_join(*a):
    return os.path.join(*a)

def exists(p):
    return os.path.exists(p)

def list_all_files(root_dir):
    """Return a list of file paths under root_dir (recursively).
    If root_dir doesn't exist, return empty list."""
    paths = []
    if not exists(root_dir):
        return paths
    for base, _dirs, files in os.walk(root_dir):
        for fn in files:
            paths.append(path_join(base, fn))
    return paths

def role_dir(base, role):
    return path_join(base, role)

def collect_role_files(base_dir, role):
    # Upload all files from common/ + <role>/
    common_dir = role_dir(base_dir, "common")
    role_specific_dir = role_dir(base_dir, role)
    files = list_all_files(common_dir) + list_all_files(role_specific_dir)
    return files

def parse_cluster(cluster_dict):
    """
    cluster.yml example:
    cluster:
      - scylla: [ip1, ip2, ip3]
        vector: ipX
        client: [ipY, ipZ]
        az: us-east-1a
      - scylla: [ip4, ip5]
        vector: ipW
        client: [ipA, ipB]
        az: us-east-1b
      - monitor: ipM
    """
    zones = []
    monitor_ip = None

    raw = cluster_dict.get("cluster", [])
    return raw

def load_params(params_path):
    if not params_path:
        return {}
    return load_yaml(params_path)


# ---------- Generated artifacts ----------
def write_generated_topology(out_dir, cluster_dict, params_dict):
    ensure_dir(out_dir)
    topo_path = path_join(out_dir, "cluster_topology.yml")
    payload = {
        "cluster": cluster_dict.get("cluster", []),
        "params": params_dict
    }
    with open(topo_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, sort_keys=False)
    return topo_path

def upload_generated_files(generated_dir):
    print("Uploading generated files", os.listdir(generated_dir))
    ips = [f for f in os.listdir(generated_dir) if os.path.isdir(os.path.join(generated_dir, f)) and f not in ('.', '.. ')]
    print("ips:", ips)
    for ip in ips:
        trace_verbose(f"Uploading generated files to {ip}")
        upload(ip, generated_dir + "/"+ ip + "/*")  # upload all generated files

def upload_apps(zones, apps):
    for z in zones:
        # scylla nodes
        for service in ["scylla", "client", "vector", "monitor"]:
            if not apps or service in apps:
                if service == "scylla":
                    for ip in z.get("scylla", []):
                        upload(ip['public'], "common" + "/*")
                        upload(ip['public'], service + "/*")  # upload all files for the role
                elif service in z and z[service]:
                    ip = z[service][0]
                    upload(ip['public'], "common" + "/*")
                    upload(ip['public'], service + "/*")  # upload all files for the role

def generate_monitor_config(base_dir, zones, params):
    trace_verbose("Generating monitor config")
    scylla_nodes = []
    vector_nodes = []
    for z in zones:
        for ip in z.get("scylla", []):
            scylla_nodes.append(ip['private'])
        v = z.get("vector")
        if v:
            vector_nodes.append(v[0]['private'])
    scylla = [{"labels": {"cluster": "test-cluster", "dc": "dc"}, "targets": scylla_nodes}]
    vector = [{"labels": {"cluster": "test-cluster", "dc": "dc"}, "targets": vector_nodes}]
    # Write to config file

    run_path = path_join(base_dir, "monitor", "scylla_servers.yml")
    with open(run_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(scylla, f, sort_keys=False)

    run_path = path_join(base_dir, "monitor", "vector_servers.yml")
    with open(run_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(vector, f, sort_keys=False)

    run_path = path_join(base_dir, "monitor", "start-cmd.sh")
    version = params["scylla"]["version"].split(":")[1]
    version = ".".join(version.split(".")[:2])
    promdir = params.get("monitor", {}).get("promdir", "prometheus_data")
    with open(run_path, "w", encoding="utf-8") as f:
        f.write("#!/bin/bash\n")
        f.write("./generate-dashboards.sh -F -v {VERSION}\n".format(VERSION=version))
        f.write("./start-all.sh -v {VERSION} --vector-search vector_search_servers.yml --target-directory targets -d {PROM_DIR}\n".format(VERSION=version, PROM_DIR=promdir))
    os.chmod(run_path, 0o755)

def generate_vector_config(base_dir, zones, generated_dir, params):
    trace_verbose("Generating vector config")
    for z in zones:
        if "vector" in z and "scylla" in z:
            run_path = path_join(base_dir, generated_dir + "/" + z["vector"][0]['public'], "start-cmd.sh")
            os.makedirs(path_join(base_dir, generated_dir, z["vector"][0]['public']), exist_ok=True)
            with open(run_path, "w", encoding="utf-8") as f:
                f.write("#!/bin/bash\n")
                f.write("cd /home/ubuntu/vector-store\n")
                f.write("export VECTOR_STORE_URI=0.0.0.0:6080\n")
                f.write("export VECTOR_STORE_SCYLLADB_URI={IP}:9042\n".format(IP=z["scylla"][0]['private']))
                f.write("./vector-store\n")
    os.chmod(run_path, 0o755)

def generate_scylla_config(base_dir, zones, generated_dir, params):
    trace_verbose("Generating scylla config")
    seed = ""
    commands= {"--batch-size-warn-threshold-in-kb": "1024"}
    config = {
        "services": {
            "scylladb": {
                "container_name": "my-vector-search-scylla-container",
                "image": "scylladb/{VERSION}".format(VERSION=params["scylla"]["version"]),
                "healthcheck": {
                    "test": ["CMD", "cqlsh", "-e", "select * from system.local WHERE key='local'"],
                    "interval": "1s",
                    "timeout": "5s",
                    "retries": 60
                },
                "ports": [
                    "7000:7000",
                    "7001:7001",
                    "7199:7199",
                    "9042:9042",
                    "9160:9160",
                    "10000:10000",
                    "19042:19042"
                ],
                "volumes": [
                    "/mnt/data/scylla:/var/lib/scylla"
                ]
            }
        }
    }

    for z in zones:
        if "vector" in z and "scylla" in z:
            for ip_struct in z["scylla"]:
                ip = ip_struct['private']
                if not seed:
                    seed = ip
                public_ip = ip_struct['public']
                compose = path_join(base_dir, generated_dir, public_ip, "docker-compose.yml")
                os.makedirs(path_join(base_dir, generated_dir, public_ip), exist_ok=True)
                commands["--vector-store-primary-uri"]  = "http://{IP}:6080".format(IP=z["vector"][0]['private'])
                commands["--broadcast-address"] = ip
                commands["--seeds"] = seed
                command = " ".join(f"{k.strip()}={v.strip()}" for k, v in commands.items())
                config["services"]["scylladb"]["command"] = command
                with open(compose, "w", encoding="utf-8") as f:
                    yaml.safe_dump(config, f, sort_keys=False)

def generate_config(base_dir, zones, generated_dir, params, apps):
    if not apps or "monitor" in apps:
        generate_monitor_config(base_dir, zones, params)
    if not apps or "scylla" in apps:
        generate_scylla_config(base_dir, zones, generated_dir, params)
    if not apps or "vector" in apps:
        generate_vector_config(base_dir, zones, generated_dir, params)

# ---------- Stage actions ----------
def stage_install(base_dir, generated_dir, zones, params, apps):
    # For each node, upload files for its role and run install.sh
    # Roles: scylla, vector, client, monitor
    generate_config(base_dir, zones, generated_dir, params, apps)
    for s in params:
        if "version" in params[s]:
            env_path = path_join(base_dir, s, "env.sh")
            with open(env_path, "w", encoding="utf-8") as f:
                f.write("export VERSION={VERSION}\n".format(VERSION=params[s]["version"]))
    upload_apps(zones, apps)
    upload_generated_files(generated_dir)
    for z in zones:
        # scylla nodes
        if (not apps or "scylla" in apps):
            for ip_struct in z.get("scylla", []):
                ip = ip_struct['public']
                trace_verbose(f"Installing scylla on {ip}")
                run_remote(ip, '~/install-python.sh')
                run_remote(ip, '~/install.sh')
        for service in ["client", "vector", "monitor"]:
            if service in z and z[service] and (not apps or service in apps):
                ip = z[service][0]['public']
                trace_verbose(f"Installing {service} on {ip}")
                run_remote(ip, '~/install-python.sh')
                run_remote(ip, '~/install.sh')

def stage_remote_install(zones, apps):
    for z in zones:
        # scylla nodes
        if not apps or "scylla" in apps:
            for ip_struct in z.get("scylla", []):
                ip = ip_struct['public']
                trace_verbose(f"configure scylla on {ip}")
                run_remote(ip, 'install.sh')
        for service in ["scylla", "client", "vector", "monitor"]:
            if service in z and z[service] and (not apps or service in apps):
                ip = z[service][0]['public']
                trace_verbose(f"Installing {service} on {ip}")
                run_remote(ip, 'install.sh')


def stage_run_cluster(zones, apps):
    # Start the cluster components (scylla, vector, monitor)
    for z in zones:
        if not apps or "scylla" in apps:
            for ip in z.get("scylla", []):
                run_remote(ip['public'], "run.sh")
        for service in ["vector", "monitor"]:
            v = z.get(service)
            if v and (not apps or service in apps):
                run_remote(v['public'], "run.sh")

def stage_run_load(zones):
    # Start client-side load on all client nodes
    for z in zones:
        for ip in z.get("client", []):
            # You mentioned run_test.sh for tests.
            run_remote(ip['public'], "bash run_test.sh")


# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser(
        description="Install, configure, and run a Scylla + vector + monitor test on EC2 nodes."
    )
    parser.add_argument(
        "--key",
        required=True,
        help="Path to SSH private key file."
    )
    parser.add_argument("-V", "--verbose", action="store_true", default=False, help="Verbose trace mode")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Dry run mode")
    parser.add_argument(
        "--cluster",
        default="cluster.yml",
        help="Path to cluster.yml (condensed cluster description)."
    )
    parser.add_argument(
        "--params",
        default="params.yml",
        help="Path to params.yml (general parameters)."
    )
    parser.add_argument(
        "-a",
        "--app",
        action='append',
        choices=["scylla", "vector", "client", "monitor"],
        help="Limit to a specific one or more applications."
    )
    parser.add_argument(
        "stage",
        choices=["install", "configure", "run-cluster", "run-load"],
        help="Which stage to execute."
    )

    args = parser.parse_args()
    if args.verbose:
        global verbose
        verbose = True
    if args.dry_run:
        global dry_run
        dry_run = True
    base = here()  # directory containing this script
    # Layout: common/, scylla/, vector/, client/, monitor/
    cluster_path = path_join(base, args.cluster)
    if not exists(cluster_path):
        print("ERROR: cluster.yml not found at:", cluster_path, file=sys.stderr)
        sys.exit(1)

    params_path = path_join(base, args.params) if args.params else None
    cluster_dict = load_yaml(cluster_path)
    params_dict = load_params(params_path) if params_path else {}

    zones = parse_cluster(cluster_dict)
    # Make a place for generated files that configure.sh will use
    generated_dir = path_join(base, "generated")
    ensure_dir(generated_dir)
    global SSH_KEY_PATH
    SSH_KEY_PATH = args.key
    if args.stage in ("install"):
        stage_install(base, generated_dir, zones, params_dict, args.app)

    if args.stage in ("configure"):
        stage_remote_install(zones, args.app)

    if args.stage in ("run-cluster"):
        stage_run_cluster(zones, args.app)

    if args.stage in ("run-load"):
        stage_run_load(zones)


if __name__ == "__main__":
    main()
