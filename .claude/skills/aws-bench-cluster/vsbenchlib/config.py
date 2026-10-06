# Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Defaults and pinned versions. See reference.md#upgrading-pins before bumping."""

from __future__ import annotations

from pathlib import Path

# --- PINS (bump here only; re-verify per reference.md#upgrading-pins) ----------
# Verified on 2026-10-06.
NODE_EXPORTER_VERSION = "1.12.1"
NODE_EXPORTER_SHA256_ARM64 = "ad35b605f9954b9f1ffddf5ba054bdc5a98d790b9eae5291e1eeb83f1ecbd0e7"
SCYLLA_MONITORING_VERSION = "4.16.1"
SCYLLA_MONITORING_SHA256 = "884d2e51f8541b75995007b6ed12c478e2906d72216bb9907b7635f6b81db230"
UBUNTU_AMI_SSM = "/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id"
# The cross image is rust:<channel of the built tree's rust-toolchain.toml>-bookworm.
CROSS_IMAGE_REPO = "vs-bench-cross"
# Highest symbol versions a binary may need (ubi9-minimal / release builds).
MAX_GLIBC = "2.34"
MAX_GLIBCXX = "3.4.29"

# --- Paths in this skill --------------------------------------------------------
SKILL_DIR = Path(__file__).resolve().parent.parent
NODE_SCRIPTS_DIR = SKILL_DIR / "node"
CROSS_DIR = SKILL_DIR / "cross"
DATASETS_FILE = SKILL_DIR / "datasets.json"

# --- AWS account / auth -----------------------------------------------------------
# rnd-core-lab; the profile name is what gimme-aws-creds writes with
# `cred_profile = acc-role` (see reference.md#aws-access).
DEFAULT_PROFILE = "797456418907-DeveloperAccessRole"
DEFAULT_REGION = "us-east-1"
EXPECTED_ACCOUNT = "797456418907"
GIMME_ROLE_ARN = "arn:aws:iam::797456418907:role/DeveloperAccessRole"
LOGIN_HINT = (
    f"run in the background: gimme-aws-creds --username <user e-mail> --roles {GIMME_ROLE_ARN} </dev/null "
    "and give the user the okta.com/activate URL it prints (see SKILL.md rule 6)"
)
INSTALL_HINT = (
    "install the AWS CLI v2 (curl -fsSL https://awscli.amazonaws.com/v2/install.sh | bash) and "
    "gimme-aws-creds (uv tool install gimme-aws-creds --with keyrings.alt, or pipx); "
    "see reference.md#aws-access"
)
# Commands that create or delete AWS resources need this much credential lifetime.
MIN_CREDENTIALS_SECONDS = 30 * 60

# --- Cluster shape ----------------------------------------------------------------
ROLES = ("scylla", "vs", "client")
DEFAULT_INSTANCE_TYPES = {"scylla": "i8g.2xlarge", "vs": "r8g.4xlarge", "client": "r8g.2xlarge"}
DEFAULT_DISK_GB = {"scylla": 50, "vs": 50, "client": 200}
DEFAULT_TTL = "24h"
MIN_TTL_SECONDS = 15 * 60
MAX_TTL_SECONDS = 7 * 24 * 3600
DEFAULT_BILLING_PROJECT = "Vector Search: Sharding"
# The Vector Search projects in scylladb/finops billing_projects/projects.yaml (2026-10). `up` warns
# about any other value (a typo would bill a project that does not exist); new ones come from Cloud FinOps.
BILLING_PROJECTS = (
    "Vector Search: Alternator API",
    "Vector Search: DiskANN",
    "Vector Search: Filtering",
    "Vector Search: Full Text Search",
    "Vector Search: Quantization",
    "Vector Search: Quantization 2.0",
    "Vector Search: Sharding",
    "Vector Search: Snapshotting",
)
CAPACITY_ERRORS = (
    "InsufficientInstanceCapacity",
    "InsufficientCapacity",
    "InsufficientHostCapacity",
    "Unsupported",
)
SCYLLA_DC = "datacenter1"
SCYLLA_CLUSTER_NAME = "vsbench"

# Approximate us-east-1 on-demand Linux $/h (instances.vantage.sh, 2026-10).
# Used for the cost estimate and the budget warnings; unknown types show "?" (budget check incomplete).
PRICES = {
    "i8g.xlarge": 0.343,
    "i8g.2xlarge": 0.686,
    "i8g.4xlarge": 1.373,
    "i8g.8xlarge": 2.746,
    "r8g.xlarge": 0.236,
    "r8g.2xlarge": 0.471,
    "r8g.4xlarge": 0.943,
    "r8g.8xlarge": 1.885,
    "r8g.16xlarge": 3.770,
}
# Not verified against the price list: linear in vCPU from the 4xlarge price (i8g 1.373, r8g 0.943 per
# 16 vCPU), which is how both families are priced. Good enough for a budget warning, not for billing.
PRICES |= {"i8g.12xlarge": 4.119, "i8g.16xlarge": 5.492, "i8g.24xlarge": 8.238, "i8g.48xlarge": 16.476}
PRICES |= {"r8g.12xlarge": 2.829, "r8g.24xlarge": 5.658, "r8g.48xlarge": 11.316}
# finops: personal budget per owner and per billing project (USD/h).
PERSONAL_BUDGET_PER_HOUR = 10.0
PROJECT_BUDGET_PER_HOUR = 20.0

# --- Images / repos ---------------------------------------------------------------
SCYLLA_NIGHTLY_REPO = "scylladb/scylla-nightly"
SCYLLA_RELEASE_REPO = "scylladb/scylla"
VS_DOCKER_REPO = "scylladb/vector-store"
VS_GIT_URL = "https://github.com/scylladb/vector-store.git"
VS_UPSTREAM_REMOTE_RE = r"[:/]scylladb/vector-store(\.git)?/?$"
VS_RELEASES_API = "https://api.github.com/repos/scylladb/vector-store/releases/latest"
DOCKER_HUB_NIGHTLY_LATEST = "https://hub.docker.com/v2/namespaces/scylladb/repositories/scylla-nightly/tags/latest"

# Precomputed io_properties (scylla-machine-image common/aws_io_params.yaml) for
# single-disk instance types: read_bandwidth, read_iops, write_bandwidth, write_iops.
# Other types run iotune (`--io-setup 1`) on every Scylla start.
IO_PROPERTIES = {
    "i8g.xlarge": (1057916224, 150059, 825837440, 82508),
    "i8g.2xlarge": (2133338752, 300481, 1662360448, 165126),
    "i8g.4xlarge": (4266699264, 572927, 3368528896, 330686),
}

# --- Ports and node paths ---------------------------------------------------------
PORT_CQL = 9042
PORT_VS = 6080
PORT_NODE_EXPORTER = 9100
PORT_SCYLLA_METRICS = 9180
PORT_PROMETHEUS = 9090
PORT_GRAFANA = 3000
SCRAPE_INTERVAL_S = 10

NODE_HOME = "/var/lib/vsbench"
NODE_SCRIPTS = f"{NODE_HOME}/scripts"
NODE_JOBS = f"{NODE_HOME}/jobs"
NODE_READY_MARKER = f"{NODE_HOME}/ready"
NODE_FAILED_MARKER = f"{NODE_HOME}/failed"
NODE_EXPIRES_FILE = "/etc/vsbench/expires_at"
NODE_VS_DIR = "/opt/vector-store"
NODE_BENCH_DIR = f"{NODE_HOME}/bench"
NODE_DATASETS_DIR = f"{NODE_HOME}/datasets"
NODE_MONITORING_DIR = f"{NODE_HOME}/monitoring"

# --- Foreground waits ---------------------------------------------------------------
# The agent's Bash tool kills commands after 10 minutes; stay below it and exit 75.
DEFAULT_FOREGROUND_SECONDS = 540
