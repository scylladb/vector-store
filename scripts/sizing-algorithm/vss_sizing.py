"""
ScyllaDB Vector Similarity Search (VSS) Sizing Algorithm
======================================================

Calculates infrastructure requirements for deploying ScyllaDB Vector Search,
covering three main components:

1. **Vector Store Node Baseline** - RAM needed to hold the HNSW (USearch)
   index entirely in memory.
2. **Throughput Sizing** - vCPUs required on Vector Store nodes to meet a
   target QPS at P99 ≤ 15 ms latency.
3. **ScyllaDB Node Sizing** - vCPUs and storage for the ScyllaDB nodes that
   back the Vector Store nodes.

The formulas and heuristics are derived from internal ScyllaDB sizing
guidelines and benchmarks.

Input Parameters
----------------
- **num_vectors** (int) - Total number of vectors to be indexed (N).
  Min: 10 000, Max: 4 000 000 000, Default: 50 000 000.
  UI note: should be visualized on a logarithmic scale.
- **dimensions** (int) - Number of dimensions per vector embedding.
  Min: 1, Max: 16 000, Default: 1 536.
  Common values: 128 (image features), 768 (BERT), 1536 (OpenAI), 4096 (LLMs).
- **target_qps** (int) - Desired queries-per-second at P99 <= 15 ms latency.
  Min: 10, Max: 1 000 000, Default: 1 000.
- **recall** (int) - Target recall accuracy as a percentage.
  Min: 70, Max: 99, Default: 95.
  Higher recall dramatically reduces throughput per vCPU.
- **k** (int) - Number of nearest-neighbour results per query.
  Min: 1, Max: 1 000, Default: 10.
  K=100 halves the throughput compared to K=10.
- **quantization** (Quantization) - Compression strategy for vector elements:
  NONE (float32, 1x), 
  SCALAR (uint8, ~3x memory savings), or
  BINARY (1-bit, ~10x memory savings).
  Default: NONE.
  SCALAR quantization doubles the effective K for throughput sizing because of oversampling.
  BINARY quantization quadruples the effective QPS for throughput sizing because of rescoring
  so that the system needs more compute resources to achieve the same latency.
- **metadata_bytes_per_vector** (int) - Average payload stored alongside each
  embedding on ScyllaDB nodes (used only for ScyllaDB-node storage calculation).
  Min: 4, Max: 1 048 576 (1 MiB), Default: 100.
  UI note: should be visualized on a logarithmic scale.
- **filtering_columns** (int) - Number of filtering columns used in queries.
  Each column adds 30 bytes per vector to Vector Store-node RAM.
  All reasonable filtering fields should be smaller.
  Min: 0, Max: 20, Default: 0.

References
----------
- ScyllaDB Cloud Vector Search docs:
  https://cloud.docs.scylladb.com/stable/vector-search/index.html
- HNSW paper: Malkov & Yashunin, 2018
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum


# ---------------------------------------------------------------------------
# Enums & constants
# ---------------------------------------------------------------------------

class CloudProvider(Enum):
    """Cloud provider for instance selection.

    * ``AWS`` - Amazon Web Services instances
    * ``GCP`` - Google Cloud Platform instances
    """
    AWS = "aws"
    GCP = "gcp"


class Quantization(Enum):
    """Vector element quantization strategy.

    Each variant maps to a *compression ratio* that reduces the per-vector
    memory footprint relative to ``float32`` storage.

    * ``NONE``   - full ``float32`` (4 bytes per dimension), ratio = 1x
    * ``SCALAR`` - scalar quantisation to ``uint8`` (1 byte per dim), ratio = 3x
    * ``BINARY`` - 1-bit quantisation (1 bit per dim), ratio = 10x

    For throughput sizing:
    * ``SCALAR`` - the effective K is doubled (re-ranking overhead)
    * ``BINARY`` - the effective QPS requirement is quadrupled (re-ranking
      overhead)
    """
    NONE = "none"
    SCALAR = "scalar"
    BINARY = "binary"


# Known data points for recall -> QPS multiplier (linear interpolation).
# Higher recall demands more compute per query.
_RECALL_QPS_POINTS: list[tuple[int, float]] = [
    (70, 2.0),
    (90, 1.0),
    (95, 0.40),
    (99, 0.02),
]

# Base QPS/vCPU at P99 ≤ 15 ms, K=10, recall ≤ 90 %.
# Bucket boundaries are defined by vector count.  QPS/vCPU is linearly
# interpolated between consecutive bucket endpoints.
_QPS_BUCKETS: list[tuple[str, int, int, int]] = [
    # (label, max_vectors, max_dims, base_qps_per_vcpu)
    ("low-medium",        5_000_000, 1024, 350),
    ("medium-large",    100_000_000, 1024, 250),
    ("large-xlarge",  1_000_000_000, 1024, 170),
    ("xlarge+",       4_000_000_000, 1024,  80),
]

# Ratio of Vector Store-node vCPUs to ScyllaDB-node vCPUs.
VS_TO_SCYLLADB_VCPU_RATIO = 6

# ScyllaDB-node replication factor (always 3 for production).
REPLICATION_FACTOR = 3

# Overhead factor applied to raw index size (metadata, alignment, etc.).
INDEX_OVERHEAD_FACTOR = 1.1

# Bytes per float32 element.
FLOAT32_BYTES = 4

# Minimum number of Vector Store node replicas for production availability.
MIN_VECTOR_STORE_REPLICAS = 2

# When the initial (smallest-fit) selection requires more than this many
# instances, the algorithm tries larger instance types to reduce the count.
INSTANCE_UPSCALE_TRIGGER = 3

# Maximum cost increase factor when upscaling instance size.  The total
# hourly cost after upscaling must not exceed this factor times the initial
# cost (e.g. 1.1 means at most 10 % more expensive).
INSTANCE_UPSCALE_COST_FACTOR = 1.1

# Dimensionality bounds.
MIN_DIMENSIONS = 1
MAX_DIMENSIONS = 16_000

# Minimum HNSW M parameter (connectivity).
MIN_M = 4

# RAM overhead per filtering column per vector (bytes).
FILTERING_COLUMN_BYTES_PER_VECTOR = 30

# Filtering columns bounds.
MIN_FILTERING_COLUMNS = 0
MAX_FILTERING_COLUMNS = 20

# Vector count bounds.
MAX_VECTORS = 4_000_000_000
MIN_VECTORS = 10_000

# Metadata bytes bounds.
MAX_METADATA_BYTES = 1_048_576   # 1 MiB
MIN_METADATA_BYTES = 4

# K bounds.
MIN_K = 1
MAX_K = 1_000

# QPS bounds (non-zero).
MAX_QPS = 1_000_000
MIN_QPS = 10

# Recall bounds.
MIN_RECALL = 70
MAX_RECALL = 99

# Compression ratios for quantization strategies.
SCALAR_COMPRESSION_RATIO = 3.0
BINARY_COMPRESSION_RATIO = 10.0

# Default values for SizingInput fields.
DEFAULT_NUM_VECTORS = 50_000_000
DEFAULT_DIMENSIONS = 1_536
DEFAULT_TARGET_QPS = 1_000
DEFAULT_RECALL = 95
DEFAULT_K = 10
DEFAULT_METADATA_BYTES = 100


# ---------------------------------------------------------------------------
# Available instance types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class InstanceType:
    """A cloud instance type available for Vector Store-node deployment."""
    name: str
    vcpus: int
    ram_gb: float
    cost_per_hour: float
    cost_per_hour_yearly: float

# ScyllaDB Cloud currently supports the following instance
# types for Vector Store nodes.
# AWS prices as of 2026-02-26 (us-east-1 region).
AWS_INSTANCES: list[InstanceType] = [
    InstanceType("t4g.medium",     2,    4,    0.067,    0.0392),
    InstanceType("r7g.medium",     1,    8,    0.107,    0.0662),
    InstanceType("r7g.large",      2,   16,    0.214,    0.1322),
    InstanceType("r7g.xlarge",     4,   32,    0.4285,   0.2644),
    InstanceType("r7g.2xlarge",    8,   64,    0.857,    0.529),
    InstanceType("r7g.4xlarge",   16,  128,    1.7135,   1.058),
    InstanceType("r7g.8xlarge",   32,  256,    3.427,    2.116),
    InstanceType("r7g.12xlarge",  48,  384,    5.141,    3.174),
    InstanceType("r7g.16xlarge",  64,  512,    6.8545,   4.232),
    InstanceType("r7i.24xlarge",  96,  768,   12.701,    7.8416),
    InstanceType("r7i.48xlarge", 192, 1536,   25.4015,  15.6828),
]

# GCP prices as of 2026-02-26 (us-east1 region).
GCP_INSTANCES: list[InstanceType] = [
    InstanceType("e2-medium",      2,    4,    0.067,    0.06701142),
    InstanceType("n4-highmem-2",   2,   16,    0.238,    0.14996),
    InstanceType("n4-highmem-4",   4,   32,    0.476,    0.29992),
    InstanceType("n4-highmem-8",   8,   64,    0.952,    0.59984),
    InstanceType("n4-highmem-16", 16,  128,    1.9045,   1.19968),
    InstanceType("n4-highmem-32", 32,  256,    3.8085,   2.39936),
    InstanceType("n4-highmem-48", 48,  384,    5.713,    3.59904),
    InstanceType("n4-highmem-64", 64,  512,    7.6175,   4.79872),
    InstanceType("n4-highmem-80", 80,  640,    9.5215,   5.9984),
]

# Default instance list (AWS) for backward compatibility.
AVAILABLE_INSTANCES: list[InstanceType] = AWS_INSTANCES


def get_instances(cloud_provider: CloudProvider = CloudProvider.AWS) -> list[InstanceType]:
    """Return the list of available instances for the given cloud provider."""
    if cloud_provider is CloudProvider.GCP:
        return GCP_INSTANCES
    return AWS_INSTANCES


# ---------------------------------------------------------------------------
# Input parameters
# ---------------------------------------------------------------------------

@dataclass
class SizingInput:
    """All user-supplied parameters needed to compute a VSS sizing estimate.

    Parameters
    ----------
    num_vectors : int
        Total number of vectors to be indexed (N).
        Min: 10 000, Max: 4 000 000 000, Default: 50 000 000.
    dimensions : int
        Number of dimensions per vector embedding.
        Min: 1, Max: 16 000, Default: 1 536.
    target_qps : int
        Desired queries-per-second at P99 ≤ 15 ms latency.
        Min: 10, Max: 1 000 000, Default: 1 000.
    recall : int
        Target recall accuracy as a percentage.
        Min: 70, Max: 99, Default: 90.
    k : int
        Number of nearest-neighbour results returned per query.
        Min: 1, Max: 1 000, Default: 10.
    quantization : Quantization
        Compression strategy applied to vector elements (default ``NONE``).
    metadata_bytes_per_vector : int
        Average additional payload stored alongside each embedding on the
        ScyllaDB nodes (used only for ScyllaDB-node storage calculation).
        Min: 4, Max: 1 048 576 (1 MiB), Default: 100.
    filtering_columns : int
        Number of filtering columns.  Each column adds 30 bytes per vector
        to Vector Store-node RAM.  Min: 0, Max: 20, Default: 0.
    """
    num_vectors: int = DEFAULT_NUM_VECTORS
    dimensions: int = DEFAULT_DIMENSIONS
    target_qps: int = DEFAULT_TARGET_QPS
    recall: int = DEFAULT_RECALL
    k: int = DEFAULT_K
    quantization: Quantization = Quantization.NONE
    metadata_bytes_per_vector: int = DEFAULT_METADATA_BYTES
    filtering_columns: int = MIN_FILTERING_COLUMNS
    cloud_provider: CloudProvider = CloudProvider.AWS

    def __post_init__(self) -> None:
        if not MIN_VECTORS <= self.num_vectors <= MAX_VECTORS:
            raise ValueError(
                f"num_vectors must be between {MIN_VECTORS:,} and {MAX_VECTORS:,}"
            )
        if not MIN_DIMENSIONS <= self.dimensions <= MAX_DIMENSIONS:
            raise ValueError(
                f"dimensions must be between {MIN_DIMENSIONS} and {MAX_DIMENSIONS}"
            )
        if not MIN_QPS <= self.target_qps <= MAX_QPS:
            raise ValueError(
                f"target_qps must be between "
                f"{MIN_QPS:,} and {MAX_QPS:,}"
            )
        if not MIN_RECALL <= self.recall <= MAX_RECALL:
            raise ValueError(
                f"recall must be between {MIN_RECALL} and {MAX_RECALL}"
            )
        if not MIN_K <= self.k <= MAX_K:
            raise ValueError(f"k must be between {MIN_K} and {MAX_K}")
        if not MIN_METADATA_BYTES <= self.metadata_bytes_per_vector <= MAX_METADATA_BYTES:
            raise ValueError(
                f"metadata_bytes_per_vector must be between "
                f"{MIN_METADATA_BYTES} and {MAX_METADATA_BYTES:,}"
            )
        if not MIN_FILTERING_COLUMNS <= self.filtering_columns <= MAX_FILTERING_COLUMNS:
            raise ValueError(
                f"filtering_columns must be between {MIN_FILTERING_COLUMNS} and "
                f"{MAX_FILTERING_COLUMNS}"
            )


# ---------------------------------------------------------------------------
# Output / result
# ---------------------------------------------------------------------------

@dataclass
class HNSWParams:
    """Resolved HNSW index configuration parameters."""
    m: int


@dataclass
class VectorStoreNodeSizing:
    """Sizing result for a single VSS (uSearch) Vector Store node replica."""
    index_ram_bytes: int
    filtering_ram_bytes: int
    total_ram_bytes: int
    total_ram_gb: float
    required_vcpus: int
    base_qps_per_vcpu: float
    effective_qps_per_vcpu: float
    throughput_bucket: str


@dataclass
class InstanceSelection:
    """Result of the Vector Store-node instance type selection.

    The algorithm picks the cheapest instance configuration that satisfies
    both the per-replica RAM requirement (every replica holds the full HNSW
    index) and the aggregate vCPU requirement (query load is distributed
    across replicas).  At least ``MIN_VECTOR_STORE_REPLICAS`` instances are
    always provisioned for high availability.
    """
    instance_type: InstanceType
    num_instances: int
    total_vcpus: int
    total_ram_gb: float
    total_cost_per_hour: float
    total_cost_per_hour_yearly: float


@dataclass
class ScyllaDBNodeSizing:
    """Sizing result for the ScyllaDB-node tier."""
    total_vcpus: int
    vcpus_per_node: int
    num_nodes: int
    total_storage_bytes: int
    total_storage_gb: float
    embedding_storage_bytes: int
    metadata_storage_bytes: int


@dataclass
class SizingResult:
    """Complete VSS sizing recommendation."""
    input: SizingInput
    hnsw_params: HNSWParams
    compression_ratio: float
    vector_store_node: VectorStoreNodeSizing
    scylladb_node: ScyllaDBNodeSizing
    instance_selection: InstanceSelection
    vector_store_replicas: int
    summary: str = field(default="", repr=False)


# ---------------------------------------------------------------------------
# Core algorithm
# ---------------------------------------------------------------------------

def _round_up_m(n: int | float) -> int:
    """Round *n* up to the next power of two, clamped to ``MIN_M``."""
    if n <= MIN_M:
        return MIN_M
    return 2 ** math.ceil(math.log2(n))


def _estimate_m(num_vectors: int, recall: int) -> int:
    """Estimate the HNSW M parameter from the dataset size and target recall.

    Formula::

        M = 1.5 * log10(N) * (-ln(1 - Recall))

    rounded up to the smallest power of two >= the raw estimate.

    The required graph connectivity is driven by two factors:

    * **Graph Size (N)** — In a Navigable Small World the number of hops
      required to traverse the graph scales logarithmically with the number
      of nodes.

    * **The "Long Tail" of Recall (R)** — Achieving 80 % recall is easy;
      pushing from 95 % to 99 % is exponentially harder because you are
      trying to capture the edge-case vectors hidden deep in local minima.
      This behaves asymptotically.
    """
    recall_fraction = recall / 100.0
    m_raw = 1.5 * math.log10(max(num_vectors, 1)) * (
        -math.log(1.0 - recall_fraction)
    )
    return _round_up_m(m_raw)


def _get_compression_ratio(quantization: Quantization) -> float:
    """Return the effective compression ratio for the given quantization.

    * ``NONE``   -> 1x (no compression)
    * ``SCALAR`` -> 3x memory savings
    * ``BINARY`` -> 10x memory savings
    """
    if quantization is Quantization.NONE:
        return 1.0
    if quantization is Quantization.SCALAR:
        return SCALAR_COMPRESSION_RATIO
    if quantization is Quantization.BINARY:
        return BINARY_COMPRESSION_RATIO
    raise ValueError(f"Unknown quantization: {quantization}")


def _compute_index_ram(
    num_vectors: int,
    dimensions: int,
    m: int,
    compression_ratio: float,
) -> int:
    """Compute the RAM (in bytes) required to store the HNSW index.

    Formula (from the sizing guide):
        RAM = N x 4 x (Dim + M x ln(N) / ln(M x 0.5)) x 1.1 / compression

    The ``x 4`` factor accounts for float32 (4 bytes) per element, and the
    ``x 1.1`` overhead covers uSearch metadata, alignment, and bookkeeping.

    Returns
    -------
    int
        RAM in bytes (rounded up to the nearest byte).
    """
    n = num_vectors
    d = dimensions
    ln_n = math.log(n) if n > 1 else 0.0
    # Guard against log(0) when M*0.5 < 1 (shouldn't happen in practice).
    denom = math.log(m * 0.5) if m * 0.5 > 1 else 1.0
    graph_component = m * ln_n / denom
    raw = n * FLOAT32_BYTES * (d + graph_component) * INDEX_OVERHEAD_FACTOR
    return math.ceil(raw / compression_ratio)


def _get_base_qps_per_vcpu(
    num_vectors: int,
    dimensions: int,
) -> tuple[str, float]:
    """Return (bucket_label, base_QPS_per_vCPU) for the dataset profile.

    QPS/vCPU is linearly interpolated between consecutive bucket endpoints.
    Vectors at or below the first bucket boundary receive that bucket's QPS.
    Vectors at or above the last bucket boundary receive the last bucket's QPS.

    When *dimensions* exceeds the bucket's reference dimensionality (1024),
    the QPS is scaled down proportionally: ``qps * (ref_dims / dimensions)``.
    Dimensions at or below 1024 have no effect.
    """
    # Below or at the first bucket boundary -> flat first-bucket QPS.
    first_label, first_max, first_ref_dims, first_qps = _QPS_BUCKETS[0]
    if num_vectors <= first_max:
        dim_factor = min(1.0, first_ref_dims / dimensions)
        return first_label, float(first_qps) * dim_factor

    # Interpolate between consecutive bucket endpoints.
    for i in range(1, len(_QPS_BUCKETS)):
        label, cur_max, cur_ref_dims, cur_qps = _QPS_BUCKETS[i]
        prev_max = _QPS_BUCKETS[i - 1][1]
        prev_qps = _QPS_BUCKETS[i - 1][3]
        if num_vectors <= cur_max:
            fraction = (num_vectors - prev_max) / (cur_max - prev_max)
            qps = prev_qps + (cur_qps - prev_qps) * fraction
            dim_factor = min(1.0, cur_ref_dims / dimensions)
            return label, qps * dim_factor

    # At or beyond the last bucket -> last bucket's QPS.
    last = _QPS_BUCKETS[-1]
    dim_factor = min(1.0, last[2] / dimensions)
    return last[0], float(last[3]) * dim_factor


def _recall_qps_multiplier(recall: int) -> float:
    """Interpolate the QPS multiplier for a given recall percentage.

    For recall ≤ 90 %, the multiplier is capped at 1.0 (baseline).
    For recall between known data points, linear interpolation is used.
    """
    if recall <= _RECALL_QPS_POINTS[0][0]:
        return _RECALL_QPS_POINTS[0][1]

    for i in range(len(_RECALL_QPS_POINTS) - 1):
        r_lo, m_lo = _RECALL_QPS_POINTS[i]
        r_hi, m_hi = _RECALL_QPS_POINTS[i + 1]
        if r_lo <= recall <= r_hi:
            fraction = (recall - r_lo) / (r_hi - r_lo)
            return m_lo + (m_hi - m_lo) * fraction

    # Beyond last known point — use the last value.
    return _RECALL_QPS_POINTS[-1][1]


def _effective_qps_per_vcpu(
    base_qps: float,
    recall: int,
    k: int,
) -> float:
    """Adjust the base QPS/vCPU for recall target and K value.

    - Higher recall tiers reduce throughput per vCPU (interpolated).
    - K = 100 halves the throughput compared to K = 10; intermediate K
      values are interpolated linearly.
    """
    recall_factor = _recall_qps_multiplier(recall)

    # K adjustment: K=10 -> factor 1.0, K=100 -> factor 0.5, linear between.
    if k <= 10:
        k_factor = 1.0
    elif k >= 100:
        k_factor = 0.5
    else:
        k_factor = 1.0 - 0.5 * (k - 10) / 90.0

    return base_qps * recall_factor * k_factor


def _select_instance(
    index_ram_gb: float,
    required_vcpus: int,
    cloud_provider: CloudProvider = CloudProvider.AWS,
) -> InstanceSelection:
    """Select the instance configuration for Vector Store-node replicas.

    The algorithm works in two phases:

    1. **Smallest-fit selection** — find the *smallest* instance type whose
       RAM is at least *index_ram_gb* (every replica holds the full HNSW
       index in memory).  Compute how many replicas are needed to meet the
       aggregate *required_vcpus* (at least ``MIN_VECTOR_STORE_REPLICAS``
       for high availability).

    2. **Upscale optimisation** — if the initial selection requires more than
       ``INSTANCE_UPSCALE_TRIGGER`` instances, try progressively larger
       instance types.  Accept a larger instance when the total hourly cost
       does not exceed ``INSTANCE_UPSCALE_COST_FACTOR`` times the initial
       cost **and** the number of instances is strictly reduced.

    When more than two replicas are needed, the count is rounded up to the
    next multiple of 3 so that replicas can be evenly distributed across
    availability zones.

    Raises
    ------
    ValueError
        If no available instance has enough RAM for the index.
    """
    instances = get_instances(cloud_provider)

    # Sort by RAM ascending (vCPUs as tiebreaker) so that eligible[0] is
    # the smallest instance that can hold the index.
    sorted_instances = sorted(instances, key=lambda i: (i.ram_gb, i.vcpus))
    eligible = [i for i in sorted_instances if i.ram_gb >= index_ram_gb]

    if not eligible:
        raise ValueError(
            f"No available instance has enough RAM ({index_ram_gb:.2f} GB) "
            f"for the HNSW index. Consider using quantization to reduce "
            f"memory requirements."
        )

    def _build_candidate(inst: InstanceType) -> InstanceSelection:
        if required_vcpus > 0 and inst.vcpus > 0:
            num = max(
                MIN_VECTOR_STORE_REPLICAS,
                math.ceil(required_vcpus / inst.vcpus),
            )
        else:
            num = MIN_VECTOR_STORE_REPLICAS

        # When more than 2 replicas are needed, round up to the next
        # multiple of 3 for even distribution across availability zones.
        if num > MIN_VECTOR_STORE_REPLICAS and num % 3 != 0:
            num = num + (3 - num % 3)

        total_cost = num * inst.cost_per_hour
        total_cost_yearly = num * inst.cost_per_hour_yearly

        return InstanceSelection(
            instance_type=inst,
            num_instances=num,
            total_vcpus=num * inst.vcpus,
            total_ram_gb=round(num * inst.ram_gb, 2),
            total_cost_per_hour=round(total_cost, 2),
            total_cost_per_hour_yearly=round(total_cost_yearly, 2),
        )

    # Phase 1: start with the smallest eligible instance.
    best = _build_candidate(eligible[0])

    # Phase 2: if too many instances are needed, try larger ones to reduce
    # the instance count while staying within the cost threshold.
    if best.num_instances > INSTANCE_UPSCALE_TRIGGER:
        initial_cost = best.num_instances * eligible[0].cost_per_hour
        max_cost = initial_cost * INSTANCE_UPSCALE_COST_FACTOR

        for inst in eligible[1:]:
            candidate = _build_candidate(inst)
            candidate_cost = candidate.num_instances * inst.cost_per_hour
            if (candidate_cost <= max_cost
                    and candidate.num_instances < best.num_instances):
                best = candidate

    return best


def compute_sizing(inp: SizingInput) -> SizingResult:
    """Run the full 3-step VSS sizing algorithm.

    Steps
    -----
    1. **Vector Store-node baseline** - compute RAM per Vector Store-node
       replica.
    2. **Throughput sizing** - compute vCPUs required on Vector Store nodes.
    3. **ScyllaDB-node sizing** - compute vCPUs and storage for ScyllaDB
       nodes.

    Parameters
    ----------
    inp : SizingInput
        All user-supplied sizing parameters.

    Returns
    -------
    SizingResult
        Complete infrastructure recommendation.
    """
    # --- Resolve HNSW M parameter ---
    m = _estimate_m(inp.num_vectors, inp.recall)
    hnsw = HNSWParams(m=m)

    # --- Step 1: Vector Store node baseline (RAM) ---
    compression = _get_compression_ratio(inp.quantization)
    index_ram = _compute_index_ram(
        inp.num_vectors, inp.dimensions, m, compression,
    )
    filtering_ram = (
        inp.filtering_columns * FILTERING_COLUMN_BYTES_PER_VECTOR
        * inp.num_vectors
    )
    total_search_ram = index_ram + filtering_ram
    total_search_ram_gb = total_search_ram / (1024 ** 3)

    # --- Step 2: Throughput sizing (Vector Store node vCPUs) ---
    bucket_label, base_qps = _get_base_qps_per_vcpu(
        inp.num_vectors, inp.dimensions,
    )

    # For SCALAR quantization, size as if K is twice as large (re-ranking).
    effective_k = (
        inp.k * 2 if inp.quantization is Quantization.SCALAR else inp.k
    )
    eff_qps = _effective_qps_per_vcpu(base_qps, inp.recall, effective_k)

    # For BINARY quantization, size as if QPS is 4x larger (re-ranking).
    # The system needs more compute to achieve the same latency because of
    # the additional overhead of rescoring more candidates.
    # K adjustment is not applied on top of this because its irrelevant
    # comparing to the QPS increase.
    effective_target_qps = (
        inp.target_qps * 4
        if inp.quantization is Quantization.BINARY
        else inp.target_qps
    )

    vs_vcpus = math.ceil(effective_target_qps / eff_qps) if eff_qps > 0 else 0

    vector_store_node = VectorStoreNodeSizing(
        index_ram_bytes=index_ram,
        filtering_ram_bytes=filtering_ram,
        total_ram_bytes=total_search_ram,
        total_ram_gb=round(total_search_ram_gb, 2),
        required_vcpus=vs_vcpus,
        base_qps_per_vcpu=base_qps,
        effective_qps_per_vcpu=round(eff_qps, 2),
        throughput_bucket=bucket_label,
    )

    # --- Step 2b: Instance selection (Vector Store nodes) ---
    instance_sel = _select_instance(
        total_search_ram_gb, vs_vcpus, inp.cloud_provider,
    )

    # --- Step 3: ScyllaDB node sizing ---
    embedding_storage = inp.num_vectors * inp.dimensions * FLOAT32_BYTES
    metadata_storage = inp.num_vectors * inp.metadata_bytes_per_vector
    total_storage = embedding_storage + metadata_storage
    total_storage_gb = total_storage / (1024 ** 3)

    data_total_vcpus = max(
        1, math.ceil(vs_vcpus / VS_TO_SCYLLADB_VCPU_RATIO),
    )
    # Distribute across replication factor; each node needs a fair share.
    vcpus_per_node = max(
        1, math.ceil(data_total_vcpus / REPLICATION_FACTOR),
    )
    # Re-derive actual total so it's consistent.
    actual_total_vcpus = vcpus_per_node * REPLICATION_FACTOR

    scylladb_node = ScyllaDBNodeSizing(
        total_vcpus=actual_total_vcpus,
        vcpus_per_node=vcpus_per_node,
        num_nodes=REPLICATION_FACTOR,
        total_storage_bytes=total_storage,
        total_storage_gb=round(total_storage_gb, 2),
        embedding_storage_bytes=embedding_storage,
        metadata_storage_bytes=metadata_storage,
    )

    # --- Build human-readable summary ---
    summary_lines = [
        "=== ScyllaDB VSS Sizing Summary ===",
        "",
        f"Dataset: {inp.num_vectors:,} vectors x {inp.dimensions} dimensions",
        f"Quantization: {inp.quantization.value}"
        f"  (compression ratio: {compression}x)",
        f"Recall target: ~{inp.recall}%  |  K = {inp.k}",
        f"Target QPS: {inp.target_qps:,}  (P99 ≤ 15 ms)",
        f"Filtering columns: {inp.filtering_columns}",
        "",
        "--- HNSW Parameters (computed) ---",
        f"  M              = {hnsw.m}",
        "",
        "--- Vector Store Nodes (computed, per replica) ---",
        f"  Index RAM      = {index_ram / (1024**3):.2f} GB",
        f"  Filtering RAM  = {filtering_ram / (1024**3):.2f} GB",
        f"  Total RAM      = {vector_store_node.total_ram_gb:.2f} GB",
        f"  vCPUs required = {vector_store_node.required_vcpus}",
        f"  QPS/vCPU       = {vector_store_node.effective_qps_per_vcpu:.1f}"
        f"  (base {vector_store_node.base_qps_per_vcpu:.1f},"
        f" bucket: {bucket_label})",
        f"  Replicas       = {instance_sel.num_instances}",
        f"  Instance type  = {instance_sel.instance_type.name}",
        f"  Total cost     = ${instance_sel.total_cost_per_hour:.2f}/hr"
        f" (on-demand), ${instance_sel.total_cost_per_hour_yearly:.2f}/hr"
        f" (1-yr commit)",
        "",
        "--- ScyllaDB Nodes (computed) ---",
        f"  Nodes          = {scylladb_node.num_nodes}"
        f"  (RF={REPLICATION_FACTOR})",
        f"  vCPUs / node   = {scylladb_node.vcpus_per_node}",
        f"  Total storage  = {scylladb_node.total_storage_gb:.2f} GB"
        f"  (embeddings {embedding_storage / (1024**3):.2f} GB"
        f" + metadata {metadata_storage / (1024**3):.2f} GB)",
    ]
    summary = "\n".join(summary_lines)

    return SizingResult(
        input=inp,
        hnsw_params=hnsw,
        compression_ratio=compression,
        vector_store_node=vector_store_node,
        scylladb_node=scylladb_node,
        instance_selection=instance_sel,
        vector_store_replicas=instance_sel.num_instances,
        summary=summary,
    )
