# VSS Sizing Algorithm

This document describes how the ScyllaDB Vector Search sizing algorithm
calculates infrastructure requirements. The implementation lives in
[`vss_sizing.py`](vss_sizing.py).

## Overview

The algorithm takes a workload description (dataset size, dimensions, target
QPS, recall, etc.) and produces a complete infrastructure recommendation
covering three tiers:

1. **Vector Store nodes** — RAM and vCPUs for the HNSW (USearch) index
   replicas.
2. **Instance selection** — which cloud instance type and how many replicas.
3. **ScyllaDB nodes** — vCPUs and storage for the backing ScyllaDB cluster.

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  User Input │────▶│  HNSW M estimate │────▶│  Index RAM (GB)  │
└─────────────┘     └──────────────────┘     └────────┬─────────┘
                                                      │
                    ┌───────────────────┐             │
                    │ Throughput sizing │◀────────────┘
                    │  (vCPUs needed)   │
                    └────────┬──────────┘
                             │
                    ┌────────▼─────────┐
                    │Instance selection│
                    │ (type × count)   │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  ScyllaDB sizing │
                    │(vCPUs + storage) │
                    └──────────────────┘
```

---

## Input Parameters

| Parameter | Type | Range | Default | Description |
|---|---|---|---|---|
| `num_vectors` | int | 10 000 – 4 000 000 000 | 50 000 000 | Total vectors to index |
| `dimensions` | int | 1 – 16 000 | 1 536 | Dimensions per vector |
| `target_qps` | int | 10 – 1 000 000 | 1 000 | Target QPS at P99 ≤ 15 ms |
| `recall` | int | 70 – 99 | 95 | Target recall accuracy (%) |
| `k` | int | 1 – 1 000 | 10 | Nearest neighbours per query |
| `quantization` | enum | `none`, `scalar`, `binary` | `none` | Vector compression strategy |
| `metadata_bytes_per_vector` | int | 4 – 1 048 576 | 100 | Avg metadata per vector (bytes) |
| `filtering_columns` | int | 0 – 20 | 0 | Number of filtering columns |
| `cloud_provider` | enum | `aws`, `gcp` | `aws` | Target cloud provider |

---

## Step 0: HNSW M Parameter Estimation

Before sizing infrastructure the algorithm estimates the HNSW graph
connectivity parameter **M**, which controls how many bidirectional links
each node maintains in the graph.

### Formula

$$M_{\text{raw}} = 1.5 \times \log_{10}(N) \times \bigl(-\ln(1 - R)\bigr)$$

where $N$ is the number of vectors and $R$ is the recall fraction
(e.g. 0.95 for 95 %).

The raw value is then rounded up to the next power of two (minimum 4):

$$M = 2^{\lceil \log_2(M_{\text{raw}}) \rceil}$$

### Rationale

Two factors drive graph connectivity:

- **Graph size** — traversal hops scale with $\log(N)$, so larger graphs
  need more links per node to stay navigable.
- **Recall tail** — pushing from 90 % to 99 % recall is exponentially
  harder. The $-\ln(1 - R)$ term captures this asymptotic behaviour.

### Examples

| Vectors | Recall | M_raw | M (rounded) |
|---|---|---|---|
| 10 000 | 70 % | 7.2 | 8 |
| 10 000 000 | 95 % | 31.5 | 32 |
| 1 000 000 000 | 99 % | 62.2 | 64 |

---

## Step 1: Vector Store Node RAM

Each Vector Store replica holds the full HNSW index in memory. The RAM
requirement has two parts: the **index itself** and the optional
**filtering column overhead**.

### Index RAM

$$\text{RAM}_{\text{index}} = \left\lceil \frac{N \times 4 \times \bigl(D + M \times \frac{\ln N}{\ln(M \times 0.5)}\bigr) \times 1.1}{C} \right\rceil$$

where:

| Symbol | Meaning |
|---|---|
| $N$ | Number of vectors |
| $D$ | Dimensions per vector |
| $M$ | HNSW connectivity parameter |
| $4$ | Bytes per `float32` element |
| $1.1$ | Overhead factor (USearch metadata, alignment) |
| $C$ | Compression ratio from quantization |

The expression $D + M \times \frac{\ln N}{\ln(M \times 0.5)}$ sums the
vector storage ($D$ float32 elements) and the HNSW graph storage (neighbour
lists).

### Quantization Compression Ratios

| Strategy | Compression Ratio ($C$) | Memory Savings |
|---|---|---|
| `none` (float32) | 1× | — |
| `scalar` (uint8) | 3× | ~67 % |
| `binary` (1-bit) | 10× | ~90 % |

### Filtering RAM

Each filtering column adds 30 bytes per vector:

$$\text{RAM}_{\text{filter}} = \text{filtering\_columns} \times 30 \times N$$

### Total RAM per Replica

$$\text{RAM}_{\text{total}} = \text{RAM}_{\text{index}} + \text{RAM}_{\text{filter}}$$

---

## Step 2: Throughput Sizing (Vector Store vCPUs)

This step determines how many vCPUs are needed across all Vector Store
replicas to sustain the target QPS at P99 ≤ 15 ms latency.

### 2a. Base QPS per vCPU

A lookup table maps dataset size to a base throughput figure (at K = 10,
recall ≤ 90 %, dimensions ≤ 1024). Values are linearly interpolated between
bucket boundaries:

| Bucket | Max Vectors | Base QPS/vCPU |
|---|---|---|
| low-medium | 5 000 000 | 350 |
| medium-large | 100 000 000 | 250 |
| large-xlarge | 1 000 000 000 | 170 |
| xlarge+ | 4 000 000 000 | 80 |

**High-dimensionality scaling:** when dimensions exceed the reference
dimensionality of 1024, the base QPS is scaled down proportionally:

$$\text{base\_qps} = \text{base\_qps} \times \min\!\Bigl(1,\; \frac{1024}{D}\Bigr)$$

### 2b. Recall Adjustment

Higher recall targets reduce throughput per vCPU. The multiplier is
linearly interpolated between known data points:

| Recall | Multiplier |
|---|---|
| 70 % | 2.0× |
| 90 % | 1.0× (baseline) |
| 95 % | 0.40× |
| 99 % | 0.02× |

### 2c. K Adjustment

Returning more results per query reduces throughput. K = 10 is the baseline;
K = 100 halves it, with linear interpolation in between:

$$k_{\text{factor}} = \begin{cases}
1.0 & \text{if } K \le 10 \\
1.0 - 0.5 \times \frac{K - 10}{90} & \text{if } 10 < K < 100 \\
0.5 & \text{if } K \ge 100
\end{cases}$$

### 2d. Quantization Effects on Throughput

Quantization reduces memory but introduces compute overhead for re-ranking:

- **Scalar** — doubles the effective K (oversampling), reducing effective
  QPS/vCPU.
- **Binary** — quadruples the effective target QPS (rescoring overhead),
  meaning more vCPUs are needed to meet the same latency target.

### 2e. Effective QPS per vCPU

$$\text{effective\_qps\_per\_vcpu} = \text{base\_qps} \times \text{recall\_factor} \times k_{\text{factor}}$$

### 2f. Required vCPUs

$$\text{vCPUs} = \left\lceil \frac{\text{effective\_target\_qps}}{\text{effective\_qps\_per\_vcpu}} \right\rceil$$

---

## Step 2b: Instance Selection

Given the per-replica RAM requirement and the total vCPU count, the
algorithm selects a cloud instance type and determines how many replicas to
provision.

### Phase 1: Smallest-Fit

1. Sort all available instance types by RAM (ascending).
2. Filter to instances with RAM ≥ the index RAM requirement.
3. Pick the **smallest** eligible instance.
4. Compute the number of replicas needed:
   $n = \max\!\bigl(\text{MIN\_REPLICAS},\; \lceil\text{vCPUs} / \text{inst\_vcpus}\rceil\bigr)$.
5. If $n > 2$, round up to the next multiple of 3 (for even distribution
   across 3 availability zones).

### Phase 2: Upscale Optimisation

If Phase 1 produces more than **3 instances** (the upscale trigger), the
algorithm tries larger instance types to reduce the replica count:

- For each larger eligible instance, compute the candidate replica count
  and total cost.
- Accept the candidate only if:
  - its total cost ≤ 1.1× the initial cost (at most 10 % more expensive), **and**
  - it uses strictly fewer instances than the current best.

This means the algorithm prefers fewer, larger instances when the cost
increase is marginal, which reduces operational complexity.

### High Availability

A minimum of **2 replicas** is always enforced for production availability,
regardless of the vCPU requirement.

---

## Step 3: ScyllaDB Node Sizing

The ScyllaDB cluster stores the raw vector embeddings and associated
metadata. It always uses a replication factor of 3.

### vCPUs

ScyllaDB nodes need fewer vCPUs than Vector Store nodes (the CPU-intensive
ANN search happens on the Vector Store tier):

$$\text{scylladb\_vcpus\_total} = \max\!\left(1,\; \left\lceil \frac{\text{vs\_vcpus}}{6} \right\rceil\right)$$

These are divided evenly across 3 nodes (RF = 3):

$$\text{vcpus\_per\_node} = \max\!\left(1,\; \left\lceil \frac{\text{scylladb\_vcpus\_total}}{3} \right\rceil\right)$$

The actual total is then re-derived as $\text{vcpus\_per\_node} \times 3$ to
ensure consistency.

### Storage

$$\text{embedding\_storage} = N \times D \times 4 \;\text{bytes}$$

$$\text{metadata\_storage} = N \times \text{metadata\_bytes\_per\_vector}$$

$$\text{total\_storage} = \text{embedding\_storage} + \text{metadata\_storage}$$

> **Note:** Storage is reported as the raw data size. ScyllaDB's internal
> compaction and replication overhead are handled at the infrastructure
> level and not included in this calculation.

---

## Worked Example

**Input:** 50M vectors, 1536 dimensions, 1000 QPS, 95 % recall, K = 10,
no quantization, 100 bytes metadata, 0 filtering columns, AWS.

1. **M estimation:**
   $M_{\text{raw}} = 1.5 \times \log_{10}(50{,}000{,}000) \times (-\ln(0.05)) = 1.5 \times 7.7 \times 3.0 = 34.6 \rightarrow M = 64$

2. **Index RAM:**
   graph component $= 64 \times \ln(50M) / \ln(32) = 64 \times 17.73 / 3.47 = 327$
   RAM $= 50M \times 4 \times (1536 + 327) \times 1.1 / 1 = 382$ GB

3. **Throughput:**
   - Base QPS/vCPU: 50M is in [5M, 100M] bucket → interpolated ≈ 303
   - Dimension scaling: $303 \times 1024/1536 ≈ 202$
   - Recall factor at 95 %: 0.40
   - K factor at 10: 1.0
   - Effective QPS/vCPU: $202 \times 0.40 \times 1.0 ≈ 81$
   - Required vCPUs: $\lceil 1000 / 81 \rceil = 13$

4. **Instance selection:**
   - Smallest fit for ~382 GB RAM: `r7g.12xlarge` (384 GB, 48 vCPUs)
   - Replicas needed: $\max(2, \lceil 13/48 \rceil) = 2$ (≤ 3, no upscale)

5. **ScyllaDB nodes:**
   - vCPUs: $\lceil 13/6 \rceil = 3$ total → 1 per node × 3 nodes
   - Embedding storage: $50M \times 1536 \times 4 = 286$ GB
   - Metadata storage: $50M \times 100 = 4.7$ GB
   - Total: ~291 GB

---

## Key Constants

| Constant | Value | Purpose |
|---|---|---|
| `INDEX_OVERHEAD_FACTOR` | 1.1 | USearch metadata / alignment overhead |
| `FLOAT32_BYTES` | 4 | Bytes per vector element |
| `MIN_VECTOR_STORE_REPLICAS` | 2 | Minimum replicas for HA |
| `INSTANCE_UPSCALE_TRIGGER` | 3 | Instance count that triggers upscale optimisation |
| `INSTANCE_UPSCALE_COST_FACTOR` | 1.1 | Max cost increase when upscaling (10 %) |
| `VS_TO_SCYLLADB_VCPU_RATIO` | 6 | VS-to-ScyllaDB vCPU ratio |
| `REPLICATION_FACTOR` | 3 | ScyllaDB replication factor |
| `SCALAR_COMPRESSION_RATIO` | 3.0 | Memory savings with scalar quantization |
| `BINARY_COMPRESSION_RATIO` | 10.0 | Memory savings with binary quantization |
| `FILTERING_COLUMN_BYTES_PER_VECTOR` | 30 | RAM per filtering column per vector |

---

## References

- [ScyllaDB Cloud Vector Search docs](https://cloud.docs.scylladb.com/stable/vector-search/index.html)
- Malkov & Yashunin, "Efficient and Robust Approximate Nearest Neighbor
  using Hierarchical Navigable Small World Graphs", 2018
