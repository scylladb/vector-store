# SEP: DiskANN Disk-Backed Index Engine for Vector Store

> ScyllaDB Enhancement Proposal — adding a DiskANN (SSD-backed) index engine to
> the Vector Store service, as an additional option alongside the existing
> in-memory USearch engine.
>
> Status: Draft · Owner: Vector Search / Core R&D · Target service:
> [`crates/vector-store`](../../../crates/vector-store)

# Product

This part makes the product case for the change: the problem and who it helps,
the goals and success criteria, the competitive position, pricing and metering,
and the adoption plan.

## Summary

Vector Store today builds every Approximate Nearest Neighbor (ANN) index fully
in RAM using the [USearch](https://github.com/unum-cloud/usearch) HNSW engine.
RAM is therefore the constraining and dominant cost resource: a 100M × 768-dim
`f32` index needs ~400 GB of memory, and billion-scale datasets require fleets
of large memory-optimized nodes even with quantization. This proposal adds
**DiskANN** as a second, selectable index engine that delegates the bulk of
index storage to local **NVMe SSD**, keeping only a compressed
(Product-Quantized) representation and a small working set in RAM.

The implementation reuses the existing `VsIndexFactory` abstraction (the same
seam the USearch backend plugs into) and integrates
[DiskANN3](https://github.com/microsoft/DiskANN) — Microsoft Research's modern,
Rust, composable rewrite of DiskANN (MIT-licensed, latest release `v0.54.0`) —
in **disk-provider** mode. The primary target users are operators of large
(100M–1B+ vector) workloads who want a materially lower **\$/indexed-vector**
and **\$/QPS** at acceptable recall, trading a modest latency increase for a
large reduction in RAM cost; the same trade-off also lets **cost-sensitive
smaller workloads** run on cheaper hardware when they can accept higher latency.
USearch remains the default for latency-critical, RAM-resident workloads;
DiskANN becomes the capacity- and cost-optimized tier.

## Motivation

- **What problem are we solving?** Vector Store's HNSW index is entirely
  RAM-resident. The published sizing model is
  `Memory ≈ N × (D × B + m × 16) × 1.2`, so memory grows linearly with dataset
  size and dimensionality. At 100M × 768-dim `f32` this is ~400 GB; at
  billion-scale it forces multi-node, high-memory fleets (the existing 1B
  benchmark used three `r7i.48xlarge`, 192 vCPU / large-RAM nodes). Memory is
  both the scaling ceiling and the primary bill driver.
- **Why is current behavior insufficient?** Quantization (`i8`, `b1`)
  helps, but it only compresses the *vector data*; the HNSW graph
  (`m × 16` term) stays in RAM at full size, so real savings are ~3× not the
  raw compression ratio, and aggressive levels (`b1`) require rescoring that
  cuts throughput ~4×. The whole structure still lives in RAM. There is no way
  to index a dataset larger than the available memory of the fleet — and for
  datasets that *do* fit, the only cost lever is quantization: there is no way
  to trade a modest latency increase for a materially cheaper, non-RAM
  deployment.
- **Which user/business scenarios justify the investment?** Two complementary
  classes. First, **large workloads** — billion-scale RAG corpora, product
  catalogs, semantic caches, log/observability embeddings, and multi-tenant
  deployments where most indexes are large but not all are latency-critical —
  that are expensive or impossible to serve economically on a pure in-memory
  engine. Second, **cost-sensitive smaller workloads** (millions to tens of
  millions of vectors) whose owners would rather run on a cheaper
  storage-optimized node and accept higher latency than pay for the RAM-resident
  performance they do not need.
- **What value does it give customers?** At the top end, the ability to run
  100M–1B+ vector indexes on smaller, storage-optimized instances at a fraction
  of the RAM cost: DiskANN's published result is indexing **5–10× more vectors
  per machine** than DRAM-based systems, sustaining ~95% recall at ~5 ms latency
  on a single node at billion scale — precisely the price/performance point
  USearch cannot reach. At the low end, it also **lowers the cost floor**: a
  smaller dataset that would fit in RAM can instead run on a cheaper
  storage-optimized node, trading some latency for a materially lower bill — an
  option USearch's RAM-only model does not offer.

## Goals

- Add DiskANN as an **additional, selectable** index engine behind the existing
  `VsIndexFactory` trait — no removal or regression of USearch.
- Use the **DiskANN3 disk provider** so index capacity is decoupled from RAM and
  bounded by local SSD capacity instead.
- Preserve the existing external contract: unchanged CQL surface
  (`[WHERE <col> = ?] ORDER BY <col> ANN OF ? LIMIT k`), CDC-based ingestion,
  and the HTTP API.
- **Measurable success criteria:**
  - *Cost/capacity:* at 100M and 1B scale, demonstrate **≥ 3× lower RAM per
    indexed vector** than USearch `f32`, and lower **\$/indexed-vector** than
    USearch `i8`/`b1`, at comparable recall.
  - *Recall:* recall@10 ≥ 0.95 and recall@100 ≥ 0.90 at the high-quality
    operating point on the benchmark datasets, validated against brute-force
    ground truth.
  - *Latency:* serial p99 within a single-to-low-double-digit-millisecond
    budget at the high-throughput operating point (**tentative** target
    p99 ≤ 15 ms at 1B scale for k=10 — largely governed by DiskANN3's own
    beam-search/PQ/SSD-I/O behavior rather than fully under our control), with
    documented Pareto curves.
  - *Correctness:* on a 50k validation set, recall@10 within tolerance of the
    exact KNN baseline; runs in CI as a fast gate.
  - *Freshness:* newly ingested vectors searchable within the existing CDC
    propagation SLO (fine-grained reader, typically < 1 s) using in-place
    updates, without full index rebuilds.

## User Stories

- As an **application developer** with a 500M-vector RAG corpus, I want to serve
  ANN queries without paying for ~2 TB of RAM, so that billion-scale semantic
  search is economically viable.
- As a **database administrator**, I want to choose a capacity-optimized vector
  engine per deployment using the same provisioning, CDC, and monitoring model
  as today, so that I gain disk-backed scale without learning a new operational
  surface.
- As a **platform/cost owner**, I want a documented \$/indexed-vector and
  \$/QPS comparison between USearch (with and without quantization) and DiskANN,
  so that I can place each workload on the right tier.
- As an **SRE**, I want clear signals (SSD utilization, page-cache hit rate,
  IOPS saturation, rebuild progress) so that I can run disk-backed indexes
  safely and alert before saturation.
- As a **ScyllaDB R&D engineer**, I want DiskANN to plug into the existing
  `VsIndexFactory`/`VsIndex` actor infrastructure, so that the integration is contained and
  does not fork the ingestion or query pipelines.

## Out Of Scope

- **Replacing USearch.** DiskANN is purely additive; USearch stays the default
  in-memory engine.
- **Changing the CQL/ANN query surface** or the public HTTP API shape.
- **Non-disk DiskANN3 providers** (in-memory, Garnet/key-value, Bf-tree,
  Cosmos DB). Only the node-local SSD disk provider is in scope.
- **S3 / remote object storage on the index hot path.** Only local NVMe SSD is
  in scope; S3-backed or otherwise network-attached storage for the graph and
  vectors is explicitly out of scope (it adds a network hop per beam-search step
  and defeats the low-latency disk-traversal design, and it would expand this
  SEP's scope well beyond a contained disk-provider integration).
- **Filtered ANN in the first increment.** `FilteredAnn` parity with USearch is
  **in scope of this SEP** but delivered under a **separate epic/milestone**
  (see Delivery Plan), not in the initial DiskANN increment. DiskANN3 exposes
  attribute-filter hooks; wiring them to ScyllaDB's query executor is tracked
  separately.
- **Distributed single-graph** indexing across nodes (DistributedANN), **GPU**
  acceleration, and **Optane/PMem** tiers. Optane/PMem is called out explicitly
  because the legacy DiskANN supported a persistent-memory tier; it is excluded
  here since Intel has discontinued Optane and this SEP targets only local NVMe.
- **Cross-engine in-place data migration.** Switching engines is handled by
  rebuilding the index from the base table via CDC, not by converting on-disk
  formats.

## Competitive Analysis

This is an **internal** comparison: the incumbent USearch (in-memory HNSW)
engine versus the proposed DiskANN (SSD-backed Vamana) engine within Vector
Store. The "competitor" is our own current implementation; the question is which
engine to place a given workload on.

- **Market landscape (internal engines).**
  - *USearch (incumbent):* in-memory HNSW; lock-free concurrent reads/writes;
    quantization `f32/f16/bf16/i8/b1`; the engine behind the published 10M and
    1B benchmarks (252k QPS, ~2 ms p99 at 1B).
  - *DiskANN (proposed):* SSD-backed Vamana graph + PQ-compressed vectors in
    RAM; built on DiskANN3 (Rust, MIT, `v0.54.0`); designed for
    larger-than-memory datasets with in-place streaming updates.

- **Feature comparison.**

  | Dimension | USearch (HNSW, in-memory) | DiskANN (Vamana, disk provider) |
  | --- | --- | --- |
  | Primary storage tier | RAM | Local NVMe SSD + PQ table in RAM |
  | Graph topology | Hierarchical (multi-layer) | Flat single-layer Vamana (α-pruned long-range edges) |
  | RAM per vector | Full `D×B` + graph (`m×16`) | PQ code (e.g. 32–96 B) + cache, **independent of D×B** |
  | Capacity ceiling | Fleet RAM | Fleet SSD capacity (5–10× more vectors/node) |
  | Query latency | Lowest (sub-ms–low-ms) | Higher (SSD I/O bound), still low-ms at scale |
  | Throughput (QPS) | Very high | Lower per node, recovered via beam width / IOPS |
  | Recall control | `ef_search`, oversampling, rescoring | search-list/beam width + full-precision re-rank from SSD |
  | Updates | Lock-free concurrent add/remove | In-place streaming (IP-DiskANN), stable recall, no rebuilds |
  | Quantization | `f16/bf16/i8/b1` | PQ / Scalar / MinMax / Spherical (x86 + aarch64) |
  | Build time | Lower (relative) | Higher (graph + PQ codebook training) |
  | Instance profile | Memory-optimized (`r7g`/`r7i`) | Storage-optimized w/ local NVMe (`i7ie`/`i8g`) |

- **Differentiation.** DiskANN's unique advantage inside Vector Store is
  **capacity decoupled from RAM** at a much lower cost per vector. It is the
  only path to economical billion-scale single-node indexes and to fitting more
  (or larger) tenant indexes per node. USearch keeps the latency/throughput
  crown for RAM-resident workloads.
- **Gaps we intentionally accept.** v1 will not match USearch on raw latency or
  on `FilteredAnn`. We accept higher p99 in exchange for cost/capacity, and defer
  filtered search. We also accept higher build time and an SSD durability story.
- **Customer expectations.** Teams moving large corpora off external,
  RAM-priced vector databases expect: predictable low-ms latency at scale, the
  same single-system operational model (no separate ETL), high recall with a
  tunable Pareto curve, and a clearly lower bill than the in-memory tier.

## Pricing Model

> The disk tier reuses the **same pricing scheme as the existing Vector Search
> offering**: customers are charged for the vector-search nodes that hold the
> index, by instance type. The notes below describe how that scheme applies;
> concrete catalog prices come from the instance catalog and Product/Finance.

- **Pricing tier.** Offered within the existing Vector Search add-on, exposed as
  a **capacity-optimized** instance choice alongside the current
  latency-optimized (USearch) instances. Not a separate SKU; same Vector Search
  feature gate.
- **Pricing structure.** Per-vector-search-node, on-demand (consistent with the
  current Vector Search billing model), with storage-optimized instance types
  whose price reflects local NVMe rather than large RAM. The NVMe is **local
  ephemeral instance-store** bundled into the instance price — not a separately
  provisioned (and separately billed) persistent volume — which is what keeps
  billing per-node with no extra storage line item.
- **Free tier / trial.** Disk-backed engine is aimed at scale; free-trial
  clusters continue to default to the smallest in-memory instance.
- **Cost drivers.** Local NVMe capacity and IOPS, a smaller RAM footprint
  (PQ table + page cache), CPU for beam search and PQ distance, and one-time
  build/compaction CPU + I/O.
- **Margin impact.** Expected to **improve** margin at scale: storage-optimized
  instances cost less per indexed vector than memory-optimized ones, so the same
  dataset is served on cheaper hardware. The benchmark must quantify the
  crossover point where DiskANN becomes cheaper than USearch `i8`/`b1`.
- **Price benchmarking.** Position the disk tier against external disk-based or
  serverless vector databases on \$/indexed-vector and \$/QPS, using the
  benchmark results below.

## Billing & Metering

- **Metering dimensions.** Vector-search node-hours (by instance type), as
  today. On-disk index size and local-SSD utilization are captured as
  capacity/observability signals (not billing dimensions).
- **Metering granularity.** Node-hours captured on the existing cadence;
  capacity/size gauges sampled periodically (e.g. per minute) and rolled up.
- **Metering implementation.** Reuse the current Vector Search node metering.
  Add engine-tagged gauges (`engine=diskann`) emitted from `metrics.rs`:
  on-disk index size, SSD utilization, and RAM (PQ + cache) usage, surfaced via
  the existing Prometheus endpoint.
- **Billing integration.** No new billing line items are required: billing stays
  **per vector-search-node-hour by instance type**, exactly as today, and the
  storage-optimized instance types carry their own price. SSD capacity is part
  of the instance, so it is **not** metered as a separate billing dimension; it
  is tracked only as an observability/capacity gauge.
- **Customer visibility.** Engine type and per-index size/SSD usage shown in the
  Cloud monitoring dashboards and the cluster details view, mirroring how
  instance type and node count are shown today.
- **Usage alerts & limits.** Soft/hard thresholds on SSD utilization and index
  size vs. node capacity, with alerts before saturation (a disk-full condition
  must degrade gracefully, not lose data — vectors remain durable in ScyllaDB).
- **Audit & accuracy.** Reconcile node-hours against the cloud provider's
  instance inventory as today; cross-check reported index size against actual
  on-disk files during periodic audits.

## Adoption & Release Strategy

- **Phased rollout.**
  1. *Internal/dev:* engine selectable via configuration (env var) on a single
     node; correctness gate on the 50k dataset in CI.
  2. *GA:* exposed in Cloud as a capacity-optimized instance choice, after the
     100M/1B benchmark targets are met.
- **Feature flag / opt-in.** Off by default. Selected explicitly per
  vector-store node via configuration, reusing the existing per-node
  engine-selection mechanism in the factory layer. USearch remains the default
  when the DiskANN configuration is absent.
- **Cloud UX (deployment switch).** In ScyllaDB Cloud the choice is surfaced as
  a single **latency-optimized vs cost-optimized** toggle on the deployment
  form. Picking a mode filters the offered instance types to the matching
  hardware profile (memory-optimized for latency-optimized/USearch,
  storage-optimized local-NVMe for cost-optimized/DiskANN) and automatically
  applies the corresponding vector-search index engine — the user selects an
  outcome, not the engine directly.
- **Supported versions & deployment.** Requires a Vector Store release bundling
  DiskANN3 and storage-optimized (local-NVMe) provisioning. **BYOA is
  supported**, on both ScyllaDB-managed and customer accounts, using the same
  model as today; the disk tier just needs NVMe-backed instance types available
  in the target account/region. The exact Cloud release train is set with
  Product alongside instance-type enablement. Proposed performance-optimized
  instances are listed under Key Operational Notes.
- **User communication.** Documentation (concepts, sizing, deployment),
  a benchmark blog in the established series, and Cloud UI affordances for the
  capacity-optimized instance choice. No breaking changes to existing clusters.
- **Deprecation plan.** None — additive. USearch is not deprecated.

## Adoption and Success Tracking

- **Adoption metrics.** Number/percentage of vector-search nodes running the
  DiskANN engine, count of indexes ≥ 100M vectors on disk, and total
  indexed-vectors served on the disk tier.
- **Success criteria.** Meet the benchmark targets (recall, latency, ≥ 3× RAM
  reduction vs USearch `f32`); zero data-loss incidents (vectors remain durable
  in ScyllaDB).
- **Telemetry & instrumentation.** Engine-tagged metrics from `metrics.rs`:
  query latency histograms, recall sampling (offline), SSD IOPS/utilization,
  page-cache hit rate, on-disk index size, build/rebuild duration, and update
  lag.
- **Dashboard.** Extend the existing Vector Search Grafana dashboards with an
  engine dimension and disk-specific panels.
- **Reporting cadence.** Reviewed in the regular Vector Search product review;
  benchmark re-runs gated to releases.
- **Customer feedback loop.** Support tickets and feature requests tracked
  against the disk engine.
- **Failure signals.** Recall regressions under long update streams, p99
  exceeding budget at target QPS, SSD saturation, or build times that block
  ingestion SLOs. Escalation to the Vector Search team via the standard R&D
  division escalation paths.

## Internal Operability Documents

| **What** | **Description** | **Link** |
| --- | --- | --- |
| - [ ] CX Enablement | Troubleshooting guide for disk-backed indexes: SSD saturation, page-cache warm-up, rebuild-from-CDC, cold-start latency, engine selection. Known-issue list + slides. | \<Links\> |
| - [ ] Sales enablement | Positioning of the capacity-optimized (DiskANN) vs latency-optimized (USearch) tier; \$/indexed-vector story; demo script at 100M scale. | \<Links\> |
| - [ ] Internal R&D training sessions | Architecture deep-dive: Vamana vs HNSW, DiskANN3 `DataProvider`/disk provider, PQ, beam search, in-place updates. Recorded. | \<Links\> |
| - [ ] Knowledge base articles | Customer-facing docs: concepts, sizing for disk, deployment, tuning, FAQ; benchmark blog. | \<Links\> |
| - [ ] Scylla University | Slides + recorded session on disk-backed vector search. | \<Links\> |

# R&D

This part covers the engineering design: how DiskANN plugs into Vector Store, the
subsystems it touches, the trade-offs we accept, and its performance, security,
compatibility, and operational characteristics.

# High-Level Design Approach

- **Add a new `VsIndexFactory` implementation, `DiskannIndexFactory`**, selected
  at startup exactly like the existing USearch engine. Today
  [`main.rs`](../../../crates/vector-store/src/main.rs) selects the index factory
  at startup through the existing `new_index_factory_*` seam (e.g.
  `new_index_factory_usearch`); we add a
  `new_index_factory_diskann` branch keyed off new configuration
  (`VECTOR_STORE_DISKANN_*`). This seam was originally built to host more than
  one engine — a legacy, unmaintained OpenSearch implementation also exists
  behind it (a leftover from an earlier experiment, not used in production) — so
  adding DiskANN reuses proven multi-engine plumbing rather than introducing a
  new abstraction. The factory produces per-index actors that consume
  the existing [`VsIndex`](../../../crates/vector-store/src/vs_index/actor.rs) enum
  (`AddVector`, `RemoveVector`, `RemovePartition`, `Ann`, `FilteredAnn`,
  `Count`) — so ingestion (CDC), routing, and the HTTP/CQL surface are unchanged.
- **Use DiskANN3 in disk-provider mode.** DiskANN3 is a stateless orchestrator
  that delegates storage of index terms (vectors + adjacency lists) to a
  `DataProvider`. We use its upstream **disk provider** so vectors and the Vamana
  graph live on local **NVMe SSD**, with **Product-Quantized** vectors held in
  RAM for fast in-graph distance and a final full-precision re-rank read from
  SSD. This is the classic DiskANN design that indexes 5–10× more vectors per
  node than DRAM systems. We deliberately keep storage **node-local** rather than
  building a custom provider over ScyllaDB storage: co-locating index terms on
  NVMe avoids an extra network hop per beam-search step, which is the dominant
  latency cost in a disk-backed graph traversal.
- **Map Vector Store's index parameters onto Vamana/DiskANN (v1 approach).**
  As an initial step, **reuse the existing**
  [`VsIndexConfiguration`](../../../crates/vector-store/src/vs_index/factory.rs)
  knobs rather than adding new ones: `connectivity` (`m`) → Vamana graph degree
  `R`; `expansion_add` (`ef_construct`) → build search-list size `L`;
  `expansion_search` (`ef_search`) → query search-list/beam width; `space_type`
  → DiskANN distance function. The α robust-pruning parameter (default ≈ 1.2) is
  configurable via an environment variable (`VECTOR_STORE_DISKANN_*`). This
  mapping is a deliberate **v1 shortcut** to keep the first increment contained;
  a proper, dedicated DiskANN configuration surface (its own knobs, and
  eventually CQL-level control) is a follow-up iteration. **PQ is configured
  internally to the engine with sensible defaults** in this increment — DiskANN's
  PQ is independent of the existing `Quantization` enum (`f32/f16/bf16/i8/b1`)
  and is not surfaced in CQL yet; exposing PQ parameters through CQL is a
  follow-up iteration.
- **Keep updates real-time.** Use DiskANN3's **in-place streaming updates
  (IP-DiskANN)** so CDC-driven inserts/deletes mutate the on-disk graph directly
  with stable recall — no periodic merges or full rebuilds. IP-DiskANN is a
  technique that applies each insert/delete straight into the on-disk Vamana
  graph while holding recall stable, avoiding the separate in-memory buffer and
  periodic merge cycle that the earlier Fresh-DiskANN approach required. It is
  supported natively by DiskANN3, so we consume it directly rather than building
  our own update path.
- **Integrate with the memory and metrics actors.** Account the PQ table +
  page-cache against the existing `Memory` budget
  (`VECTOR_STORE_MEMORY_LIMIT`); add SSD/IOPS/cache/index-size metrics.

Why this direction: it contains the change to the index layer behind the
existing `VsIndexFactory` seam (already built to host a pluggable engine), adopts
the actively maintained, Rust-native DiskANN3 rather than the unmaintained
legacy C++ code, and matches
the requested "disk provider delegating storage to SSD" exactly. Engine
selection is **per node** (not per index): USearch and DiskANN target different
hardware profiles — memory-optimized vs storage-optimized with local NVMe — so
the engine is chosen together with the instance type at deployment time, and a
given vector-store node runs exactly one engine.

## Affected Subsystems

- **Index layer** — new
  [`vs_index/diskann.rs`](../../../crates/vector-store/src/vs_index) implementing
  `VsIndexFactory` + the per-index actor, plus a DiskANN3 disk-provider binding;
  module wiring in [`vs_index/mod.rs`](../../../crates/vector-store/src/vs_index/mod.rs).
- **Factory selection** —
  [`main.rs`](../../../crates/vector-store/src/main.rs) and the
  `new_index_factory_*` constructors in
  [`lib.rs`](../../../crates/vector-store/src/lib.rs).
- **Configuration** —
  [`config_manager.rs`](../../../crates/vector-store/src/config_manager.rs):
  new env vars (SSD path, PQ params, cache size, beam width, engine selector)
  and the README config table.
- **Memory accounting** —
  [`memory.rs`](../../../crates/vector-store/src/memory.rs): track PQ table +
  cache instead of full-vector RAM.
- **Observability** —
  [`metrics.rs`](../../../crates/vector-store/src/metrics.rs): SSD utilization,
  IOPS, page-cache hit rate, on-disk index size, build/rebuild duration,
  update lag.
- **Engine/version reporting** — `index_engine_version()` returns a
  `diskann-<version>` string consumed by `monitor_indexes`.
- **Benchmark crate** —
  [`crates/benchmark`](../../../crates/benchmark) (`vector-search-benchmark`)
  extended to record RAM/SSD footprint and to compare engines.
- **Provisioning** — Packer images and Cloud instance types that include local
  NVMe; sizing documentation for the disk tier.
- **Not changed** — CDC readers, CQL/ANN surface, HTTP API, routing,
  partitioning/tablets.

## Trade-offs & Rationale

- **Latency vs. cost.** SSD I/O adds per-query latency versus pure RAM, but
  removes the RAM ceiling and slashes \$/vector. Chosen because a large class of
  workloads is cost/capacity-bound, not latency-bound; USearch still serves the
  latency-critical tier.
- **Operational complexity vs. capacity.** Requires local NVMe, a durability/
  rebuild story, and warm-up management — versus USearch's RAM-only model. We
  accept this for 5–10× capacity per node.
- **Build/reuse.** Adopt DiskANN3 (maintained, Rust, MIT) rather than
  re-implementing Vamana or reviving the legacy C++ code. Lower long-term
  maintenance, at the cost of a new pre-1.0 dependency to track.
- **Recall vs. footprint.** PQ yields approximate in-graph distances; mitigated
  by a full-precision re-rank read from SSD. Tunable via beam width and re-rank
  depth.
- **Rejected alternatives:** (a) keep relying on quantization in USearch — does
  not remove the RAM ceiling; (b) an external disk-based vector DB —
  reintroduces ETL/dual-write and a second operational system, contrary to the
  unified-store value proposition; (c) a bespoke DiskANN provider over ScyllaDB
  storage — larger scope than the requested SSD disk provider (kept as a future
  option).

## Performance, Security, and Scalability Impact

- **Throughput/latency.** Expect lower per-node QPS and higher p99 than USearch,
  bounded by beam width and SSD IOPS. Target: low-ms p50 and a single-to-low-
  double-digit-ms p99 at the high-throughput operating point at 1B scale,
  characterized as a Pareto curve (recall vs QPS vs p99) like the existing
  benchmarks. DiskANN's reference point is ~95% recall at ~5 ms at billion scale
  on a single node.
- **Resource usage.** RAM drops from full-vector + graph to PQ table + page
  cache; SSD capacity and IOPS become the new primary resources; CPU for PQ
  distance and beam search.
- **Security.** No new network surface — DiskANN3 is an in-process Rust library.
  The new exposure is **vectors at rest on local NVMe**: require encryption at
  rest (instance-store encryption / dm-crypt) and ensure the same internal-
  authorization model and TLS/mTLS API boundaries apply unchanged. Source
  vectors remain durable and authoritative in ScyllaDB.
- **Known regressions during rollout.** Longer index build times, page-cache
  warm-up after (re)start, and cold-start tail latency before the cache is warm.
- **Expected post-rollout state.** A capacity-optimized engine that serves
  100M–1B+ indexes on storage-optimized instances at materially lower RAM cost,
  with recall and latency within documented budgets.

## Compatibility and Migration

- **Version compatibility / mixed-version.** Additive and opt-in; existing
  USearch indexes and clusters are unaffected. Engine is selected per node via
  configuration (initial design), consistent with the existing per-node
  engine-selection mechanism.
  `index_engine_version()` changes for DiskANN nodes; `monitor_indexes` must
  treat the engine string as engine-specific and not assume USearch.
- **Data migration.** None in-place. The authoritative vectors live in ScyllaDB;
  an index on a DiskANN node is (re)built from the base table via the normal CDC
  path. Switching a workload between engines = build a fresh index on the target
  engine.
- **Restart & durability.** Losing RAM alone never forces a rebuild: the
  PQ-compressed data held in RAM is a loaded copy of files that already live on
  the SSD next to the graph (full-precision vectors + adjacency lists, the PQ
  codebook, and the PQ codes). The durability boundary is therefore the **SSD
  volume, not RAM**:
  - *NVMe survives* (process restart/reboot, or a persistent volume) → recover
    by **reloading the index from SSD** — a sequential read, no ScyllaDB stream;
    only the OS page cache is cold, so we pay warm-up tail latency, not a
    rebuild.
  - *NVMe is wiped* (ephemeral instance-store on stop/terminate) → **rebuild
    from ScyllaDB via CDC**.

  Because the proposed instances use ephemeral instance-store NVMe,
  rebuild-from-CDC is treated as the default HA path, covered by replication
  across vector-search nodes/AZs. Two items to validate in the disk provider:
  crash consistency under in-place updates (atomic/checkpointed writes, or
  rebuild on detecting a torn index) and cold-cache warm-up (correct but slower
  until warm).
- **Upgrade / rollback / fallback.** Rollback = point the workload back at a
  USearch node and rebuild; no on-disk format conversion.

## Key Operational Notes

- **Provisioning.** Requires storage-optimized instances with **local NVMe SSD**
  (high IOPS, low latency). Proposed performance-optimized defaults:
  - *AWS:* `i7ie` (Intel + local NVMe, high NVMe density) and
    `i8g` (Graviton4 + local NVMe) storage-optimized families.
  - *GCP:* `z3` storage-optimized instances (Local SSD), or general-purpose
    instances with Local SSD attached where `z3` is unavailable.

  Packer images and the Cloud instance catalog must include these, and **BYOA**
  accounts must expose equivalent NVMe-backed types in the target region. Avoid
  network-attached storage on the hot path; if persistence across stop/start is
  required, see the durability note in Compatibility and Migration.
- **Configuration.** New env vars for the SSD index path, PQ parameters,
  in-RAM cache size, default beam width, and the engine selector. Documented in
  the README config table.
- **Observability.** New metrics/alerts: SSD utilization and IOPS saturation,
  page-cache hit rate, on-disk index size vs node capacity, build/rebuild
  progress and duration, and update lag. Alert before disk-full.
- **Cache warm-up.** After any (re)start where the NVMe files survive, the index
  is correct immediately but the OS page cache is cold, so early queries pay
  extra SSD reads and elevated tail latency until the working set is resident. An
  **optional pre-warm pass** mitigates this: on load, sequentially read the hot
  index structures (PQ codebook + PQ codes, and the graph's entry-point
  neighborhood) into the page cache before the node is marked ready to serve, so
  the first real queries hit a warm cache. The pass is best-effort and bounded
  (skippable via config), and warm-up progress/page-cache hit rate are exported
  so operators can gate traffic on a warm replica while others warm. Combined
  with replication across vector-search nodes, this keeps cold-start tail latency
  off the serving path.
- **Co-tenancy with full-text search.** Vector-search nodes also host full-text
  search (FTS) indexes, so the latency- vs cost-optimized choice also changes
  the RAM available to FTS on the same node: cost-optimized (DiskANN) instances
  trade RAM for SSD, leaving less RAM for FTS. Factor this into sizing guidance
  and surface it in the deployment switch so operators understand the FTS
  headroom trade-off, not just the vector-search one.
- **Operational constraints during rollout.** Plan for build/warm-up windows;
  ensure replication across vector-search nodes so a single SSD/instance loss
  triggers rebuild without query-availability loss.

# QA Strategy

- **Functional & integration tests.** Mirror
  [`tests/integration/usearch.rs`](../../../crates/vector-store/tests/integration/usearch.rs)
  for a new engine: factory construction, add/remove/search/count via the
  `VsIndex` actor, engine-version reporting, and end-to-end CDC ingestion → search.
- **Correctness (fast gate).** On the **50k** validation dataset, assert
  recall@10 within tolerance of exact KNN ground truth; run in CI on every
  change (cheap, deterministic, quick).
- **Failure & recovery.** Node restart (rebuild from CDC and cache re-warm),
  local SSD loss, disk-full handling (graceful degradation, no data loss),
  corrupt-index detection and rebuild.
- **Performance & longevity.** Full **100M** and **1B** benchmarks (below);
  long update-stream tests asserting **recall stability** under sustained
  insert/delete load (the IP-DiskANN guarantee).
- **Upgrade & mixed-mode.** A cluster with both USearch and DiskANN
  vector-search nodes; verify routing, monitoring, and engine-version handling.

### Benchmark plan (USearch vs DiskANN)

Methodology follows the established ScyllaDB vector-search benchmarks
(the [10M](https://www.scylladb.com/2026/05/01/vector-search-10m-benchmark/) and
[1B](https://www.scylladb.com/2025/12/01/scylladb-vector-search-1b-benchmark/)
posts): drive load with
[VectorDBBench](https://github.com/scylladb/VectorDBBench) and the in-repo
[`vector-search-benchmark`](../../../docs/benchmarking.md) tool, ramp concurrency
from 1 → 150 clients, measure peak QPS, p50/p99 (concurrent and serial), and a
separate serial run of ~1,000 queries for **recall@k** against
brute-force ground truth.

- **Datasets.**
  - *50k (correctness / quick validation):* small set for CI and fast recall
    checks against exact KNN.
  - *100M:* mid-large scale (e.g. a 100M slice of a standard high-dim set such
    as Cohere/OpenAI-style 768–1536-dim embeddings).
  - *1B:* billion-scale, reusing the existing 1B setup
    (`yandex-deep-1b`, 96-dim) for direct comparability with published results.

  Note: the 100M and 1B datasets use different dimensionalities (768–1536-dim vs
  96-dim), so absolute RAM, \$/indexed-vector, and \$/QPS figures are **not**
  directly comparable across the two scales. Each scale is a self-contained
  USearch-vs-DiskANN comparison at a fixed dimensionality; cross-scale numbers
  indicate the trend, not an apples-to-apples ratio.
- **Engines / configurations under test (the internal competition):**
  1. **USearch — no quantization** (`f32`): the current default and recall/latency
     reference.
  2. **USearch — with quantization** (`i8` and `b1`, the latter with
     rescoring/oversampling): the current cost-reduction baseline.
  3. **DiskANN — disk provider** (PQ in RAM, full vectors + graph on SSD), at
     matched recall operating points.
- **Operating points.** Sweep recall-vs-throughput as in the 10M study
  (e.g. high-quality, balanced, high-throughput, max-throughput) by varying
  graph degree and search/beam width for each engine, and report the Pareto
  frontier at **k = 10 and k = 100**.
- **Metrics captured per run.** recall@10 / recall@100, peak QPS, p50/p99
  (serial and at each concurrency level), **RAM footprint**, **SSD footprint**,
  index build time, and **\$/indexed-vector** + **\$/QPS** derived from the
  chosen instance types.
- **Hardware framing (cost is a first-class result).** Run USearch on
  memory-optimized search nodes (`r7g`/`r7i`, as in the published runs) and
  DiskANN on storage-optimized nodes with local NVMe (`i7ie`/`i8g`). The headline
  comparison is **equal-recall cost**: how much cheaper DiskANN serves the same
  recall at 100M and 1B than USearch `f32` and USearch `i8`/`b1`.

Test plan link: [Add link]

# Delivery Plan

| Delivery Sprints | Deliverable | Acceptance Criteria |
| --- | --- | --- |
| VS Sprint 1 | DiskANN3 dependency spiked; `DiskannIndexFactory` skeleton + disk-provider binding; engine selected via config in `main.rs` | Service starts in DiskANN mode; `index_engine_version()` reports `diskann-<ver>`; builds in CI |
| VS Sprint 1 | `VsIndex` actor wiring (Add/Remove/RemovePartition/Ann/Count) over DiskANN3 update/query API | Integration test (parity with `usearch.rs`) green end-to-end via CDC |
| VS Sprint 2 | 50k correctness gate; parameter mapping (`m/ef_construct/ef_search/α/PQ`) | recall@10 within tolerance vs exact KNN; runs in CI |
| VS Sprint 2 | Memory accounting + metrics (SSD/IOPS/cache/index size) | Metrics exported; PQ+cache counted against `VECTOR_STORE_MEMORY_LIMIT` |
| VS Sprint 3 | In-place streaming updates; recall-stability longevity test | Recall stable under long insert/delete stream; no full rebuild |
| VS Sprint 4 | 100M benchmark (USearch f32/quantized vs DiskANN) | Targets met; Pareto curves + \$/vector documented |
| VS Sprint 5 | 1B benchmark + blog | 1B targets met; docs + dashboards ready |
| VS Sprint 6 | GA: Cloud UI capacity-optimized choice; docs (concepts/sizing/deploy/FAQ) | GA checklist complete; enablement materials published |
| Infra Sprint 6 (parallel, gated on benchmarks) | Productized storage-optimized instance support (Packer + Cloud catalog) | Disk-tier nodes provisionable with local NVMe; started only after 100M/1B benchmark results justify GA (benchmarks themselves run on manually provisioned NVMe instances) |
| VS Sprint 7 (separate epic) | Filtered ANN: wire DiskANN3 attribute-filter hooks to the query executor | `FilteredAnn` parity with USearch; filtered recall validated |

## Shared

This part covers concerns that span both product and engineering: delivery-time
technical risks, implementation-level open questions to close during the work,
external dependencies, and references.

## Risks and Open Questions (Shared)

> All product/design decisions raised during review have been resolved and folded
> into the sections above (engine selection per node, durability/restart,
> DiskANN3 disk provider, engine-internal PQ defaults, filtered ANN as a separate
> milestone, pricing reuse, and BYOA/instance types). The remaining items below
> are the technical risks to manage during delivery, a set of
> implementation-level open questions to close as part of that work, and external
> dependencies.

**Technical risks & mitigations**

- **Dependency maturity & licensing.** DiskANN3 is pre-1.0 (`v0.54.0`),
  fast-moving, **MIT**-licensed, while Vector Store is ScyllaDB Source Available.
  *Mitigation:* pin/vendor a known-good revision, verify Rust toolchain/edition
  compatibility, track upstream releases, and confirm license compatibility for
  bundling.
- **Latency budget.** SSD I/O may push p99 beyond target at high concurrency.
  *Mitigation:* tune beam width, PQ size, and re-rank depth; size NVMe IOPS;
  characterize warm vs cold cache; publish Pareto curves.
- **Cold start / warm-up.** Page cache must warm after (re)start.
  *Mitigation:* the optional pre-warm pass described under Key Operational Notes
  (sequentially load PQ table + graph entry-point neighborhood before marking the
  node ready); replication so warm replicas serve while others warm.
- **Ephemeral storage / HA.** Instance-store NVMe is ephemeral — its contents do
  not survive an instance stop/terminate or host/hardware failure (a plain reboot
  is fine).
  *Mitigation:* rely on ScyllaDB as the durable source of truth; replicate
  indexes across vector-search nodes/AZs; rebuild-on-loss with monitored
  progress.
- **Memory accounting accuracy.** PQ table + OS page cache must be reflected in
  the memory budget to avoid over-allocation. *Mitigation:* explicit accounting
  in `memory.rs` and metrics.
- **Update-path freshness.** In-place updates must keep pace with CDC to honor
  the sub-second freshness SLO. *Mitigation:* benchmark sustained update
  throughput; fall back to batched minibatch updates if needed (DiskANN3's update
  API supports both single and minibatch updates, so this is a supported mode
  rather than new work).

**Open questions**

- **PQ defaults.** What PQ code size / number of subquantizers should the v1
  engine-internal defaults use, and how are they chosen per dimensionality to hit
  the recall targets without over-spending RAM? Settle during the 50k/100M runs.
- **Cache vs FTS RAM split.** How is the in-RAM cache budget sized against
  `VECTOR_STORE_MEMORY_LIMIT` when full-text search shares the node? Needs a
  concrete split (or a dynamic policy) so DiskANN's page cache and FTS do not
  starve each other.
- **Latency target validation.** Is the tentative p99 ≤ 15 ms at 1B (k=10)
  achievable at the chosen recall, or does it need revising once the 1B Pareto
  curves exist? Confirm post-benchmark.
- **Crash consistency strategy.** In the disk provider, do we rely on
  atomic/checkpointed writes for in-place updates, or detect a torn index on load
  and rebuild from CDC? Decide and validate before GA.

**Dependencies**

- DiskANN3 (`microsoft/DiskANN`) Rust crates (disk provider, quantization).
- Cloud/Infra: storage-optimized instance types with local NVMe; Packer images.
- ScyllaDB side: not required for engine selection (per-node config). Only the
  later CQL-facing follow-ups — exposing PQ parameters and filtered ANN — touch
  the CQL surface and need ScyllaDB-side work.

## References

- DiskANN3 library (Microsoft Research, Rust, MIT, `v0.54.0`):
  <https://github.com/microsoft/DiskANN>
- DiskANN project & research overview:
  <https://github.com/microsoft/DiskANN/wiki/DiskANN-Project-and-Research-Overview-(2018%E2%80%90present)>
- DiskANN (NeurIPS'19): *Fast Accurate Billion-point Nearest Neighbor Search on
  a Single Node* — <https://harsha-simhadri.org/pubs/DiskANN19.pdf>
- Fresh-DiskANN (streaming updates): <https://arxiv.org/abs/2105.09613>
- IP-DiskANN (in-place updates): <https://arxiv.org/abs/2502.13826>
- Filtered-DiskANN: <https://harsha-simhadri.org/pubs/Filtered-DiskANN23.pdf>
- ScyllaDB Vector Search 10M benchmark (methodology reference):
  <https://www.scylladb.com/2026/05/01/vector-search-10m-benchmark/>
- ScyllaDB Vector Search 1B benchmark:
  <https://www.scylladb.com/2025/12/01/scylladb-vector-search-1b-benchmark/>
- Building a Low-Latency Vector Search Engine for ScyllaDB:
  <https://www.scylladb.com/2025/10/08/building-a-low-latency-vector-search-engine/>
- USearch: <https://github.com/unum-cloud/usearch>
- In-repo benchmarking guide: [`docs/benchmarking.md`](../../benchmarking.md)
- VectorDBBench (ScyllaDB fork): <https://github.com/scylladb/VectorDBBench>
- Cloud docs (concepts, sizing, quantization, deployments): ScyllaDB Cloud
  Vector Search documentation set.
- Design: [Add link]
- Test plan: [Add link]
- Jira epics: [Add link]
- Related SEPs: [Add links]
