# Longbow Observability

This directory contains the monitoring configuration for Longbow: Grafana
dashboards, Prometheus alerting rules, and the promtool harness that validates
the rules.

## Dashboards

There are **11 dashboards**. All are valid Grafana JSON, and all UIDs are unique.

### Operational dashboards

Use these day to day.

| Dashboard | UID | Panels | Description |
|-----------|-----|-------:|-------------|
| [Overview](dashboards/overview.json) | `longbow-overview` | 19 | High-level cluster health, key metrics at a glance |
| [Search & Query](dashboards/search-query.json) | `longbow-search-query` | 39 | gRPC operations, search latency, connection pools |
| [Index & Storage](dashboards/index-storage.json) | `longbow-index-storage` | 34 | HNSW, vectors, WAL, compaction |
| [Ingestion Performance](dashboards/ingestion-performance.json) | `longbow-ingestion` | 11 | Ingest queue depth, queue latency, batch sizes |
| [Memory & Performance](dashboards/memory-performance.json) | `longbow-memory-performance` | 38 | Memory, SIMD, GPU acceleration |
| [GPU & ONNX Health](dashboards/gpu-onnx-health.json) | `gpu-onnx-health` | 20 | Metal/CUDA performance and ONNX model execution health |
| [Cluster & Replication](dashboards/cluster-replication.json) | `longbow-cluster-replication` | 27 | Gossip, sharding, quorum, global search |
| [Advanced Features](dashboards/advanced.json) | `longbow-advanced` | 41 | Hybrid search, pipelines, HNSW adaptive, quantization |
| [Advanced Internals](dashboards/advanced-internals.json) | `longbow_adv_int` | 81 | Low-level internals: pools, arenas, adjacencies, codepaths |

### Conformance and legacy

| Dashboard | UID | Panels | Description |
|-----------|-----|-------:|-------------|
| [All-Metrics Audit](dashboards/all-metrics-audit.json) | `longbow-audit` | 493 | One panel per exported metric family. The reference surface for checking that a metric exists before writing a rule or a panel against it — see roadmap Item 18. |
| [Monolithic (deprecated)](dashboards/longbow.json) | `longbow-cluster` | 143 | Original all-in-one dashboard. Superseded by the nine operational dashboards above; kept for historical dashboards that still reference its UID. Do not add panels to it. |

> The `longbow-` prefix is the convention for operational dashboards. `longbow.json`
> and `advanced-internals.json` predate it (`longbow-cluster`, `longbow_adv_int`),
> so filter on the UID prefix carefully when scripting imports.

## Metrics Overview

Longbow exposes **789 `longbow_*` metric families** on `/metrics`
(`LONGBOW_METRICS_ADDR`, default `:6000` for the bare binary — see
`docs/metrics.md`). Key categories:

- **Flight / gRPC**: `longbow_flight_ops_total`, `longbow_flight_duration_seconds`, `longbow_flight_rows_processed_total`
- **Vector index**: `longbow_hnsw_search_duration_seconds`, `longbow_index_queue_depth`, `longbow_hnsw_nodes_visited`
- **Memory**: `longbow_memory_heap_in_use_bytes`, `longbow_arena_off_heap_bytes`, `longbow_slab_fragmentation_ratio`
- **Reliability**: `longbow_evictions_total`, `longbow_tombstones_total`, `longbow_ipc_buffer_pool_utilization`
- **Bulk construction**: `longbow_hnsw_bulk_insert_latency_by_type_seconds`, `longbow_hnsw_bulk_insert_latency_by_dim_seconds`, `longbow_hnsw_average_degree`

`docs/metrics.md` is the full per-metric reference.

## Prometheus Rules

`rules.yml` defines **28 rule groups** containing **72 alerts** (15 `critical`,
43 `warning`, 14 `info`) and **5 recording rules**.

| Group | Alerts | Covers |
|---|---|---|
| `longbow_search_alerts` | 2 | Vector search latency, global search |
| `longbow_hybrid_search_alerts` | 2 | Hybrid/BM25 activity — *currently dead, see below* |
| `longbow_wal_alerts` | 3 | WAL queue depth, fsync latency, write errors |
| `longbow_index_alerts` | 5 | Indexing queue depth, job latency, build failures, repair, adaptive |
| `longbow_mesh_alerts` | 3 | Gossip membership, split brain, ping failures |
| `longbow_replication_alerts` | 3 | Replication lag, quorum failures, stall |
| `longbow_memory_alerts` | 4 | Pressure level, backpressure rejections, fragmentation |
| `longbow_performance_alerts` | 5 | IPC errors, Flight latency, GPU, dataset lock contention |
| `longbow_compaction_alerts` | 2 | Compaction stall, auto-compaction churn |
| `longbow_recording_rules` | — | 5 recording rules (QPS, throughput, ratios) |
| `longbow_s3_alerts` | 2 | S3 failure rate and latency |
| `longbow_system_alerts` | 1 | IPC buffer pool saturation |
| `longbow_distributed_alerts` | 3 | Circuit breaker, partial failures, vector-clock conflicts |
| `longbow_simd_alerts` | 2 | SIMD dispatch latency, generic fallback rate |
| `longbow_graph_navigation_alerts` | 2 | GraphRAG navigation latency and stall |
| `longbow_rdma_alerts` | 3 | RDMA errors and inactivity, tombstone density |
| `longbow_gpu_alerts` | 3 | GPU memory pressure, index desync, CPU fallback |
| `longbow_adaptive_gc_alerts` | 2 | GOGC escalation, allocation spikes |
| `longbow_storage_alerts` | 2 | io_uring availability, I/O wait |
| `longbow_pipeline_alerts` | 1 | Pipeline stage latency |
| `longbow_temporal_alerts` | 3 | Temporal indexing activity, as-of cache drop causes |
| `longbow_layer_eviction_alerts` | 4 | Eviction errors, active layers, heap pressure, restore |
| `longbow_slab_alerts` | 3 | Slab pool hit ratio, boundary violations, leak probability |
| `longbow_arena_alerts` | 2 | Arena nil errors, off-heap usage |
| `longbow_simd_kernel_alerts` | 2 | Kernel rejection, scalar fallback ratio |
| `longbow_bulk_insert_alerts` | 4 | Bulk-insert construction cost and graph health |
| `longbow_hnsw_contention_alerts` | 2 | Update contention, CoW copy rate |
| `longbow_gpu_ingest_alerts` | 2 | GPU ingest kernel latency, neighbour prune rate |

### Known gaps

The 2026-10-11 audit found that **10 of these alerts, plus one recording rule,
reference a metric family the binary does not register**, and are therefore
permanently silent. An expression over an unregistered metric evaluates to an
empty series rather than to an error, so nothing distinguishes "healthy" from
"wired to nothing". The affected alerts are
`LongbowGPUFailure`, `LongbowGPUMemoryPressure`, `LongbowHybridSearchStalled`,
`LongbowBM25IndexStalled`, `LongbowIndexBuildFailures`, `LongbowHighIpcErrors`,
`LongbowReplicationStalled`, `LongbowHighIOWait`,
`LongbowIOUringNotAvailable`, and the `job:longbow_zero_copy_ratio` recording
rule (plus `job:longbow_put_qps`, whose label matcher selects nothing).

None of this is repaired yet. Tracked as roadmap Items 18-21, which also add the
CI gate that prevents the same class of gap recurring.

The test harness below also covers only **2 of the 28 groups**, so the remaining
26 have a parse check and no behavioural test — which is how the above survived.

### Bulk-insert construction alerts

The `longbow_bulk_insert_alerts` group covers HNSW graph construction cost, which
was previously charted but never alerted. A 250k-vector TurboQuant build could run
for 45 minutes with nothing firing.

| Alert | Severity | Fires when |
|---|---|---|
| `LongbowSlowBulkInsertByType` | warning | P95 bulk insert for a type exceeds 300s |
| `LongbowBulkInsertStalled` | critical | Nothing has completed in 15m while the indexing queue is non-empty |
| `LongbowBulkInsertFasterThanFloat32` | critical | TurboQuant construction runs at <0.8x float32 |
| `LongbowLowHNSWAverageDegree` | critical | Mean layer-0 degree for a dataset falls below 8 |

The last two are **correctness** alerts, not performance alerts.

TurboQuant building *faster* than float32 looks like a win and is not one. Before
`a955a0c1`, neighbour selection read the float32 arena, which is empty for every
element type other than float32, so every TurboQuant candidate was rejected and the
graph was left partly disconnected: mean layer-0 degree 6.92 against an `MMax0` of 16,
with 27% of nodes unreachable at any `ef`. A cheap index that cannot find a quarter of
the corpus. If either alert fires, investigate graph connectivity.

Measured reference for 250,000 vectors at `dim=128`, bulk-inserted in 10,000-row
batches:

| Vector type | Construction time | Relative to float32 |
|---|---|---|
| float32 | 36.8s | 1.00x |
| turboquant 4-bit | 107.2s | **2.91x** |

Resulting layer-0 connectivity, and the graph-state change that fixed the defect:

| Revision | layer-0 edges | mean degree | reachable from entry point |
|---|---|---|---|
| `7f872022` (pre-fix) | 276,940 | 6.92 | **29,215 / 40,000 (73.0%)** |
| `a955a0c1` (post-fix) | 628,013 | 15.70 | **39,251 / 40,000 (98.1%)** |

TurboQuant building *slower* than float32 is the expected, correct outcome; a ratio
below 1.0 is the signal worth investigating.

> The `LongbowSlowBulkInsertByType` threshold of 300s is only meaningful because the
> bulk-insert histogram buckets were widened past 30s in
> `internal/metrics/hnsw_metrics.go`. With the old top bucket at 30, every build slower
> than 30s reported P95 as `+Inf`, so a 107s healthy build was indistinguishable from
> one that never finishes, and no threshold could separate them. If you change those
> buckets, re-check this alert.

### Testing the rules

```bash
PROMTOOL=/path/to/promtool grafana/tests/test-rules.sh
```

This parses every rule in `rules.yml` and runs promtool unit tests for the
`longbow_bulk_insert_alerts` and `longbow_temporal_alerts` groups, covering the
alerts above: a healthy 250k build (all quiet), the pre-`a955a0c1` defect shape
(both signature alerts fire), a genuinely slow build, and a stalled queue. The
other 26 groups are parse-checked only — see **Known gaps** above.

The script rewrites `humanizeBytes` to `humanize` in a temporary copy before
handing the file to promtool, because `humanizeBytes` is a Grafana template
function that promtool cannot resolve. The checked-in `rules.yml` is never
modified.

`promtool` is not vendored; install it from a Prometheus release archive or set
`PROMTOOL` to an existing binary.

## Importing Dashboards

Dashboards are not provisioned automatically — there is no Grafana service in
`docker-compose.yml`, no Grafana resources in `helm/`, and no provisioning
directory under `grafana/`. Import them manually:

```bash
for d in dashboards/*.json; do
  curl -sS -X POST http://localhost:3000/api/dashboards/import \
    -H "Content-Type: application/json" \
    -d "{\"dashboard\": $(cat "$d"), \"overwrite\": true}"
done
```

Set `${datasource}` to your Prometheus data source name when importing. Note
that `longbow.json` shares a UID-prefix style with the operational dashboards but
is deprecated; exclude it from a fresh install.

Adding provisioning and an Alertmanager route is tracked as roadmap Items 25
and 26.
