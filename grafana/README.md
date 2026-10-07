# Longbow Observability

This directory contains the necessary configuration for monitoring Longbow with Prometheus and Grafana.

## Dashboards

Longbow now has **7 focused dashboards** for better organization and performance:

| Dashboard | UID | Description |
|-----------|-----|-------------|
| [Overview](dashboards/overview.json) | `longbow-overview` | High-level cluster health, key metrics at a glance |
| [Search & Query](dashboards/search-query.json) | `longbow-search-query` | gRPC operations, search latency, connection pools |
| [Index & Storage](dashboards/index-storage.json) | `longbow-index-storage` | HNSW, vectors, WAL, compaction |
| [Memory & Performance](dashboards/memory-performance.json) | `longbow-memory-performance` | Memory, SIMD, GPU acceleration |
| [GPU & ONNX Health](dashboards/gpu-onnx-health.json) | `longbow-gpu-onnx` | Metal/CUDA performance and ONNX model execution health |
| [Cluster & Replication](dashboards/cluster-replication.json) | `longbow-cluster-replication` | Gossip, sharding, quorum, global search |
| [Advanced Features](dashboards/advanced.json) | `longbow-advanced` | Hybrid search, pipelines, HNSW adaptive, quantization |

### Legacy Dashboard

- `dashboards/longbow.json`: Original monolithic dashboard (3137 lines) - **deprecated**

## Metrics Overview

Longbow exposes **500+ metrics** on port `:9090/metrics`. Key metric categories:

### Flight Operations

- `longbow_flight_ops_total`: Request counts by method and status
- `longbow_flight_duration_seconds`: Response time histograms
- `longbow_flight_rows_processed_total`: Throughput in rows

### Vector Index (HNSW)

- `longbow_hnsw_search_duration_seconds`: k-NN search latency
- `longbow_index_queue_depth`: Async indexing lag
- `longbow_hnsw_nodes_visited`: Search complexity

### Memory & Performance

- `longbow_memory_heap_in_use_bytes`: Heap memory
- `longbow_simd_operations_total`: SIMD acceleration
- `longbow_gpu_*`: GPU metrics

### Reliability

- `longbow_evictions_total`: Cache evictions
- `longbow_tombstones_total`: Active deletions
- `longbow_ipc_buffer_pool_utilization`: IPC pool health

## Prometheus Rules

`rules.yml` contains:

- **Critical Alerts**: High search latency (>1s p99)
- **Warning Alerts**: IPC errors, indexing lag, memory pressure
- **Recording Rules**: Pre-calculated QPS for search/ingestion

### Bulk-insert construction alerts

The `longbow_bulk_insert_alerts` group covers HNSW graph construction cost, which was
previously charted but never alerted. A 250k-vector TurboQuant build could run for
45 minutes with nothing firing.

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
the corpus. If either alert fires, investigate graph connectivity - see
`docs/roadmap.md` §9.2.

Healthy reference for 250,000 vectors at `dim=128`: float32 ~37s, TurboQuant 4-bit
~107s, mean layer-0 degree ~15.7 at `MMax0=16`.

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

This parses every rule in `rules.yml` and runs four promtool unit-test scenarios
covering the alerts above: a healthy 250k build (all quiet), the pre-`a955a0c1` defect
shape (both signature alerts fire), a genuinely slow build, and a stalled queue.

The script rewrites `humanizeBytes` to `humanize` in a temporary copy before handing
the file to promtool, because `humanizeBytes` is a Grafana template function that
promtool cannot resolve. The checked-in `rules.yml` is never modified.

## Importing Dashboards

```bash
# Import each dashboard via Grafana UI or API

curl -X POST http://localhost:3000/api/dashboards/import \
  -H "Content-Type: application/json" \
  -d @dashboards/overview.json
```

Set `${datasource}` to your Prometheus data source name when importing.
