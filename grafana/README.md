# Longbow Observability

This directory contains the necessary configuration for monitoring Longbow with Prometheus and Grafana.

## Dashboards

Longbow now has **6 focused dashboards** for better organization and performance:

| Dashboard | UID | Description |
|-----------|-----|-------------|
| [Overview](dashboards/overview.json) | `longbow-overview` | High-level cluster health, key metrics at a glance |
| [Search & Query](dashboards/search-query.json) | `longbow-search-query` | gRPC operations, search latency, connection pools |
| [Index & Storage](dashboards/index-storage.json) | `longbow-index-storage` | HNSW, vectors, WAL, compaction |
| [Memory & Performance](dashboards/memory-performance.json) | `longbow-memory-performance` | Memory, SIMD, GPU acceleration |
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

## Importing Dashboards

```bash
# Import each dashboard via Grafana UI or API
curl -X POST http://localhost:3000/api/dashboards/import \
  -H "Content-Type: application/json" \
  -d @dashboards/overview.json
```

Set `${datasource}` to your Prometheus data source name when importing.
