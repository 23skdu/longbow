# Longbow Observability

This directory contains the necessary configuration for monitoring Longbow with Prometheus and Grafana.

## Files

- `dashboards/longbow.json`: A comprehensive Grafana dashboard.
- `rules.yml`: Prometheus alerting and recording rules.

## Metrics Overview

Longbow exposes 100+ metrics on port `:9090/metrics`. Key metric categories include:

### Flight Operations

Metrics tracking the performance and volume of `DoPut` and `DoGet` operations.

- `longbow_flight_operations_total`: Request counts by method and status.
- `longbow_flight_duration_seconds`: Response time histograms.
- `longbow_flight_rows_processed_total`: Throughput measured in rows.

### Vector Index (HNSW)

Metrics related to the performance of vector searches and background indexing.

- `longbow_vector_search_latency_seconds`: k-NN search latency distribution.
- `longbow_index_queue_depth`: Lag indicator for asynchronous indexing.
- `longbow_hnsw_nodes_visited`: Search complexity indicator.

### Reliability & Lifecycle

- `longbow_ipc_decode_errors_total`: Decoding failures or recovered panics.
- `longbow_tombstones_total`: Count of active deletions.
- `longbow_evictions_total`: Cache eviction counts.

## Prometheus Rules

The `rules.yml` file includes:

1. **Critical Alerts**: High search latency (>1s p99).
2. **Warning Alerts**: High IPC error rates, indexing lag (>10k jobs), and memory pressure.
3. **Recording Rules**: Pre-calculated QPS rates for search and ingestion.

## Grafana Dashboard

Import the `dashboards/longbow.json` file into Grafana to visualize:

- **Operation Overview**: QPS and Latency for all Flight methods.
- **Index Performance**: HNSW search latency and queue depth.
- **Resource Usage**: Memory fragmentation and Arrow allocator stats.
- **Error Tracking**: IPC decodes and validation failures.
