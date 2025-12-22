# Longbow Configuration & Tuning Guide

This document details the configuration parameters for Longbow and provides tuning recommendations based on performance validation tests (Dec 2025).

## Core Configuration

Longbow is configured via environment variables.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_DATA_HOME` | `/var/lib/longbow` | Base directory for data storage (WAL, indexes). |
| `LONGBOW_BIND_ADDR` | `0.0.0.0` | Network interface to bind to. |
| `LONGBOW_GRPC_PORT` | `9000` | Port for Flight (Data Plane) traffic. |
| `LONGBOW_HTTP_PORT` | `8080` | Port for metrics and health checks. |

## gRPC & Networking

These settings are critical for handling large vector payloads (e.g., 1536+ dimensions).

| Variable | Default | Tuning Recommendation |
| :--- | :--- | :--- |
| `LONGBOW_GRPC_MAX_RECV_MSG_SIZE` | `536870912` (512MB) | **Increase** if encountering "resource exhausted" errors with large batches. Validated up to 512MB. |
| `LONGBOW_GRPC_MAX_SEND_MSG_SIZE` | `536870912` (512MB) | Keep consistent with Recv size. |

## Indexing & Performance

| Variable | Default | Tuning Recommendation |
| :--- | :--- | :--- |
| `LONGBOW_HNSW_EF_CONSTRUCTION` | `200` | Higher = better recall, slower build. `200` is a good balance. |
| `LONGBOW_HNSW_M` | `16` | Max connections per layer. `16-32` recommended for high-dim vectors. |
| `LONGBOW_AUTO_SHARDING_THRESHOLD` | `10000` | Rows per shard. Decrease for very large dimensions to improve parallelism. |

## Hybrid Search (New)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_HYBRID_SEARCH_ENABLED` | `true` | Enables BM25 inverted indexes alongside HNSW. |
| `LONGBOW_BM25_K1` | `1.2` | Term saturation parameter. |
| `LONGBOW_BM25_B` | `0.75` | Length normalization parameter. |

## Memory Management

- **Heap Sizing**: Go GC is aggressive. Set `GOMEMLIMIT` to 90% of container memory (e.g., `GOMEMLIMIT=14GiB` for a 16GB node) to prevent OOM kills.
- **Index Overhead**: Approx `(4 * d + 8 * M) * N` bytes.
  - Example (1M vectors, 128-dim, M=16): `(512 + 128) * 1M` ≈ 640MB RAM + metadata.

## Runbook: Common Operations

### Restarting a Node

1. Longbow uses a WAL. Safe to `kill -SIGTERM`.
2. Upon restart, it replays the WAL. This may take time for large datasets.
3. **Monitor**: `longbow_wal_replay_duration_seconds`.

### Adding a Node

1. Start new node pointing to an existing seed node.
2. `LONGBOW_JOIN_ADDRS=seed-node:7946`.
3. Cluster will auto-rebalance (future feature). Currently, requires manual shard movement or dual-write.

### Alert Thresholds

- **High Latency**: `longbow_request_duration_seconds_bucket{le="0.5"} < 0.95` (If <95% of requests are faster than 500ms).
- **Disk Space**: Alert at 80% usage on `LONGBOW_DATA_HOME` volume.
