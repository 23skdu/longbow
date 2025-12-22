# Longbow Configuration Guide

Longbow is stateless and configured entirely via environment variables.

## Storage Configuration

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_DATA_PATH` | `./data` | Directory where Longbow stores its indices and WALs. |
| `STORAGE_USE_IOURING` | `false` | Enable `io_uring` WAL backend (Linux only). Requires kernel 5.10+. |

## Cluster & Networking

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_NODE_ID` | `hostname` | Unique identifier for this node in the cluster. |
| `LONGBOW_LISTEN_ADDR` | `:9000` | gRPC listen address for client and inter-node communication. |
| `LONGBOW_META_ADDR` | `:9001` | gRPC listen address for metadata services. |
| `LONGBOW_GOSSIP_ENABLED` | `false` | Enable distributed discovery via SWIM protocol. |
| `LONGBOW_GOSSIP_PORT` | `7946` | Port for gossip UDP/TCP traffic. |
| `LONGBOW_GOSSIP_SEED_PEERS`| `""` | Comma-separated list of seed nodes (e.g., `10.0.0.1:7946,10.0.0.2:7946`). |

## Performance Tuning

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_NUMA_ENABLE` | `false` | Enable NUMA-aware memory allocation and thread pinning. |
| `LONGBOW_WAL_BATCH_SIZE` | `1000` | Max records to batch before flushing WAL. |
| `LONGBOW_WAL_FLUSH_INTERVAL`| `100ms` | Max time to wait before flushing incomplete batches. |

## Observability

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_METRICS_ADDR` | `:2112` | Address to expose Prometheus metrics (e.g., `/metrics`). |

## Feature Flags

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_EXPERIMENTAL_LOCKFREE` | `false` | Enable experimental lock-free HNSW indexing. |
