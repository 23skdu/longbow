# Longbow Configuration Guide

Longbow is configured entirely via environment variables, adhering to the Twelve-Factor App methodology.
This document details all available configuration parameters, their defaults, and tuning
recommendations.

## Core Configuration

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_DATA_PATH` | `./data` | Base directory for all persistent data (WAL, indexes, snapshots). |
| `LONGBOW_LISTEN_ADDR` | `0.0.0.0:3000` | Network interface and port to bind for Data Plane (Arrow Flight). |
| `LONGBOW_META_ADDR` | `0.0.0.0:3001` | Network interface and port to bind for Metadata and Control Plane services. |
| `LONGBOW_METRICS_ADDR` | `0.0.0.0:9090` | Port for Prometheus metrics and health checks. |
| `LONGBOW_NODE_ID` | `""` | Unique identifier for the node in the cluster. Defaults to hostname. |

## gRPC & Networking Tuning

These settings optimize the high-throughput vector transport layer.

| Variable | Default | Tuning Recommendation |
| :--- | :--- | :--- |
| `LONGBOW_GRPC_MAX_RECV_MSG_SIZE` | `536870912` (512MB) | Increase if sending massive vector batches. |
| `LONGBOW_GRPC_MAX_SEND_MSG_SIZE` | `536870912` (512MB) | Should match Receive size. |
| `LONGBOW_GRPC_INITIAL_WINDOW_SIZE` | `1048576` (1MB) | High window sizes improve throughput on high-latency links. |
| `LONGBOW_GRPC_KEEPALIVE_TIME` | `2h` | Frequency of TCP-level keepalive pings. |
| `LONGBOW_GRPC_KEEPALIVE_TIMEOUT` | `20s` | Timeout for keepalive pings. |
| `LONGBOW_GRPC_KEEPALIVE_MIN_TIME` | `5m` | Minimum time between consecutive keepalive pings. |
| `LONGBOW_GRPC_MAX_CONCURRENT_STREAMS`| `250` | Maximum number of concurrent gRPC streams. |

## Indexing & HNSW Configuration (Pragmas)

These parameters control the HNSW graph construction and search behavior.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_HNSW_M` | `16` | Max connections per node. Use `32-48` for high-dimensional vectors (768+). |
| `LONGBOW_HNSW_EF_CONSTRUCTION` | `200` | Complexity of graph building. Increase to `400-800` for 99.9% recall. |
| `LONGBOW_HNSW_ALPHA` | `1.0` | **Pragma**: Diversity parameter for neighbor selection. Set to `1.2` for better connectivity at scale. |
| `LONGBOW_HNSW_KEEP_PRUNED` | `false` | **Pragma**: If true, ensures nodes always reach `M` connections by backfilling pruned edges. |
| `LONGBOW_HNSW_SQ8_ENABLED` | `false` | Enable SQ8 scalar quantization for 4x memory reduction. |
| `LONGBOW_HNSW_FLOAT16_ENABLED` | `false` | Enable native Float16 storage for 2x memory reduction. |
| `LONGBOW_USE_DISK` | `false` | Enable SSD-based vector offloading (Disk-ANN) for Reduced RAM usage. |

| `LONGBOW_HNSW_REFINEMENT_FACTOR` | `1.0` | Refinement factor for SQ8 search. `2.0-4.0` recommended for high recall. |
| `LONGBOW_HNSW_PQ_ENABLED` | `false` | Enable Product Quantization. |
| `LONGBOW_USE_HNSW2` | `false` | Enable the new Arrow-native HNSW implementation. |
| `LONGBOW_USE_DISK` | `false` | Enable SSD-based vector offloading (Disk-ANN) for Reduced RAM usage. |
| `LONGBOW_AUTO_SHARDING_THRESHOLD` | `10000` | Number of vectors per shard before triggering a split. |
| `LONGBOW_AUTO_SHARDING_SPLIT_THRESHOLD` | `65536` | Chunk size for sharded HNSW. |
| `LONGBOW_RING_SHARDING_ENABLED` | `true` | Enable consistent hashing ring sharding. |

## Cluster Discovery (Gossip)

Longbow uses the SWIM protocol for decentralized membership.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_GOSSIP_ENABLED` | `false` | Set to `true` to enable mesh networking. |
| `LONGBOW_GOSSIP_PORT` | `7946` | UDP port for gossip traffic. |
| `LONGBOW_GOSSIP_DISCOVERY_PROVIDER` | `static` | `static`, `k8s`, or `dns`. |
| `LONGBOW_GOSSIP_STATIC_PEERS` | `""` | Comma-separated list of seed nodes (e.g., `node1:7946,node2:7946`). |
| `LONGBOW_GOSSIP_INTERVAL` | `200ms` | Frequency of membership probes. |

## Storage & Persistence

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_STORAGE_USE_IOURING` | `false` | Enable `io_uring` for high-performance WAL writes (Linux only). |
| `LONGBOW_STORAGE_USE_DIRECT_IO` | `false` | Enable direct I/O for WAL writes. |
| `LONGBOW_STORAGE_ASYNC_FSYNC` | `true` | When true, WAL flushes don't block the ingestion hot path. |
| `LONGBOW_STORAGE_DOPUT_BATCH_SIZE`| `100` | Target batch size for WAL writes. |
| `LONGBOW_SNAPSHOT_INTERVAL` | `1h` | Frequency of full index snapshots to disk. |
| `LONGBOW_MAX_WAL_SIZE` | `100MB` | Maximum size of a WAL segment before rotation. |

## Memory Management & GC

- **GOMEMLIMIT**: Crucial for Kubernetes. Set to 90% of your container memory limit to prevent OOM kills.
- **LONGBOW_MAX_MEMORY**: Bound the vector store memory usage (bytes). Defaults to 1GB.
- **LONGBOW_MEMORY_EVICTION_POLICY**: `lru` or `random`. Defaults to `lru`.
- **LONGBOW_GC_BALLAST_G**: Ballast size in GB to stabilize GC.
- **LONGBOW_GOGC**: Go Garbage Collector percentage.

## Hybrid Search (New)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_HYBRID_SEARCH_ENABLED` | `false` | Enables BM25 inverted indexes alongside HNSW. |
| `LONGBOW_HYBRID_TEXT_COLUMNS` | `""` | Comma-separated list of columns to index for keyword search. |
| `LONGBOW_HYBRID_ALPHA` | `0.5` | Weighting between Vector (1.0) and Keyword (0.0) results. |

## Observability

- **Metrics**: Available at `http://<LONGBOW_METRICS_ADDR>/metrics` in Prometheus format.
- **Pprof**: Profiling endpoints are available under `/debug/pprof/` on the metrics port.
- **Tracing**: Longbow supports OpenTelemetry. Use `OTEL_EXPORTER_OTLP_ENDPOINT` to direct traces
  to a collector.

## Circuit Breaker & Rate Limiting

- **LONGBOW_RATE_LIMIT_RPS**: Limit incoming requests per second.
- **Circuit Breaker**: Automatically trips after 10 consecutive failures to protect downstream resources.
  Cooldown is 30 seconds.
