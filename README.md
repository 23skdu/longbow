![CI](https://github.com/23skdu/longbow/actions/workflows/ci.yml/badge.svg)
![Helm Validation](https://github.com/23skdu/longbow/actions/workflows/helm-validation.yml/badge.svg)
![Markdown Lint](https://github.com/23skdu/longbow/actions/workflows/markdown-lint.yml/badge.svg)

# Longbow

<img width="1024" height="559" alt="image" src="https://github.com/user-attachments/assets/775eb0b4-7e55-4524-abda-c9489de0194e" />


Longbow is a distributed, high-performance vector cache built for modern AI/Agentic workloads. It leverages zero-copy data paths, SIMD optimizations, and advanced storage backends to deliver sub-millisecond latency.

## Key Features

- **High Performance**: Built on Apache Arrow for zero-copy data transfer.
- **Distributed**: Consistent hashing and gossip-based membership (SWIM protocol).
- **Optimized Storage**: Optional `io_uring` WAL backend for high-throughput ingestion.
- **Hardware Aware**: NUMA-aware memory allocation and SIMD vector distance calculations.
- **Smart Client**: Resilient Go SDK that handles request routing transparently.

## Architecture

Longbow uses a shared-nothing architecture where every node is loosely coupled.

See [Architecture Guide](docs/architecture.md) for a deep dive.

## Getting Started

### Prerequisites

- Go 1.25+
- Linux (recommended for best performance) or macOS

### Installation

```bash
git clone https://github.com/23skdu/longbow.git
cd longbow
go build -o bin/longbow ./cmd/longbow
```

### Running a Local Cluster

```bash
./scripts/start_local_cluster.sh
```

### Running Benchmarks

Longbow includes a comprehensive, multi-platform benchmark suite:

```bash
# Run a targeted benchmark for specific types and dimensions
python3 scripts/unified_benchmark.py --modes cpu,metal --dtypes float32,turboquant --dims 128,384,768

# Results are generated as a Markdown matrix in docs/performance.md
```

## Configuration

Longbow is configured via environment variables. See [Deployment & Configuration](docs/deploy.md#2-configuration) for details.

Notable flags:

- `STORAGE_USE_IOURING=true` (Enable new Linux storage engine)
- `LONGBOW_HNSW_TURBOQUANT_ENABLED=true` (Enable SIMD-accelerated bit-packing)
- `LONGBOW_LEARNED_INDEX_ENABLED=true` (Enable adaptive learned index routing)

- **Protocol**: Apache Arrow Flight (over gRPC/HTTP2).
- **Search**: High-performance HNSW vector search with hybrid (Dense + Sparse) support and polymorphic vector types.
- **Distance Metrics**: Pluggable metrics (Euclidean, Cosine, Dot Product) with SIMD optimizations for all supported types.
- **Filtering**: Metadata-aware predicate filtering for searches and scans.
- **Lifecycle**: Support for vector deletion via tombstones.
- **Durable**: WAL with Apache Parquet format snapshots.
- **Storage**: In-memory ephemeral storage for zero-copy high-speed access.
- **Observability**: Structured JSON logging and 100+ Prometheus metrics.

## Supported Data Types & Dimensions

Longbow supports the following vector data types with optimized SIMD kernels:

| Data Type | Dimensions Supported | Notes |
|-----------|---------------------|-------|
| **float32** | 128 - 3072 | Full SIMD optimization (AVX2/AVX-512/Neon) |
| **float16** | 128 - 3072 | Metal/CUDA GPU kernels + CPU fallback |
| **float64** | 128 - 3072 | Full SIMD optimization |
| **int8/uint8** | 128 - 3072 | AVX2/Neon optimized |
| **int16/uint16** | 128 - 3072 | AVX2/Neon optimized |
| **int32/uint32** | 128 - 3072 | AVX2/Neon optimized |
| **int64/uint64** | 128 - 3072 | Generic SIMD & Metadata Filter support |
| **complex64/128**| 128 - 3072 | Full SIMD optimization |
| **turboquant** | 128 - 3072 | NEON/AVX2 FWHT optimized |

### SIMD-Accelerated Metadata Filtering

As of 0.1.9, Longbow supports **SIMD-accelerated predicate filtering** within the HNSW search path. This allows for extremely high QPS on filtered searches by pushing Boolean logic down into the vector traversal loop.

- **Supported Ops**: `=`, `!=`, `>`, `>=`, `<`, `<=`
- **Optimizations**: AVX-512 (AMD64) and Neon (ARM64) specialized kernels.
- **Impact**: Up to 5x higher QPS for highly selective filters.

### Optimized Kernel Dimensions

The following dimensions have dimension-specific optimized kernels:

| Dimension | Block Size | Optimization |
|----------|------------|--------------|
| 128 | N/A | Direct SIMD unroll |
| 256 | N/A | Direct SIMD unroll |
| 384 | N/A | AVX2/NEON specific kernels |
| 768 | 256 | Blocked SIMD |
| 1024 | 256 | Blocked SIMD |
| 1536 | 256 | Blocked SIMD |
| 2048 | 512 | Blocked SIMD |
| 3072 | 512 | Blocked SIMD |

## Architecture & Ports

To ensure high performance under load, Longbow splits traffic into two dedicated gRPC servers:

- **Data Server (Port 3000)**: Handles heavy I/O operations (`DoGet`, `DoPut`, `DoExchange`).
- **Meta Server (Port 3001)**: Handles lightweight metadata operations (`ListFlights`, `GetFlightInfo`, `DoAction`).

**Why?**
Separating these concerns prevents long-running data transfer operations from blocking metadata requests. This ensures
that clients can always discover streams and check status even when the system is under heavy write/read load.

## Observability & Metrics

Longbow exposes Prometheus metrics on a dedicated port to ensure observability without impacting the main Flight
service.

- **Scrape Port**: 9090
- **Scrape Path**: /metrics

### Custom Metrics

### Key Metrics

| Metric Name | Type | Description |
| :--- | :--- | :--- |
| `longbow_flight_ops_total` | Counter | Total number of Flight operations (DoGet, DoPut, etc.) |
| `longbow_flight_duration_seconds` | Histogram | Latency distribution of Flight operations |
| `longbow_flight_rows_processed_total` | Counter | Total rows processed in scans and searches |
| `longbow_hnsw_search_duration_seconds` | Histogram | Latency of k-NN search operations |
| `longbow_hnsw_node_count` | Gauge | Current number of vectors in the index |
| `longbow_tombstones_total` | Gauge | Number of active deleted vector tombstones |
| `longbow_index_queue_depth` | Gauge | Depth of the asynchronous indexing queue |
| `longbow_memory_fragmentation_ratio` | Gauge | Ratio of system memory reserved vs used |
| `longbow_wal_bytes_written_total` | Counter | Total bytes written to the WAL |
| `longbow_snapshot_duration_seconds` | Histogram | Duration of the Parquet snapshot process |
| `longbow_evictions_total` | Counter | Total number of evicted records (LRU) |
| `longbow_ipc_decode_errors_total` | Counter | Count of IPC decoding errors or panics |

For a detailed explanation of all 100+ metrics, see [Metrics Documentation](docs/metrics.md).

Standard Go runtime metrics are also exposed.

## Usage

### Running locally

```bash
go run cmd/longbow/main.go
```

### Docker

```bash
docker build -t longbow .
docker run -p 3000:3000 -p 3001:3001 -p 9090:9090 longbow
```

## Documentation

- [Distance Metrics & Functions](docs/functions.md)
- [Deployment & Configuration](docs/deploy.md)
- [Performance Benchmarks](docs/performance.md)
- [Persistence & Snapshots](docs/persistence.md)
- [Vector Search Architecture](docs/vectorsearch.md)
- [Troubleshooting Guide](docs/troubleshooting.md)
- [Metrics Documentation](docs/metrics.md)
