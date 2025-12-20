<!-- markdownlint-disable MD033 MD041 -->
# Longbow

<img width="1024" height="559" alt="image" src="https://github.com/user-attachments/assets/16b4632a-f09b-42ab-9b05-9ab5a25566bf" />

Longbow is a high-performance, in-memory vector store implementing the Apache Arrow Flight protocol.
It is designed for efficient, zero-copy transport of large datasets and vector embeddings between agents and services.

## Features

* **Protocol**: Apache Arrow Flight (over gRPC/HTTP2).
* **Search**: High-performance HNSW vector search with hybrid (Dense + Sparse) support.
* **Filtering**: Metadata-aware predicate filtering for searches and scans.
* **Lifecycle**: Support for vector deletion via tombstones.
* **Durable**: WAL with Apache Parquet format snapshots.
* **Storage**: In-memory ephemeral storage for zero-copy high-speed access.
* **Observability**: Structured JSON logging and 100+ Prometheus metrics.

## Architecture & Ports

To ensure high performance under load, Longbow splits traffic into two dedicated gRPC servers:

* **Data Server (Port 3000)**: Handles heavy I/O operations (`DoGet`, `DoPut`, `DoExchange`).
* **Meta Server (Port 3001)**: Handles lightweight metadata operations (`ListFlights`, `GetFlightInfo`, `DoAction`).

**Why?**
Separating these concerns prevents long-running data transfer operations from blocking metadata requests. This ensures
that clients can always discover streams and check status even when the system is under heavy write/read load.

## Observability & Metrics

Longbow exposes Prometheus metrics on a dedicated port to ensure observability without impacting the main Flight
service.

* **Scrape Port**: 9090
* **Scrape Path**: /metrics

### Custom Metrics

### Key Metrics

| Metric Name | Type | Description |
| :--- | :--- | :--- |
| `longbow_flight_operations_total` | Counter | Total number of Flight operations (DoGet, DoPut, etc.) |
| `longbow_flight_duration_seconds` | Histogram | Latency distribution of Flight operations |
| `longbow_flight_rows_processed_total` | Counter | Total rows processed in scans and searches |
| `longbow_vector_search_latency_seconds` | Histogram | Latency of k-NN search operations |
| `longbow_vector_index_size` | Gauge | Current number of vectors in the index |
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

* [Persistence & Snapshots](docs/persistence.md)
* [Vector Search Architecture](docs/vectorsearch.md)
* [Troubleshooting Guide](docs/troubleshooting.md)
* [Metrics](docs/metrics.md)
