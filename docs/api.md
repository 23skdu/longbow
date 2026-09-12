# Longbow API Reference

## Overview

Longbow provides **gRPC + Apache Arrow Flight only**. No REST/HTTP API for data operations.

- **Arrow Flight API** - Primary gRPC-based protocol for high-performance operations
- **Admin Actions** - Flight-based administrative operations
- **Prometheus Metrics** - HTTP metrics endpoint on port 9090

### Protocol Ports

| Port | Service | Protocol |
|------|---------|----------|
| 3000 | Data Server | gRPC/Arrow Flight |
| 3001 | Meta Server | gRPC/Arrow Flight |
| 9090 | Metrics | HTTP/Prometheus |

### Data vs Meta Server

- **Data Plane (Port 3000)**: Optimized for raw throughput and local node operations. Handles ingestion, bulk scans, and local search.
- **Meta/Query Plane (Port 3001)**: Optimized for coordination and global visibility. Handles global search, advanced analytics, management, and CDC/discovery.

> **Note:** `DoPut` (Ingestion) is **DISABLED** on the Meta/Query port to prevent control-plane saturation. All data ingestion MUST target Port 3000.

## Sequence Diagram

```mermaid
sequenceDiagram
    participant Client
    participant DataServer as Data Server (3000)
    participant MetaServer as Meta Server (3001)

    Client->>DataServer: DoPut (Ingest)
    DataServer-->>Client: PutResult (backpressure if WAL > 80%)

    Client->>DataServer: DoGet (Local Search)
    DataServer-->>Client: RecordBatch Stream

    Client->>MetaServer: DoGet (Global Search)
    MetaServer-->>Client: RecordBatch Stream

    Client->>MetaServer: DoAction (Admin/Graph)
    MetaServer-->>Client: Result Stream

    Client->>DataServer: DoExchange (Sync/Mesh)
    DataServer-->>Client: Bidirectional Stream
```

## Arrow Flight Protocol

Longbow implements the Apache Arrow Flight protocol version 18.

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `DoPut` | RPC | Ingest vector data |
| `DoGet` | RPC | Execute vector search or bulk retrieval |
| `DoAction` | RPC | Administrative and graph operations |
| `DoExchange` | RPC | Streaming bidirectional communication |
| `ListFlights` | RPC | List available datasets |
| `GetFlightInfo` | RPC | Schema and metadata for a dataset path |

### Protocol Buffers

```protobuf
service FlightService {
    rpc DoPut(stream PutResult) returns (PutResult) {}
    rpc DoGet(Ticket) returns (stream RecordBatch) {}
    rpc DoAction(Action) returns (stream Result) {}
    rpc DoExchange(stream PutResult) returns (stream RecordBatch) {}
    rpc ListFlights(Criteria) returns (stream FlightInfo) {}
}

message Ticket {
    bytes data = 1;  // JSON-encoded ticket data
}

message Action {
    string type = 1;
    bytes body = 2;
}

message ArrowRecordBatch {
    bytes data = 1;  // IPC-serialized Arrow RecordBatch
}
```

### Ticket Format

```json
{
  "dataset": "dataset_name",
  "operation": "search|ingest|delete",
  "k": 10,
  "filter": "optional filter expression"
}
```

### ListFlights Filtering

Clients can filter results by providing a JSON-serialized `TicketQuery` in the `expression` field.

Supported Filters:

- `name`: Match by dataset name (operator: `contains`).
- `rows`: Match by row count (operators: `==`, `>`, `<=`).

### DoExchange Modes

- **VectorSearch**: High-performance bidirectional search.
- **sync**: Delta synchronization using WAL sequence numbers.
- **merkle_node**: Merkle tree navigation for data consistency checks.

## Data Plane

All data plane operations target **Port 3000**.

### DoPut - Ingestion

Stream Arrow RecordBatches to ingest data into a dataset.

- **Behavior**: Auto-creates dataset if missing. Supports batched writes, WAL logging, and async indexing.
- **Backpressure**: Signals `slow_down` metadata if WAL queue > 80%.

**Request:**

```protobuf
message ArrowRecordBatch {
    bytes data = 1;  // IPC-serialized Arrow RecordBatch
}
```

**Python Example:**

```python
import pyarrow.flight as pf

client = pf.connect("longbow-server:3000")

table = pa.table({
    "id": [1, 2, 3],
    "vector": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]]
})

writer, reader = client.do_put(
    pf.descriptor("dataset_name"),
    table.schema
)
writer.write_table(table)
writer.close()
reader.done()
```

**CLI:**

```bash
python3 scripts/ops_test.py put --dataset my_dataset --rows 1000
```

### DoGet - Local Search / Bulk Retrieval

**Local Search (single-node vector/hybrid search):**

```python
import pyarrow.flight as pf
import numpy as np

client = pf.connect("longbow-server:3000")

query = np.array([0.15, 0.25], dtype=np.float32)
ticket = pf.Ticket(json.dumps({
    "dataset": "my_dataset",
    "k": 10
}))

reader = client.do_get(ticket, query)
results = reader.read_all()
```

**Bulk Retrieval (stream all records):**

- **Input**: Ticket containing JSON `{"name": "dataset_name", "filters": [...]}`.
- **Output**: Stream of Arrow RecordBatches.

```bash
python3 scripts/ops_test.py get --dataset my_dataset
```

### DoExchange - Bidirectional Stream

Used for synchronization and advanced bidirectional protocols (verification echo/fetch, delta sync, Merkle tree navigation).

```bash
python3 scripts/ops_test.py exchange
```

## Control Plane

All control plane operations target **Port 3001**.

### DoGet - Global Search

Performs distributed Vector or Hybrid search across the entire cluster (automatically handles scatter-gather).

**Input Ticket:**

```json
{
  "search": {
    "dataset": "name",
    "vector": [...],
    "text_query": "optional",
    "k": 10,
    "graph_alpha": 0.5,
    "graph_depth": 2
  }
}
```

**Output:** Arrow RecordBatch stream with `id` (uint64) and `score` (float32).

```bash
# Vector Search
python3 scripts/ops_test.py search --dataset my_dataset --k 5

# Hybrid Search
python3 scripts/ops_test.py search --dataset my_dataset --text-query "apple" --alpha 0.5
```

### DoAction - Administrative Operations

#### Cluster & Mesh

| Action | Parameters | Description | CLI Command |
|--------|------------|-------------|-------------|
| `cluster-status` | - | Get node identity and member list | `ops_test.py status` |
| `MeshIdentity` | - | Get local node identity | (Internal) |
| `MeshStatus` | - | Get mesh membership list | (Internal) |
| `DiscoveryStatus` | - | Get peer discovery diagnostics | (Internal) |

#### Dataset Operations

| Action | Parameters | Description | CLI Command |
|--------|------------|-------------|-------------|
| `create_dataset` | schema | Create new dataset | (SDK) |
| `delete-dataset` | `{"dataset": "name"}` | Permanently delete dataset | (Used in `validate` or `scripts/cleanup.py`) |
| `delete` | `{"dataset": "ds", "id": "123"}` | Soft-delete by Primary ID | `ops_test.py delete --dataset <name> --ids 1,2` |
| `delete-vector` | `{"dataset": "ds", "vector_id": 123}` | Soft-delete by Internal ID | (Internal) |
| `compact` | name | Force compaction | (SDK) |
| `ForceSnapshot` | - | Force database snapshot to disk | `ops_test.py snapshot` |
| `check_readiness` | `{"dataset": "ds"}` | Check if index is ready | (SDK) |
| `wait-for-indexing` | `{"dataset": "ds"}` | Block until indexing completes | (SDK) |

#### Soft Deletions & Tombstones

Longbow uses a **Tombstone** mechanism for efficient deletions without blocking high-speed indexing:

1. **Logical Deletion**: When an ID is deleted, a bit is set in the batch's bitset.
2. **Search Exclusion**: The search engine automatically skips any record marked with a tombstone.
3. **Primary ID Support**: `delete` action by Primary ID (string/int64) uses the PrimaryIndex for O(1) location lookup.
4. **Internal ID Support**: `delete-vector` action by Internal ID (uint32) is useful for graph and edge operations.

#### Namespace Management

| Action | Parameters | Description | CLI Command |
|--------|------------|-------------|-------------|
| `CreateNamespace` | `{"name": "ns"}` | Create a new namespace | `ops_test.py namespaces` |
| `DeleteNamespace` | `{"name": "ns"}` | Delete an entire namespace | `ops_test.py namespaces` |
| `ListNamespaces` | (empty) | List all namespaces | `ops_test.py namespaces` |
| `GetTotalNamespaceCount` | - | Count total namespaces | `ops_test.py namespaces` |
| `GetNamespaceDatasetCount` | - | Count datasets in a namespace | `ops_test.py namespaces` |

**Examples:**

```python
# Create namespace
action = pf.Action("CreateNamespace", b'{"name": "my_namespace"}')
client.do_action(action)

# List namespaces
action = pf.Action("ListNamespaces", b"")
for result in client.do_action(action):
    print(result)
```

#### Graph & Relationship API

| Action | Parameters | Description | CLI Command |
|--------|------------|-------------|-------------|
| `add-edge` | `{"dataset": "ds", "source_id": 1, "target_id": 2, "predicate": "related"}` | Add semantic edge (Subject->Predicate->Object) | `ops_test.py add-edge ...` |
| `traverse-graph` | `{"dataset": "ds", "start_id": 1, "max_depth": 3}` | Traverse graph from start node | `ops_test.py traverse ...` |
| `calculate-pagerank` | `{"dataset": "ds", "iterations": 20}` | Compute importance scores for nodes | `ops_test.py pagerank ...` |
| `detect-communities` | `{"dataset": "ds"}` | Group nodes into clusters based on topology | `ops_test.py communities ...` |
| `GetGraphStats` | - | Get graph statistics (nodes, edges) | `ops_test.py graph-stats ...` |

#### Search Actions

| Action | Parameters | Description | CLI Command |
|--------|------------|-------------|-------------|
| `VectorSearch` | JSON with `dataset`, `vector`, `k`, optional `filters` | Unary vector search (alternative to DoGet) | (Internal) |
| `VectorSearchByID` | `{"dataset": "...", "id": "...", "k": 10}` | Find similar vectors to a given ID | `ops_test.py similar --dataset <name> --id <ID>` |

**Examples:**

```python
# Get dataset details
action = pf.Action("get_dataset", b"dataset_name")
for result in client.do_action(action):
    print(result)

# Add edge
action = pf.Action("add-edge", json.dumps({
    "dataset": "ds",
    "source_id": 1,
    "target_id": 2,
    "predicate": "related"
}).encode())
client.do_action(action)

# Graph traversal
action = pf.Action("traverse-graph", json.dumps({
    "dataset": "ds",
    "start_id": 1,
    "max_depth": 3
}).encode())
for result in client.do_action(action):
    print(result)
```

### Backpressure Monitoring

The Data Server (`DoPut`) monitors the Write-Ahead Log (WAL) queue depth. If the queue exceeds **80% capacity**, the server applies backpressure:

1. Server logs a `wal_pressure` warning.
2. `DoPut` responses include metadata: `{"status": "slow_down", "reason": "wal_pressure"}`.

Clients (including the Python SDK) monitor this metadata and should implement backoff or throttling to avoid overloading the persistence layer.

## Prometheus Metrics

Longbow exposes Prometheus metrics on port 9090 (configurable via `METRICS_ADDR`).

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `longbow_flight_ops_total` | Counter | Total Flight operations |
| `longbow_flight_duration_seconds` | Histogram | Operation latency |
| `longbow_search_duration_seconds` | Histogram | Search latency |
| `longbow_ingestion_records_total` | Counter | Ingested records |
| `longbow_memory_bytes` | Gauge | Current memory usage |
| `longbow_dataset_count` | Gauge | Number of datasets |
| `longbow_gpu_memory_bytes` | Gauge | GPU memory usage |
| `longbow_gc_pause_duration_seconds` | Histogram | GC pause times |

### Example Prometheus Queries

```promql
# Search latency percentiles
histogram_quantile(0.99, rate(longbow_search_duration_seconds_bucket[5m]))

# Operations per second
rate(longbow_flight_ops_total[1m])

# Memory utilization
longbow_memory_bytes / longbow_max_memory_bytes
```

## CLI Testing Tools

### `scripts/ops_test.py`

The primary functional CLI tool. Use this for:

- Manual testing of all features.
- CI/CD integration smoke tests (`validate` subcommand).
- Debugging specific operations.

```bash
# Run full smoke test suite
python3 scripts/ops_test.py validate

# Inspect cluster membership
python3 scripts/ops_test.py status

# Upload 1000 rows to 'my_dataset'
python3 scripts/ops_test.py put --dataset my_dataset --rows 1000

# Download 'my_dataset'
python3 scripts/ops_test.py get --dataset my_dataset

# Vector Search
python3 scripts/ops_test.py search --dataset my_dataset --k 5

# Hybrid Search
python3 scripts/ops_test.py search --dataset my_dataset --text-query "apple" --alpha 0.5

# Delete records
python3 scripts/ops_test.py delete --dataset my_dataset --ids 1,2

# Namespace management
python3 scripts/ops_test.py namespaces

# Graph operations
python3 scripts/ops_test.py add-edge ...
python3 scripts/ops_test.py traverse ...
python3 scripts/ops_test.py pagerank ...
python3 scripts/ops_test.py communities ...
python3 scripts/ops_test.py graph-stats ...

# Snapshot
python3 scripts/ops_test.py snapshot
```

### `scripts/perf_test.py`

The high-concurrency benchmarking tool. Use this for:

- Throughput/Latency testing.
- Load testing (Soak tests).
- Measuring ingestion speed.

```bash
# Run standard benchmark (10k rows, 128 dim)
python3 scripts/perf_test.py --rows 10000 --dim 128

# Run Hybrid Search benchmark
python3 scripts/perf_test.py --hybrid --search
```

## Client Libraries

### Python

```bash
pip install longbowclientsdk
```

#### Quick Start

```python
from longbow import LongbowClient

client = LongbowClient()

# Insert data (Pandas DataFrame)
import pandas as pd
df = pd.DataFrame({
    "id": ["1", "2"],
    "vector": [[0.1, 0.2], [0.3, 0.4]],
    "metadata": ['{"key": "val"}', '{"key": "val2"}']
})
client.insert("my_dataset", df)

# Search
results = client.search("my_dataset", vector=[0.1, 0.2], k=5)
print(results)  # Returns Pandas DataFrame

# High-Performance Download
table = client.download_arrow("my_dataset")
print(f"Rows: {table.num_rows}")

# Graph Traversal
paths = client.traverse("my_graph", start=101, max_hops=2)

# Geospatial Search
results = client.geo_search(
    "cities",
    center={"lat": 40.7, "lon": -74.0},
    radius_km=10.0,
    k=10
)
```

#### Data Operations

- `insert(dataset, data)`: Ingest Pandas DataFrames or lists of dicts.
- `search(dataset, vector, k, filters, ef_search_pid=True)`: Perform K-NN search with optional post-filtering and PID-tuned depth.
- `download_arrow(dataset, filter)`: Download entire dataset as `pyarrow.Table`.
- `download_stream(dataset, filter)`: Generator yielding `pyarrow.RecordBatch`.
- `graph_rag_expand(dataset, node_ids)`: Retrieve neighbor lists for multiple nodes (distributed).

#### Management & Control

- `create_namespace(name)`: Create a new tenant isolation namespace.
- `create_dataset(name, dimensions, vector_type="float32", geo_enabled=True)`: Create a dataset with all features enabled.
- `delete_namespace(name)`: Delete an entire namespace.
- `list_namespaces()`: List all active datasets.
- `snapshot()`: Trigger a manual snapshot.
- `delete(dataset, ids)`: Delete records by ID.
- `get_flight_info_metadata(dataset)`: Retrieve dataset schema and endpoint distribution (MetaServer).

#### Graph RAG

- `add_edge(dataset, subject, predicate, object, weight)`: Add directed edge.
- `traverse(dataset, start, max_hops)`: Breadth-first graph traversal.
- `get_graph_stats(dataset)`: Metrics on edges and node degrees.

#### Features

- **Geospatial Search**: Native `geo_search()` for radius and bounding box queries.
- **TurboQuant (TQ) & Quantization**: Support for SIMD-accelerated bit-packing during ingestion.
- **Disk-ANN Offloading**: Configurable SSD-based storage for massive datasets.
- **Integrated Search**: Unified `search()` method supporting pure vector, filtered, and Advanced SQL (CTEs/Subqueries) queries.
- **Graph RAG Expansion**: Native `graph_rag_expand()` for distributed neighborhood retrieval.
- **Autonomous efSearch Tuning**: Support for `ef_search_pid` in `search()` to auto-optimize recall.
- **Graph Operations**: Dedicated methods for `add_edge()`, `traverse()`, `graph_rag_expand()`, and `get_graph_stats()`.

### Go

```go
import "github.com/23skdu/longbow/longbowclientsdk"

client, _ := longbow.Dial("localhost:3000")

// Create dataset
client.CreateDataset("my_dataset", 128)

// Add vectors
ids := []int64{1, 2, 3}
vectors := [][]float32{{0.1, 0.2}, {0.3, 0.4}, {0.5, 0.6}}
client.AddRecords("my_dataset", ids, vectors)

// Search
results := client.Search("my_dataset", []float32{0.15, 0.25}, 10)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LISTEN_ADDR` | `0.0.0.0:3000` | gRPC/Flight server |
| `META_ADDR` | `0.0.0.0:3001` | Meta server |
| `METRICS_ADDR` | `0.0.0.0:9090` | Prometheus metrics |
| `DATA_PATH` | `./data` | Data directory |
| `MAX_MEMORY` | `1073741824` | Max memory (bytes) |
| `LONGBOW_PQ_INGEST` | `0` | Enable PQ compression during ingest (1=enabled) |
| `LONGBOW_GPU_ENABLED` | `false` | Enable GPU acceleration |
| `LONGBOW_LOW_MEM` | `0` | Enable low memory mode for 512MB devices |
