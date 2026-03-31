# Longbow API Reference

## Overview

Longbow provides **gRPC + Apache Arrow Flight only**. No REST/HTTP API for data operations.

- **Arrow Flight API** - Primary gRPC-based protocol for high-performance operations
- **Admin Actions** - Flight-based administrative operations
- **Prometheus Metrics** - HTTP metrics endpoint on port 9090

## Protocol Ports

| Port | Service | Protocol |
|------|---------|----------|
| 3000 | Data Server | gRPC/Arrow Flight |
| 3001 | Meta Server | gRPC/Arrow Flight |
| 9090 | Metrics | HTTP/Prometheus |

## Arrow Flight API

Longbow implements the Apache Arrow Flight protocol for efficient zero-copy data transfer.

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `DoPut` | RPC | Ingest vector data |
| `DoGet` | RPC | Execute vector search |
| `DoAction` | RPC | Administrative operations |
| `DoExchange` | RPC | Streaming bidirectional communication |
| `ListFlights` | RPC | List available datasets |

### DoPut - Data Ingestion

**Request:**

```protobuf
message ArrowRecordBatch {
    bytes data = 1;  // IPC-serialized Arrow RecordBatch
}
```

**Example (Python):**

```python
import pyarrow.flight as pf

client = pf.connect("longbow-server:3000")

# Create Arrow table
table = pa.table({
    "id": [1, 2, 3],
    "vector": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]]
})

# Ingest data
writer, reader = client.do_put(
    pf.descriptor("dataset_name"),
    table.schema
)
writer.write_table(table)
writer.close()
reader.done()
```

### DoGet - Vector Search

**Request:**

```protobuf
message SearchTicket {
    string dataset = 1;
    bytes query_vector = 2;  // Float32 array
    int32 k = 3;             // Number of results
    bytes filter = 4;        // Optional predicate
}
```

**Example (Python):**

```python
import pyarrow.flight as pf
import numpy as np

client = pf.connect("longbow-server:3000")

# Search query
query = np.array([0.15, 0.25], dtype=np.float32)
ticket = pf.Ticket(json.dumps({
    "dataset": "my_dataset",
    "k": 10
}))

# Execute search
reader = client.do_get(ticket, query)
results = reader.read_all()
```

### DoAction - Administrative Operations

| Action | Parameters | Description |
|--------|------------|-------------|
| `create_dataset` | schema | Create new dataset |
| `drop_dataset` | name | Delete dataset |
| `compact` | name | Force compaction |
| `snapshot` | name | Create snapshot |
| `metrics` | - | Get runtime metrics |

**Example:**

```python
# Get dataset details
action = pf.Action("get_dataset", b"dataset_name")
for result in client.do_action(action):
    print(result)
```

## gRPC + Arrow Flight Only

**Longbow does NOT provide a REST HTTP API.** All data operations are via gRPC + Apache Arrow Flight.

- Port 3000: Data Server (DoGet, DoPut, DoExchange)
- Port 3001: Meta Server (ListFlights, DoAction)
- Port 9090: Prometheus metrics (HTTP)

### Administrative Actions

```python
# Create dataset
action = pf.Action("create_dataset", schema.serialize())
client.do_action(action)

# Drop dataset
action = pf.Action("drop_dataset", b"dataset_name")
list(client.do_action(action))

# Get metrics
action = pf.Action("metrics", b"")
for result in client.do_action(action):
    print(result)
```

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

### Example Prometheus Query

```promql
# Search latency percentiles
histogram_quantile(0.99, rate(longbow_search_duration_seconds_bucket[5m]))

# Operations per second
rate(longbow_flight_ops_total[1m])

# Memory utilization
longbow_memory_bytes / longbow_max_memory_bytes
```

## gRPC API Reference

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

## Client Libraries

### Python

```bash
pip install longbowclientsdk
```

```python
from longbow import VectorStore

client = VectorStore("localhost:3000")

# Create dataset
client.create_dataset("my_dataset", dimension=128)

# Add vectors
client.add_records("my_dataset", ids=[1, 2, 3], vectors=[[0.1, 0.2], ...])

# Search
results = client.search("my_dataset", query=[0.15, 0.25], k=10)
```

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

---

## Admin API

Longbow provides management capabilities via the **Meta Server** (`DoAction` and `ListFlights`).

### Namespace Management

#### `CreateNamespace`

Creates a new namespace for vector data.

- **Type**: `CreateNamespace`
- **Body**: `{"name": "my_namespace"}`
- **Response**: `{"status": "created"}`

#### `DeleteNamespace`

Deletes an entire namespace and its associated datasets.

- **Type**: `DeleteNamespace`
- **Body**: `{"name": "my_namespace"}`
- **Response**: `{"status": "deleted"}`

#### `ListNamespaces`

Returns a list of all active namespaces.

- **Type**: `ListNamespaces`
- **Body**: (empty)
- **Response**: `{"namespaces": ["ns1", "ns2"], "count": 2}`

### Dataset Management

#### `delete-dataset`

Removes a dataset from memory.

- **Type**: `delete-dataset`
- **Body**: `{"dataset": "my_dataset"}`
- **Response**: `"deleted"` (string)

#### `delete-vector`

Deletes a specific vector by its internal `VectorID`.

- **Type**: `delete-vector`
- **Body**: `{"dataset": "my_dataset", "vector_id": 123}`
- **Response**: `"deleted"` (string)

### Mesh & Cluster Status

#### `MeshStatus`

Retrieves the status of the gossip mesh and connected members.

- **Type**: `MeshStatus`
- **Response**: List of member objects including ID, Addr, and Status.

#### `cluster-status`

Retrieves cluster-level health and member identity.

- **Type**: `cluster-status`
- **Response**: JSON object containing `self` identity and `members` list.

### Backpressure Monitoring

The Data Server (`DoPut`) monitors the Write-Ahead Log (WAL) queue depth. If the queue exceeds **80% capacity**, the server applies backpressure:

1. Server logs a `wal_pressure` warning.
2. `DoPut` responses include metadata: `{"status": "slow_down", "reason": "wal_pressure"}`.

Clients (including the Python SDK) monitor this metadata and should implement backoff or throttling to avoid overloading the persistence layer.
