# Longbow API Reference

## Overview

Longbow provides multiple APIs for interacting with the vector store:

- **Arrow Flight API** - Primary gRPC-based protocol for high-performance operations
- **HTTP REST API** - Web UI and administrative endpoints
- **Admin Actions** - Flight-based administrative operations

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

## HTTP REST API

Longbow provides a built-in web UI and REST API on port 8080 (configurable via `WEBUI_ADDR`).

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Web UI dashboard |
| GET | `/api/health` | Health check |
| GET | `/api/metrics` | Runtime metrics |
| GET | `/api/datasets` | List all datasets |
| GET | `/api/dataset?name=XXX` | Get dataset details |
| POST | `/api/search` | Execute vector search |

#### GET /api/health

Response:

```json
{
  "success": true,
  "data": {
    "status": "healthy",
    "datasets": 5,
    "memory_usage": 1073741824
  }
}
```

#### GET /api/datasets

Response:

```json
{
  "success": true,
  "data": [
    {
      "name": "my_dataset",
      "record_count": 10000,
      "vector_size": 128,
      "status": "active",
      "memory_bytes": 5242880,
      "dimensions": 128
    }
  ]
}
```

#### GET /api/dataset?name=XXX

Response:

```json
{
  "success": true,
  "data": {
    "name": "my_dataset",
    "record_count": 10000,
    "vector_size": 128,
    "status": "active",
    "memory_bytes": 5242880,
    "dimensions": 128
  }
}
```

#### POST /api/search

**Request:**

```json
{
  "dataset": "my_dataset",
  "query": [0.1, 0.2, 0.3, ...],
  "k": 10,
  "filter": "id > 100"
}
```

**Response:**

```json
{
  "success": true,
  "data": {
    "results": [
      {"id": 123, "distance": 0.123, "score": 0.877},
      {"id": 456, "distance": 0.234, "score": 0.766}
    ],
    "took_ms": 5
  }
}
```

#### GET /api/metrics

Response:

```json
{
  "success": true,
  "data": {
    "current_memory": 524288000,
    "peak_memory": 1073741824,
    "dataset_count": 5
  }
}
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
| `WEBUI_ADDR` | `0.0.0.0:8080` | Web UI / REST API |
| `DATA_PATH` | `./data` | Data directory |
| `MAX_MEMORY` | `1073741824` | Max memory (bytes) |
