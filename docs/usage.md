
# Usage Guide

## Installation

### Helm Chart

To install the chart with the release name `my-release`:

```bash
helm install my-release helm/longbow
```

### Configuration

The following table lists the configurable parameters of the Longbow chart and their default values.

| Parameter | Description | Default |
| :--- | :--- | :--- |
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Image repository | `ghcr.io/23skdu/longbow` |
| `image.tag` | Image tag | `latest` |
| `service.port` | Service port | `3000` |
| `metrics.enabled` | Enable Prometheus metrics | `true` |
| `persistence.wal.enabled` | Enable WAL persistence | `false` |
| `persistence.wal.size` | Size of WAL PVC | `5Gi` |
| `persistence.wal.path` | Mount path for WAL | `/data` |
| `persistence.snapshots.enabled` | Enable Snapshot persistence | `false` |
| `persistence.snapshots.size` | Size of Snapshot PVC | `10Gi` |
| `persistence.snapshots.path` | Mount path for Snapshots | `/snapshots` |

## CLI Tools

Longbow provides a comprehensive operations script `scripts/ops_test.py` for interacting with the server.

### Basic Usage

```bash
# Upload data
python scripts/ops_test.py put --dataset test_data --rows 1000

# Download data with filter
python scripts/ops_test.py get --dataset test_data --filter "id:>:500"

# Vector Search with filter
python scripts/ops_test.py search --dataset test_data --k 5 --filter "category:=:news"

# Run full validation suite
python scripts/ops_test.py validate
```

## Client Example (Python)

```python
import pyarrow.flight as flight
import pyarrow as pa
import json

client = flight.FlightClient("grpc://localhost:3000")

# Create a schema
schema = pa.schema([("id", pa.int64()), ("vector", pa.list_(pa.float32(), 128))])

# Upload data
writer, _ = client.do_put(flight.FlightDescriptor.for_path("test_dataset"), schema)
# ... write data ...
writer.close()

# Query data
reader = client.do_get(flight.Ticket('{"dataset": "test_dataset"}'))
table = reader.read_all()
print(table)

# List and Filter Flights
# Create a filter query
query = {
  "name": "test",
  "limit": 10,
  "filters": [
    {"field": "rows", "operator": ">", "value": "100"}
  ]
}

# Serialize query to JSON bytes
criteria = json.dumps(query).encode('utf-8')

# List flights with criteria
flights = client.list_flights(criteria=criteria)

print("Filtered Flights:")
for flight_info in flights:
    print(flight_info.descriptor.path)
```

## Persistence and Hot Reload

For details on persistence (WAL, Snapshots) and hot reloading, please refer to [persistence.md](persistence.md).
