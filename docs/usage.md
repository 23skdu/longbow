# Usage Guide

## Installation

### Helm Chart

To install the chart with the release name `my-release`:

```bash
helm install my-release helm/longbow
```

### Configuration

<!-- markdownlint-disable MD013 -->
The following table lists the configurable parameters of the Longbow chart and their default values.
<!-- markdownlint-enable MD013 -->

| Parameter | Description | Default |
| :--- | :--- | :--- |
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Image repository | `ghcr.io/23skdu/longbow` |
| `image.tag` | Image tag | `latest` |
| `service.port` | Service port | `3000` |
| `metrics.enabled` | Enable Prometheus metrics | `true` |

## Client Example (Python)

```python
import pyarrow.flight as flight
import pyarrow as pa

client = flight.FlightClient("grpc://localhost:3000")

# Create a schema
<!-- markdownlint-disable MD013 -->
schema = pa.schema([("id", pa.int64()), ("vector", pa.list_(pa.float32(), 128))])
<!-- markdownlint-enable MD013 -->

# Upload data
<!-- markdownlint-disable MD013 -->
writer, _ = client.do_put(flight.FlightDescriptor.for_path("test_dataset"), schema)
<!-- markdownlint-enable MD013 -->
# ... write data ...
writer.close()

# Query data
reader = client.do_get(flight.Ticket("{"dataset": "test_dataset"}"))
table = reader.read_all()
print(table)
```

## Persistence and Hot Reload

<!-- markdownlint-disable MD013 -->
For details on persistence (WAL, Snapshots) and hot reloading, please refer to [WAL.md](WAL.md).
<!-- markdownlint-enable MD013 -->
