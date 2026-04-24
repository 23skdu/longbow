# Deployment, Operations, and Usage Guide

Longbow is a high-performance, distributed vector database designed for cloud-native environments. This guide covers installation, configuration, operational management, and basic usage.

---

## 1. Installation

### Helm Chart (Recommended)
The recommended way to deploy Longbow is using the official Helm chart.

```bash
# Add repository (if applicable) or install from local chart
helm install my-release ./helm/longbow
```

### Docker & Multi-Platform Support
Official images are available on GitHub Container Registry (`ghcr.io/23skdu/longbow`):

- **Apple Silicon (`arm64`)**: `latest-arm64-metal` - Optimized for Metal GPU and Mach CPU clusters.
- **NVIDIA GPU (`amd64`)**: `latest-amd64-nvidia` - Includes custom CUDA 12.6 kernels and zero-copy tensor bridge.
- **General CPU (`amd64`)**: `latest-amd64-cpu` - Broadwell-level AVX2 optimizations with `io_uring` support.

---

## 2. Configuration

Longbow follows the **Twelve-Factor App** methodology and is configured entirely via environment variables.

### Core Settings
| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_LISTEN_ADDR` | `0.0.0.0:3000` | gRPC Data Plane (Arrow Flight). |
| `LONGBOW_META_ADDR` | `0.0.0.0:3001` | gRPC Control Plane. |
| `LONGBOW_METRICS_ADDR` | `0.0.0.0:9090` | Prometheus metrics and health checks. |
| `LONGBOW_DATA_PATH` | `./data` | Base directory for WAL, snapshots, and indexes. |
| `LONGBOW_MAX_MEMORY` | `1GB` | Bound the total memory usage for vector storage. |

### Indexing & HNSW Tuning
| Variable | Default | Tuning Recommendation |
| :--- | :--- | :--- |
| `LONGBOW_HNSW_M` | `16` | Connections per node. Use `32-48` for high-dim (768+). |
| `LONGBOW_HNSW_EF_CONSTRUCTION` | `200` | Increase to `400-800` for 99.9% recall. |
| `LONGBOW_HNSW_SQ8_ENABLED` | `false` | 4x memory reduction via 8-bit quantization. |
| `LONGBOW_HNSW_TURBOQUANT_ENABLED`| `true` | **Default 0.1.9**: SIMD-accelerated bit-packing. |
| `LONGBOW_USE_DISK` | `false` | Enable SSD offloading (Disk-ANN style). |

### Storage & Persistence
| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_STORAGE_USE_IOURING` | `false` | High-perf WAL writes (Linux 5.6+). |
| `LONGBOW_STORAGE_ASYNC_FSYNC` | `true` | Non-blocking WAL flushes for faster ingestion. |
| `LONGBOW_SNAPSHOT_INTERVAL` | `1h` | Frequency of full index disk snapshots. |

---

## 3. Distributed Architecture & Scaling

Longbow uses a "Dynamo-style" architecture to scale horizontally.

### Consistent Hashing & Sharding
- **Vnodes**: Each node uses 20 virtual nodes for uniform data distribution.
- **Gossip (SWIM)**: Decentralized membership via the SWIM protocol.
- **Auto-Sharding**: Automatically migrates from a flat index to a sharded HNSW index as data grows.
  - `LONGBOW_AUTO_SHARDING_THRESHOLD`: Default `10000`.

### High Availability
Nodes detect failures through periodic direct and indirect pings. The cluster automatically rebalances when nodes join or leave the mesh.

---

## 4. CLI Tools

Longbow includes a CLI for administrative tasks and data management.

### Installation
```bash
go build -o bin/longbow-cli ./cmd/cli
```

### Common Commands
- **Import Data**: `longbow-cli import -dataset demo -count 5000`
- **Search**: `longbow-cli search -dataset demo -mode hybrid -vector "0.1,..." -text "query"`
- **Manage Namespaces**: `longbow-cli create-namespace -name tenant-a`
- **Stats**: `longbow-cli stats -dataset demo`

---

## 5. Client Usage (Python Example)

Longbow uses the Arrow Flight protocol for zero-copy data transfer.

```python
import pyarrow.flight as flight
import pyarrow as pa
import json

client = flight.FlightClient("grpc://localhost:3000")

# 1. Ingest Data
schema = pa.schema([("id", pa.int64()), ("vector", pa.list_(pa.float32(), 128))])
writer, _ = client.do_put(flight.FlightDescriptor.for_path("test"), schema)
# ... write data ...
writer.close()

# 2. Vector Search (via Ticket)
query = {"dataset": "test", "k": 10}
reader = client.do_get(flight.Ticket(json.dumps(query)))
results = reader.read_all()
```

---

## 6. Operational Maintenance

### Monitoring
Metrics are available at `http://<METRICS_ADDR>/metrics`. Key namespaces include:
- `longbow_search_`: Latency and throughput.
- `longbow_gossip_`: Cluster membership status.
- `longbow_storage_`: WAL and disk usage.

### Memory Management
Set `GOMEMLIMIT` to 90% of the container's hard limit. Longbow's internal **GCTuner** will manage allocations to stay within `LONGBOW_MAX_MEMORY` while maximizing performance.
