
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

## Server Configuration (Environment Variables)

When running the Longbow container directly (e.g., via Docker), you can configure the server using the following environment variables.

### Core

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_LISTEN_ADDR` | Address for Data gRPC server | `0.0.0.0:3000` |
| `LONGBOW_META_ADDR` | Address for Meta gRPC server | `0.0.0.0:3001` |
| `LONGBOW_METRICS_ADDR` | Address for Prometheus metrics | `0.0.0.0:9090` |
| `LONGBOW_MAX_MEMORY` | Max memory limit (bytes) | `1073741824` (1GB) |
| `LONGBOW_LOG_FORMAT` | Log format (`json` or `console`) | `json` |
| `LONGBOW_LOG_LEVEL` | Log level (`debug`, `info`, `warn`, `error`) | `info` |

### Persistence

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_DATA_PATH` | Path for WAL storage | `./data` |
| `LONGBOW_SNAPSHOT_INTERVAL` | Interval for snapshots | `1h` |
| `LONGBOW_MAX_WAL_SIZE` | Max WAL size before rotation | `104857600` (100MB) |
| `LONGBOW_STORAGE_ASYNC_FSYNC` | Enable async fsync for WAL | `true` |

### Compaction & Maintenance

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_COMPACTION_ENABLED` | Enable background compaction | `true` |
| `LONGBOW_COMPACTION_INTERVAL` | Interval between compaction runs | `30s` |
| `LONGBOW_COMPACTION_TARGET_BATCH_SIZE` | Target rows per record batch | `10000` |
| `LONGBOW_COMPACTION_MIN_BATCHES` | Min batches to trigger compaction | `10` |
| `LONGBOW_TTL` | Time-to-live for data (0s = disabled) | `0s` |

### Sharding & Scaling

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_RING_SHARDING_ENABLED` | Enable Consistent Hashing Ring | `true` |
| `LONGBOW_AUTO_SHARDING_ENABLED` | Enable auto-migration to sharded index | `true` |
| `LONGBOW_AUTO_SHARDING_THRESHOLD` | Vector count to trigger sharding | `10000` |
| `LONGBOW_AUTO_SHARDING_SPLIT_THRESHOLD` | Size of split shards | `65536` |

### gRPC Tuning

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_GRPC_MAX_RECV_MSG_SIZE` | Max receive message size | `536870912` (512MB) |
| `LONGBOW_GRPC_MAX_SEND_MSG_SIZE` | Max send message size | `536870912` (512MB) |
| `LONGBOW_GRPC_MAX_CONCURRENT_STREAMS` | Max concurrent streams | `250` |

### HNSW Configuration

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_HNSW_M` | Max connections per node | `32` |
| `LONGBOW_HNSW_EF_CONSTRUCTION` | Size of dynamic candidate list | `400` |
| `LONGBOW_HNSW_ALPHA` | Graph density factor | `1.0` |
| `LONGBOW_HNSW_KEEP_PRUNED` | Keep pruned connections | `false` |
| `LONGBOW_HNSW_SQ8_ENABLED` | Enable 8-bit quantization | `false` |
| `LONGBOW_HNSW_FLOAT16_ENABLED` | Enable Float16 storage | `false` |
| `LONGBOW_USE_DISK` | Enable Disk-based vector storage | `false` |
| `LONGBOW_HNSW_REFINEMENT_FACTOR` | Re-ranking refinement multiple | `1.0` |

### Hybrid Search

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_HYBRID_SEARCH_ENABLED` | Enable hybrid search | `false` |
| `LONGBOW_HYBRID_TEXT_COLUMNS` | Comma-separated columns to index | `` |
| `LONGBOW_HYBRID_ALPHA` | Weight for dense vectors (0.0-1.0) | `0.5` |

### Learned Index

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_LEARNED_INDEX_ENABLED` | Enable learned index predictor | `false` |
| `LONGBOW_LEARNED_INDEX_MIN_SAMPLES` | Min training samples required | `100` |
| `LONGBOW_LEARNED_INDEX_CONFIDENCE_THRESH` | Confidence threshold (0.0-1.0) | `0.7` |
| `LONGBOW_LEARNED_INDEX_UPDATE_INTERVAL` | Model update interval | `1h` |

### Ollama Integration

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_OLLAMA_ENABLED` | Enable Ollama for embeddings | `false` |
| `LONGBOW_OLLAMA_ENDPOINT` | Ollama server endpoint | `http://localhost:11434` |
| `LONGBOW_OLLAMA_MODEL` | Ollama model name for embeddings | `` |
| `LONGBOW_OLLAMA_TIMEOUT` | Request timeout in seconds | `30` |

### ML Inference (ONNX & WASM)

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_ML_RUNNER` | ML inference engine (`wazero`, `onnx`, or `quarrel`) | `wazero` |
| `LONGBOW_ML_MODEL_PATH` | Path to the `.onnx` or `.wasm` model file | `` |
| `LONGBOW_ML_TOKENIZER_PATH` | Path to `vocab.txt` for tokenization | `` |
| `ONNX_RUNTIME_LIB_PATH` | Path to `libonnxruntime.dylib` or `.so` | - |
| `LONGBOW_RERANKER_ENABLED` | Enable cross-encoder reranking | `false` |
| `LONGBOW_RERANKER_TOP_K` | Number of documents to rerank | `10` |

### SIMD & Acceleration

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LONGBOW_SIMD_IMPL` | Force SIMD implementation (`avx512`, `avx2`, `neon`, `generic`) | `auto` |
| `LONGBOW_JIT` | Enable Just-In-Time compiled SIMD kernels (experimental) | `false` |
| `LONGBOW_SIMD_FALLBACK` | Allow fallback to generic paths if hardware support is missing | `true` |

## CLI Tools

Longbow provides a unified administrative tool at `scripts/longbow.py`.

### Basic Usage

```bash
# Seed data
python scripts/longbow.py data seed --dataset test --count 1000

# Run benchmarks
python scripts/longbow.py bench run --mode cpu

# Enhance Grafana dashboard
python scripts/longbow.py dashboard enhance
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
