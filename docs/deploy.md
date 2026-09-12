# Longbow Deployment, Operations, and Usage Guide

Longbow is a high-performance, distributed vector engine designed for cloud-native environments. This guide covers installation, configuration, operational management, and usage.

---

## 1. Quick Start

### Client Usage (Python)

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

### CLI Quick Commands

```bash
# Import demo data
longbow-cli import -dataset demo -count 5000

# Search
longbow-cli search -dataset demo -mode hybrid -vector "0.1,..." -text "query"

# Manage namespaces
longbow-cli create-namespace -name tenant-a

# Stats
longbow-cli stats -dataset demo
```

---

## 2. Docker / Helm Installation

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
- **EMLGo CPU (`amd64`)**: `latest-amd64-emlgo-cpu` - Standard build with EMLGo SIMD math backend (`-tags emlgo`).
- **EMLGo GPU (`amd64`)**: `latest-amd64-emlgo-gpu` - CUDA + EMLGo SIMD math backend for maximum throughput.

### Building Docker Images Locally

```bash
# Standard CPU build
docker build -f Dockerfile.cpu -t longbow:cpu .

# NVIDIA GPU build
docker build -f Dockerfile.nvidia -t longbow:nvidia .

# EMLGo CPU build (high-performance SIMD math)
docker build -f Dockerfile.emlgo-cpu -t longbow:emlgo-cpu .

# EMLGo GPU build (CUDA + SIMD math)
docker build -f Dockerfile.emlgo-gpu -t longbow:emlgo-gpu .

# With io_uring support
docker build -f Dockerfile.cpu -t longbow:cpu-iouring . --build-arg ENABLE_IOURING=true
docker build -f Dockerfile.emlgo-cpu -t longbow:emlgo-cpu-iouring . --build-arg ENABLE_IOURING=true
```

### Distributed Architecture & Scaling

Longbow uses a "Dynamo-style" architecture to scale horizontally.

#### Consistent Hashing & Sharding

- **Vnodes**: Each node uses 20 virtual nodes for uniform data distribution.
- **Gossip (SWIM)**: Decentralized membership via the SWIM protocol.
- **Auto-Sharding**: Automatically migrates from a flat index to a sharded HNSW index as data grows.
  - `LONGBOW_AUTO_SHARDING_THRESHOLD`: Default `10000`.

#### High Availability

Nodes detect failures through periodic direct and indirect pings. The cluster automatically rebalances when nodes join or leave the mesh.

---

## 3. Environment Variables

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
| `LONGBOW_USE_DISK` | `false` | Force all vector reads through disk (including HNSW indexing). **Warning:** Makes HNSW graph construction 10-100x slower. Prefer `LONGBOW_AUTO_SPILL_DISK` for most use cases. |
| `LONGBOW_AUTO_SPILL_DISK` | `true` | Auto-spill vectors to disk when memory exceeds threshold. HNSW indexing still runs in-memory; only spills after indexing completes. Recommended for large datasets. |
| `LONGBOW_SPILL_THRESHOLD_RATIO` | `0.70` | Memory threshold (0.0-1.0) at which auto-spill triggers. Lower values spill earlier, using more disk but less RAM. |

### Storage & Persistence

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_STORAGE_USE_IOURING` | `false` | High-perf WAL writes (Linux 5.6+). |
| `LONGBOW_STORAGE_ASYNC_FSYNC` | `true` | Non-blocking WAL flushes for faster ingestion. |
| `LONGBOW_SNAPSHOT_INTERVAL` | `1h` | Frequency of full index disk snapshots. |

### Memory & Limits

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_MAX_MEMORY` | `1GB` | Soft memory limit enforced by GC tuner. Exceeding this triggers eviction of least-recently-used record batches to disk and applies exponential backpressure delay (5ms to 100ms) on ingestion. |
| `LONGBOW_MAX_MEMORY_HARD` | `0` (off) | Hard memory ceiling. If exceeded, server immediately stops accepting ingestion and returns `ResourceExhausted` (gRPC status code 8). Protects against OOM crashes. |
| `LONGBOW_MAX_WAL_SIZE` | `1GB` | Maximum WAL size. |
| `LONGBOW_TTL_SECONDS` | `0` (off) | Time-to-live for records in seconds. |

### Temporal Search & Advanced Modules

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_TEMPORAL_ENABLED` | `false` | Enable temporal versioning and time-travel search. |
| `LONGBOW_TEMPORAL_AGGREGATION_ENABLED` | `false` | Enable time-series bucketing and aggregation (`min`, `max`, `sum`). |
| `LONGBOW_OLLAMA_ENABLED` | `false` | Enable local LLM embedding via Ollama (`LONGBOW_OLLAMA_ENDPOINT`). |
| `LONGBOW_CDC_ENABLED` | `false` | Enable Change Data Capture for streaming data out. |
| `LONGBOW_MQ_ENABLED` | `false` | Export vectors/CDC via Kafka/Pulsar. |
| `LONGBOW_LEARNED_INDEX_ENABLED` | `false` | Enable ML-based index selection for faster routing. |
| `LONGBOW_STRICT_MODELS` | `false` | **New in 0.1.9**: If `true`, fail fast if embedding models are missing instead of using stubs. |

---

## 4. CLI Reference

The Longbow CLI is an administrative tool for managing datasets, namespaces, and performing vector searches from the terminal.

### Installation

Build from source (requires Go 1.24+):

```bash
go build -o bin/longbow-cli ./cmd/cli
```

The binary will be created in the `bin/` directory.

### Global Options

All commands support the following global options:

- `-uri string`: Longbow server URI (default: `grpc://127.0.0.1:3000`)

### Import Data

Import vectors from Parquet, NumPy, or generate demo data. Supports local filesystem and remote S3 buckets.

```
longbow-cli import -dataset <name> [options]
```

**Options:**

- `-dataset string`: Target dataset name (required)
- `-input string`: Path to `.parquet`, `.npy`, or `s3://bucket/key`
- `-dim int`: Vector dimension (default: 128, used for demo data)
- `-count int`: Number of vectors to generate (default: 1000, used for demo data if no input file)

**Examples:**

```bash
# Local file
longbow-cli import -dataset my-collection -input data.parquet

# S3 bucket
longbow-cli import -dataset my-collection -input s3://my-bucket/vectors.parquet

# Generate demo data
longbow-cli import -dataset demo-ds -dim 1536 -count 10000
```

### Search Commands

#### Vector Search

Perform high-performance vector searches using various modes.

```
longbow-cli search -dataset <name> -mode <type> [options]
```

**Options:**

- `-dataset string`: Dataset name (required)
- `-mode string`: Search mode (dense, sparse, filtered, hybrid)
- `-vector string`: Query vector as comma-separated floats
- `-text string`: Text query for sparse/hybrid search
- `-alpha float`: Hybrid weighting (0=sparse, 1=dense)
- `-k int`: Number of results to return
- `-filters string`: JSON filter expression or path to JSON file

#### Geospatial Search

Search for vectors within a physical radius.

```
longbow-cli geo-search -dataset <name> -lat <val> -lon <val> -radius <km> -k <n>
```

#### Recommendations

Get similar vectors based on existing IDs.

```
longbow-cli recommend -dataset <name> -seeds <id1,id2> -k <n> -alpha <f>
```

### Advanced Filtering

The `-filters` flag in `search` accepts a JSON object representing complex boolean logic:

```json
{
  "logic": "AND",
  "filters": [
    {"field": "category", "operator": "=", "value": "electronics"},
    {"field": "price", "operator": "<", "value": "100"}
  ]
}
```

### Namespace & Dataset Management

Manage logical groupings and lifecycle of data.

- **Create Namespace:** `longbow-cli create-namespace -name <name> [-dims <n>] [-data_type <type>]`
- **Create Dataset:** `longbow-cli create-dataset -name <name> -dims <n> -type <type> [-geo]`
- **Delete Namespace:** `longbow-cli delete-namespace -name <name>`
- **List Namespaces:** `longbow-cli list-namespaces`
- **List Datasets:** `longbow-cli list-datasets-in-namespace -namespace <name>`
- **Delete ID:** `longbow-cli delete -dataset <name> -id <id>`
- **Snapshot:** `longbow-cli snapshot` (Triggers manual disk flush)
- **Stats:** `longbow-cli stats -dataset <name>`
- **Drop Dataset:** `longbow-cli drop -dataset <name>` (Evicts dataset from memory and clears RCU/COW structures)

### Graph & GraphRAG Operations

Administrative tools for managing the HNSW graph as a knowledge graph.

- **Add Edge:** `longbow-cli add-edge -dataset <ds> -subject <id> -predicate <p> -object <id> -weight <f>`
- **Traverse:** `longbow-cli traverse -dataset <ds> -start <id> -hops <n>`
- **Graph Stats:** `longbow-cli get-graph-stats -dataset <ds>`
- **PageRank:** `longbow-cli pagerank -dataset <ds> -iterations <n>`
- **Community Detection:** `longbow-cli detect-communities -dataset <ds>`

### ONNX Model Management

Manage and download ONNX models from external repositories like Hugging Face.

```
longbow-cli download-model -repo <repo_id> [-dest <path>]
```

**Example:**

```bash
longbow-cli download-model -repo sentence-transformers/all-MiniLM-L6-v2 -dest models/all-mini
```

### Temporal Search

Query the temporal index for versioned data.

```
longbow-cli temporal-search -dataset <name> -type <as_of|range|window> [options]
```

**Options:**

- `-dataset string`: Target dataset name (required)
- `-type string`: Search type (as_of, range, window)
- `-ts int`: Unix nanosecond timestamp for `as_of`
- `-start int`: Start time for `range`
- `-end int`: End time for `range`
- `-k int`: Number of results (default: 10)

---

## 5. Limits & Constraints

### gRPC Message Size Limits

| Limit | Default | Env Variable | Description |
|-------|---------|-------------|-------------|
| Max receive | 512MB | `GRPC_MAX_RECV_MSG_SIZE` | Max size of any single gRPC request (ingest, search, etc.) |
| Max send | 512MB | `GRPC_MAX_SEND_MSG_SIZE` | Max size of any single gRPC response (DoGet results) |

Both limits are configurable per-deployment. All ingest requests (vectors + metadata + all columns) must fit within the receive limit. All search results must fit within the send limit.

### Metadata / Text Storage

There is no hardcoded per-field size limit on metadata columns. Metadata is stored as part of the Arrow RecordBatch payload, which is bounded by the gRPC receive limit.

Practical text storage estimates at 512MB request limit:

| Text Size | Characters | Approximate Pages |
|-----------|------------|-----------------|
| 512MB | ~536,870,912 | ~107,000 |
| 100MB | ~104,857,600 | ~21,000 |
| 10MB | ~10,485,760 | ~2,100 |
| 1MB | ~1,048,576 | ~210 |
| 100KB | ~102,400 | ~20 |
| 10KB | ~10,240 | ~2 |

**Recommendation**: For agent memory use cases, typical text chunks are 512-4,096 tokens (~0.5-4KB). This allows storing millions of memory records comfortably within the 512MB window. Avoid embedding multi-megabyte text strings in a single metadata cell -- chunk text externally and store a reference ID instead.

### Record & Batch Sizes

| Parameter | Default | Notes |
|-----------|---------|-------|
| Search batch size | 32 | Concurrent searches per pool |
| Index batch size | 1,000 | MaxBatchSize for HNSW neighbor updates |
| Record batches | Unlimited | Append-only; managed by eviction/compaction |

Single RecordBatch sizes are unbounded but typically 1KB-10MB in practice.

### Dataset Limits

| Constraint | Limit | Notes |
|------------|-------|-------|
| Max dimensions | 3,072 | HNSW + SIMD paths validated up to this |
| Vector types | 14 | float32/64/16, int8/16/32/64, uint8/16/32/64, complex64/128, turboquant |
| Max datasets | Unlimited | Bounded by available memory |
| Max vectors per dataset | Unlimited | Bounded by available memory + disk |
| Max datasets per node | Unlimited | Bounded by available memory |

### GraphRAG & Temporal

| Parameter | Limit | Notes |
|-----------|-------|-------|
| GraphRAG alpha | 0.0-1.0 | 0.0 = pure graph, 1.0 = pure ANN |
| GraphRAG max hops | Configurable | BFS traversal depth |
| Temporal windows | Unlimited | Bounded by dataset time range |
| Temporal precision | nanosecond | int64 nanosecond timestamps |

### Concurrency & Connections

| Parameter | Default | Notes |
|-----------|---------|-------|
| Ingestion workers | `runtime.NumCPU()` | Parallel Arrow batch processing |
| Indexing workers | `runtime.NumCPU()` | HNSW index construction |
| Flight connections | Pooled | SmartClient manages connection reuse |
| Max concurrent searches | Bounded by search pool | `searchBatchSize=32` per pool |

### Network & Storage

| Parameter | Default | Notes |
|-----------|---------|-------|
| Max WAL segments | Unlimited | Rotating WAL with configurable size |
| Snapshot format | Parquet | Zstd compressed by default |
| io_uring | Linux only | Falls back to standard I/O on macOS |
| RDMA | Linux only | Configurable via `LONGBOW_RDMA_ENABLED` |

**Platform**: All limits apply to both macOS (CPU/Metal) and Linux (CPU/CUDA).

---

## 6. Security

### Current State

- No authentication mechanism implemented
- No authorization layer
- APIs are open by default

### Security Implementation Plan

1. **Input Validation**
   - Validate all gRPC messages
   - Sanitize input parameters
   - Implement request size limits

2. **Authentication Methods**
   - API Key authentication
   - TLS/mTLS support
   - JWT token validation (optional)

3. **Authorization Framework**
   - RBAC (Role-Based Access Control)
   - Namespace-level permissions
   - Operation-level permissions

### Audit Logging

```go
// Security audit log entry
type AuditEntry struct {
    Timestamp   time.Time
    UserID      string
    Operation   string
    Resource    string
    IPAddress   string
    Success     bool
    Reason      string
}
```

### Input Sanitization

```go
// Validate and sanitize input parameters
func ValidateInput(input string) error {
    // Length checks
    // Character validation
    // SQL injection prevention
    // Path traversal protection
}
```

### Security Scanning

#### CI/CD Integration

- Dependency vulnerability scanning
- Container image scanning
- Static code analysis
- Security testing in CI pipeline

#### Monitoring

- Failed authentication attempts
- Suspicious activity detection
- Rate limiting per client
- Anomaly detection

### Best Practices

1. **Secure by Default**
   - All APIs require authentication
   - Minimal permissions by default
   - Secure configurations

2. **Defense in Depth**
   - Multiple security layers
   - Fail-safe defaults
   - Comprehensive logging

3. **Least Privilege**
   - Minimal required permissions
   - Namespace isolation
   - Resource-specific access

---

## 7. Troubleshooting

### Slow Write Performance

**Symptom**: `DoPut` operations are taking longer than expected.

**Check Metrics**:

* `longbow_wal_writes_total`: Is the rate consistent?
* `longbow_wal_bytes_written_total`: Are you writing unusually large batches?

**Potential Causes**:

* **Slow Disk**: The WAL requires high IOPS. Ensure `LONGBOW_DATA_PATH` is on an SSD.
* **Large Batches**: Extremely large Arrow batches can cause GC pauses. Try reducing batch size.

### High Memory Usage

**Symptom**: Pod is getting OOMKilled or memory usage is climbing indefinitely.

**Check Metrics**:

* `longbow_vector_index_size`: Is the index growing as expected?
* `longbow_memory_fragmentation_ratio`: Is Go runtime retaining memory?

**Potential Causes**:

* **Snapshot Lag**: If snapshots are failing, the WAL grows, and memory isn't freed. Check `longbow_snapshot_operations_total{status="error"}`.
* **Configuration**: Ensure `LONGBOW_MAX_MEMORY` is set to a value lower than your container's hard limit.

### Memory Spikes during Index Migration

**Symptom**: Memory usage suddenly doubles, leading to OOM kills, even when data ingestion rate is stable.

**Check Metrics**:

- `longbow_learned_index_adaptations_total{status="running"}`: Is a background index swap in progress?
- `longbow_store_memory_usage_bytes`: Identify the spike onset.

**Cause**: Longbow's **Adaptive Learned Index** and **Auto-Sharding** mechanisms build replacement indices in the background to ensure zero-downtime search. This process temporarily doubles the memory footprint of the index being replaced.

**Solution**:

1. **Increase Buffer**: Ensure `LONGBOW_MAX_MEMORY` is set with at least a 50% buffer above your steady-state index size.
2. **Limit Concurrent Migrations**: Avoid triggering multiple collection migrations simultaneously.
3. **Disable Auto-Adaptation**: If memory is critical, disable automatic switching via config:
   ```yaml
   learned_index:
     adaptation:
       enable_adaptation: false
   ```

### Slow Startup

**Symptom**: Longbow takes a long time to become ready after a restart.

**Check Metrics**:

* `longbow_wal_replay_duration_seconds`: High values indicate a large WAL.

**Solution**:

* Decrease `LONGBOW_SNAPSHOT_INTERVAL`. A shorter interval means a smaller WAL to replay on startup, as older data is already in Parquet.

### Permission Denied on /data

**Symptom**: Pod crashes with `open /data/wal.log: permission denied`.

**Cause**: The application runs as a non-root user (UID 1000) while `/data` is owned by root or the filesystem is read-only.

**Solution**:

* Ensure `persistence.wal.enabled` is `true` in Helm values to mount a PersistentVolume.
* Verify `podSecurityContext.fsGroup` is set to `2000` (or similar) to ensure the volume is writable by the app user.

### Config Parsing Errors

**Symptom**: `panic: Failed to process config: converting '6.7108864e+07' to type int`.

**Cause**: Helm passes large numeric values as floating-point scientific notation if not explicitly quoted.

**Solution**:

* Quote all large integer values in `values.yaml` (e.g., `maxRecvMsgSize: "67108864"`).

### Replication Lag

**Symptom**: `longbow_replication_lag_seconds` is high (> 30s) on follower nodes.

**Cause**: Network variance, slow disk I/O on follower, or high write throughput overwhelming replication stream.

**Solution**:

* Check follower disk IOPS and CPU.
* Ensure network connectivity between Leader and Follower is stable (`longbow_gossip_pings_total{direction="failed"}`).
* If persisting, consider scaling out with more shards to distribute write load.

### GPU Initialization Failure

**Symptom**: Log shows `WARN GPU initialization failed, using CPU-only`.

**Cause**:

* **CUDA/Metal**: Missing drivers or unsupported hardware.
* **Memory**: Insufficient GPU memory (OOM).
* **Permissions**: Access to GPU device denied.

**Solution**:

* Verify NVIDIA drivers/CUDA toolkit (Linux) or macOS version (Apple Silicon).
* Check `nvidia-smi` or `powermetrics` (macOS).
* Ensure `GPU_ENABLED=true` is set.

### S3 Backup Failures

**Symptom**: `longbow_s3_operations_total{status="error"}` is increasing.

**Cause**: AWS Credentials expiry, bucket policy denial, or network timeouts.

**Solution**:

* Verify `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.
* Check IAM permissions for `s3:PutObject` and `s3:GetObject`.
* Inspect logs for specific S3 error codes (e.g., `403 Forbidden`, `503 Slow Down`).

---

## 8. Monitoring & Operational Maintenance

### Metrics

Metrics are available at `http://<METRICS_ADDR>/metrics`. Key namespaces include:

- **longbow_onnx_metal_memory_used_bytes**: (Gauge) VRAM utilization on Apple Silicon.
- **longbow_gpu_memory_bytes**: (Gauge) VRAM utilization on NVIDIA/CUDA systems.
- **longbow_stub_model_usage_total**: (Counter) **New in 0.1.9**: Count of times a stub embedding model was used due to missing configuration. Labels: `model_path`.
- `longbow_search_`: Latency and throughput.
- `longbow_gossip_`: Cluster membership status.
- `longbow_storage_`: WAL and disk usage.

### Memory Management

Set `GOMEMLIMIT` to 90% of the container's hard limit. Longbow's internal **GCTuner** will manage allocations to stay within `LONGBOW_MAX_MEMORY` while maximizing performance.
