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

Official images are available on GitHub Container Registry (`ghcr.io/23skdu/longbow`). `.github/workflows/release.yml` builds exactly two of them:

- **Standard image** -- Multi-arch (`linux/amd64`, `linux/arm64`), published as `latest`. The `linux/arm64` slice covers ARM64 hosts such as AWS Graviton.
- **NVIDIA image** -- `linux/amd64` only, built from `Dockerfile.nvidia` with the custom CUDA kernels and zero-copy tensor bridge, published with the `-nvidia` suffix as `latest-nvidia`.

Each is also published under a `sha-<short-sha>` tag and, on a git tag push, under the tag name. The Helm chart defaults to `image.tag: "latest"`.

Two caveats when reproducing this locally:

- The standard-image build step sets no `file:`, so it resolves to a `Dockerfile` at the repository root. The repo does not track one -- the only Dockerfiles present are `Dockerfile.cpu`, `Dockerfile.metal`, `Dockerfile.nvidia`, `Dockerfile.tpu`, and `Dockerfile.emlgo-cpu` / `Dockerfile.emlgo-gpu`. Pass `-f` explicitly when building.
- There is no separate Metal, CPU-only, or EMLGo image tag. `Dockerfile.metal` and the `Dockerfile.emlgo-*` variants are not referenced by any workflow, so build them yourself if you need those configurations (see below).

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
| `LONGBOW_METRICS_ADDR` | `:6000` | Prometheus metrics and health checks. The Helm chart and `docker-compose.yml` both set this to `0.0.0.0:9090`; the bare binary falls back to `:6000` when the variable is unset. |
| `LONGBOW_DATA_PATH` | `./data` | Base directory for WAL, snapshots, and indexes. |
| `LONGBOW_MAX_MEMORY` | `1GB` | Bound the total memory usage for vector storage. |

### Indexing & HNSW Tuning

| Variable | Default | Tuning Recommendation |
| :--- | :--- | :--- |
| `LONGBOW_HNSW_M` | `32` | Connections per node. Use `32-48` for high-dim (768+). |
| `LONGBOW_HNSW_MMAX` | `64` | Ceiling on connections per node for the upper layers (level 1+). |
| `LONGBOW_HNSW_MMAX0` | `64` | Ceiling on connections per node at level 0, which dominates memory. Lower it to trade recall for footprint. |
| `LONGBOW_HNSW_EF_CONSTRUCTION` | `400` | Increase to `400-800` for 99.9% recall. |
| `LONGBOW_LOW_MEM` | unset (off) | Set to `1` or `true` to start from a reduced baseline: `M=16`, `MMAX`/`MMAX0=32`, initial capacity 5,000. Individual HNSW overrides above still win. |
| `LONGBOW_AUTO_QUANTIZE` | `false` | Standardize a dataset on 4-bit TurboQuant once it passes `LONGBOW_AUTO_QUANTIZE_THRESHOLD` (`100000`). |
| `LONGBOW_USE_DISK` | `false` | Force all vector reads through disk (including HNSW indexing). **Warning:** Makes HNSW graph construction 10-100x slower. Prefer `LONGBOW_AUTO_SPILL_DISK` for most use cases. |
| `LONGBOW_AUTO_SPILL_DISK` | `true` | Auto-spill vectors to disk when memory exceeds threshold. HNSW indexing still runs in-memory; only spills after indexing completes. Recommended for large datasets. |
| `LONGBOW_SPILL_THRESHOLD_RATIO` | `0.70` | Memory threshold (0.0-1.0) at which auto-spill triggers. Lower values spill earlier, using more disk but less RAM. |
| `LONGBOW_HNSW_BULK_CHAIN_LINKS` | `1` (on) | Bulk-insert every node to its insertion-order predecessor at layer 0. Set to `0` to disable. See the tradeoff note below. |

#### Bulk-insert chain links: `LONGBOW_HNSW_BULK_CHAIN_LINKS`

Bulk insertion needs each new node to have at least one inbound link, otherwise part
of the corpus is unreachable. Reverse links a fresh node hands to its pre-batch
neighbours do not always survive: they are pruned as soon as such a neighbour is
already at its connection limit. The chain link adds an edge to the
insertion-order predecessor, which always survives.

That fix has a cost on unsorted input, because the edge connects node *i* to node
*i-1*, which are generally not near each other. Measured against `2f4dc1c4` on a
250,000-vector float32 index:

| Input | With chain links | Without | Change |
|---|---|---|---|
| dense | baseline | — | **-60.9%** recall |
| filteredstring | baseline | — | **-81.0%** recall |

**Keep it on (the default) unless your vectors are already sorted or clustered by a
distance-relevant key.** It is the only thing preventing stranding, and
`TestBulkInsert_CollinearGraphStaysConnected` covers that case. Disabling it does not
speed up construction: at `a955a0c1` a 250k TurboQuant build took 236.1s without the
chain links against 142.0s with them.

`SQ8Enabled` and `TurboQuantEnabled` are not controlled by per-feature environment switches. They are `ArrowHNSWConfig` fields (`internal/store/types/index_types.go`) set programmatically: both default to `false`, SQ8 follows the configured `ArrowHNSWConfig` carried on the dataset, and TurboQuant is switched on when a dataset is created with the `turboquant` vector type (`internal/store/store_actions.go`, `internal/store/index/arrow_hnsw.go`). Use `LONGBOW_AUTO_QUANTIZE` (or `LONGBOW_LOW_MEM` for the memory budget) to influence the outcome from the environment.

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
| `LONGBOW_MAX_WAL_SIZE` | `100MB` | Maximum WAL size before segments rotate. |
| `LONGBOW_TTL` | `0s` (off) | Time-to-live for records, as a Go duration (e.g. `24h`, `30m`). There is no integer-seconds variant. |

### Temporal Search & Advanced Modules

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_TEMPORAL_ENABLED` | `false` | Enable temporal versioning and time-travel search. |
| `LONGBOW_TEMPORAL_AGGREGATION_ENABLED` | `false` | Enable time-series bucketing and aggregation (`min`, `max`, `sum`). |
| `LONGBOW_OLLAMA_ENABLED` | `false` | Enable local LLM embedding via Ollama (`LONGBOW_OLLAMA_ENDPOINT`). |
| `LONGBOW_CDC_ENABLED` | `false` | Enable Change Data Capture for streaming data out. |
| `LONGBOW_MQ_ENABLED` | `false` | Export vectors/CDC via Kafka/Pulsar. |
| `LONGBOW_LEARNED_INDEX_ENABLED` | `false` | Enable ML-based index selection for faster routing. |

The server has no "strict models" mode. The Helm chart sets `LONGBOW_STRICT_MODELS`, but no Go code reads it, and builds without the `onnx` build tag return `ONNX Runtime not available in this build` from `internal/onnx/onnx_stub.go` rather than failing fast on missing configuration.

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

```bash
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

```bash
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

```bash
longbow-cli geo-search -dataset <name> -lat <val> -lon <val> -radius <km> -k <n>
```

#### Recommendations

Get similar vectors based on existing IDs.

```bash
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

```bash
longbow-cli download-model -repo <repo_id> [-dest <path>]
```

**Example:**

```bash
longbow-cli download-model -repo sentence-transformers/all-MiniLM-L6-v2 -dest models/all-mini
```

### Temporal Search

Query the temporal index for versioned data.

```bash
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
| Max receive | 2GB | `LONGBOW_GRPC_MAX_RECV_MSG_SIZE` | Max size of any single gRPC request (ingest, search, etc.) |
| Max send | 2GB | `LONGBOW_GRPC_MAX_SEND_MSG_SIZE` | Max size of any single gRPC response (DoGet results) |

Both limits are configurable per-deployment, and the Helm chart raises them to 20GB by default. All ingest requests (vectors + metadata + all columns) must fit within the receive limit. All search results must fit within the send limit. Note that a 2GB gRPC message is bounded in practice by the client: a single Arrow Flight `DoPut` still has to fit in one call, so chunk large ingests client-side.

### Metadata / Text Storage

There is no hardcoded per-field size limit on metadata columns. Metadata is stored as part of the Arrow RecordBatch payload, which is bounded by the gRPC receive limit.

Practical text storage estimates at the 2GB default request limit:

| Text Size | Characters | Approximate Pages |
|-----------|------------|-----------------|
| 2GB | ~2,147,483,648 | ~430,000 |
| 512MB | ~536,870,912 | ~107,000 |
| 100MB | ~104,857,600 | ~21,000 |
| 10MB | ~10,485,760 | ~2,100 |
| 1MB | ~1,048,576 | ~210 |
| 100KB | ~102,400 | ~20 |
| 10KB | ~10,240 | ~2 |

**Recommendation**: For agent memory use cases, typical text chunks are 512-4,096 tokens (~0.5-4KB). At the 2GB receive limit that is hundreds of thousands of chunks per request. The binding constraint is no longer the gRPC window -- it is `LONGBOW_MAX_MEMORY`, the per-batch memory pressure, and how long a single write transaction holds the index. Avoid embedding multi-megabyte text strings in a single metadata cell -- chunk text externally and store a reference ID instead, and batch ingest in the low hundreds of MB rather than pushing the full 2GB in one call.

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

Automated by `.github/workflows/security.yml` (push/PR to main, weekly schedule, `workflow_dispatch`):

- **govulncheck** via `scripts/check_govuln.sh` — Go module vulnerability scanning with an allowlist for unfixed accepted risks (see `.trivyignore`)
- **Trivy filesystem scan** — dependency vulnerabilities, secrets, and misconfigurations (`HIGH`/`CRITICAL`, skips `vendor/`, `data/`, `bin/`)
- **Trivy config scan** — Dockerfile and Helm chart IaC misconfigurations
- **gosec** — run locally via `gosec ./...` (integrated into the development workflow)
- **Dependabot** — daily `gomod` updates with auto-merge for minor/patch PRs

Accepted risks (documented in `.trivyignore`): `hamba/avro` GO-2026-5046/5047/5048 and `x/crypto` openpgp GO-2026-5932 — all Fixed in: N/A upstream.

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

- `longbow_wal_writes_total`: Is the rate consistent?
- `longbow_wal_bytes_written_total`: Are you writing unusually large batches?

**Potential Causes**:

- **Slow Disk**: The WAL requires high IOPS. Ensure `LONGBOW_DATA_PATH` is on an SSD.
- **Large Batches**: Extremely large Arrow batches can cause GC pauses. Try reducing batch size.

### High Memory Usage

**Symptom**: Pod is getting OOMKilled or memory usage is climbing indefinitely.

**Check Metrics**:

- `longbow_vector_index_size`: Is the index growing as expected?
- `longbow_memory_fragmentation_ratio`: Is Go runtime retaining memory?

**Potential Causes**:

- **Snapshot Lag**: If snapshots are failing, the WAL grows, and memory isn't freed. Check `longbow_snapshot_operations_total{status="error"}`.
- **Configuration**: Ensure `LONGBOW_MAX_MEMORY` is set to a value lower than your container's hard limit.

### Memory Spikes during Index Migration

**Symptom**: Memory usage suddenly doubles, leading to OOM kills, even when data ingestion rate is stable.

**Check Metrics**:

- `longbow_learned_index_adaptations_total{status="triggered"}`: A background index swap has started. Match `status` against the real lifecycle values (`triggered`, `completed`, `failed`, `rolled_back`, `rollback_failed`); a `triggered` count that never reaches `completed` identifies a stalled migration.
- `longbow_store_vectors_managed_count`: Track vector population across datasets, and correlate the spike onset with this gauge.

**Cause**: Longbow's **Adaptive Learned Index** and **Auto-Sharding** mechanisms build replacement indices in the background to ensure zero-downtime search. This process temporarily doubles the memory footprint of the index being replaced.

**Solution**:

1. **Increase Buffer**: Ensure `LONGBOW_MAX_MEMORY` is set with at least a 50% buffer above your steady-state index size.
2. **Limit Concurrent Migrations**: Avoid triggering multiple collection migrations simultaneously.
3. **Disable Learned Index Adaptation**: If memory is critical, disable it via environment variable (`false` is the default, so leave it unset in production):

   ```bash
   LONGBOW_LEARNED_INDEX_ENABLED=false
   ```

   Related knobs: `LONGBOW_LEARNED_INDEX_MIN_SAMPLES` (`100`), `LONGBOW_LEARNED_INDEX_CONFIDENCE_THRESH` (`0.7`), `LONGBOW_LEARNED_INDEX_UPDATE_INTERVAL` (`1h`). Learned index has no config-file equivalent.

### Slow Startup

**Symptom**: Longbow takes a long time to become ready after a restart.

**Check Metrics**:

- `longbow_wal_replay_duration_seconds`: High values indicate a large WAL.

**Solution**:

- Decrease `LONGBOW_SNAPSHOT_INTERVAL`. A shorter interval means a smaller WAL to replay on startup, as older data is already in Parquet.

### Permission Denied on /data

**Symptom**: Pod crashes with `open /data/wal.log: permission denied`.

**Cause**: The application runs as a non-root user (UID 1000) while `/data` is owned by root or the filesystem is read-only.

**Solution**:

- Ensure `persistence.wal.enabled` is `true` in Helm values to mount a PersistentVolume.
- Verify `podSecurityContext.fsGroup` is set to `2000` (or similar) to ensure the volume is writable by the app user.

### Config Parsing Errors

**Symptom**: `panic: Failed to process config: converting '6.7108864e+07' to type int`.

**Cause**: Helm passes large numeric values as floating-point scientific notation if not explicitly quoted.

**Solution**:

- Quote all large integer values in `values.yaml` (e.g., `maxRecvMsgSize: "67108864"`).

### Replication Lag

**Symptom**: `longbow_replication_lag_seconds` is high (> 30s) on follower nodes.

**Cause**: Network variance, slow disk I/O on follower, or high write throughput overwhelming replication stream.

**Solution**:

- Check follower disk IOPS and CPU.
- Ensure network connectivity between Leader and Follower is stable (`longbow_gossip_pings_total{direction="failed"}`).
- If persisting, consider scaling out with more shards to distribute write load.

### GPU Initialization Failure

**Symptom**: Log shows `WARN GPU initialization failed, using CPU-only`.

**Cause**:

- **CUDA/Metal**: Missing drivers or unsupported hardware.
- **Memory**: Insufficient GPU memory (OOM).
- **Permissions**: Access to GPU device denied.

**Solution**:

- Verify NVIDIA drivers/CUDA toolkit (Linux) or macOS version (Apple Silicon).
- Check `nvidia-smi` or `powermetrics` (macOS).
- Ensure `GPU_ENABLED=true` is set.

### S3 Backup Failures

**Symptom**: `longbow_s3_operations_total{status="error"}` is increasing.

**Cause**: AWS Credentials expiry, bucket policy denial, or network timeouts.

**Solution**:

- Verify `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.
- Check IAM permissions for `s3:PutObject` and `s3:GetObject`.
- Inspect logs for specific S3 error codes (e.g., `403 Forbidden`, `503 Slow Down`).

---

## 8. Monitoring & Operational Maintenance

### Metrics

Metrics are available at `http://<LONGBOW_METRICS_ADDR>/metrics` -- `http://localhost:9090/metrics` under the Helm chart or `docker-compose.yml`, `http://localhost:6000/metrics` for a bare binary that has not set the variable. Key namespaces include:

- **longbow_onnx_metal_memory_used_bytes**: (Gauge) VRAM utilization on Apple Silicon.
- **longbow_gpu_memory_bytes**: (Gauge) VRAM utilization on NVIDIA/CUDA systems.
- **longbow_stub_model_usage_total**: (Counter) **New in 0.1.9**: Count of times a stub embedding model was used due to missing configuration. Labels: `model_path`.
- `longbow_search_`: Latency and throughput.
- `longbow_gossip_`: Cluster membership status.
- `longbow_storage_`: WAL and disk usage.

### Memory Management

Set `GOMEMLIMIT` to 90% of the container's hard limit. Longbow's internal **GCTuner** will manage allocations to stay within `LONGBOW_MAX_MEMORY` while maximizing performance.
