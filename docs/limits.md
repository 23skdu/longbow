# Longbow Limits & Constraints

This document outlines the runtime limits, constraints, and boundaries for Longbow deployments.

---

## 1. gRPC Message Size Limits

| Limit | Default | Env Variable | Description |
|-------|---------|-------------|-------------|
| Max receive | 512MB | `GRPC_MAX_RECV_MSG_SIZE` | Max size of any single gRPC request (ingest, search, etc.) |
| Max send | 512MB | `GRPC_MAX_SEND_MSG_SIZE` | Max size of any single gRPC response (DoGet results) |

Both limits are configurable per-deployment. All ingest requests (vectors + metadata + all columns) must fit within the receive limit. All search results must fit within the send limit.

---

## 2. Metadata / Text Storage

There is **no hardcoded per-field size limit** on metadata columns. Metadata is stored as part of the Arrow RecordBatch payload, which is bounded by the gRPC receive limit.

Practical text storage estimates at 512MB request limit:

| Text Size | Characters | Approximate Pages |
|-----------|------------|-----------------|
| 512MB | ~536,870,912 | ~107,000 |
| 100MB | ~104,857,600 | ~21,000 |
| 10MB | ~10,485,760 | ~2,100 |
| 1MB | ~1,048,576 | ~210 |
| 100KB | ~102,400 | ~20 |
| 10KB | ~10,240 | ~2 |

**Recommendation**: For agent memory use cases, typical text chunks are 512–4,096 tokens (~0.5–4KB). This allows storing millions of memory records comfortably within the 512MB window. Avoid embedding multi-megabyte text strings in a single metadata cell — chunk text externally and store a reference ID instead.

---

## 3. Record & Batch Sizes

| Parameter | Default | Notes |
|-----------|---------|-------|
| Search batch size | 32 | Concurrent searches per pool |
| Index batch size | 1,000 | MaxBatchSize for HNSW neighbor updates |
| Record batches | Unlimited | Append-only; managed by eviction/compaction |

Single RecordBatch sizes are unbounded but typically 1KB–10MB in practice.

---

## 4. Memory Limits

| Parameter | Default | Env Variable |
|-----------|---------|-------------|
| Soft memory limit | 10GB | `LONGBOW_MAX_MEMORY` |
| Hard memory limit | 0 (off) | `LONGBOW_MAX_MEMORY_HARD` |
| WAL size | 1GB | `LONGBOW_MAX_WAL_SIZE` |
| TTL | 0 (off) | `LONGBOW_TTL_SECONDS` |

### Soft vs Hard Memory Limits

- **`LONGBOW_MAX_MEMORY` (Soft Limit)**: Enforced by the GC tuner. Exceeding this limit triggers eviction of the least-recently-used record batches to disk. The system will also apply an exponential backpressure delay (scaling from 5ms up to 100ms) on ingestion requests as memory approaches this limit to allow background eviction to catch up.
- **`LONGBOW_MAX_MEMORY_HARD` (Hard Limit)**: An absolute hard ceiling. If memory exceeds this threshold, the server immediately stops accepting new ingestion requests and returns a `ResourceExhausted` (gRPC status code 8) error to the client. This protects the server from Out-Of-Memory (OOM) crashes under extreme sudden load.

---

## 5. Dataset Limits

| Constraint | Limit | Notes |
|------------|-------|-------|
| Max dimensions | 3,072 | HNSW + SIMD paths validated up to this |
| Vector types | 14 | float32/64/16, int8/16/32/64, uint8/16/32/64, complex64/128, turboquant |
| Max datasets | Unlimited | Bounded by available memory |
| Max vectors per dataset | Unlimited | Bounded by available memory + disk |
| Max datasets per node | Unlimited | Bounded by available memory |

---

## 6. GraphRAG & Temporal

| Parameter | Limit | Notes |
|-----------|-------|-------|
| GraphRAG alpha | 0.0–1.0 | 0.0 = pure graph, 1.0 = pure ANN |
| GraphRAG max hops | Configurable | BFS traversal depth |
| Temporal windows | Unlimited | Bounded by dataset time range |
| Temporal precision | nanosecond | int64 nanosecond timestamps |

---

## 7. Concurrency & Connections

| Parameter | Default | Notes |
|-----------|---------|-------|
| Ingestion workers | `runtime.NumCPU()` | Parallel Arrow batch processing |
| Indexing workers | `runtime.NumCPU()` | HNSW index construction |
| Flight connections | Pooled | SmartClient manages connection reuse |
| Max concurrent searches | Bounded by search pool | `searchBatchSize=32` per pool |

---

## 8. Network & Storage

| Parameter | Default | Notes |
|-----------|---------|-------|
| Max WAL segments | Unlimited | Rotating WAL with configurable size |
| Snapshot format | Parquet | Zstd compressed by default |
| io_uring | Linux only | Falls back to standard I/O on macOS |
| RDMA | Linux only | Configurable via `LONGBOW_RDMA_ENABLED` |

---

**Platform**: All limits apply to both macOS (CPU/Metal) and Linux (CPU/CUDA)
**Generated**: 2026-04-23