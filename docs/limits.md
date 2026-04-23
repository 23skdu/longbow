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
| Max memory | 10GB | `LONGBOW_MAX_MEMORY` |
| WAL size | 1GB | `LONGBOW_MAX_WAL_SIZE` |
| TTL | 0 (off) | `LONGBOW_TTL_SECONDS` |

The memory limit is a hard ceiling enforced by the GC tuner. Exceeding it triggers eviction of the least-recently-used record batches before new data is ingested.

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