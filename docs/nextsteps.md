# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-11
**Purpose**: Create competitive features comparable to Pinecone, Milvus, Qdrant, Weaviate

---

## Executive Overview: Longbow vs Apache DataFusion

### Fundamental Differences

| Aspect | Longbow | Apache DataFusion |
|--------|---------|-------------------|
| **Primary Purpose** | Distributed vector database for AI/ML workloads | General-purpose SQL query engine |
| **Language** | Go | Rust |
| **Data Model** | Vectors + metadata + embeddings | Tabular (rows/columns) with complex types |
| **Core API** | gRPC + Apache Arrow Flight | SQL + DataFrame API |
| **Query Language** | Vector search (HNSW, BM25) + filtering | Full SQL (SELECT, JOIN, GROUP BY, Window functions) |
| **Storage** | In-memory + WAL + Parquet snapshots | File-based (Parquet, CSV, JSON, Avro) |
| **Distributed** | Built-in gossip, sharding, replication | Ballista (separate subproject) |
| **Indexing** | HNSW, DiskANN, PQ, Bloom filters | Row-group statistics, bloom filters |

### Key Architectural Differences

1. **Query Model**: Longbow = vector-centric search; DataFusion = SQL relational queries
2. **State Management**: Longbow = persistent vector store with WAL; DataFusion = stateless queries
3. **Protocol**: Longbow = Arrow Flight only; DataFusion = multiple (SQL CLI, Python, Ballista)
4. **Extension Model**: Longbow = Go plugins; DataFusion = Rust traits (TableProvider, OptimizerRule)

---

## 🎯 REMAINING WORK SUMMARY

### HIGH PRIORITY

| # | Feature | Status | Files |
|---|---------|--------|-------|
| 3 | Hardware-Accelerated PQ (VNNI/GPU) | 🚧 PARTIAL | `internal/simd/cpu_detection.go`, `internal/simd/simd_amd64.go` |
| 4 | Dataset Import/Export | ⏳ PENDING | `internal/store/dataset_io.go` |
| 5 | ONNX Runtime Integration | 🚧 STUB | `internal/store/ml_reranker.go`, `internal/store/embedding_generator.go` |

### MEDIUM PRIORITY

| # | Feature | Status | Files |
|---|---------|--------|-------|
| 6 | CUDA Memory Operations | 🚧 STUB | `internal/gpu/cuda/cuda_index.go` |
| 7 | Window Functions | 🚧 PARTIAL | Advanced SQL features |
| 8 | Subqueries/CTE | ⏳ NOT IMPLEMENTED | Advanced SQL features |

### LOW PRIORITY

| # | Feature | Status | Files |
|---|---------|--------|-------|
| 9 | Documentation Updates | ⏳ PENDING | API docs, migration guide |
| 10 | Monitoring & Observability | ⏳ PENDING | CDC metrics, tracing |

---

---

## Part 3: Hardware-Accelerated PQ (VNNI/GPU) — HIGH PRIORITY

### Overview

**Description**: Move Product Quantization (PQ) encoding from the CPU software path directly to the GPU or utilize specialized CPU SIMD instructions (like AVX-512 VNNI for INT8 dot products) to drastically cut ingestion latency.

### Implementation Subtasks

| Step | Task | Status |
|------|------|--------|
| 1 | Integrate AVX-512 VNNI kernels in assembly for fast INT8 distance calculation during PQ centroid matching. | ✅ DONE |
| 2 | Implement CUDA/Metal kernels for offloading batch PQ compression when GPU is available. | 🚧 IN PROGRESS |
| 3 | Add dynamic dispatch (similar to SIMD distance functions) to route PQ encoding to the optimal hardware. | ✅ DONE |
| 4 | Update ingestion pipeline to utilize hardware-accelerated PQ encode paths. | PENDING |

---

## Part 4: Dataset Import/Export — HIGH PRIORITY

### Overview

**Description**: ImportDataset and ExportDataset methods return "not implemented" errors.
These will be implemented following S3 snapshot patterns using Apache Arrow/Parquet format.

### Parquet Schema

| Column     | Type                     | Description                             |
| :--------- | :----------------------- | :-------------------------------------- |
| id         | int64                    | Unique identifier for the vector.       |
| vector     | fixed_size_binary_array  | The embedding vector (e.g., 1536 dims). |
| metadata   | binary (JSON)            | Associated metadata blob.              |
| created_at | timestamp                | Ingestion timestamp.                    |

### Implementation Subtasks

| Step | Task | File | Status |
|------|------|------|--------|
| 1 | Create `dataset_io.go` with Export/Import Parquet methods | internal/store/dataset_io.go | PENDING |
| 2 | Implement Arrow Record to Parquet stream conversion | internal/store/dataset_io.go | PENDING |
| 3 | Wire ExportDataset/ImportDataset to use StorageBackend | internal/store/rate_limit.go | PENDING |
| 4 | Add unit tests for dataset export/import/cloning | internal/store/dataset_io_test.go | PENDING |
| 5 | Add Prometheus metrics and benchmarks | internal/metrics/ | PENDING |

---

## Part 5: ONNX Runtime Integration — HIGH PRIORITY

### Overview

**Description**: ML reranker and embedding generator use stub implementations.

### Implementation Subtasks

| Step | Task | File | Status |
|------|------|------|--------|
| 1 | Integrate ONNX Runtime Go bindings for ML Reranker | internal/store/ml_reranker.go | PENDING |
| 2 | Integrate ONNX Runtime Go bindings for Embedding Generator | internal/store/embedding_generator.go | PENDING |
| 3 | Replace stub models with real ONNX inference engine | internal/store/ml_reranker.go | PENDING |

---

## Part 6: CUDA Memory Operations — MEDIUM PRIORITY

### Overview

**Description**: CUDA memory stub returns "not implemented" errors.

### Implementation Subtasks

| Step | Task | File | Status |
|------|------|------|--------|
| 1 | Implement allocateCUDAMemory | internal/gpu/memory/memory_cuda.go | PENDING |
| 2 | Implement freeCUDAMemory | internal/gpu/memory/memory_cuda.go | PENDING |
| 3 | Implement cudaMemcpyHostToDevice and DeviceToHost | internal/gpu/memory/memory_cuda.go | PENDING |

---

## ✅ COMPLETED FEATURES

### Part 1: Zero-Copy Network-to-GPU (RDMA/RoCEv2)

| Feature | Status |
|---------|--------|
| libibverbs CGO bindings for Linux/RoCEv2 | ✅ DONE |
| RDMA-aware Arrow Flight handshake (rkey/addr exchange) | ✅ DONE |
| Fallback stubs for non-IB environments | ✅ DONE |
| Dockerized IB toolchain integration | ✅ DONE |

### Part 2: Finer-Grained Locking in ShardedHNSW

| Feature | Status |
|---------|--------|
| Concurrent Skip-List for entry points | ✅ DONE |
| Lock-free Shard metadata (sync.Map + ChunkedStore) | ✅ DONE |
| Unified per-node CAS spinlocks for graph updates | ✅ DONE |
| Removal of redundant [1024]Mutex array | ✅ DONE |

### Part 0.4: Vectorized Metadata Filtering

| Feature | Status |
|---------|--------|
| SIMD comparison kernels for common Arrow types (Uint8, Float32, String) | ✅ DONE |
| Bitmask merging using AVX-512 `k-mask` registers | ✅ DONE |
| Refactored `Filter` interface for pre-computed SIMD bitmasks | ✅ DONE |
| "Early Exit" logic bypassing index traversal on empty bitmasks | ✅ DONE |

### Part 0.5: Optimized Parquet Snapshot Generation

| Feature | Status |
|---------|--------|
| Streaming Parquet encoder avoiding `reflect` | ✅ DONE |
| Asynchronous, non-blocking disk writes via `io_uring` | ✅ DONE |
| `SlabArena` off-heap memory integration | ✅ DONE |
| Shredding background snapshotting | ✅ DONE |

### Part 20: Predicate & Projection Pushdown

| Feature | Status |
|---------|--------|
| Predicate Pushdown and Projection Pushdown | ✅ DONE |
| FilterEvaluator (int64, float32, string, compound) | ✅ DONE |
| HNSW Predicate Integration | ✅ DONE |

### Part 21: Temporal Query Capabilities

| Feature | Status |
|---------|--------|
| TemporalIndex, Time-based TTL, Temporal Aggregation | ✅ DONE |
| gRPC API and Python SDK support | ✅ DONE |

### Server Integration & Tests

| Feature | Status |
|---------|--------|
| CDC, WebSocket Server, MQ Exporter, Learned Index Predictor | ✅ DONE |
| Federated, Geo, Temporal, CDC, and WebSocket Test Coverage | ✅ DONE |

---

## Architecture Notes

### Protocol
Longbow uses **gRPC + Apache Arrow Flight only**. No REST/HTTP API for data operations.

### Build Tags - Expected Stubs (NOT Issues)
The following are intentional stubs for cross-platform compilation:
- `internal/gpu/memory/memory_metal_stub.go`
- `internal/gpu/memory/memory_cuda_stub.go`
- `internal/simd/simd_stubs*.go`
- `internal/storage/wal_backend_arrow_iouring_stub.go`
