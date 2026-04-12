# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-12
**Purpose**: Create competitive features comparable to Pinecone, Milvus, Qdrant, Weaviate

---

## 🎯 REMAINING WORK SUMMARY

### HIGH PRIORITY

| # | Feature | Status | Files |
|---|---------|--------|-------|
| 1 | Window Functions | ⏳ NOT IMPLEMENTED | Advanced SQL features |
| 2 | Subqueries/CTE | ⏳ NOT IMPLEMENTED | Advanced SQL features |
| 3 | Monitoring & Observability | ⏳ PENDING | CDC metrics, tracing |

### MEDIUM PRIORITY

| # | Feature | Status | Files |
|---|---------|--------|-------|
| 4 | ONNX Benchmarks | ⏳ PENDING | `internal/onnx/benchmarks_test.go` |
| 5 | Documentation Updates | ⏳ PENDING | API docs, migration guide |

---

## Part 1: Window Functions — HIGH PRIORITY

### Overview

**Description**: Implement window functions (e.g., `ROW_NUMBER() OVER (...)`, `RANK()`) to allow users to perform complex analytical queries directly over vector search results.

### Implementation Subtasks

| Step | Task | Status |
|------|------|--------|
| 1 | Extend `TicketQuery` to support `OVER` clause and partition/order specifications | PENDING |
| 2 | Implement window operator in the query engine | PENDING |
| 3 | Add support for common window functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`) | PENDING |
| 4 | Optimize window function execution using SIMD for partitioned sorts | PENDING |

---

## Part 2: Subqueries/CTE — HIGH PRIORITY

### Overview

**Description**: Add support for Common Table Expressions (CTEs) and nested subqueries to enable multi-stage search pipelines (e.g., search documents, then join with metadata from another dataset).

### Implementation Subtasks

| Step | Task | Status |
|------|------|--------|
| 1 | Update parser to support `WITH` clauses and nested `SELECT` | PENDING |
| 2 | Implement query plan nodes for subquery results | PENDING |
| 3 | Add support for correlated subqueries | PENDING |
| 4 | Optimize subquery execution via temporary Arrow buffers | PENDING |

---

## Part 3: Monitoring & Observability — HIGH PRIORITY

### Overview

**Description**: Enhance the visibility into the system's performance and behavior, especially for the new GPU and ONNX components.

### Implementation Subtasks

| Step | Task | Status |
|------|------|--------|
| 1 | Add GPU utilization and memory metrics to Prometheus | PENDING |
| 2 | Implement tracing for ONNX inference pipelines | PENDING |
| 3 | Add metrics for PQ training and encoding latency | PENDING |
| 4 | Create Grafana dashboards for GPU/ONNX health | PENDING |

---

## ✅ COMPLETED FEATURES (2026)

### Hardware-Accelerated PQ (VNNI/GPU)
- AVX-512 VNNI lookup kernels implemented.
- CUDA/Metal kernels for batch PQ compression.
- ADC (Asymmetric Distance Computation) table builders.
- Ingestion pipeline updated for GPU encoding.

### Dataset Import/Export
- `dataset_io.go` with Parquet Export/Import.
- Arrow Record to Parquet stream conversion.
- ExportDataset/ImportDataset wired to StorageBackend.
- Unit tests and metrics added.

### ONNX Runtime Integration
- Unified ONNX bridge for Metal (macOS) and ONNX Runtime (Linux/CUDA).
- Functional reranker and embedding generator.
- Graceful fallbacks for non-GPU environments.

### CUDA Memory Operations
- `allocateCUDAMemory` and `freeCUDAMemory` implemented.
- `cudaMemcpyHostToDevice` and `DeviceToHost` implemented.
- Unified memory management in `GPUMemPool`.

### Vectorized Metadata Filtering
- SIMD comparison kernels for Arrow types.
- Bitmask merging using AVX-512 `k-mask` registers.
- "Early Exit" logic for index traversal.

### Zero-Copy Network-to-GPU
- libibverbs CGO bindings for Linux/RoCEv2.
- RDMA-aware Arrow Flight handshake.
- Fallback stubs for non-IB environments.

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
