# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-19

---

## 🎯 REMAINING WORK

### Performance Optimization (Priority: HIGH)

- [ ] **Numerical Parity**: Verify SIMD results against high-precision float64 baseline.
- [ ] **DiskGraph Load Caching**: 💡 PLANNED | Cache DiskGraph refs in SearchContext for lower latency.
- [ ] **Adaptive M-Param**: 💡 PLANNED | implementation of dynamic connectivity scaling.
- [ ] **Metrics Sampling**: 💡 PLANNED | Reduce atomic overhead in metrics collection.
- [ ] **Zero-Copy Ingest**: 💡 PLANNED | Direct Arrow-to-HNSW memory mapping.

### Store Modularization (Priority: HIGH)

The `internal/store` package is currently over-bloated (500+ files). This 6-part plan aims to restructure it for scalability.

| Phase | Component | Priority | Status |
|-------|-----------|----------|--------|
| 1 | Foundation & Types | P0 | ✅ COMPLETE |
| 2 | Core HNSW Algorithms | P0 | ✅ COMPLETE |
| 3 | Indexing & Sharding | P1 | ✅ COMPLETE |
| 4 | Persistence & Storage | P1 | ✅ COMPLETE |
| 5 | Background Workers | P2 | ✅ COMPLETE |
| 6 | API Refinement & Metrics | P2 | ✅ COMPLETE |

### New Features

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 6 | ONNX Benchmarks | ⏳ NOT STARTED | `internal/onnx/benchmarks_test.go` missing (only `onnx_benchmark_test.go` exists). |
| 7 | COW Optimization | ✅ COMPLETE | Optimized `GraphData.Clone()` and pre-allocation strategy. |
| 8 | Search Result Pooling | ✅ COMPLETE | Implemented `sync.Pool` and `ArrowSearchContext` pooling. |
| 9 | Fast-Path Search | ✅ COMPLETE | Specialized kernels for all types to bypass `any` overhead. |
| 10 | allocation Buffer Pool | ✅ COMPLETE | Pooled buffers for type conversions in `SearchContext`. |
| 11 | Lock-Free Adjacency | 🚀 NEXT | Expand lock-free patterns to layer 0 updates. |
| 12 | Zero-Copy Tensor Stream | 💡 PLANNED | Direct GPU-to-GPU tensor transfer via Arrow Flight. |

---

## ✅ VERIFIED COMPLETED (2026)

- [x] **Comprehensive Multi-Metric Support**: Full Euclidean, Cosine, and DotProduct support across all supported data types (int8-uint64, complex, turboquant). Verified with matrix correctness tests and 1M+ exec fuzzing.
- [x] **Advanced SQL (Subqueries/CTE)**: Nested query resolution and CTE support fully implemented.
- [x] **ONNX Linux/CUDA Backend**: Functional `onnxruntime_go` integration with CUDA EP support.
- [x] **SIMD String Filtering**: Semi-vectorized length-first string equality kernels for AVX2.
- [x] **Float64 SIMD Match**: Full AVX2/AVX-512 comparison kernels integrated into query engine.
- [x] **AVX-512 Filter Masks**: True K-mask based kernels for all numeric types implemented.
- [x] **SQL Window Functions**: Analytical functions (Rank, RowNumber, Sum, etc.) fully implemented and integrated.
- [x] **Dataset Import/Export**: Parquet and Arrow IPC export/import routines implemented.
- [x] **CUDA Memory Ops**: Stable CGO-based unified memory management.
- [x] **Zero-Copy RDMA**: `libibverbs` integration for Linux/RoCEv2 transport.
- [x] **Metal ONNX**: Reranker and embedding generation functional on macOS ARM64.
- [x] **Core Coverage Stabilization**: Reached ~67% statement coverage across `simd`, `query`, and `onnx`.

---

## Architecture Notes

### Build Tags - Expected Stubs (NOT Issues)

- `internal/gpu/memory/memory_metal_stub.go`
- `internal/gpu/memory/memory_cuda_stub.go`
- `internal/simd/simd_stubs*.go`
- `internal/storage/wal_backend_arrow_iouring_stub.go`
