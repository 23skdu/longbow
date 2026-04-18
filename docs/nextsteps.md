# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-17

---

## 🎯 REMAINING WORK

### Store Modularization (Priority: HIGH)
The `internal/store` package is currently over-bloated (500+ files). This 6-part plan aims to restructure it for scalability.

| Phase | Component | Priority | Status |
|-------|-----------|----------|--------|
| 1 | Foundation & Types | P0 | ✅ COMPLETE |
| 2 | Core HNSW Algorithms | P0 | ✅ COMPLETE |
| 3 | Indexing & Sharding | P1 | ✅ COMPLETE |
| 4 | Persistence & Storage | P1 | ⏳ IN PROGRESS |
| 5 | Background Workers | P2 | ⏳ NOT STARTED |
| 6 | API Refinement & Metrics | P2 | ⏳ NOT STARTED |

### New Features

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 6 | ONNX Benchmarks | ⏳ NOT STARTED | `internal/onnx/benchmarks_test.go` missing (only `onnx_benchmark_test.go` exists). |

---

## ✅ VERIFIED COMPLETED (2026)

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
- [x] **Core Coverage Stabilization**: Reached ~67% statement coverage across `simd`, `query`, and `onnx` (100% of reachable ARM64 logic). Exhaustive type tests and JSON parser verification implemented.

---

## Architecture Notes

### Build Tags - Expected Stubs (NOT Issues)

- `internal/gpu/memory/memory_metal_stub.go`
- `internal/gpu/memory/memory_cuda_stub.go`
- `internal/simd/simd_stubs*.go`
- `internal/storage/wal_backend_arrow_iouring_stub.go`
