# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-16

---

## 🎯 REMAINING WORK

### New Features

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 5 | Subqueries/CTE | ⏳ NOT STARTED | Advanced SQL - parser updates needed for `WITH` and nested `SELECT`. |
| 6 | ONNX Benchmarks | ⏳ NOT STARTED | `internal/onnx/benchmarks_test.go` missing (only `onnx_benchmark_test.go` exists). |

---

## ✅ VERIFIED COMPLETED (2026)

- [x] **ONNX Linux/CUDA Backend**: Functional `onnxruntime_go` integration with CUDA EP support.
- [x] **SIMD String Filtering**: Semi-vectorized length-first string equality kernels for AVX2.
- [x] **Float64 SIMD Match**: Full AVX2/AVX-512 comparison kernels integrated into query engine.
- [x] **AVX-512 Filter Masks**: True K-mask based kernels for all numeric types implemented.
- [x] **SQL Window Functions**: Analytical functions (Rank, RowNumber, Sum, etc.) fully implemented and integrated.
- [x] **Dataset Import/Export**: Parquet and Arrow IPC export/import routines implemented.
- [x] **CUDA Memory Ops**: Stable CGO-based unified memory management.
- [x] **Zero-Copy RDMA**: `libibverbs` integration for Linux/RoCEv2 transport.
- [x] **Metal ONNX**: Reranker and embedding generation functional on macOS ARM64.

---

## Architecture Notes

### Build Tags - Expected Stubs (NOT Issues)

- `internal/gpu/memory/memory_metal_stub.go`
- `internal/gpu/memory/memory_cuda_stub.go`
- `internal/simd/simd_stubs*.go`
- `internal/storage/wal_backend_arrow_iouring_stub.go`
