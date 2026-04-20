# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-19

---

## 🎯 REMAINING WORK

### Stability & Production Readiness (Priority: CRITICAL)

- [ ] **Fix Parallel SQ8 Ingestion Contention**: Resolve the race condition in `TestArrowHNSW_AddBatch_Parallel_SQ8` where parallel ingestion with SQ8 quantization results in poor graph connectivity (search returning only 1 result).
- [ ] **Release 0.1.9 Stabilization**: Perform a full soak test for at least 6 hours under 10k QPS mixed read/write load following the zero-copy ingest/streaming changes.
- [ ] **Gosec Hardening**: Systematically address the remaining 14 high-confidence security findings in the `internal/simd` and `internal/gpu` CGO bridge layers.
- [ ] **Achieve >95% Test Coverage**: Expand unit and integration test suites across `internal/store/core`, `internal/onnx`, and `internal/simd` to reach the 95% statement coverage threshold before the final 0.1.9 release.

---

## ✅ VERIFIED COMPLETED (2026)

- [x] **Zero-Copy Tensor Stream**: Direct GPU-to-GPU tensor transfer via Arrow Flight (RoCEv2/PeerDirect).
- [x] **Zero-Copy HNSW Ingest**: Direct Arrow-to-HNSW memory mapping for zero-copy bulk ingestion.
- [x] **ONNX Multi-Backend Benchmarks**: Comprehensive benchmarking suite covering CPU, CUDA, and Metal backends.
- [x] **Store Modularization (Phases 1-6)**: Cleanly decoupled HNSW internals into modular sub-packages.
- [x] **Lock-Free Adjacency (Layer 0)**: Optimized high-contention graph updates for 100k+ TPS ingestion.
- [x] **Numerical Parity & FP64 Match**: Verified SIMD kernels against high-precision float64 baselines.
- [x] **Adaptive M-Param & Search Context**: Dynamic connectivity scaling and pooled context management.
- [x] **Advanced SQL (Subqueries/CTE)**: Nested query resolution and CTE support fully integrated.
- [x] **Metal ONNX & CUDA Backend**: Functional GPU acceleration on macOS ARM64 and Linux NVIDIA.
- [x] **Core Coverage Coverage**: Stabilized ~67% statement coverage across core performance packages.

---

## Architecture Notes

### Build Tags - Expected Stubs (NOT Issues)

- `internal/gpu/memory/memory_metal_stub.go`
- `internal/gpu/memory/memory_cuda_stub.go`
- `internal/simd/simd_stubs*.go`
- `internal/storage/wal_backend_arrow_iouring_stub.go`
