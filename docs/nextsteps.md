# Longbow Next Steps — Feature Roadmap 2026

**Last Updated**: 2026-04-20

---

## 🎯 REMAINING WORK

### Stability & Production Readiness (Priority: CRITICAL)

- [x] **Release 0.1.9 Deployment**: Finalize the multi-platform Docker push (ARM64 Metal / AMD64 NVIDIA) and tag the 0.1.9 production release.
- [x] **Gosec Hardening**: Systematically address the remaining 14 high-confidence security findings in the `internal/simd` and `internal/gpu` CGO bridge layers.
- [x] **Expand Test Coverage**: Expand unit and integration test suites across `internal/store/core`, `internal/onnx`, and `internal/simd`. Added comprehensive SIMD test suite and core search context lifecycle tests. Achieved 100% coverage in `internal/onnx`.


## 🎯 CURRENT RELEASE: 0.1.9-rc3

- **Total Coverage**: ~53% (Target: >95%)
- **Status Headroom**: Functional features complete. Focus shifted to deep unit test coverage.

### Completed (Session 0.1.9-rc3-B)

- [x] Parquet High-Throughput IO (+tests)
- [x] ML Cross-Encoder Integration (+mock tests)
- [x] Wazero WASM Runner Finalization
- [x] Adaptive Index Lifecycle Testing
- [x] Storage Backend Unit Tests (FileBackend)
- [x] Query Engine Extended Type Coverage (Int32, Uint64, Float64, String)
- [x] Sharding Result Aggregator Coverage (Merge & Sort)

### Immediate Next Steps (Coverage Push)

1. **Security & RBAC Coverage**: Implement tests for `internal/store/rbac.go`.
2. **GPU Mocking**: Create a mock layer for `internal/gpu`.
3. **Remote Storage Mocks**: Mock S3/GCS in `internal/storage`.
4. **Final 0.1.9 Tagging**: Once coverage hits critical mass (>80%+).

## 🚀 Future Roadmap (0.1.10+)

- [ ] **Transformer Mean Pooling**: Proper pooling across transformer hidden states.
- [ ] **Dynamic Sharding**: Auto-rebalancing shards based on node load.

---

## ✅ VERIFIED COMPLETED (2026)

- [x] **Post-0.1.9 Remediation Plan**: Completed full implementation of functional ML/IO/Infra layers (WordPiece, Wazero, Parquet, Darwin NUMA).
- [x] **Parallel SQ8 Ingestion Stabilization (0.1.9)**: Resolved structural races, deadlocks, and recall failures (1/100) in the HNSW bulk ingestion engine.
- [X] **Zero-Copy Tensor Stream**: Direct GPU-to-GPU tensor transfer via Arrow Flight (RoCEv2/PeerDirect).
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
