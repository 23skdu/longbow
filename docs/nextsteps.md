# Longbow Storage Engine Hardening - Next Steps

## P0 Blockers: Incomplete & Stubbed Code Remediation

The following items represent incomplete implementations or stubbed functionality that must be resolved before production deployment.

- [x] **Remediate `io_uring` Stubs for Linux Production**
  - [x] Replaced `internal/store/disk_writer_uring.go` with full asynchronous implementation.
  - [x] Hardened `internal/store/uring_reader_linux.go` for thread-safe concurrent reads.
  - [x] Verified interface stability via high-concurrency stress tests.

- [ ] **TPU Index Kernel Remediation**
  - [ ] Replace CPU fallback in `internal/gpu/tpu/tpu_index.go` with real XLA-compiled distance kernels.
  - [ ] Implement TPU-specific memory management for HBM (High Bandwidth Memory) allocation.
  - [ ] *Rationale:* Current TPU implementation is an emulated stub that computes distances on the CPU, providing no hardware acceleration.

- [x] **Replace Fallback Heuristic Models**
  - [x] Enforced strict model validation by removing the `LONGBOW_ALLOW_STUBS` environment variable bypass.
  - [x] Disabled keyword-matching fallback in `internal/store/ml_reranker.go` to ensure production model usage.

- [ ] **Security & Bounds Hardening**
  - [ ] Complete 100% `gosec` audit and remediate all remaining G115 (integer overflow) and G304 (path traversal) findings.
  - [ ] Implement exhaustive bounds checking for all CGO/assembly boundary crossings in the SIMD engine.
  - [ ] *Rationale:* Production release requires guaranteed memory safety, especially at the hardware interface level.

This document tracks the remaining tasks for hardening the Longbow storage engine for production readiness.

## Completed Tasks (v0.2.1-rc1)

- [x] **Interface Compliance & Adapter Remediation**
  - [x] Implemented `GetVectorID` and location mapping in IVF and HNSW adapters.
  - [x] Standardized `Location` type usage via aliases and packing helpers.
- [x] **SIMD Engine Hardening**
  - [x] Verified tiled batching remainder logic for non-standard vector dimensions.
  - [x] Validated alignment safety with unaligned assembly loads (VMOVUPS).
- [x] **Persistence & Integration Validation**
  - [x] Migrated from stubbed RCU tests to full-flow persistence lifecycle validation.
  - [x] Audited and implemented accurate `EstimateMemory` via `SlabArena` stats.
- [x] **HNSW Security Hardening (Gosec G115)**
  - [x] Remediated integer overflow vulnerabilities in `insertion_core.go` and `arrow_hnsw.go`.
  - [x] Implemented robust bounds checking and clamping for atomic configuration parameters (M, MMax, MMax0).
  - [x] Validated fixes with `gosec` and `go test -race`.
- [x] **Release Preparation**
  - [x] Finalized static analysis audit.
  - [x] Verified thread-safety for concurrent ingestion paths.

## Ongoing Work (v0.2.1)

- [ ] **EMLgo Math Library Evaluation**
  - [ ] Implement prototype vector distance benchmarks.
  - [ ] Compare precision and performance against current SIMD kernels.
- [ ] **Enhanced Build Matrix validation**
  - [ ] Extend ARM64 specific optimization tests.
  - [ ] formalize CI bench performance gates.

---

- [x] **DIMENSION-SPECIALIZED SIMD KERNELS (v0.2.2-rc1)**
  - [x] Implemented manual assembly kernels for common dimensions (128, 384, 768, 1024, 3072) for L2Squared.
  - [x] Enabled specialized kernels across NEON (ARM64), AVX2 (AMD64), and AVX512 (AMD64).
  - [x] Validated ingestion throughput gains (up to 2x improvement for 128d).
- [x] **PERFORMANCE MATRIX VALIDATION (v0.2.2-rc1)**
  - [x] Orchestrated full 18-run benchmark matrix (1440+ tests per host).
  - [x] Hardened `unified_benchmark.py` orchestrator for port stability and pprof reliability.
  - [x] Verified 18GB memory limit compliance during high-load benchmarks.

## Future Optimization Paths (v0.2.3+)

- [ ] **HNSW Indexing Parallelism**: Preliminary pprof data indicates a stall in HNSW construction during high-load ingestion. Investigating lock-free entry point selection to further reduce contention.
- [ ] **SIMD DotProduct Specialization**: Extend the dimension-specialization pattern to DotProduct kernels for cosine similarity acceleration.
- [ ] **Quantization-Aware Kernels**: Implement specialized kernels for TurboQuant 2-bit and 4-bit formats to eliminate bit-unpacking overhead in the distance computation hot path.
- [ ] **NUMA-Aware Memory Affinity**: Optimize memory allocation on multi-socket AMD64 nodes to reduce cross-socket latency during large-scale searches.

*Document updated for v0.2.2-rc1 release preparation.*
