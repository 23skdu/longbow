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

## Observations & Performance Recommendations (2026-05-07)

Based on the performance validation matrix for v0.2.1-rc1, the following regressions and stability risks were identified:

1. **WAL Replay Bottleneck**: Background indexing throughput for replayed WAL records is significantly lower than real-time ingestion. 
   - *Recommendation*: Implement batch-vectorized WAL decoding and increase the `SharedWorkerPool` priority for replay-phase indexing to reduce system startup time.
2. **Filtering Concurrency Stability**: Observed sporadic panics in `roaring.Bitmap.Contains` during high-concurrency `Search_Filtered` benchmarks.
   - *Recommendation*: Audit all `filterBitmap` usage for thread-safety. Although workers have independent state, the underlying bitmaps generated from Arrow filters may share memory. Implement a "Copy-on-Write" or "Clone" strategy for shared filter bitmaps in concurrent search paths.
3. **M3 vs Xeon SIMD Gap**: Local M3 (NEON) consistently underperforms remote Xeon (AVX-512) for float32 dot-product and L2 search by ~30%.
   - *Recommendation*: Optimize NEON kernels with dual-issue instruction scheduling and investigate `AMX` (Apple Matrix Extension) integration for high-dimensional vector math.
4. **HNSW Indexing Stalls**: pprof data indicates high contention in `AddConnection` for datasets >100k.
   - *Recommendation*: Implement the "Lock-Free Entry Point" strategy mentioned in Future Optimization Paths to allow multiple indexing workers to traverse the graph without acquiring global locks.
5. **Memory Pressure & GC Jitter**: Aggressive GCTuner cycles were observed at 18GB limit.
   - *Recommendation*: Refine the `SlabArena` to use larger chunks (e.g., 64MB) to reduce the number of individual allocations tracked by the Go runtime, thereby reducing GC overhead.
