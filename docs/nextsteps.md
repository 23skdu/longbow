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

- [x] **Security & Bounds Hardening**
  - [x] Remediated all G115 (integer overflow) findings in `internal/simd/amx` for Darwin ARM64.
  - [x] Completed 100% `gosec` audit for G115 (integer overflow) and G304 (path traversal) findings across core storage engine.
  - [x] Implemented explicit bounds checking for CGO/AMX boundary crossings.
  - [x] *Rationale:* Production release requires guaranteed memory safety, especially at the hardware interface level.

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
  - [x] **Temporal Index Fix**: Corrected initialization path to ensure `TEMPORAL_ENABLED=true` is honored during server startup.
  - [x] **TurboQuant Validation**: Confirmed high-throughput ingestion and search for 2-bit, 4-bit, and 8-bit quantized formats.

## Future Optimization Paths (v0.2.3+)

- [x] **HNSW Indexing Parallelism**: Implemented sharded locking and lock-free entry point updates to reduce contention during high-load HNSW construction.
- [x] **SIMD DotProduct Specialization**: Extended the dimension-specialization pattern to DotProduct kernels for cosine similarity acceleration.
- [x] **Quantization-Aware Kernels**: Implemented bit-specialized kernels and trigonometric lookup tables for TurboQuant 2-bit, 4-bit, and 8-bit formats to eliminate bit-unpacking overhead in the distance computation hot path.
- [x] **NUMA-Aware Memory Affinity**: Optimized memory allocation on multi-socket AMD64 nodes to reduce cross-socket latency during large-scale searches.

*Document updated for v0.2.2-rc1 release preparation.*

## Observations & Performance Recommendations (2026-05-07)

Based on the performance validation matrix for v0.2.1-rc1, the following regressions and stability risks were identified:

1. **WAL Replay Bottleneck**: [x] Fixed: Implemented batch-vectorized WAL decoding and increased the `SharedWorkerPool` priority for replay-phase indexing to reduce system startup time.
2. **Filtering Concurrency Stability**: Observed sporadic panics in `roaring.Bitmap.Contains` during high-concurrency `Search_Filtered` benchmarks.
   - *Recommendation*: Audit all `filterBitmap` usage for thread-safety. Although workers have independent state, the underlying bitmaps generated from Arrow filters may share memory. Implement a "Copy-on-Write" or "Clone" strategy for shared filter bitmaps in concurrent search paths.
3. **M3 vs Xeon SIMD Gap**: Local M3 (NEON) consistently underperforms remote Xeon (AVX-512) for float32 dot-product and L2 search by ~30%.
   - *Recommendation*:
      - [x] **NEON Dual-Issue**: Refactored `dotHighDimNEONKernel` and `euclideanHighDimNEONKernel` with 8-way interleaving to utilize the Apple M3's dual-issue pipeline.
      - [x] **AMX Integration**: Integrated `Accelerate.framework` for high-dimensional distance calculations (>1024 dimensions) on Apple Silicon.
      - [x] **TurboQuant ARM**: Implemented bit-specialized NEON kernels and lookup tables for TurboQuant 2/4/8-bit distance calculations.
4. **HNSW Indexing Stalls**: pprof data indicates high contention in `AddConnection` for datasets >100k.
   - [x] *Fix*: Implemented sharded locking for HNSW `AddConnection` and `Insert` paths, allowing multiple indexing workers to operate with minimal contention.
5. **Memory Pressure & GC Jitter**: Aggressive GCTuner cycles were observed at 18GB limit.
   - [x] *Fix*: Refined `SlabArena` to use 64MB minimum chunks, significantly reducing the number of individual allocations tracked by the Go runtime and lowering GC overhead.
6. **Temporal Index Initialization (v0.2.2-rc1)**: Identified and resolved a systematic failure in temporal benchmarks due to the `temporalIndex` being nil even when enabled.
   - [x] *Fix*: Validated that `SetTemporalIndex` is called BEFORE the server starts listening and that environment variables are correctly exported to background processes.
7. **TurboQuant Scaling**: Verified that TurboQuant 4-bit ingestion throughput matches target expectations (>1M vec/s), but search latency scales non-linearly at >250k vectors.
    - [x] *Fix*: Implemented bit-specialized SIMD kernels and lookup tables for TurboQuant to eliminate bit-shifting and trigonometric overhead during distance computation.

## Refactoring & Maintainability Plan (v0.2.3+)

The following 10-part plan outlines the strategy for improving the Longbow codebase to ensure long-term maintainability, faster debugging (specifically for concurrency), and better context-window fit for AI-assisted development.

1.  **Modularize Storage Layers**: Decompose `arrow_hnsw.go` into specialized modules: `navigation.go` (search/traversal), `persistence.go` (mmap/disk), and `quantization_bridge.go`.
2.  **Atomic Snapshot Registry**: Implement a `MetadataRegistry` that uses a single atomic pointer to a versioned struct containing `entryPoint`, `maxLevel`, and `nodeCount`. This prevents inconsistent views of index state during concurrent ingestion.
3.  **Arena Isolation (Generation Tracking)**: Add "generation IDs" to memory chunks in `internal/memory`. Workers will only access chunks belonging to their snapshot's generation, preventing cross-batch data corruption.
4.  **Unified Computer Interface**: Refactor distance computers into a cleaner interface hierarchy, reducing the polymorphic overhead and simplifying the `resolveHNSWComputer` logic.
5.  **Lock-Free Neighbor Management**: Expand the use of `PackedNeighbors` to more layers and formalize the transition from lock-based to lock-free access as nodes stabilize.
6.  **Deterministic Concurrency Testing**: Implement a suite of tests that use fixed seeds and controllable worker interleaving to reproduce race conditions reliably.
7.  **SIMD Kernel Abstraction**: Decouple assembly kernels from the storage engine via a unified `internal/simd` API that supports runtime dispatching without circular dependencies.
8.  **Fast-Path Race Auditing**: Optimize `go test -race` by creating "micro-indices" (tiny capacities and dimensions) that exercise the same logic paths with 10x less memory pressure and faster execution.
9.  **Pluggable Ingestion Policies**: Refactor `AddBatchBulk` to support different ingestion strategies (e.g., sort-by-level, spatial clustering) to improve bootstrap quality and reduce graph construction time.
10. **Function Decomposition for Context Clarity**: Systematically break down large functions into smaller, logically complete units (<100 lines) to ensure that code changes and reviews fit reliably within modern AI context windows.
11. **HNSW Concurrency Stability (v0.2.2-rc2)**: [x] Resolved data race conditions and inconsistent metadata registry snapshots during bulk ingestion. Verified with `go test -race`.
