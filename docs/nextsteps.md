# Next Steps for Longbow (Updated 2026-04-25)

## 0.1.9 Release Candidate (FINAL)

The 0.1.9 release is now in the hardening phase. All critical P0 blockers have been resolved or deferred to 0.2.0 for deeper architectural refinement.

### Active P0 Blocker: High-Dimensional NEON SIMD Distance Kernels
The current NEON distance kernels (`euclideanNEONKernel`) on Darwin arm64 exhibit severe performance regressions for high dimensions (dim > 384) due to lack of loop unrolling and prefetching, causing cache trashing and instruction pipeline stalls. We need to implement a fully unrolled and prefetched ASM kernel.

**Subtasks:**
- [x] **Step 1: Baseline Profiling Benchmark**: Write a targeted Go benchmark in `internal/simd/` to isolate `euclideanNEONKernel` for `dim=768` to measure the baseline cycle cost and cache behavior.
- [x] **Step 2: ASM Kernel Development**: Implement a new `euclideanHighDimNEONKernel` in `simd_arm64.s` that unrolls the loop by 4x (processes 16 floats/64 bytes per iteration) and utilizes `PRFM PLDL1KEEP` for memory prefetching.
- [x] **Step 3: Verification and Benchmarking**: Run the targeted benchmark against the new ASM kernel to verify performance exceeds the Go compiler's generic `Unrolled4x` fallback.
- [x] **Step 4: Integration**: Wire the optimized kernel into `internal/simd/simd_arm64.go` and restore its usage in `dispatch.go` for Darwin `dim > 384`.

### Completed P0 Blockers
- [x] **WAL Truncation**: Implemented proper truncation and batcher restart in `StorageEngine`.
- [x] **CPU Index PQ/TurboQuant**: Wired quantized search to SIMD kernels in `internal/simd`.
- [x] **cuVS GPU Search**: Implemented CGO bindings for NVIDIA cuVS backend.
- [x] **AMD64 SIMD Parity**: Resolved numerical correctness bugs in comparison assembly.
- [x] **ML Reranker Validation**: Added fallback logging and hardening for ONNX/WASM paths.
- [x] **Embedding Generator Hardening**: Added visibility into stub model usage.
- [x] **NEON Cosine/Dot/L2 Kernels**: Complete float32 SIMD kernels for all dimensions.
- [x] **NEON Fixed-Dim Kernels**: Added 128/384/768/1024/1536/3072 optimized kernels.

---

## Technical Debt Remediation (2026-04-26)

### Findings from Deep Code Review

#### P1: Critical Stubs Requiring Implementation

1. **TPUIndex** (`internal/gpu/tpu/tpu_index.go`)
   - AddPQ, TrainPQ, SearchPQ, EncodePQ: Not implemented (emulated)
   - SearchFloat16, SearchComplex64/128: Not implemented (emulated)
   - AddTurboQuant, SearchTurboQuant: Not implemented (emulated)
   - UpdateGraph, GraphExpand: Experimental stub
   - **Remediation**: Implement XLA kernels or document as experimental-only

2. **IVF-OPQ Index** (`internal/store/ivf_opq_index.go`)
   - AddByRecord: Not implemented
   - AddBatch: Not implemented
   - **Remediation**: Implement interface methods or deprecate index type

3. **IVF-HNSW Composite** (`internal/store/ivf_hnsw_composite.go`)
   - AddBatch: Not implemented
   - Vector fetching from location: Partial
   - **Remediation**: Complete implementation or document limitations

4. **IVF-PQ Index** (`internal/store/ivf_pq_index.go`)
   - Search, GetRawNeighbors, GetNeighbors, AddBatch: Not supported
   - **Remediation**: Implement or deprecate

#### P2: Metal Index Gaps

5. **MetalIndex** (`internal/gpu/metal/metal_gpu.go`)
   - AddTurboQuant, SearchTurboQuant: Not implemented (use optimized variant)

6. **MetalIndexOptimized** (`internal/gpu/metal/metal_gpu_optimized.go`)
   - UpdateGraph, GraphExpand: Not implemented

7. **MetalHybridIndex** (`internal/gpu/metal/metal_gpu_hybrid.go`)
   - AddTurboQuant, SearchTurboQuant: Not implemented
   - AddPQ, UpdateGraph, GraphExpand: Not implemented

#### P3: SIMD Optimization Opportunities

8. **TurboQuant NEON Kernels** (`internal/simd/simd_arm64.s`)
   - dotInt4NeonKernel, dotInt2NeonKernel: Stubs (rely on Go fallback)
   - **Remediation**: Implement optimized bit-pack kernels or document behavior

9. **Metal TurboQuant** (`internal/gpu/metal/metal_gpu_optimized.go`)
   - SearchTurboQuant: Implement CUDA kernels for 2/4/8-bit TQ

#### P4: Test Flakiness

10. **Skipped Tests** (171 t.Skip calls found)
    - Various timing-dependent, platform-specific, and flaky tests
    - **Remediation**: Fix flakiness or document as known limitations

### Remediation Task List

- [ ] **TPU Stub Remediation**: Either implement XLA kernels or document as experimental-only feature
- [ ] **IVF-OPQ Completion**: Implement missing AddBatch/AddByRecord or deprecate
- [ ] **IVF-HNSW Composite**: Implement AddBatch or document limitations
- [ ] **IVF-PQ Deprecation**: Implement missing methods or mark deprecated
- [ ] **Metal TurboQuant**: Implement SearchTurboQuant in optimized Metal index
- [ ] **NEON TurboQuant Optimization**: Implement proper bit-pack assembly or document Go-only behavior
- [ ] **Test Fixes**: Reduce skipped tests by fixing platform-specific test failures

---

## 0.2.0 Roadmap (Planned)

### 1. TPU Production Implementation
Production-ready implementation of Google TPU-accelerated indexing and search.
- [ ] **XLA Kernels**: Replace slow CPU emulation with XLA-compiled kernels.
    - [ ] Define XLA kernel interfaces in `internal/gpu/tpu/xla`.
    - [ ] Implement HNSW graph traversal in Pallas/XLA.
    - [ ] Implement IVF centroid assignments in XLA.
- [ ] **Memory Mapping**: Implement zero-copy tensor transfers to TPU memory using Arrow Flight.
- [ ] **TPU Orchestration**: Add Cloud TPU node discovery, orchestration, and health-checking mechanisms.
- [ ] **Feature Parity**: Replace the experimental `TPUIndex` stub with full `TrainPQ` and `Search` implementations.

### 2. Reliability & Test Coverage
Comprehensive hardening of core packages to meet enterprise stability standards.
- [ ] **Fuzzing**: Implement Go fuzz tests for high-risk components.
    - [ ] Fuzz test WAL parsing in `internal/storage`.
    - [ ] Fuzz test ingestion pipeline in `internal/store`.
    - [ ] Fuzz test HNSW graph mutations for structural integrity.
- [ ] **Concurrency Tests**: Add `sync/atomic` and `sync.RWMutex` stress tests.
    - [ ] Implement read/write contention stress test for `VectorStore`.
    - [ ] Validate race-free operation of the compaction worker.
- [ ] **Mocking Framework**: Isolate package tests from external dependencies.
    - [ ] Introduce mock network interfaces for `query` package.
    - [ ] Implement mock disk layer for `storage` package validation.
- [ ] **CI Enforcement**: Configure GitHub Actions to fail PRs that drop coverage below 95%.

### 3. GPU Sharding & Multi-Device Support
Automated partitioning of billion-scale indices across multiple GPUs.
- [ ] **Partitioning Algorithm**: Design a cluster-aware index splitting algorithm.
    - [ ] Implement K-Means based graph partitioning.
    - [ ] Add support for non-uniform partition sizes based on VRAM.
- [ ] **Inter-Device Communication**: Implement multi-GPU peer-to-peer data transfers.
    - [ ] Wire NCCL (Nvidia) bindings for intra-node transfers.
    - [ ] Wire RCCL (AMD) bindings for multi-GPU communication.
- [ ] **Query Routing & Aggregation**:
    - [ ] Build a fast dispatcher to route batched queries to GPU shards.
    - [ ] Implement optimized device-to-host Top-K merge algorithm.

### 4. Hardware Extensions & OS Portability
- [ ] **AMD ROCm Support**: Port existing CUDA kernels to HIP/ROCm.
- [ ] **Windows Support**: Native compilation and `mmap` portability.
    - [ ] Port Unix `mmap` to Windows `MapViewOfFile`.
    - [ ] Resolve CGO path and file locking differences.
- [ ] **Edge Optimization**: PiZero and ARMv6/v7/v8 low-memory constraints.
    - [ ] Implement `LONGBOW_LOW_MEM=1` mode for 512MB RAM devices.

### 5. Advanced Optimization (Ongoing)
- [ ] **ARM64 NEON Float64**: Resolve ABI issues in assembly kernels (currently using Go fallback).
- [ ] **Metal Ingest Acceleration**: Investigate Metal-specific ingest kernels for high-dim vectors.
- [ ] **TurboQuant Bit-Pack**: Implement SIMD bit-pack distance for 2/4/8-bit TQ.

### 6. Release Management & Documentation
- [ ] **Versioning**: Transition to semantic versioning (SemVer).
- [ ] **Migration Guides**: Document disk format changes for 0.1.8 -> 0.1.9.
- [ ] **API Reference**: Complete documentation for all hardware-specific flags.

### 7. Technical Debt & Stub Remediation
Systematic replacement of stubs and placeholders identified in the 0.1.9 deep code review.
- [ ] **Subsystem Implementation Completion**
    - [ ] **TPU Index**: Replace experimental stubs in `internal/gpu/tpu/tpu_index.go` with full `TrainPQ` and `SearchPQ` implementations.
    - [ ] **IVF-OPQ Index**: Complete missing interface implementations in `internal/store/ivf_opq_index.go` (Persistence, Sharding, Lifecycle).
    - [ ] **IVF-HNSW Composite**: Implement `ApplyDelta` anti-entropy logic in `internal/store/ivf_hnsw_composite.go`.
- [ ] **Mocking & Validation Framework**
    - [ ] **Network Isolation**: Implement `MockNetwork` for `internal/query` to enable deterministic failure testing.
    - [ ] **Disk Isolation**: Implement `MockDisk` for `internal/store` to validate IO error handling.
    - [ ] **Strict Model Validation**: Introduce `LONGBOW_STRICT_MODELS=1` to fail fast instead of falling back to `stubEmbeddingModel`.
- [ ] **Tests & Metrics**
    - [ ] **Integration Tests**: Add persistence validation tests for all composite indices.
    - [ ] **Metrics**: Add `longbow_stub_model_usage_total` and `longbow_index_sync_delta_total` to Prometheus exporter.
    - [ ] **Unit Tests**: Achieve 100% coverage for new TPU kernels and OPQ persistence logic.
