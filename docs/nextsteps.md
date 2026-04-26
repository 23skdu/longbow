# Next Steps for Longbow (Updated 2026-04-25)

## 0.1.9 Release Candidate (FINAL)

The 0.1.9 release is now in the hardening phase. All critical P0 blockers have been resolved or deferred to 0.2.0 for deeper architectural refinement.

### Completed P0 Blockers
- [x] **WAL Truncation**: Implemented proper truncation and batcher restart in `StorageEngine`.
- [x] **CPU Index PQ/TurboQuant**: Wired quantized search to SIMD kernels in `internal/simd`.
- [x] **cuVS GPU Search**: Implemented CGO bindings for NVIDIA cuVS backend.
- [x] **AMD64 SIMD Parity**: Resolved numerical correctness bugs in comparison assembly.
- [x] **ML Reranker Validation**: Added fallback logging and hardening for ONNX/WASM paths.
- [x] **Embedding Generator Hardening**: Added visibility into stub model usage.

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
