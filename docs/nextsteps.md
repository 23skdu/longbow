# Longbow Performance & Stability Roadmap

This document outlines the high-priority tasks and architectural milestones remaining for the Longbow storage engine.

---

## 1. SIMD Kernel Expansion
Accelerate specialized search modes by moving remaining generic fallbacks to optimized assembly.

- **Implement GraphRAG Acceleration**
    - [ ] Write NEON assembly kernel for `accumulateWeightedScatterNEON`.
    - [ ] Port scatter-add kernels to AVX-512 for Linux AMD64 performance parity.
- **Transcendental Function Optimization**
    - [ ] Implement SIMD-optimized `Exp` and `Log` kernels for probability-based scoring.
    - [ ] Integrate assembly-based Haversine distance for Geo-spatial search scaling.

## 2. Search Pipeline Optimizations
Refine the hot path to reduce per-query overhead and metadata lookups.

- **Zero-Copy Metadata Management**
    - [ ] Optimize schema mapping lookups in `ArrowHNSW` to eliminate map access per query.
    - [ ] Implement pre-calculated field offsets for faster record batch attribute extraction.
- **Search Execution Efficiency**
    - [ ] Implement pre-calculated distance bounds for early HNSW search termination.
    - [ ] Evaluate bit-vector filters for sparse indices to reduce cache line misses.

## 3. Stability & Resilience
Enhance system reliability under extreme saturation and resource contention.

- **Fault Tolerance**
    - [ ] Implement gRPC/Flight level circuit breakers to prevent cascade failures.
    - [ ] Add configurable timeouts and deadlines for cross-node distributed searches.
- **Load Management**
    - [ ] Add explicit backpressure to the `IngestionRingBuffer` when WAL persistence lags.
    - [ ] Implement dynamic worker pool resizing based on CPU/Memory pressure metrics.

## 4. Operational Maturity
Automate performance validation and regression detection.

- **Continuous Benchmarking**
    - [ ] Finalize and validate results for the full 480-point performance matrix.
    - [ ] Create an automated dashboard for comparing pprof profiles between releases.
- **Memory Observability**
    - [ ] Add granular metrics for `SearchAttemptBuffers` pool utilization.
    - [ ] Implement slab-arena fragmentation monitoring.

## 5. Multi-Architecture Evolution
Prepare Longbow for next-generation hardware and distributed topologies.

- **Hardware Offloading**
    - [ ] Evaluate GPU compute offloading for large-scale IVF index clusters.
    - [ ] Benchmark Apple Silicon Neural Engine (ANE) for low-power embedding generation.
- **Distributed Scale**
    - [ ] Design cross-region replication protocols for high-availability deployments.
    - [ ] Optimize RDMA/RoCEv2 zero-copy ingest for multi-node clusters.
