# Longbow Storage Engine Hardening - Next Steps

This document tracks the remaining tasks for hardening the Longbow storage engine for production readiness.

## 0. P0 Blockers: Performance Optimization & Infrastructure

- [ ] **Metal Initialization & AOT Shaders**
  - [ ] Refactor `internal/gpu/metal` to persist `MTLDevice` and `MTLCommandQueue` across index lifecycles.
  - [ ] Implement `ShaderCache` for persistent `MTLFunction` and `MTLPipelineState` management.
  - [ ] Move runtime shader compilation to AOT `.metallib` using a new `scripts/compile_metal.sh` build step.
  - [ ] **Testing**: Unit tests for `ShaderCache` hits; Fuzz testing `ComputeKernel` with variable dimensions.
- [ ] **Benchmarking Infrastructure (Large Scale)**
  - [ ] Modify `unified_benchmark.py` to support ingestion from `.fbin` and Arrow IPC binary files.
  - [ ] Implement `--generate-only` mode to decouple data generation from performance measurement.
  - [ ] Dynamically scale gRPC `MaxCallMsgSize` and `MaxRecvMsgSize` for workloads >100k vectors.
  - [ ] **Testing**: Fuzz test binary readers for malformed headers; Integration test for 500k+ vector ingestion.
- [ ] **TurboQuant ARM64 NEON Kernels**
  - [ ] Implement NEON-optimized search kernels for TurboQuant2/4/8 in `internal/gpu/metal` and `internal/simd`.
  - [ ] Apply 4x/8x unrolling to bit-unpacking and dot-product accumulation paths.
  - [ ] **Testing**: Cross-validation against scalar Go implementation; Fuzz testing high-dimensional bit-packed queries.
- [ ] **NUMA Affinity & Jitter Mitigation**
  - [ ] Automated `pprof` analysis in `scripts/analyze_jitters.py` to detect voluntary context switches in worker pools.
  - [ ] Tighten `internal/memory` affinity masks to enforce strict core-pinning on multi-socket systems.
  - [ ] **Testing**: Stress test under 100% CPU load to verify thread-to-core stability; `sched_getaffinity` unit tests.
- [ ] **Temporal Adaptive Rate-Limiting**
  - [ ] Implement `LearnedIndexRateLimiter` to protect search hot-paths during background model training.
  - [ ] Integrate latency-aware feedback loops to reduce training frequency if p99 latency spikes.
  - [ ] **Testing**: Fuzz test rate-limiter with ingestion bursts; Integration test 300k+ vectors with active training.

## 1. P0 Blockers: Performance & Scaling (COMPLETED)

- [x] **Temporal Search Scaling**: Optimized `TemporalTree` with 64-byte cache-line alignment and contiguous memory layout.
- [x] **Learned Index Capacity**: Implemented `DiskBackedLearnedIndex` using `mmap` for scalable node management (1M+ vectors).
- [x] **TurboQuant Ingestion**: Implemented `MetadataRegistry` in `ArrowHNSW` to eliminate metadata lookup overhead.
- [x] **NUMA Affinity**: Implemented `PinThreadToCore` and updated `SharedWorkerPool` for granular worker pinning on Linux/Ancalagon.

## 2. Stabilization & Reliability (COMPLETED)

- [x] Implement `RetryPolicy` with Exponential Backoff and jitter in `pkg/retry`.
- [x] Integrate `pkg/retry` into `DistributedSearch` and ingestion dispatch paths.
- [x] Add `LoadHints` serialization/deserialization in `pkg/loadbalancing`.
- [x] Instrument `GetFlightInfo` and `ListFlights` with real-time load balancing hints.
- [x] Resolve "dataset not found" race condition via atomic `IsReady` synchronization.
- [x] Achieve 100% test coverage for `pkg/retry` and `pkg/loadbalancing`.

## 3. Performance Validation & Benchmarking

- [ ] Execute the full 480-point benchmark suite (`local_bench.sh`) and analyze long-tail latencies.
- [ ] Conduct multi-node scalability tests for distributed search and ingestion.
- [ ] Collect pprof profiling data on `ancalagon` for NUMA jitter analysis.

## 4. Documentation & Maintenance

- [ ] Update `docs/performance.md` with latest benchmark results from the 0.2.0-rc2 release.
- [ ] Update `docs/architecture.md` to reflect the new `pkg/retry` and `pkg/loadbalancing` protocols.
