# Longbow Storage Engine Hardening - Next Steps

This document tracks the remaining tasks for hardening the Longbow storage engine for production readiness.

## 0. P0 Blockers: Performance & Scaling (2026-05-03)

### [ ] Temporal Search Scaling: Cache Locality Optimizations

- **Objective**: Recover QPS gap between Temporal and Dense Search via cache-aligned tree traversals.
- **Subtasks**:
  - [ ] Profile `TemporalTree` traversal using `pprof` (CPU/Cache misses).
  - [ ] Align tree nodes to cache lines (e.g., 64-byte padding/struct alignment).
  - [ ] Implement block-based layouts or BFS traversal for better prefetching.
- **Testing**:
  - [ ] **Unit**: Validate search correctness across varying tree depths and densities.
  - [ ] **Fuzz**: Fuzz temporal ranges to ensure performance stability for skewed distributions.

### [ ] Learned Index Capacity: Disk-Backed Node Management

- **Objective**: Scale Learned Index to 1M+ vectors without OOM by implementing disk spill-over.
- **Subtasks**:
  - [ ] Audit node metadata overhead; implement `DiskBackedLearnedIndex` using `mmap`.
  - [ ] Implement LRU eviction for in-memory learned index nodes.
- **Testing**:
  - [ ] **Unit**: Compare accuracy/performance parity between in-memory and disk-backed paths.
  - [ ] **Fuzz**: Stress-test memory pressure and eviction logic under heavy ingestion load.

### [ ] TurboQuant Ingestion: Metadata Pre-caching

- **Objective**: Eliminate the 5-8% ingestion penalty by caching Arrow metadata field lookups.
- **Subtasks**:
  - [ ] Implement a `MetadataRegistry` in `ArrowHNSW` for pre-cached field lookups.
  - [ ] Optimize `extractMetadata` to bypass per-batch string lookups.
- **Testing**:
  - [ ] **Unit**: Benchmark ingestion speed with and without metadata registry.
  - [ ] **Fuzz**: Fuzz metadata strings to ensure no registry collisions or leaks.

### [ ] NUMA Affinity: Thread-to-Core Pinning (Linux/ancalagon)

- **Objective**: Reduce search jitter on many-core systems by pinning workers to physical cores.
- **Subtasks**:
  - [ ] Implement `PinThreadToCore` using `runtime.LockOSThread` and `sched_setaffinity`.
  - [ ] Update `SharedWorkerPool` to support NUMA-aware worker assignment.
- **Testing**:
  - [ ] **Unit**: Verify thread pinning via `/proc/self/status` or `cpuid` on Linux.
  - [ ] **Fuzz**: N/A; verify pool stability during high-concurrency churn.

## 1. Stabilization & Reliability (COMPLETED)

- [x] Implement `RetryPolicy` with Exponential Backoff and jitter in `pkg/retry`.
- [x] Integrate `pkg/retry` into `DistributedSearch` and ingestion dispatch paths.
- [x] Add `LoadHints` serialization/deserialization in `pkg/loadbalancing`.
- [x] Instrument `GetFlightInfo` and `ListFlights` with real-time load balancing hints.
- [x] Resolve "dataset not found" race condition via atomic `IsReady` synchronization.
- [x] Achieve 100% test coverage for `pkg/retry` and `pkg/loadbalancing`.

## 1. Performance Validation & Benchmarking

- [ ] Execute the full 480-point benchmark suite (`local_bench.sh`) and analyze long-tail latencies.
- [ ] Conduct multi-node scalability tests for distributed search and ingestion.
- [ ] Collect pprof profiling data on `ancalagon` for NUMA jitter analysis.

## 2. Documentation & Maintenance

- [ ] Update `docs/performance.md` with latest benchmark results from the 0.2.0-rc2 release.
- [ ] Update `docs/architecture.md` to reflect the new `pkg/retry` and `pkg/loadbalancing` protocols.
