# Longbow Storage Engine Hardening - Next Steps

This document tracks the remaining tasks for hardening the Longbow storage engine for production readiness.

## 0. P0 Blockers: Performance & Scaling (COMPLETED)
- [x] **Temporal Search Scaling**: Optimized `TemporalTree` with 64-byte cache-line alignment and contiguous memory layout.
- [x] **Learned Index Capacity**: Implemented `DiskBackedLearnedIndex` using `mmap` for scalable node management (1M+ vectors).
- [x] **TurboQuant Ingestion**: Implemented `MetadataRegistry` in `ArrowHNSW` to eliminate metadata lookup overhead.
- [x] **NUMA Affinity**: Implemented `PinThreadToCore` and updated `SharedWorkerPool` for granular worker pinning on Linux/Ancalagon.

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
