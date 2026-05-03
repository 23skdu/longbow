# Longbow Storage Engine Hardening - Next Steps

This document tracks the remaining tasks for hardening the Longbow storage engine for production readiness.

## 1. SIMD Kernel Expansion & Build Parity

- [x] Integrate `HaversineBatch` into SIMD dispatch.
- [x] Wire ARM64-specific kernels (`AccumulateWeightedScatter`, `BM25ScoreBatch`, `HaversineBatch`).
- [x] Implement remaining AVX-512 kernels for transcendental functions (`Exp`/`Log`) in assembly.
- [x] Implement NEON-specific kernels for `Exp`/`Log` for ARM64 parity.
- [x] Ensure runtime dispatch handles all architecture-specific fallbacks correctly.

## 2. Search Pipeline & Data Path Optimization

- [x] Implement zero-copy schema column index caching (`colIdxCache`).
- [x] Optimize vector retrieval to use pre-allocated buffers in `ArrowSearchContext`.
- [x] Implement budget-based early-termination logic for HNSW searches.
- [x] Research and implement bit-vector filters for sparse indices to reduce cache line misses.
- [x] Optimize `ArrowRefs` data path in `GraphData` for direct Arrow array access.
- [x] Optimize Quadtree depth and node-locking for high-density Geo Search.

## 3. Stability, Resilience & Backpressure

- [x] Implement gRPC/Flight level circuit breakers to prevent cascade failures.
- [x] Integrate admission control (backpressure) based on memory pressure and health signals.
- [ ] Develop a retry-with-backoff policy for distributed search failures.
- [ ] Implement client-side load balancing hints in Flight Info responses.

## 4. Performance Validation

- [x] Run quick validation benchmark suite for basic stability and performance verification.
- [ ] Execute the full 480-point benchmark suite (`local_bench.sh`) and analyze long-tail latencies.
- [ ] Conduct multi-node scalability tests for distributed search and ingestion.

## 5. Documentation & Maintenance

- [x] Consolidated and updated `docs/nextsteps.md`.
- [ ] Update `docs/performance.md` with latest benchmark results.
- [x] Finalize Go doc comments for all new SIMD and search pipeline components.

## 6. Performance & Stability Recommendations (2026-05-03)

Based on the 0.2.0-rc2 stabilization benchmarks:

1. **Temporal Search Scaling**: Temporal QPS has recovered from ~600 to ~7,500 (+1100%), but still trails behind Dense Search. The $O(\log N)$ tree traversal should be profiled for cache locality optimizations.
2. **Learned Index Capacity**: Learned index metadata overhead remains significant. For counts exceeding 1M, consider increasing `LONGBOW_MAX_MEMORY` beyond 18GB or implementing disk-backed learned index nodes.
3. **TurboQuant Ingestion Metadata**: Observed a minor (5-8%) ingestion throughput penalty when using TurboQuant metadata tags. Pre-caching these Arrow metadata fields in the `ArrowHNSW` schema could eliminate per-batch string lookups.
4. **NUMA Affinity (ancalagon)**: Remote AMD64 search performance shows higher jitter than local M3. Implementing thread-to-core affinity for search workers is recommended for many-core Linux environments.
