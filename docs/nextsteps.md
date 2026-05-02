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
- [ ] Optimize `ArrowRefs` data path in `GraphData` for direct Arrow array access.

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
- [ ] Finalize Go doc comments for all new SIMD and search pipeline components.
