# Next Steps & Priorities

## Actionable Recommendations (Derived from Benchmarks)
- **Implement Batched GPU Distance Computations**: The `float32` ingestion scale is fundamentally bottlenecked by L1/L2 cache latency (fetching 1,536 scattered bytes per neighbor). Shifting distance compute arrays to the GPU can alleviate memory-bandwidth ceilings for high-density indexing.
- **Tune `efSearch` Autonomously based on Data Type**: Increase the `efSearch` buffer heavily for lower-precision types (`int8`, `turboquant8`) to maintain recall, since they perform significantly faster with less memory-bound limitations compared to `float32` and `complex128`.
- **Mitigate Benchmark Timeout Cliffs**: Ensure benchmark scripts (`unified_benchmark.py`) gracefully checkpoint or dynamically adjust timeouts rather than hard-killing `bench-tool` (SIGKILL -9) after 30 minutes, which causes zombie process buildup and requires manual intervention.

> [!IMPORTANT]
> **P0 Blockers: Test Suite Optimization & Context Window Refactoring**
> We must address the test execution time (especially race detection timeouts) and file sizes to ensure maintainability and agent context limits.

## 1. Test Suite Optimization
- ~~**Parallelization vs. Serial Tests**: Identify CPU-bound index tests and prevent them from running in parallel with `t.Parallel()` during race detection, which causes excessive context switching and timeouts.~~ (Completed)
- ~~**Test Consolidation**: Combine frivolous or overly granular tests (e.g., small individual getter/setter tests) into single table-driven tests to reduce overhead.~~ (Completed)
- ~~**Mocking & Isolation**: Mock `mesh.Gossip` and heavy network/RPC components in `store` tests instead of spinning up full simulated clusters for basic unit tests.~~ (Completed)
- ~~**Timeout Adjustments**: Increase timeout flags for `go test -race` specifically on heavy packages (e.g. `internal/store/index`), but prioritize optimizing the code first.~~ (Completed)

## 2. Refactoring for Context Windows
- ~~**`navigation.go`**: Split into `navigation_search.go` (vector searching logic), `navigation_parallel.go` (parallel search host logic), and `navigation_properties.go` (getters/warmup).~~ (Completed)
- ~~**`arrow_hnsw.go`**: Extract insertion and graph mutation logic into `arrow_hnsw_insert.go` and `arrow_hnsw_delete.go`.~~ (Completed)
- ~~**`store.go`**: Move lifecycle methods (`Start`, `Stop`) to `store_lifecycle.go` and configuration to `store_config.go`.~~ (Completed)

---

## Other Ongoing Tasks
- **Exhaustive SIMD Batching Support**:
  - Implement and wire `DistanceComputer` interfaces (`ComputeBatch` and `Prefetch`) for all remaining types: `float[16,32,64]`, `int[8,16,32,64]`, `uint[8,16,32,64]`, `complex[64,128]`, `turboquant[2,4,8]`.
  - Provide Generic Loop-Unrolled (4x) fallbacks for true SIMD batching across all dimensions (`128, 384, 768, 1024, 3072`).
  - Wire dispatch tables strictly so that AVX2, AVX512, and NEON architectures natively accelerate batch queries when assembly implementations are fully mapped.
  - Mitigate memory fragmentation by using zero-allocation pre-sized buffers (e.g., `ArrowSearchContext.batchVecsFloat32`).
- Implement/integrate GPU index types for advanced hardware acceleration.
- Update `Makefile` and `Dockerfile` for `GOAMD64=v3`.
- Benchmark Execution on `ancalagon` hardware profile.

