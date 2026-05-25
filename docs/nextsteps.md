# Next Steps & Priorities

## Actionable Recommendations (Derived from Benchmarks)
- **Investigate QPS Regressions in `complex128`**: While the new atomic chunk pooling vastly improved ingestion throughput (+176%), it caused a slight 22% regression in `complex128` Dense QPS. This implies that the aggressive ingestion hotpath optimizations may be causing cache thrashing or branch misprediction during complex scalar vector search.
  - [ ] Profile CPU cache misses during `complex128` dense ingestion.
  - [ ] Analyze branch prediction metrics for the new atomic chunk pooling.
  - [ ] Isolate and benchmark the complex scalar vector search path.
  - [ ] Prototype alternative atomic pooling structures to minimize thrashing.
- **Implement Batched GPU Distance Computations**: The `float32` ingestion scale is fundamentally bottlenecked by L1/L2 cache latency (fetching 1,536 scattered bytes per neighbor). Shifting distance compute arrays to the GPU can alleviate memory-bandwidth ceilings for high-density indexing.
  - [ ] Design GPU memory layout for scattered byte fetching.
  - [ ] Implement CUDA/Metal kernels for distance computations.
  - [ ] Integrate GPU compute queue with the current ingestion pipeline.
  - [ ] Benchmark memory bandwidth utilization against CPU L1/L2 cache.
- **Tune `efSearch` Autonomously based on Data Type**: Increase the `efSearch` buffer heavily for lower-precision types (`int8`, `turboquant8`) to maintain recall, since they perform significantly faster with less memory-bound limitations compared to `float32` and `complex128`.
  - [x] Benchmark `efSearch` configurations across `int8` and `turboquant8`.
  - [x] Implement logic to adjust `efSearch` automatically at index creation based on type.
  - [ ] Validate recall retention with the adjusted `efSearch` parameters.
- **Mitigate Benchmark Timeout Cliffs**: Ensure benchmark scripts (`unified_benchmark.py`) gracefully checkpoint or dynamically adjust timeouts rather than hard-killing `bench-tool` (SIGKILL -9) after 30 minutes, which causes zombie process buildup and requires manual intervention.
  - [x] Implement graceful checkpointing in `unified_benchmark.py`.
  - [x] Add dynamic timeout adjustment logic based on dataset size and dimensionality.
  - [x] Ensure proper cleanup of `bench-tool` child processes to prevent zombie build-up.

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
  - [x] Implement and wire `DistanceComputer` interfaces (`ComputeBatch` and `Prefetch`) for all remaining types: `float[16,32,64]`, `int[8,16,32,64]`, `uint[8,16,32,64]`, `complex[64,128]`, `turboquant[2,4,8]`.
  - [x] Provide Generic Loop-Unrolled (4x) fallbacks for true SIMD batching across all dimensions (`128, 384, 768, 1024, 3072`).
  - [x] Wire dispatch tables strictly so that AVX2, AVX512, and NEON architectures natively accelerate batch queries when assembly implementations are fully mapped.
  - [x] Mitigate memory fragmentation by using zero-allocation pre-sized buffers (e.g., `ArrowSearchContext.batchVecsFloat32`).
- **Implement/integrate GPU index types for advanced hardware acceleration**:
  - [ ] Research and select target GPU index implementations (e.g., cuVS, Faiss GPU).
  - [ ] Define the abstraction layer for CPU-GPU index type switching.
  - [ ] Implement a basic working prototype with a subset of hardware.
- **Update `Makefile` and `Dockerfile` for `GOAMD64=v3`**:
  - [x] Modify `GOAMD64` environment variable settings in `Makefile`.
  - [x] Update Go build commands in `Dockerfile` to target `v3`.
  - [ ] Verify builds pass on CI environments with AVX2 support.
- **Benchmark Execution on `ancalagon` hardware profile**:
  - [ ] Provision and configure the `ancalagon` hardware environment.
  - [ ] Deploy the Longbow benchmark suite.
  - [ ] Execute the full matrix of data types and dimensionalities.
  - [ ] Gather, analyze, and publish the results compared to standard profiles.

