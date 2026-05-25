# Next Steps & Priorities

> [!IMPORTANT]
> **Post-Benchmark Commit d1cc8e38 — Actionable Performance Recommendations**
> The following items are organized and prioritized based on findings in [performance.md](file:///Users/rsd/REPOS/longbow/docs/performance.md). 
> Key findings show that **Quantized Types** (turboquant8/int8) offer massive 30–40× throughput advantages and 10× memory savings over float32, while **GPU Mode** currently causes a 5–10× wall-clock slowdown due to data-transfer overhead.

---

## P0 — Fix Existing Test Failures

Fixing existing unit and integration failures is the absolute top priority to establish a stable and reliable baseline for all performance and scale optimizations.

### Group A: Store Workers Not Started (3 failures)
These tests create data via `StoreRecordBatch` but never call `StartIngestionWorkers` / `StartIndexingWorkers`, so data sits in queues forever.
- [x] **A1 — `TestStore_EndToEnd_TDD`** (`store_e2e_test.go:109`): Workers already started. PASSES.
- [x] **A2 — `FuzzIngestionPipelineConcurrentWrites`** (`ingestion_fuzz_test.go:82`): Workers already started. PASSES.
- [x] **A3 — `FuzzIngestionIntegrityConcurrent`** (`ingestion_integrity_fuzz_test.go:100`): Workers already started. PASSES.

### Group B: Zero-Alloc Parser Missing `ef_search` Key (1 failure, 3 subtests)
The zero-alloc vector search parser silently ignores the `"ef_search"` JSON key, so validation never fires.
- [x] **B1 — `TestVectorSearchAction_EfSearchValidation`** (`vector_search_action_test.go:133`): `ef_search` case already present in zero-alloc parser. PASSES.

### Group C: Upsert Tombstoning Lazy Initialization (1 failure)
The upsert path doesn't initialize the Tombstones bitset map entry for a batch that previously had no tombstones.
- [x] **C1 — `TestStore_Upsert`** (`upsert_test.go:92`): Lazy Tombstone init already present in `UpdatePrimaryIndex`. PASSES.

### Group D: Tiered Storage Key & Tier Comparison Bugs (2 failures)
- [x] **D1 — `TestTieredStorage_OffloadAndFetch`** (`tiered_storage_test.go:48`): PASSES.
- [x] **D2 — `TestTieredStorage_EnforcePolicy`** (`tiered_storage_test.go:85`): PASSES.

### Group E: Dataset Readiness & Seed Connectivity (3 failures)
- [x] **E1 — `TestVectorStore_GetFlightInfo/Success`** (`store_query_coverage_test.go:86`): `IsReady.Store(true)` already in test. PASSES.
- [x] **E2 — `TestRecommend` (3 subtests)** (`recommend_test.go:433,447,462`): Graph edges use correct VectorIDs via `applyReplayBatch`. PASSES.
- [x] **E3 — `TestDataset_PerRecordEviction`** (`record_eviction_test.go:327,335`): `ds.Records.Read()` already used in assertion. PASSES.

### Group F: Flaky / Race Tests (2 failures)
- [x] **F1 — `TestVectorStore_DoAction_Extended/check_readiness`** (`store_actions_coverage_test.go:69`): PASSES.
- [x] **F2 — `FuzzCompaction/seed#0`** (`fuzz_test.go`): PASSES.

### Group G: Memory Pressure in Integration Test (1 failure)
- [x] **G1 — `TestDataServerDoPutDoGet`** (`cmd/longbow/main_test.go:326`): `LONGBOW_MAX_MEMORY` already set to 1GB in test. PASSES.

---

## P0 — Fix Indexing Bottleneck to Reach 500K+ Vectors

The system currently hits `ResourceExhausted` at ~425K vectors under the 18 GB memory cap, and HNSW graph construction shows high O(N²) single-threaded contention.

- **Parallelize HNSW index construction**: The single-threaded indexer creates a 25K-job backlog at ≥75K vectors. Spawn 2–4 indexer workers and partition the HNSW graph by shard to avoid O(N²) contention.
  - [x] Design a thread-safe partitioning or sharding scheme for the HNSW graph to minimize node insertion locks.
  - [x] Implement a concurrent worker pool (2–4 goroutines) to consume indexing tasks from the global queue.
  - [x] Introduce a striped or fine-grained locking mechanism for graph node updates instead of a global index lock.
  - [ ] Profile indexing throughput and lock wait-time using `pprof` block/mutex profiles under heavy ingest load.

> **What was implemented** (`internal/store/index/sharded_hnsw.go`):
> - Per-shard `sync.Mutex` (`shardLocks`) in `ShardedHNSW` — each shard processes one batch at a time, while N different shards run in parallel across M index workers. This eliminates the single `bulkMu` contention point that caused the 25K backlog.
> - Auto-sharding threshold lowered from 10K → **256** vectors (`DefaultAutoShardingConfig`) so the sharded index activates much earlier, minimizing the single-ArrowHNSW phase.
> - Two-level striped locking: (1) per-shard mutex at the ShardedHNSW level + (2) existing `insertMus [131072]` per-vector spinlocks inside each ArrowHNSW shard.
- **Increase memory cap or add disk-backed index**: With 18 GB the system hits `ResourceExhausted` at ~425K. Bumping `LONGBOW_MAX_MEMORY` to 32 GB (if available) or enabling `LONGBOW_USE_DISK=1` would allow 500K–1M vectors.
  - [ ] Profile memory allocations using `pprof` heap snapshots at 350K+ vectors to pinpoint largest overhead contributors.
  - [ ] Implement or stabilize `LONGBOW_USE_DISK=1` to dump cold HNSW vectors/nodes to disk (using memory-mapped files or a key-value store for block storage).
  - [ ] Validate system stability and error handling when the maximum memory cap is reached.
- **Adopt PQ compression during ingest**: `LONGBOW_PQ_INGEST=1` stores vectors as product-quantized codes, reducing per-vector memory ~10×. This should extend the ceiling to ~4M vectors at the same memory cost.
  - [ ] Design and implement the Product Quantization (PQ) training phase during initial ingest or using a pre-trained codebook.
  - [ ] Integrate PQ encoding/decoding directly into the ingestion pipeline, ensuring direct streaming of quantized codes.
  - [ ] Update the distance computers to support symmetric/asymmetric distance computations on PQ codes.
  - [ ] Evaluate recall metrics under PQ vs. raw float32 on dimensions 128 and 384.

---

## P1 — Quantized Types for Production (Elevated Priority)

> [!TIP]
> **Performance Rationale:**
> Benchmarks show that `turboquant8` achieves **9,491 vec/s** (384-dim) vs **226 vec/s** for float32 — a **42× speedup**. It also uses **10× less memory**, making it the primary path to surpass the 500K vector limit under low memory footprints. `int8` is also highly competitive.

- **Promote turboquant8 to recommended default**: turboquant8 delivers 42× higher ingest throughput and 3× lower search latency than float32 at 384-dim/150K. Set the production default to turboquant8 with float32 fallback for exact-search workloads.
  - [ ] Modify default configuration to initialize datasets with `turboquant8` precision when unspecified.
  - [ ] Implement a fallback search coordinator path that dynamically routes exact-search queries to a `float32` fallback index.
- **Int8 is competitive**: int8 matches turboquant8 for ingest (4,879 vs 9,491 vec/s at 384/150K) and provides higher dense-search QPS than float32 at all scales.
  - [ ] Ensure full API and engine parity for `int8` across all ingestion and search paths.
  - [ ] Benchmark recall of `int8` vs `turboquant8` vs `float32` on standard datasets (SIFT/GIST) to document quality trade-offs.
- **Add auto-quantization path**: Automatically quantize float32 vectors to turboquant8 at ingest time when memory pressure exceeds 70%.
  - [ ] Implement a real-time memory monitor within the ingestion coordinator.
  - [ ] Implement a runtime vector conversion handler to convert incoming `float32` batches to `turboquant8` representation when memory utilization surpasses 70%.
  - [ ] Gracefully transition active index building to the quantized format without dropping or corrupting in-flight vectors.

---

## P2 — GPU Acceleration Strategy (Demoted Priority)

> [!WARNING]
> **Performance Rationale:**
> GPU mode (Metal/CUDA) is currently **5–10× slower** wall-clock than CPU mode at ≤150K scale (45 min vs 5 min) due to intense data-transfer overhead and blocking kernel launch latency. It should be disabled by default and restricted to heavy distance-only workloads.

- **Disable GPU by default**: `LONGBOW_GPU_ENABLED=true` adds data-transfer overhead with no throughput benefit at ≤150K vectors. The flag should default to `false` and only be toggled for workloads exceeding ~500K vectors.
  - [ ] Change the default value of `LONGBOW_GPU_ENABLED` to `false` in configuration loading logic.
  - [ ] Update CLI, documentation, and error/log outputs to recommend enabling GPU acceleration only for datasets >500K.
- **Investigate async GPU transfer**: Current GPU integration blocks on every kernel launch. Using a CUDA stream pool and double-buffered host→device transfers would hide latency.
  - [ ] Refactor the GPU backend to utilize asynchronous execution streams (CUDA streams / Metal command buffers).
  - [ ] Implement double-buffered host-to-device (H2D) and device-to-host (D2H) queues to overlap memory copy operations with computation.
  - [ ] Verify latency reduction by running micro-benchmarks with a profiler (e.g., nvprof, Metal System Profiler).
- **Offload HNSW distance computation only, not graph construction**: HNSW neighbor selection dominates CPU time; the graph traversal itself is not GPU-accelerated. Focus GPU effort on the distance kernel alone.
  - [ ] Profile and decouple distance computation logic from the CPU graph-traversal loop.
  - [ ] Design a batched distance interface to execute distance kernels on the GPU in batches of queries/neighbors rather than single vectors.
  - [ ] Build fallback heuristics to run distance computation on the CPU when the batch size is too small to justify kernel launch overhead.
- **Implement Batched GPU Distance Computations (all datatypes, Metal arm64 + CUDA amd64)**: Ingestion scale is fundamentally bottlenecked by L1/L2 cache latency (fetching 1,536 scattered bytes per neighbor). Shifting distance compute arrays to the GPU can alleviate memory-bandwidth ceilings for high-density indexing.
  - [ ] Design a GPU memory layout optimized for coalesced or batched scattered byte fetching.
  - [ ] Implement custom CUDA/Metal kernels for batched distance computations (L2, Cosine).
  - [ ] Integrate GPU compute queue with the current ingestion pipeline to batch distance queries.
  - [ ] Benchmark memory bandwidth utilization against CPU L1/L2 cache under multi-threaded search loads.

---

## P3 — Benchmark Infrastructure Improvements

- **Add early-abort for resource-exhausted tests**: The script retries indefinitely when `ResourceExhausted` is hit, wasting hours. Add a `--max-retries` flag (default 1) and skip to the next test.
  - [ ] Add a `--max-retries` command line argument to `scripts/unified_benchmark.py`.
  - [ ] Implement logic in the test execution loop to detect `ResourceExhausted` status codes and skip subsequent steps for the current test configuration.
- **Reduce test matrix for CI**: The full 72-test matrix takes ≥4 hours. Add `--ci` mode (already partially implemented) to run only float32/int8 at two key counts.
  - [ ] Finalize the `--ci` argument parsing and define the minimal test matrix (e.g., only `float32` and `int8` at 10K and 50K vector counts).
  - [ ] Integrate the `--ci` benchmark run into the GitHub Actions/CI configuration.
- **Save pprof profiles before cleanup**: `pprof` URL collection happens asynchronously and profiles are deleted when the server shuts down. Snapshot profiles at the completion of each test, not during the run.
  - [ ] Update benchmark runner scripts to explicitly fetch and snapshot `/debug/pprof` endpoints upon test completion prior to invoking server shutdown commands.
  - [ ] Save output profiles with timestamped and structured filenames matching the test metadata.

---

## Actionable Recommendations (Derived from Benchmarks)

- **Investigate QPS Regressions in `complex128`**: While the new atomic chunk pooling vastly improved ingestion throughput (+176%), it caused a slight 22% regression in `complex128` Dense QPS. This implies that the aggressive ingestion hotpath optimizations may be causing cache thrashing or branch misprediction during complex scalar vector search.
  - [ ] Profile CPU cache misses during `complex128` dense vector search using `pprof` / `perf`.
  - [ ] Compare trace metrics of atomic chunk pooling under `float32` vs `complex128`.
  - [ ] Audit memory alignment of `complex128` pooled chunks to ensure they don't cross cache lines.
  - [ ] Prototype alternative atomic pooling structures to minimize thrashing.

- **Tune `efSearch` Autonomously based on Data Type**: Increase the `efSearch` buffer heavily for lower-precision types (`int8`, `turboquant8`) to maintain recall, since they perform significantly faster with less memory-bound limitations compared to `float32` and `complex128`.
  - [x] Benchmark `efSearch` configurations across `int8` and `turboquant8`.
  - [x] Implement logic to adjust `efSearch` automatically at index creation based on type.
  - [ ] Validate recall retention with the adjusted `efSearch` parameters under high-load search scenarios.
- **Mitigate Benchmark Timeout Cliffs**: Ensure benchmark scripts (`unified_benchmark.py`) gracefully checkpoint or dynamically adjust timeouts rather than hard-killing `bench-tool` (SIGKILL -9) after 30 minutes, which causes zombie process buildup and requires manual intervention.
  - [x] Implement graceful checkpointing in `unified_benchmark.py`.
  - [x] Add dynamic timeout adjustment logic based on dataset size and dimensionality.
  - [x] Ensure proper cleanup of `bench-tool` child processes to prevent zombie build-up.

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
  - [ ] Verify correctness and recall against the baseline CPU HNSW implementation.
- **Update `Makefile` and `Dockerfile` for `GOAMD64=v3`**:
  - [x] Modify `GOAMD64` environment variable settings in `Makefile`.
  - [x] Update Go build commands in `Dockerfile` to target `v3`.
  - [ ] Verify builds pass on CI environments with AVX2 support.
- **Benchmark Execution on `ancalagon` hardware profile**:
  - [ ] Provision and configure the `ancalagon` hardware environment.
  - [ ] Deploy the Longbow benchmark suite.
  - [ ] Execute the full matrix of data types and dimensionalities.
  - [ ] Gather, analyze, and publish the results compared to standard profiles.
