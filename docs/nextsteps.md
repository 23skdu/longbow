# Next Steps & Priorities

> [!IMPORTANT]
> **P0 Blockers: Fix All Pre-Existing Test Failures**
> Before any new feature work, all pre-existing test failures must be resolved to establish a reliable regression baseline. These failures span `internal/store` (12+ failures) and `cmd/longbow` (1 failure). Each has been root-caused with a clear fix path.

## P0 — Fix Existing Test Failures

### Group A: Store Workers Not Started (3 failures)
These tests create data via `StoreRecordBatch` but never call `StartIngestionWorkers` / `StartIndexingWorkers`, so data sits in queues forever.
- [ ] **A1 — `TestStore_EndToEnd_TDD`** (`store_e2e_test.go:109`): EOF from empty DoGet stream.
  - *Root cause:* No ingestion/indexing workers started; data never applied to dataset Records.
  - *Fix:* Add `store.StartIngestionWorkers(2)` and `store.StartIndexingWorkers(2)` after store creation.
- [ ] **A2 — `FuzzIngestionPipelineConcurrentWrites`** (`ingestion_fuzz_test.go:82`): expected 50 records, actual 0 ("Data loss detected").
  - *Root cause:* Same missing worker startup — data enqueued but never consumed.
  - *Fix:* Add `store.StartIngestionWorkers(4)` and `store.StartIndexingWorkers(4)` after store creation.
- [ ] **A3 — `FuzzIngestionIntegrityConcurrent`** (`ingestion_integrity_fuzz_test.go:100`): "Condition never satisfied" + WAL truncation errors.
  - *Root cause:* Missing ingestion workers + WAL file existence race in snapshot truncation.
  - *Fix:* Add worker startup; in `storage/engine.go` create WAL file before truncate if missing.

### Group B: Zero-Alloc Parser Missing `ef_search` Key (1 failure, 3 subtests)
The zero-alloc vector search parser silently ignores the `"ef_search"` JSON key, so validation never fires.
- [ ] **B1 — `TestVectorSearchAction_EfSearchValidation`** (`vector_search_action_test.go:133`): error says "dataset not found" instead of "ef_search must be between 16 and 4096".
  - *Root cause:* `ZeroAllocVectorSearchParser` has no `case "ef_search":` handler; field is zero, validation short-circuits.
  - *Fix:* Add `case "ef_search":` to `internal/query/zero_alloc_vector_search.go` to parse int64 and set `p.result.EfSearch`.

### Group C: Upsert Tombstoning Lazy Initialization (1 failure)
The upsert path doesn't initialize the Tombstones bitset map entry for a batch that previously had no tombstones.
- [ ] **C1 — `TestStore_Upsert`** (`upsert_test.go:92`): "Expected value not to be nil" — `ds.Tombstones[0]` is nil after upsert.
  - *Root cause:* Tombstone application code does not lazily create `Tombstones[batchIdx]` if it doesn't exist.
  - *Fix:* In the upsert tombstone logic, initialize `ds.Tombstones[batchIdx] = types.NewBitset()` before setting bits.

### Group D: Tiered Storage Key & Tier Comparison Bugs (2 failures)
- [ ] **D1 — `TestTieredStorage_OffloadAndFetch`** (`tiered_storage_test.go:48`): `remote.Exists` returns false.
  - *Root cause:* `OffloadBlock` remote key format (`"blocks/%s/%d"`) uses `dvs.path` which may differ from raw `path` argument (normalization or extension).
  - *Fix:* Export key format or add `RemoteKey()` method to `DiskVectorStore`; align test expectation.
- [ ] **D2 — `TestTieredStorage_EnforcePolicy`** (`tiered_storage_test.go:85`): expected 1 offload, actual 0.
  - *Root cause:* `time.Since(b.CreatedAt) > maxAge` with `maxAge=0` may be false at nanosecond granularity, or tier string constants mismatch between packages.
  - *Fix:* Ensure `CreatedAt` is set to a past timestamp; verify `StorageTier` string constants match across packages.

### Group E: Dataset Readiness & Seed Connectivity (3 failures)
- [ ] **E1 — `TestVectorStore_GetFlightInfo/Success`** (`store_query_coverage_test.go:86`): "dataset test-1 is being initialized".
  - *Root cause:* Dataset created manually bypasses ingestion pipeline; `IsReady` never set to true.
  - *Fix:* Add `ds.IsReady.Store(true)` after manual dataset creation.
- [ ] **E2 — `TestRecommend` (3 subtests)** (`recommend_test.go:433,447,462`): "no valid seeds found in dataset".
  - *Root cause:* Graph edges reference literal internal IDs (0-4) that don't match the computed VectorIDs from batch/row positions.
  - *Fix:* Align graph edge subject IDs with internal VectorIDs computed from `(BatchIdx, RowIdx)` positions.
- [ ] **E3 — `TestDataset_PerRecordEviction`** (`record_eviction_test.go:327,335`): `"could not be applied builtin len()"`.
  - *Root cause:* `assert.Len(t, ds.Records, N)` passes a `*LockFreeSlice` struct pointer, not a slice.
  - *Fix:* Use `assert.Len(t, ds.Records.Read(), N)` to access the underlying slice.

### Group F: Flaky / Race Tests (2 failures)
- [ ] **F1 — `TestVectorStore_DoAction_Extended/check_readiness`** (`store_actions_coverage_test.go:69`): Expected "READY" got "BUSY".
  - *Root cause:* Background goroutines (compaction worker, index adapter) enqueue jobs before test checks. Index queue has pending items.
  - *Fix:* Add polling loop to wait for index queue to drain before check_readiness, or disable background workers in test config.
- [ ] **F2 — `FuzzCompaction/seed#0`** (`fuzz_test.go`): "race detected during execution of test".
  - *Root cause:* Test overwrites `store.compactionWorker` without stopping original, creating two concurrent workers on shared state.
  - *Fix:* Stop original worker before replacing, or create store with compaction disabled from start.

### Group G: Memory Pressure in Integration Test (1 failure)
- [ ] **G1 — `TestDataServerDoPutDoGet`** (`cmd/longbow/main_test.go:326`): ListFlights rejected — "critical memory pressure (106.0% usage)".
  - *Root cause:* Test sets `LONGBOW_MAX_MEMORY=104857600` (100MB); process overhead pushes usage past hard limit.
  - *Fix:* Increase to `536870912` (512MB) or disable admission controller in test config.

---

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

