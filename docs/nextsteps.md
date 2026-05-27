# Next Steps & Priorities

> [!IMPORTANT]
> **Post-Benchmark Commit d1cc8e38 — Actionable Performance & Optimization Plan**
> This document outlines the validated status of all next steps and priorities for the Longbow vector search engine. It consolidates completed achievements and prioritizes the remaining optimization roadmap to safely cross the **500K to 1M+ vector scale** under tight memory and resource caps.

---

## 🔥 Critical Regression Recommendations

Derived from `docs/performance.md` (commit `d1cc8e38`). Each item is tied to a measured regression and maps to a concrete code-level action.

---

### R1 — Memory Wall: 500K/1M Scale Is Unreachable (Blocker)

**Regression**: Both hosts hit `ResourceExhausted` at ~425K vectors under an 18 GB cap. HNSW graph data grows O(N) in RAM with no eviction path.

| Action | File / Component | Expected Impact |
|---|---|---|
| Replace in-RAM adjacency lists with mmap-backed off-heap arena; use `internal/store/types/graph_data.go` `RelocateToOffHeap` for all graph layers, not just migration | `internal/store/types/graph_data.go`, `hnsw_autoshard.go` | Breaks O(N) RAM curve; allows 2–4× higher vector counts under same cap |
| Activate tiered storage eviction for cold HNSW layers (lower layers are rarely traversed at search time) | `internal/store/tiered/`, `internal/store/index/` | Reduces hot RSS by ~40% at 250K+ vectors |
| Pre-quantize float32 vectors to turboquant8 at ingest when RSS > 60% of cap (currently threshold is 70%); lower threshold prevents hitting the wall before eviction engages | `internal/store/ingestion.go` or equivalent | Defers exhaustion by ~30% on float32 workloads |

---

### R2 — O(N²) HNSW Ingestion Collapse at High Dimension (Severe)

**Regression**: float32 dim=384, count=150K → **226 vec/s** ingestion — a 99.95% collapse from the 459K vec/s baseline. dim=128 also degrades from 838→780 vec/s at 100K→150K.

| Action | File / Component | Expected Impact |
|---|---|---|
| Reduce default `M` (max neighbors per node) from current value to 12–16 for dim > 256; expose as config knob `LONGBOW_HNSW_M` | `internal/store/index/hnsw.go`, `hnsw_sharded.go` | Sub-quadratic neighbor-search cost; 3–5× ingestion speedup at dim=384 |
| Implement IVF (Inverted File Index) coarse quantizer as pre-filter before HNSW entry-point selection, capping candidate set for each insert | `internal/store/index/` (new file `ivf.go`) | Reduces O(N) scan in `searchLayer` during construction |
| Make `efConstruction` adaptive: start at 64 for the first 10K vectors, linearly reduce to 16 as N grows past 100K | `internal/store/index/hnsw.go` | ~50% construction time reduction at 150K+ without recall loss |
| Profile `searchLayer` hot path via `go tool pprof` with `--alloc_space` during a 384-dim, 100K insert to confirm bottleneck before implementing above | `scripts/` or `Makefile` | Validates root cause; avoids premature optimization |

---

### R3 — Indexing Backlog Causes Ingestion Stalls (High)

**Regression**: ≥25K pending index jobs observed at counts ≥75K. Ingestion pipeline outruns indexer, blocking new mutations and causing retry loops.

| Action | File / Component | Expected Impact |
|---|---|---|
| Add a back-pressure signal: block `Upsert` callers when the pending job queue exceeds a configurable threshold (e.g. 5K jobs), returning a retryable `Unavailable` instead of silently queueing | `internal/store/workers.go` or ingestion path | Eliminates silent backlog accumulation; makes pressure visible to callers |
| Implement bulk-insert path: batch N vectors into a single HNSW construction call rather than N individual graph updates, amortizing lock acquisition and entry-point lookups | `internal/store/index/hnsw_sharded.go` | 2–4× indexing throughput for bulk ingest workloads |
| Expose `LONGBOW_INDEXING_WORKER_COUNT` (separate from ingestion workers) and default it to `runtime.NumCPU() / 2` | `cmd/longbow/main.go`, `internal/store/workers.go` | Allows hosts like `ancalagon` (many cores) to use more parallelism without manual tuning |

---

### R4 — GPU Acceleration Provides No Benefit at Current Scales (High)

**Regression**: Metal/CUDA modes are 5–10× slower wall-clock than CPU for 150K-count tests (45 min vs 5 min). GPU QPS is within noise of CPU QPS in all measured configurations.

| Action | File / Component | Expected Impact |
|---|---|---|
| **Immediately**: flip `GPUEnabled` default to `false`; log a `WARN` if user explicitly enables GPU for datasets < 500K vectors | `cmd/longbow/main.go` | Removes silent 5–10× slowdown for all default deployments |
| Gate GPU execution on batch query size ≥ 256 (current heuristic threshold is 64 — too low); validate threshold empirically with Metal/CUDA kernel profiling | `internal/store/index/hnsw_gpu.go` | Ensures CPU fallback for all real-world single-query and low-batch paths |
| Profile Metal command-buffer submission latency separately from compute time using `MTLCaptureManager`; if submission > 1 ms per query, move to persistent command buffers | `internal/gpu/metal/metal_gpu_optimized.go` | Determines whether double-buffering alone is sufficient or kernel dispatch needs restructuring |

---

### R5 — Dense Search QPS Degrades 50% from 10K→150K Vectors (Medium)

**Regression**: CPU M3 float32 dense search: 1,316 QPS at 75K → 1,153 QPS at 150K (dim=128). Ancalagon drops from 778→655 QPS. Root cause: HNSW `efSearch` fixed while graph grows.

| Action | File / Component | Expected Impact |
|---|---|---|
| Validate PID-controller `efSearch` tuning is active and converging in production runs; add a `pprof` label `efSearch=<value>` so profiler traces show which value was live | `internal/store/index/hnsw.go`, PID tuner | Confirms whether auto-tuning is engaged or bypassed |
| Log efSearch value and resulting recall estimate per-query at `DEBUG` level; surface P95 recall estimate in benchmark JSON output | `scripts/unified_benchmark.py`, server logging | Makes recall/QPS tradeoff observable without a separate recall benchmark |
| Set `efSearch` lower bound dynamically: `max(efSearch_pid, ceil(log2(N) * 4))` to track graph growth | `internal/store/index/hnsw.go` | Keeps QPS stable as N grows instead of degrading |

---

### R6 — Sparse Search Stable; Use It as a Reference Baseline (Low / Informational)

**Finding**: Sparse search holds ~11–12K QPS on M3 and ~7–8K QPS on ancalagon across all tested counts. The inverted index is not affected by HNSW graph growth.

| Action | File / Component | Expected Impact |
|---|---|---|
| Add a `--mode sparse-only` fast-path to `unified_benchmark.py` for regression detection: sparse QPS should remain constant; any deviation signals a regression in the inverted-index path | `scripts/unified_benchmark.py` | Free canary metric requiring no new infrastructure |
| Document the sparse QPS floor as the stability SLO in `docs/performance.md` | `docs/performance.md` | Establishes a measurable contract for CI regression detection |

---


## 🚀 Completed Milestones & Achievements

The following items from the previous next steps roadmap have been successfully implemented and validated in the codebase:

### 1. Fix Existing Test Failures (100% Completed)
- [x] **Store Workers Initialization**: Resolved ingestion and indexing worker startup issues in end-to-end tests (`TestStore_EndToEnd_TDD`, fuzzer pipelines), eliminating data ingestion stalls.
- [x] **Zero-Alloc Parser Validation**: Fixed parsing of `"ef_search"` JSON keys in vector search actions.
- [x] **Upsert Tombstone Initialization**: Added lazy tombstone bitset initialization in `UpdatePrimaryIndex`.
- [x] **Tiered Storage Integrity**: Fixed key and tier comparison bugs in offload policies and fetch pipelines (`TestTieredStorage_OffloadAndFetch`, `TestTieredStorage_EnforcePolicy`).
- [x] **Dataset Readiness & Eviction**: Added explicit readiness toggles and proper record reading/assertion hooks in eviction and query tests.
- [x] **Flaky & Race Tests**: Cleared compaction fuzzing and actions check-readiness race conditions.

### 2. High-Density Ingestion & Indexing Optimizations
- [x] **Parallel HNSW Graph Construction**: Replaced the global `bulkMu` lock with per-shard mutexes (`shardLocks`) and striped locking inside `ShardedHNSW`. Enabled a concurrent workers pool to process tasks across shards, eliminating single-threaded lock contention.
- [x] **Adaptive/Auto-Sharding Thresholds**: Reduced the auto-sharding threshold from 10K to **256** vectors, activating the parallelized indexer earlier to prevent large single-threaded workloads.
- [x] **Memory Pressure Validation**: Validated the system's ability to gracefully return `ResourceExhausted` under a hard memory limit of 18 GB rather than crashing (no OOM kill).
- [x] **Product Quantization (PQ) Compression Pipeline**:
  - [x] Implemented the PQ and OPQ training phases (`TrainPQ` using `pq.OPQEncoder`).
  - [x] Integrated PQ encoding/decoding directly into the ingestion flow (encoding float32 batches on-the-fly via `AddPQ`).
  - [x] Designed and implemented asymmetric distance computers (ADC) supporting SIMD-accelerated batch distance calculations on PQ codes.
- [x] **Autonomous efSearch Tuning**: Designed and wired a dynamic PID controller (`PIDTuner`) to auto-adjust `efSearch` parameters to target a specific recall proxy under variable load, optimizing accuracy vs performance.
- [x] **Build Optimizations**: Updated `Makefile`, `Dockerfile.cpu`, and `Dockerfile.nvidia` to compile using `GOAMD64=v3` for modern x86_64 AVX2/AVX-512 optimizations.

---

## 🎯 Prioritized Remaining Steps

To reach 1M+ vectors under tight memory constraints and fully unlock high-throughput performance, the remaining tasks are organized by priority:

### P0 — Production Quantization & Defaults (High Impact)
Set quantized types as the default path for production scale to exploit their 30-40× throughput and 10× memory advantages over float32.

1. **Promote `turboquant8` to the Default Workload Precision**
   - [x] Modify config processing in `cmd/longbow/main.go` and `internal/store` to initialize new datasets/namespaces using `turboquant8` precision when unspecified, falling back to `float32` only when exact matching is explicitly requested.
   - [x] Implement a fallback search coordinator path that dynamically routes exact-search queries to a `float32` index.
2. **Implement Auto-Quantization Path**
   - [x] Implement a runtime vector conversion handler to convert incoming `float32` batches to `turboquant8` representation when memory utilization exceeds 70%.
   - [x] Ensure that transition of active index building to the quantized format happens gracefully without dropping or corrupting in-flight vectors.
3. **Change the Default value of `LONGBOW_GPU_ENABLED` to `false`**
   - [x] Change the default of `GPUEnabled` from `true` to `false` in `cmd/longbow/main.go` configuration loading logic.
   - [x] Update CLI help text, error output, and logs to recommend enabling GPU acceleration only for datasets exceeding 500K vectors.

### P1 — GPU Acceleration Strategy (Refinement & Optimization)
Address the data-transfer and kernel-launch overhead currently bottlenecking GPU mode.

1. **Investigate Async GPU Transfers**
   - [ ] Refactor the GPU backend to utilize asynchronous execution streams (CUDA streams / Metal command buffers).
   - [ ] Implement double-buffered host-to-device (H2D) and device-to-host (D2H) queues to overlap memory copy operations with computation.
2. **Offload HNSW Distance Computation Only, Not Graph Construction**
   - [ ] Profile and decouple distance computation logic from the CPU graph-traversal loop.
   - [ ] Design a batched distance interface to execute distance kernels on the GPU in batches of queries/neighbors rather than single vectors.
   - [ ] Build fallback heuristics to run distance computation on the CPU when the batch size is too small to justify kernel launch overhead.
3. **Implement Batched GPU Distance Computations**
   - [ ] Design a GPU memory layout optimized for coalesced or batched scattered byte fetching.
   - [ ] Implement custom CUDA/Metal kernels for batched distance computations (L2, Cosine).

### P2 — Benchmark & CI Infrastructure Improvements
Improve benchmark robustness, speed up CI cycles, and protect profiling artifacts.

1. **Add Early-Abort for Resource-Exhausted Tests**
   - [ ] Add a `--max-retries` flag (default 1) to `scripts/unified_benchmark.py`.
   - [ ] Detect gRPC `ResourceExhausted` status codes (code 8) during benchmark runs and skip subsequent steps for the current test configuration rather than retrying indefinitely.
2. **Save pprof Profiles Prior to Server Shutdown**
   - [ ] Update the benchmark runner script to fetch and snapshot `/debug/pprof` endpoints upon test completion before sending shutdown commands to the server.
   - [ ] Save output profiles with timestamped and structured filenames matching the test metadata.
3. **Reduce CI Test Matrix**
   - [ ] Finalize the `--ci` argument parsing and define a minimal test matrix (e.g., only `float32` and `int8` at 10K and 50K vector counts).
   - [ ] Integrate the `--ci` benchmark run into the GitHub Actions/CI configuration.

### P3 — Diagnostics & Recall Research
Refine edge cases, analyze regressions, and conduct hardware-specific validations.

1. **Investigate QPS Regressions in `complex128`**
   - [ ] Profile CPU cache misses during `complex128` dense vector search using `pprof` / `perf`.
   - [ ] Compare trace metrics of atomic chunk pooling under `float32` vs `complex128`.
   - [ ] Audit memory alignment of `complex128` pooled chunks to ensure they don't cross cache lines.
   - [ ] Prototype alternative atomic pooling structures to minimize thrashing.
2. **Validate Recall Retention with Adjusted `efSearch`**
   - [ ] Validate recall retention under high-load search scenarios using the PID-controller adjusted `efSearch` parameters for low-precision types (`int8`, `turboquant8`).
3. **Implement/Integrate GPU Index Types**
   - [ ] Research and select target GPU index implementations (e.g., cuVS, Faiss GPU).
   - [ ] Define the abstraction layer for CPU-GPU index type switching.
4. **Benchmark Execution on `ancalagon` Hardware Profile**
   - [ ] Deploy the unified benchmark suite on `ancalagon` hardware profile, execute the full matrix of data types, and publish the results compared to standard profiles.
