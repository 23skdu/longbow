# Longbow Roadmap

**Last updated**: 2026-10-16  
**Status**: Active roadmap focusing on remaining performance, algorithmic scaling, and observability milestones. All completed historical items (Items 4, 5, 14–17, 28, 29, 31, 33, 34) have been verified and removed to maintain execution clarity.

---

## 1. Top 5 Immediate Priorities

| Pri | # | Item | Area | Impact / Rationale |
|:---|:---|:---|:---|:---|
| **P1** | **32** | **Baseline Re-cut & Index-Time Regression CI Gate** | Performance / CI | Blocks safe PR merges. Implementation exists (`scripts/check_index_regression.py`); requires re-cutting baseline JSONs with deterministic seeds (post-Item 31) and wiring into `.github/workflows/benchmark-ab.yml`. |
| **P1** | **1** | **End-to-End GPU Search Dispatch & Complex128 Bottleneck** | GPU Engine | Item 34 revealed that server search currently bypasses GPU kernels entirely (`CUDAIndex.SearchBatchDistances` returns "not implemented"). Wire GPU search dispatch and resolve complex128 scaling bottleneck. |
| **P1** | **6/27** | **AVX-512 VNNI (`VPDPBUSD`) & Saturated Int8/Uint8 Dispatch** | SIMD Kernels | In `dispatch.go`, int8/uint8 dot product and uint8 Euclidean distance fall back to scalar 4x unrolled loops. Wire native 64-byte VNNI kernels for 4x–8x throughput on Cascade Lake / Sapphire Rapids. |
| **P1** | **7** | **Direct Base Pointer Caching in SearchContext for SlabArena** | Memory / Hotpath | Profile shows `GetWithGeneration` accounts for 14.08% flat CPU time due to per-hop integer divisions and atomic pointer reloads. Cache raw slab base pointers during reader-pinned searches. |
| **P1** | **25/26** | **TurboQuant 4-Lane Polar Reconstruction & Cross-Hop Caching** | Quantization | Scalar polar reconstruction accounts for 89.2% of TQ distance evaluation time. Interleave reconstruction across 4 AVX2 lanes and memoize reconstructed float32 vectors in scratch arena to cut P99 latency by 20%. |

```mermaid
graph TD
    subgraph P1["Top 5 Priorities"]
        I32["32: Re-cut Baselines & Wire Index-Time CI Gate"]
        I1["1: GPU Search Dispatch & complex128 Bottleneck"]
        I6["6/27: AVX-512 VNNI & Saturated Kernels in Dispatch"]
        I7["7: Base Pointer Caching in SearchContext"]
        I25["25/26: TurboQuant 4-Lane Vectorization & Hop Cache"]
    end
    subgraph P2["Algorithmic & Storage Optimizations"]
        I9["9: Inlined 4-Ary Candidate Heap"]
        I8["8: Lock-Free Adjacency & NUMA Worker Queues"]
        I12["12: Lock-Free Map for Metadata & IDs"]
        I13["13: SIMD Fused BM25 Posting Intersection"]
        I10["10: Iterative Quadtree Stack Traversal"]
        I3["3: Pooled Disk Row Buffers"]
        I2["2: ARM NEON Dequant Assembly"]
        I11["11: Hardware-Aware Dynamic EMLGo Thresholds"]
        I30["30: EMLGo 0.7 15-DType Accuracy Gate"]
    end
    subgraph P3["Observability & CI Conformance"]
        I18["18: Fix Dead Alert Rules on Missing Metrics"]
        I19["19/24: Metric-Existence & Docs CI Gate"]
        I21["21: Alert Coverage for Critical Failure Counters"]
        I20["20: Promtool Test Coverage across 28 Groups"]
        I22["22: Alertmanager Routing & Runbook Links"]
        I23["23: Declarative Compose/Helm Provisioning"]
    end

    I32 --> I1
    I6 --> I25
    I7 --> I9
    I18 --> I19
    I19 --> I21
    I20 --> I22
```

---

## 2. P1 — Core Performance & Hardware Acceleration

### Item 32: Per-Dtype Index-Time Regression Guard at 250k and 500k
- **Status**: **PARTIAL** (Evaluation script complete; baseline re-cut and CI workflow wiring pending)
- **Files**: `scripts/check_index_regression.py`, `.github/workflows/benchmark-ab.yml`, `benchmarks/baseline_*.json`
- **State**: The comparison script `scripts/check_index_regression.py` exists with lockable exit codes for paired index time and search QPS. However, Item 31 replaced static `--seed 42` with derived seeds per configuration (`LONGBOW_BENCH_SEED`), making historical baseline JSON files incompatible with fresh benchmark runs.
- **Plan**:
  1. Re-generate canonical baseline files (`benchmarks/baseline_*.json`) across 25k, 100k, and 250k scales using the new deterministic seed derivation.
  2. Integrate `scripts/check_index_regression.py` into `.github/workflows/benchmark-ab.yml` to enforce a ≤5% index-time regression gate on scheduled interleaved A/B runs.
- **Success Criteria**: Zero false-positive alerts on self-noise tests; automated CI cancellation on true index-time regressions ≥5%.

### Item 1: End-to-End GPU Search Dispatch & Complex128 Bottleneck
- **Status**: **OPEN**
- **Files**: `internal/store/index/navigation_search.go`, `internal/store/index/cuda_index.go`, `internal/gpu/cuda/cuda_index_search.go`
- **State**: Item 34 profiling confirmed that the dataset index in production is a CPU HNSW instance; `navigation_search.go` only invokes GPU kernels if attached as `*ArrowHNSW`. Crucially, `CUDAIndex.SearchBatchDistances` currently returns `errors.New("not implemented")`. Additionally, complex128 distance evaluation exhibits poor GPU kernel occupancy compared to float32.
- **Plan**:
  1. Complete `CUDAIndex.SearchBatchDistances` to enable batch distance offloading from the search beam.
  2. Implement native complex128 batched Euclidean distance CUDA kernel matching the float32 kernel memory throughput.
  3. Wire GPU index initialization directly into store dataset startup when `LONGBOW_GPU_ENABLED=1`.
- **Success Criteria**: 250k GPU search benchmarks record active kernel compute via `gpu_kernel_profile`; complex128 GPU search QPS exceeds CPU AVX-512 baseline by ≥2.5x.

### Item 6 & 27: Wire AVX-512 VNNI (`VPDPBUSD`) and Saturated Int8/Uint8 Kernels
- **Status**: **OPEN**
- **Files**: `internal/simd/dispatch.go`, `internal/simd/all_kernels_avo_amd64.s`, `internal/simd/dot_product_amd64.go`
- **State**: In `dispatch.go` lines 623–625 under AVX-512 detection, `dotProductInt8Impl`, `dotProductUint8Impl`, and `euclideanDistanceUint8Impl` fall back to scalar 4x unrolled loops (`dotInt8Unrolled4x`, `dotUint8Unrolled4x`, `euclideanUint8Unrolled4x`), leaving modern server CPUs (Cascade Lake, Sapphire Rapids) underutilized despite available VNNI instructions.
- **Plan**: Expose and bind native 64-byte `VPDPBUSD` dot product and saturated AVX-512 int8/uint8 kernels into the primary dispatch table when `features.HasAVX512VNNI` is set.
- **Success Criteria**: Vectorized 64-byte throughput on AVX-512 hosts; quantized dot-product and angular search throughput increases by 4x–8x with exact bit-identity against generic reference implementations.

### Item 7: Direct Base Pointer Caching in SearchContext for SlabArena and TypedArena
- **Status**: **OPEN**
- **Files**: `internal/memory/arena.go`, `internal/store/types/graph_data_accessors.go`, `internal/store/index/arrow_search_context.go`
- **State**: CPU profiling (`profile.pprof`) shows `(*SlabArena).GetWithGeneration` accounts for 11.08% flat CPU time and `(*TypedArena).GetWithGeneration` accounts for 3.00% flat CPU time. Every vector lookup in graph traversal performs division (`offset / slabCap`), slice bounds checks, and atomic pointer reads.
- **Plan**: Cache raw chunk base pointers directly on `ArrowSearchContext`. During search traversal under pinned reader generation, calculate candidate vector addresses via direct pointer arithmetic without per-hop slab index division.
- **Success Criteria**: Eliminate 14% flat CPU time spent in slab dereferencing; search latency across all dtypes drops by 10%–18%.

### Item 25 & 26: TurboQuant 4-Lane Polar Reconstruction & Cross-Hop Caching
- **Status**: **OPEN**
- **Files**: `internal/store/index/turboquant_distance*.go`, `internal/tensor/quantization/turboquant.go`, `internal/store/index/arrow_hnsw_search.go`
- **State**: Profiling demonstrates that 89.2% of distance evaluation time during TurboQuant search is scalar polar reconstruction and QJL sign correction. Furthermore, visited nodes are repeatedly reconstructed across beam search hops, inflating P99 search latency.
- **Plan**:
  1. **Item 25**: Implement 4-lane AVX2 vectorized polar reconstruction with independent accumulator registers, amortizing radius scaling per chunk.
  2. **Item 26**: Add an ephemeral, per-search scratch arena in `ArrowSearchContext` to memoize reconstructed float32 vectors across graph hops.
- **Success Criteria**: TurboQuant dense search QPS increases from ~300 to ≥600 at 100k; total polar reconstructions per query drop from ≥1.8x to ≤1.1x at `efSearch=500`; P99 latency decreases by ≥20%.

---

## 3. P2 — Algorithmic & Storage Optimizations

### Item 9: Inlined 4-Ary Candidate Heap & Branchless Struct Swapping
- **Status**: **OPEN**
- **Files**: `internal/store/index/candidate_heap.go`, `internal/store/index/arrow_search_context.go`
- **State**: Candidate heap maintenance (`down`, `Less`, `Swap`) accounts for ~15.2% combined flat time during beam search. Value-receiver calls incur slice copies and boundary checks.
- **Plan**: Implement an inlined 4-ary candidate heap with branchless struct swapping and cache-aligned candidate entries directly in `ArrowSearchContext`.
- **Success Criteria**: Priority queue operation overhead eliminated; graph search layer traversal accelerates by 8%–12%.

### Item 8: Lock-Free Adjacency Updates & NUMA-Local Index Worker Queueing
- **Status**: **OPEN**
- **Files**: `internal/store/index/flat_adjacency.go`, `internal/store/index/arrow_hnsw_bulk.go`, `internal/store/vector_store.go`
- **State**: Lock profiling (`mutex.pprof`) reveals 69.10% flat lock delay in `sync.(*Mutex).Unlock`, with `StartIndexingWorkers` contributing 53.8% cumulative lock delay during concurrent batch indexing.
- **Plan**: Migrate adjacency list mutations to generation-stamped lock-free atomic compare-and-swap (CAS), and shard incoming ingestion batches into NUMA-local worker queues.
- **Success Criteria**: Mutex contention during parallel ingest drops below 5%; index construction scales linearly across 16+ cores.

### Item 12: Lock-Free Concurrent Map for Metadata & ID Translation
- **Status**: **OPEN**
- **Files**: `internal/store/types/map_rcu.go`
- **State**: Copy-on-Write cloning in `MapRCU` triggers >1,200 MB of heap allocations during streaming ingestion batches due to repeated `make(map[K]V)` allocations.
- **Plan**: Replace `MapRCU` with a chunked Trie or lock-free concurrent hash map for VectorID ↔ Document ID mappings and string metadata.
- **Success Criteria**: Zero map reallocations on batch writes; significant reduction in GC pause durations during streaming ingestion.

### Item 13: SIMD Fused BM25 Term Scoring & Posting List Intersection
- **Status**: **OPEN**
- **Files**: `internal/store/index/bm25_inverted_index.go`, `internal/simd/dispatch.go`
- **State**: BM25 multi-term queries evaluate posting lists sequentially, looking up document lengths and term frequencies via Go map lookups.
- **Plan**: Implement vectorized posting list intersection using AVX-512 `VFMADD` instructions, scoring 16 candidate documents simultaneously from contiguous document length arrays.
- **Success Criteria**: 3x–5x throughput improvement in sparse and hybrid BM25 ranking.

### Item 10: Iterative Stack Traversal for Geospatial Quadtree Queries
- **Status**: **OPEN**
- **Files**: `internal/store/geo_index.go`
- **State**: `(*Quadtree).queryBoxRecursive` and `SearchRadius` allocate temporary slices at each recursive quadrant subdivision (~1,985 MB cumulative allocations).
- **Plan**: Replace recursive quadtree bounding box queries with an iterative traversal using a fixed-depth stack and reusable pre-allocated ID buffer in `SearchContext`.
- **Success Criteria**: Zero heap allocations during geospatial bounding box and radius queries; geo search QPS increases by 25%–40%.

### Item 3: Pooled Row Buffers in `DiskVectorStore`
- **Status**: **OPEN**
- **Files**: `internal/store/index/disk_vector_store.go`, `internal/store/index/buffer_pool.go`
- **State**: Non-vectorized disk scans allocate slice headers and temporary row buffers on every row extraction.
- **Plan**: Pool row buffers to eliminate transient allocations during disk traversals.
- **Success Criteria**: Zero-allocation row decoding in `DiskVectorStore`.

### Item 2: ARM NEON Dequant Kernel & Remaining Scalar SQ8 Fallbacks
- **Status**: **OPEN**
- **Files**: `internal/simd/dequant_other.go`, `internal/store/index/distance_dispatch.go`
- **State**: AVX2+FMA fused dequant-L2 kernels exist for AMD64, while ARM64 relies on scalar fallbacks.
- **Plan**: Implement native ARM NEON assembly for fused dequantization and L2 distance calculation.
- **Success Criteria**: ARM64 parity tests pass with vectorized performance.

### Item 11: Dynamic Hardware-Aware EMLGo Math Dispatch Threshold Calibration
- **Status**: **OPEN**
- **Files**: `internal/tensor/math_dispatch_env.go`
- **State**: `ResolveBackend` uses hardcoded static vector cutoffs regardless of microarchitecture capabilities.
- **Plan**: Calibrate dispatch cutoffs dynamically based on detected CPU instruction set extensions (`HasAVX512`, `HasAVX512VNNI`, `HasAMX`) and L2/L3 cache sizes.
- **Success Criteria**: Optimal math backend selection across both client and server architectures.

### Item 30: EMLGo 0.7 Accuracy Gating Across All 15 DTypes
- **Status**: **OPEN**
- **Files**: `internal/mathutil/mathutil_emlgo.go`, `internal/simd/*_test.go`
- **State**: EMLGo 0.7 introduces updated vector kernels whose numerical precision needs comprehensive verification.
- **Plan**: Execute the full `simd_accuracy` suite across all 15 dtypes, validating strict precision bounds.
- **Success Criteria**: 100% test pass on SIMD accuracy suites with documented precision guarantees.

---

## 4. P3 — Observability, Alerting & CI Conformance

### Item 18: Repair Alert Rules Bound to Missing Metrics
- **Status**: **OPEN**
- **Files**: `grafana/rules.yml`, `internal/metrics/`
- **State**: 10 of 72 alerts in `grafana/rules.yml` query metric names that are not exported by the Go binary (e.g., `longbow_hybrid_search_enabled`, `longbow_gpu_enabled`, `longbow_storage_iouring_available`, `longbow_index_creation_failures_total`). These alerts evaluate to empty series and will never fire during outages.
- **Plan**: Register missing metrics in the Go runtime where critical failure modes exist, or correct typos in `rules.yml` where registered names diverge.
- **Success Criteria**: Every expression in `grafana/rules.yml` resolves to an active, registered Prometheus metric family.

### Item 19 & 24: Metric-Existence and Docs-Code Consistency Conformance Gates in CI
- **Status**: **OPEN**
- **Files**: `internal/metrics/conformance_test.go`, `.github/workflows/ci.yml`, `docs/metrics.md`
- **State**: No automated test verifies that metric references in alerts, dashboards, and documentation match registered symbols in Go source code.
- **Plan**: Implement a Go test and CI check that parses `rules.yml`, `dashboards/*.json`, and `docs/metrics.md`, asserting 100% bidirectional consistency with `internal/metrics/`.
- **Success Criteria**: CI immediately fails if an alert, dashboard, or document references an unregistered metric or if documentation drifts from code.

### Item 21: Alert Coverage for Critical Failure Counters
- **Status**: **OPEN**
- **Files**: `grafana/rules.yml`
- **State**: Over 35 exported error counters (e.g., `longbow_wal_flush_errors_total`, `longbow_checkpoint_timeouts_total`, `longbow_replication_queue_dropped_total`, `longbow_index_jobs_dropped_total`) currently have no associated alert rules.
- **Plan**: Add Prometheus alert rules for all unmonitored durability, replication, and data-loss failure counters.
- **Success Criteria**: All exported failure counters are either actively alerted or explicitly documented in `rules.yml` as intentionally unwatched.

### Item 20: Extend Promtool Unit-Test Coverage Beyond 4 of 28 Rule Groups
- **Status**: **OPEN**
- **Files**: `grafana/tests/*_alerts_test.yml`, `grafana/tests/test-rules.sh`
- **State**: Only 4 rule groups (`bulk_insert`, `temporal`, `gpu`, `hybrid_search`) currently have unit test fixtures in `grafana/tests/`. The remaining 24 groups have only basic syntax validation.
- **Plan**: Author unit test scenarios (`*_alerts_test.yml`) for the remaining rule groups covering firing and recovery states.
- **Success Criteria**: 100% of rule groups validated via automated `promtool test rules`.

### Item 22 & 23: Alertmanager Configuration, Runbook Links & Provisioning
- **Status**: **OPEN**
- **Files**: `grafana/alertmanager.yml`, `docker-compose.yml`, `helm/longbow/`, `grafana/provisioning/`
- **State**: No default Alertmanager configuration is provided; alerts lack runbook links; Grafana dashboards require manual import.
- **Plan**: Provide production-ready `alertmanager.yml` with severity-based routing; add runbook URLs to all alerts; provide declarative provisioning YAML for Docker Compose and Helm deployments.
- **Success Criteria**: `docker compose up` brings up Longbow and pre-provisioned Grafana with all active dashboards and alerts loaded.

---

## 5. Verified & Removed Completed Items (Changelog)

| # | Item | Verified Completion Date | Key Commit | Verification Evidence |
|:---|:---|:---|:---|:---|
| **4** | BM25 Inverted Index COW Elimination | 2026-10-15 | `5514faf9` | `BM25InvertedIndex.AddBatch` and `BM25ArenaIndex` eliminate per-doc map copying; >12 GB heap churn removed during ingest. |
| **5** | Native L2-Squared Integer SIMD Kernels | 2026-10-15 | `cb6c2712` | Native AVX-512 `l2SquaredInt8AVX512Kernel` implemented; eliminates redundant terminal `VSQRTSS` and `d * d` squaring. |
| **14** | Ingestion Zero-Copy & Timer Elimination | 2026-10-11 | `1b3927e4` | Geometric slice growth in `LockFreeSlice.Append`; wake channels replace polling timers. |
| **15** | Eliminate Synchronous BM25 Double-Indexing | 2026-10-11 | `ca764e14` | Removed redundant indexing pass in index worker; bounded background text queue added. |
| **16** | Bulk Linkage Candidate Buffer Pooling | 2026-10-11 | `9d8d60af` | Pooled search context buffers in `AddConnectionsBatch`; pre-resolved Arrow offsets. |
| **17** | TurboQuant Indexing Scalability & Dequant Caching | 2026-10-11 | `cf88f68e` | Capped `TQScratchArena` residency at 64 chunks; fixed geometric chunk table growth. |
| **28** | uint8 Neighbour Selection Parity on All Corpora | 2026-10-15 | `01c46e2f` | `VectorTypeUint8` diversity branch added; `TestUint8GraphReachabilityAcrossDistributions` passes ≥99.9% across 5 distributions. |
| **29** | uint8 Connection-Degree Invariant under Bulk | 2026-10-16 | `23ca00bf` | `selectNeighbors` made single entry point for all paths; `TestBulkInsert_Uint8MeanDegreeFloor` verifies 16.00 mean degree. |
| **31** | Deterministic Seed Plumbing & Ingest Repeatability | 2026-10-16 | `0ac9fb45` | `LONGBOW_BENCH_SEED` plumbing; 3-pass linker; fixed RNG seeding and level generation; bit-identical search digests. |
| **33** | Graph-Linker Heap Churn Reduction | 2026-10-16 | `9d8d60af` | Heap allocations cut 76% (4,289 MB → 1,015 MB); resident memory reduced by 34.5% on 100k ingest. |
| **34** | GPU Kernel Profiling via CUDA Events | 2026-10-16 | `66c3a8f9` | CUDA event timing per kernel; `gpu_kernel_profile` DoAction; 100% device time attribution verified on 250k run. |
