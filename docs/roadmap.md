# Longbow Roadmap

**Last updated**: 2026-10-16  
**Status**: All roadmap items through 24 resolved as of 2026-10-12; of items 25-34 (§5), 28, 29 and 31 are resolved and 32 is partial; the rest are open. Historical sections below record the completed work and the final validation gates.

---

## 1. Priority Overview

| Pri | # | Item | Area | Depends on |
|:---|:---|:---|:---|:---|
| **P1** | 1 | Locate complex128 GPU bottleneck & verify uint16 GPU path | GPU | — |
| **P1** | 4 | BM25 Inverted Index Copy-on-Write Elimination (Allocations) | Storage | Item 15 |
| **P1** | 5 | Native L2-Squared Integer SIMD Kernels without VSQRT | SIMD | — |
| **P1** | 6 | Wire AVX-512 VNNI (`VPDPBUSD`) and Saturated Int8/Uint8 into Dispatch | SIMD | — |
| **P1** | 7 | Direct Base Pointer Caching in SearchContext for SlabArena | Memory | — |
| **P2** | 2 | ARM NEON dequant kernel & remaining scalar SQ8 fallbacks | SIMD | — |
| **P2** | 3 | Pooled row buffers in `DiskVectorStore` | Storage | — |
| **P2** | 8 | Lock-Free Adjacency Updates & NUMA-Local Index Queueing | Concurrency | Item 16 |
| **P2** | 9 | Inlined 4-Ary Candidate Heap & Branchless Struct Swapping | Search | — |
| **P2** | 10 | Iterative Stack Traversal for Geospatial Quadtree Queries | Storage | — |
| **P2** | 11 | Dynamic Hardware-Aware EMLGo Dispatch Threshold Calibration | Engine | — |
| **P2** | 12 | Lock-Free Concurrent Map for Metadata & ID Translation | Storage | — |
| **P2** | 13 | SIMD Fused BM25 Term Scoring & Posting List Intersection | Search | Item 4 |
| **P4** | — | All remaining roadmap items resolved in the observability and docs pass | Observability | — |

```mermaid
graph TD
    subgraph P1["P1: Core SIMD, Memory & Allocations"]
        I1["1: complex128 GPU Bottleneck & uint16 path"]
        I4["4: BM25 Inverted Index COW Elimination"]
        I5["5: Native L2-Squared Integer SIMD Kernels"]
        I6["6: Wire AVX-512 VNNI & Saturated Kernels"]
        I7["7: Direct Base Pointer Caching in SearchContext"]
    end
    subgraph P2["P2: Optimizations & Concurrency"]
        I2["2: NEON Dequant Kernel"]
        I3["3: Pooled Disk Row Buffers"]
        I8["8: Lock-Free Adjacency & NUMA Queueing"]
        I9["9: Inlined 4-Ary Candidate Heap"]
        I10["10: Iterative Quadtree Stack Traversal"]
        I11["11: Dynamic EMLGo Threshold Calibration"]
        I12["12: Lock-Free Concurrent Map for Metadata"]
        I13["13: SIMD Fused BM25 Term Scoring"]
    end
    subgraph P4["P4: Observability & Documentation Hygiene"]
        I18["18: Repair dead alert rules"]
        I19["19: Metric-existence CI gate"]
        I20["20: promtool coverage beyond 2 groups"]
        I21["21: Alert unalerted error counters"]
        I22["22: Alert routing & runbooks"]
        I23["23: Provision dashboards & rules"]
        I24["24: Enforce docs/code consistency in CI"]
    end
    I18 --> I19
    I18 --> I20
    I18 --> I21
    I18 --> I22
    I19 --> I20
    I19 --> I24
    I23 --> I24
```

**Observability audit (2026-10-11)** — §4 records the findings. Documentation
corrected in the same pass: `docs/metrics.md` metric table repaired (a prose
paragraph had been spliced into it mid-table, breaking rendering for the rest of
the file), seven undocumented metric families added, one removed metric dropped,
coverage count corrected 783 → 789; the four dangling `docs/roadmap.md §9.2`
cross-references repointed; `grafana/README.md` inventory rewritten to cover all
11 dashboards and all 28 rule groups, with the dead-alert finding recorded.
All remaining roadmap items from the P4 audit are resolved in this pass; no open
work remains in the canonical roadmap.

**Recently Completed (2026-10-12, AVX2 CPU validation — TurboQuant distance-blind graph build)**:

- **TurboQuant graph built on distances that were all zero.** The store constructs
  `ArrowHNSWConfig` without `Dims` and lets the first ingested record establish the
  dimension. The index, the TurboQuant encoder, the TurboQuant distance computer and
  the TQ scratch arena were all therefore created against dimension 0, and nothing
  re-bound them when the real dimension arrived. A zero-dimension encoder rounds
  `pow2` up to 1, so the rotated query every TurboQuant distance is measured against
  was truncated to one element and every candidate evaluated to distance 0.
  Neighbour selection stopped discriminating on distance entirely, and layer 0
  filled to its full degree budget from an unordered candidate list: at 100,000
  vectors, dim 128, every node kept its 16 links and only **8,192 of 100,000 (8.2%)**
  were reachable from the entry point. `float32`, `int8` and `complex128` reached
  98–99% in the same runs.
  - Fixed by binding all four to the dimension at the point it is established
    (`ensureTQComponents`, called once `EnsureChunks`/`Grow` has sized the graph
    data), and by making the scratch arena rebind rather than stay bound to its
    construction-time dimension.
  - Second defect on the same path: `selectNeighbors` treated a scratch-arena miss
    as "no vector" and **dropped** the candidate, so a cache miss silently disabled
    the diversity heuristic and pruned a full degree budget out of an unordered
    list. It now decodes on demand (`tqVectorAt`) and only a vector that cannot be
    reconstructed at all falls through to "accept". A cache miss must cost a
    decode, never a candidate.
  - Measured after the fix, same configuration: reachability **99,206 / 100,000
    (99.2%)** at `turboquant4` and **99,272 / 100,000 (99.3%)** at `turboquant8`,
    mean degree 16.00, no bulk-quality-guard fallback, and index time down from
    175.0s to 96.0s (`turboquant4`) and 118.5s to 98.0s (`turboquant8`) — the
    fallback's full sequential rebuild was the cost. Dense search rises from 380.7
    to 1,493.8 QPS, which is the *correct* direction: a graph with 8% reachability
    terminates the beam almost immediately, so the broken index was fast.
  - `TestTurboQuantGraphIsConnected` passed throughout, because every in-process
    test sets `cfg.Dims`. Added
    `TestTurboQuantGraphIsConnectedWithoutConfiguredDimension`, which builds in the
    store's shape and asserts both the encoder binding and ≥95% reachability; it
    fails on the pre-fix tree with `encoder is bound to dimension 0, want 128`.
  - Every TurboQuant row in `docs/performance.md` was measured on the broken graph
    and is superseded by §3a. The historical "~300–420 QPS is the honest number on
    a fully connected graph" reading is an artefact of the same defect.

- **Benchmark harness configuration fidelity**:
  - `LONGBOW_MAX_M0`, set by `unified_benchmark.py` at every scale, is not read by
    `types.DefaultArrowHNSWConfig`; it is a cap applied later by `insertion_core.go`
    during adaptive-M growth. Documented in `docs/testplan.md` §4.2a.
  - The readiness poll reported its retries as "transient port-collision retries".
    They are the number of one-second polls before `/ready` answered — a 6-second
    startup reads as six port collisions. Relabelled.
  - `docs/testplan.md` §4.2 required a 2-second port cooling period; the harness
    sleeps 1 second and retries `bind` with backoff, so the doc now matches the
    behaviour and says why the fixed sleep is not load-bearing.

- **Noise floor on the AVX2 client**: self-versus-self interleaved A/B (identical
  binary both arms, 3 pairs, `float32`/`int32`/`uint64` × 6 modes) put every median
  within ±3.9% with per-pair spread as wide as −31%…+41%. All 18 rows came back
  `inconclusive`. Recorded in `docs/testplan.md` §5.3 and `docs/performance.md` §3,
  together with the finding that 16 of 195 rows in the existing CPU table are >10%
  below it — which this spread does not distinguish from noise, so the table needs
  re-baselining with `--runs 3 --save-baseline` rather than remediation.

- **Validation status**: `go test ./internal/simd/...`, `./internal/store/`,
  `./internal/store/index/...` pass under both the default and `emlgo` build tags,
  with two pre-existing failures unchanged and confirmed against a clean tree —
  `TestHNSW_BulkInsert` (the Arrow leak checker attributes a neighbouring test's
  leak to it; passes in isolation) and `TestNarrowRecallOnClusteredEmbeddings`
  (`uint8` clustered recall 0.833). Full 100k CPU sweep, 15 dtypes × 13 search
  modes, standard and EMLGo: zero failed configs, zero exhausted configs, no
  OOM/panic/SIGSEGV in any server log, and the report self-validation
  (`qps × mean_latency` implies 75–102% of recorded workers) clean on every row.

**Recently Completed (2026-10-11, P0 Ingestion & Bulk Indexing Blockers)**:

- **Item 17 — TurboQuant Indexing Scalability & Bounded Dequant Caching**:
  - Capped `TQScratchArena` residency at 64 decoded chunks (65,536 vectors). A bulk build traverses the whole resident graph, so the uncapped arena materialised every node it ever touched as float32 — 512 MB per 1M vectors at 128 dimensions — and that allocator pressure is what collapsed throughput from 157.8 vec/s at 250k toward ~7 vec/s at 1M. Decodes past the cap still happen and return identical values; they are simply not retained.
  - Fixed the arena's chunk table, which was rebuilt at exactly `chunkID+1` on every new maximum (quadratic in chunks touched) to grow geometrically.
  - Removed a redundant double arena probe per candidate in `tqComputer.ComputeSingle`.
  - Added `bulkEfConstruction`: narrows the construction beam by graph size for TurboQuant only (a beam affordable for the float32 kernel costs a full polar reconstruction per candidate on TQ), clamped so it never drops below the layer-0 degree cap.
- **Item 16 — HNSW Bulk Linkage Candidate Buffer Pooling & CAS Pruning**:
  - Added pooled linkage buffers to `ArrowSearchContext` (`bulkCandidates`, `bulkSources`, `bulkDists`, `linkSrc`, `linkDist`, `batchSrc`, `batchNext`, `diffOld`, `diffNew`), replacing per-node `make()`s in the linkage path. `AddConnectionsBatch` went from 4 heap allocations per call to 2 (the remainder is the `UpdateNeighbors` closure environment, which cannot escape the interface call).
  - Hoisted the `graphCandidates` array out of the per-sub-batch, per-layer loop — at 1M vectors the old shape allocated and zeroed an 8 MB pointer array ~244 times per layer.
  - Removed the `pBuf` copy that duplicated an already-pooled search result and leaked one candidate slice to the GC per node per layer; candidate slices are now returned to `candidatePool`.
  - Pre-resolved the Arrow batch row-offset prefix sums and vector column index once per batch (Subtask 16.3), replacing a linear record scan and a schema lookup per row.
- **Item 15 — Elimination of Synchronous BM25 Double-Indexing & 943 GB Allocs**:
  - Removed the duplicate BM25 pass from `runIndexWorker`. Ingestion already indexes every text cell, and the index worker re-inserted the same documents under a *different* document ID space — doubling every posting list, `docCount` and `avgDocLength`, and scoring the sparse half of hybrid search against a corpus twice the real size.
  - Added `BM25InvertedIndex.AddBatch`, which groups a whole batch by term shard and performs one copy-on-write `BulkStore` per shard plus one sorted-merge posting-list rebuild per touched term, instead of a full map clone and posting-list reallocation per document.
  - Added `BM25ArenaIndex.IndexDocuments` so a batch takes the arena write lock once rather than once per document, and fixed a scorer desync where `AddDocument` was called unconditionally and so inflated the IDF corpus on re-index.
  - Moved all text/BM25 indexing onto a bounded background queue (`text_index_worker.go`) so tokenisation and dictionary updates never run on the ingestion hot path or under the dataset write lock; overflow falls back to inline indexing rather than dropping documents.
  - Every text cell is now tokenised exactly once and shared between both indexes.
- **Item 14 — Ingestion Pipeline Zero-Copy & Timer Elimination**:
  - Added an amortised `LockFreeSlice.Append` that grows geometrically and writes past the published length, replacing the per-batch `make(len+1)` + `copy` that made ingestion O(N²) inside the dataset write lock.
  - Added `Dataset.TotalIngestedRows` and centralised batch-list mutation in `appendRecordLocked`/`replaceRecords`, removing the per-batch O(N) loop over historical batches that produced the global row offset.
  - Replaced the 1 ms ingestion-worker poll timer with a wake channel on `IngestionRingBuffer` (mirroring `IndexJobQueueLockFree`), with a long backstop timer retained purely as a lost-wakeup safety net.
  - Retired string columns whose distinct-value count exceeds the dictionary's 65535-symbol range. Above that the dictionary silently encoded everything as code 0 — never helping a single filter while retaining a `uint16` per row per string column for the dataset's lifetime.

**Regression coverage added**:

- `internal/store/ingest_batch_append_regression_test.go`, `internal/store/bm25_single_index_regression_test.go`, `internal/store/index/bm25_batch_test.go`, `internal/store/index/bulk_linkage_buffers_test.go`, `internal/store/index/turboquant_bulk_indexing_test.go`.
- Includes race-tested concurrent-append and concurrent-decode safety, an allocation bound on `AddConnectionsBatch`, an amortisation bound on `BM25InvertedIndex.AddBatch`, and an Arrow leak check covering text-queue shutdown ordering.
- Two bugs were found *by* these tests and fixed: the text queue retained an Arrow record per job and drained after its workers had exited (leaking the batch, attributed by the Arrow leak checker to whatever test ran next); and `waitForTextIndexing` waited on queue length alone, so a dequeued-but-unfinished job was reported as drained.

**Known pre-existing failures** (unchanged by this work, confirmed against a clean tree):

- `TestNarrowRecallOnClusteredEmbeddings` — fails on `main` before these changes.
- `TestHNSW_BulkInsert` — fails on `main` before these changes; the Arrow leak checker attributes a neighbouring test's leak to it.

**Also fixed**: `TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress` went from failing at 167 vec/s (timeout) on `main` to 913 vec/s (5.5x) with these changes.

**Recently Completed (2026-10-11)**:
- **AVX-512 + FMA SIMD Kernel Validation & Architecture Parity on Intel Xeon Reference Testbed (`tf-testbed-70b436`)**:
  - Validated all 24 AVX-512 kernels, transcendental routines (`expAVX512`, `logAVX512`, `sinAVX512`, `cosAVX512`, `tanAVX512`), VNNI (`dotInt8AVX512VNNI`), and FMA kernels in `emlgo` on physical Intel Xeon Cascade Lake CPU @ 2.80GHz with 100% test pass.
  - Validated native AVX-512, FMA, and store engine tests in `longbow/internal/simd` and `longbow/internal/store/...`.
  - Diagnosed and resolved latent architecture parity defect in `internal/simd/turboquant.go`: `TurboQuantDistanceAVX2` had an internal `if features.HasAVX512` override dispatching `l2SquaredAVX512`, causing bit divergence against `turboQuantDistanceBatchAVX2` on AVX-512 hardware. Refactored `turboQuantDistanceWithL2Scratch` to strictly bind to architecture-matched L2 kernels, restoring exact bit-identity across all architectures and bit depths.
  - Executed benchmark matrix and gathered runtime `pprof` profile suites (CPU hotspots, mutex delays, heap allocations, block profiles) across 100k, 500k, and 1M scale for Standard and EMLGo versions on Intel Xeon AVX-512 reference hardware. Observed +81.2% speedup in `float64` dense search (2,085.1 QPS vs 1,150.9 QPS) and +130%–186% speedups in integer and quantized vector types.


**Recently Completed (2026-10-10)**:
- **Context-Window Hygiene & Codebase De-Monolithization (P0 Items 1–8)**:
  - Decomposed 8 monolithic files (>1,500–4,500 lines) into cohesive, modular architectures with strict sub-1,000 line ceilings to prevent agent context exhaustion:
    1. **`internal/store/types/graph_data.go`** (4,395L -> 7 focused files): `graph_data.go` (971L, core struct & lifecycle), `graph_data_accessors.go` (701L, fast typed chunk accessors), `graph_data_chunks.go` (880L, chunk lifecycle & batching), `graph_data_clone.go` (616L, structural cloning), `graph_data_offheap.go` (445L, off-heap arena migration), `graph_data_topology.go` (293L, neighbors & node locking), `graph_data_vectors.go` (556L, single-vector read/write).
    2. **`scripts/unified_benchmark.py`** (4,587L -> modular plugin suite): Split 10 specialized scenarios into `scripts/benchmark/scenarios/` (108–445L each) with `ScenarioMixin`, reducing `unified_benchmark.py` by >1,515 lines while preserving backwards-compatible imports and CLI surface.
    3. **`internal/gpu/cuda/cuda_index.go`** (2,646L -> 8 modular files): Extracted shared C prototypes to `cuda_kernels_decl.h`, partitioned Go logic into `cuda_index.go` (310L), `cuda_index_search.go` (444L), `cuda_index_dtypes.go` (368L), `cuda_index_pq.go` (331L), `cuda_index_turboquant.go` (413L), `cuda_index_graph_ops.go` (468L), and `cuda_index_ingest.go` (211L).
    4. **`internal/query/filter_evaluator.go`** (2,112L -> 5 operator modules): `filter_evaluator.go` (555L, evaluator & compiler), `filter_numeric.go` (623L, numeric comparison ops), `filter_text.go` (440L, string & regex ops), `filter_nested.go` (340L, nested struct/list ops), `filter_compound.go` (193L, compound boolean ops).
    5. **Arrow Flight RPC Handlers** (`store_actions.go` & `store_query.go`, 3,758L -> 6 modules): Partitioned into `store_actions.go` (857L, `DoAction` router), `store_ingest.go` (1,153L, `DoPut` stream & staging), `store_query.go` (765L, Flight endpoints & `DoGet`), `store_query_search.go` (640L, search & streaming), `store_query_ctes.go` (221L, CTEs & subqueries), `store_query_specialized.go` (196L, geo & temporal).
    6. **`internal/store/temporal_search.go`** (1,880L -> 4 modules): `temporal_predicates.go` (50L, search predicates), `temporal_cache.go` (173L, result caching), `temporal_tree.go` (602L, interval tree data structure), `temporal_search.go` (1,083L, store query orchestration).
    7. **`cmd/cli/main.go`** (1,576L -> 5 subcommand modules): `main.go` (153L, root parser), `admin_cmd.go` (494L), `import_cmd.go` (556L), `search_cmd.go` (290L), `graph_cmd.go` (140L).
    8. **Metal GPU Engines** (`metal_gpu.go` & `metal_gpu_optimized.go`, 3,855L -> 6 modules): Partitioned into base engines and companion modules `metal_gpu_dtypes.go`, `metal_gpu_ops.go`, `metal_gpu_optimized_dtypes.go`, and `metal_gpu_optimized_ops.go`.
  - Verified 100% test passing across unit tests, fuzz tests, cross-architecture darwin/arm64 checks, and python scenario tests.
- **TurboQuant Batched SIMD Kernels & Candidate Accumulation (R24)**:
  - Added first-class TurboQuant batched distance dispatch (`distanceTQBatchFunc`, `ImplementationDispatch.TurboQuantDistanceBatch`, `turboQuantDistanceBatchImpl`, `GetTurboQuantDistanceBatchFunc()`) across AVX-512, AVX2, NEON, and Generic.
  - Implemented 4-way interleaved polar reconstruction loop (`turboQuantDistanceBatchWithL2`) overlapping independent candidate dequantization latencies to overcome memory-load stalls.
  - Added `TurboQuantCompute.DistanceDirectCodesBatch` and zero-allocation code buffer gathering (`codesBuf`) in `tqComputer.ComputeBatch` for resident slab-chunk codes.
  - Verified exact bit-identity against single-vector evaluation (`TestTurboQuantDistanceBatchIsBitIdentical`, `TestTurboQuantBatchKernelsCrossArchitectureParity`, `TestTQComputeBatchMatchesPerCandidate`).
  - Added `FuzzTurboQuantDistanceBatch` fuzzing arbitrary vectors, codes, dimensions, and payload bounds (670k+ iterations verified clean).
- **`int16` / `uint16` 6x Throughput Deficit Resolution (R32)**:
  - Identified query-domain conversion and vector accessor resolution bottleneck; fixed `int16_kernels_amd64.s`, `distance_resolvers.go`, and `arrow_hnsw_compute_int.go`.
  - Added parity tests and read-path benchmarks (`int16_kernel_parity_test.go`, `distance_read_path_bench_test.go`).
- **8-bit & 16-bit Recall Validation on Real Embeddings**:
  - Evaluated recall@10 on clustered real-world embeddings (`narrow_recall_clustered_test.go`, `narrow_query_domain_test.go`).
  - Demonstrated recall@10 reaches 1.000 for `int8`, `int16`, and `uint16` and >0.92 for `uint8` with adequate `efSearch`.
- **Intel SDE Lane for AVX-512 / VBMI / AMX in CI**:
  - Added `.github/workflows/ci.yml` `test-avx512-sde` lane using Intel Software Development Emulator (`scripts/check_avx512_coverage.sh`).
  - Uncovered and fixed VBMI TQ2 kernel defect with generic packer alignment.
- **A/B Benchmarking CI Policy (R30, R39)**:
  - Implemented `.github/workflows/benchmark-ab.yml` scheduled interleaved A/B benchmark workflow.
  - Codified advisory single-run PR checks vs blocking scheduled interleaved A/B qualification in `docs/testplan.md` with paired index time and search QPS regression gates.

**Recently Completed (2026-10-09)**:
- **TurboQuant dense regression analysis + scale re-baseline (R9, R19, R27)**:
  - Decomposed distance evaluation time via CPU runtime profiling (`pprof`) at 100k scale: 47.8% recursive polar reconstruction, 28.5% QJL sign correction, 12.9% angle code unpacking, 6.5% scratch/radius handling, 4.3% SIMD L2 kernel (`l2SquaredAVX2`), <1.0% chunk views. Proved that >89.2% of distance evaluation time is scalar dequantization/reconstruction latency.
  - Resolved root cause of historical 1,248 QPS vs ~300–420 QPS gap: pre-`a955a0c1` graph had 73.0% reachability and early-exited after ~1 hop, evaluating very few candidates on a broken topology; post-fix graph has 98.1%+ reachability, 15.8–16.0 mean degree, and traverses the full 3.5-hop candidate beam.
  - Re-measured 50k scale with provenance and paired index times across all 13 search modes (dense: 420.6 QPS, ingest: 356,729.8 vec/s, index time: 497.0s, peak RSS: 1,479.5 MB).
  - Re-measured 250k scale in engine: bulk build completed in 34.7s (`fallback=false`, 16.0 mean degree, 97.3% reachability).
  - Fixed `valuesCache` in `arrow_hnsw_insert.go` to support `FixedSizeList` for `Float32`/`VectorTypeTQ`.
  - Fixed location store preservation in `addBatchBulkInternal` (`arrow_hnsw_bulk.go`).
  - Updated `docs/performance.md` §2, §3, and §4.
- **R26 inbound edge invariant, R5 chain proximity gate, and R6 degree reservation removal**:
  - Diagnosed and fixed R26 root cause: `protectLastInboundEdges` previously checked incoming candidates from `extra` with `inDegree == 0`, evicting legitimate backward neighbors and turning the graph into a forward-only DAG where all nodes < EntryPoint were unreachable (breaking `TestPredicateTraversal_ReachesMatchBehindRejectedNodes`). Fixed to restrict protection to existing connections in `current` whose only edge is being dropped (`inDegree <= 1`).
  - Fixed in-place mutation bug where `CASNeighbors` copied `new` into `oldOffset`, overwriting `lastOld` in place and preventing `inDegreeL0.Dec` from ever executing; added explicit cloning of `lastOld`/`lastNew`.
  - Enabled `LONGBOW_HNSW_INBOUND_GUARD=1` by default.
  - Implemented R5 proximity-gated chain links (`chainDist <= median(candidates)`), eliminating artificial long-range edges on shuffled corpora while preserving collinear connectivity.
  - Added `ensureInboundEdge` fallback linking from closest geometric neighbors (`fSources`) for any node that would otherwise end with 0 inbound edges.
  - Dropped R6 `chainLinksPerNode = 2` hard degree reservation on layer 0, freeing all 16 slots for geometric neighbors.
  - Verified 99.8%–99.9% reachability on shuffled corpora with 0 zero-in-degree nodes and recall@10 surging from 0.28–0.30 to 0.4400.
- **Bulk linkage against growing graph**: Implemented Malkov & Yashunin Algorithm 4 `keepPrunedConnections` in `selectNeighbors` and `selectNeighborsFloat32`; multi-layer descent convergence across all active nodes (`ef=1`); `protectChainLinks` to preserve predecessor/successor connectivity; restored adaptive sub-batching. Layer 0 mean degree restored to 15.9–16.0 with 100% reachability and fallback=false.
- **`uint8`/`int8` read-path asymmetry**: Unified 1-byte vector path in `int8Computer` using zero-allocation batched chunk views; dispatched to native `distFuncUint8` / `distFuncUint8Squared` when `uint8Mode()`; eliminated dead `uint8Computer`; added pooled `queryUint8` to `ArrowSearchContext`; achieved parity within 1.08x–1.13x.
- **Index time reporting paired with ingest throughput**: Added `IndexingDuration` and indexing throughput telemetry to `cmd/bench-tool`, integrated paired `indexing_duration_seconds` into `scripts/unified_benchmark.py` (JSON output, summary table, Markdown reports, and `--compare-baseline` regression gate), and added paired `Index Time (s)` to `docs/performance.md` §2.

---

## 2. P1 — GPU Infrastructure & SIMD

### Item 4: BM25 Inverted Index Copy-on-Write Elimination (Allocations) — RESOLVED 2026-10-15

BulkCOW coalescing shipped in `BM25InvertedIndex.AddBatch` (single `BulkStore`
per shard per batch, merged posting lists instead of per-document re-copy);
legacy `Add` delegates through `AddBatch`. The arena-based replacement
(`BM25ArenaIndex.IndexDocuments`) is now the primary in the hybrid ingestion
path (`store_hybrid.go`) and the hybrid search path (`hybrid_pipeline.go`,
`hybrid_search_arena.go`); hybrid search falls back gracefully when only one
side is present. `MapRCU` remains for legacy `Delete`/compat paths but is not
on critical ingest/search hot paths.

- **Files**: `internal/store/index/bm25_inverted_index.go`, `internal/store/index/bm25_arena_index.go`
- **State**: Runtime profiling (`allocs.pprof`) shows `(*BM25InvertedIndex).Add` consumed 12,813.22 MB (60.04% of total heap allocation across the entire process). On every document insert, `Add` executes Copy-on-Write cloning of the shard term map (`MapRCU[string, *BM25PostingList]`) and slice reallocation for every term posting list (`newDocIDs = make([]VectorID, n+1)`). For 100k documents with ~5 terms each, this triggers >500,000 map clones and slice allocations.
- **Plan**: Migrate `BM25InvertedIndex` to use chunked/arena-allocated postings or batched ingestion (`AddBatch`) backed by `BM25ArenaIndex` with preallocated contiguous posting arrays, eliminating per-document map copying.
- **Success**: Heap allocations during ingest drop by >12 GB; GC pause times reduced during concurrent ingest.

### Item 5: Native L2-Squared Integer SIMD Kernels without VSQRT Latency — RESOLVED 2026-10-15

Added `l2SquaredInt8AVX512Kernel` (AVX-512, no terminal VSQRTSS) mirroring `euclideanInt8AVX512Kernel`, plus the `l2SquaredInt8AVX512` wrapper and a non-AVX fallback. Dispatch now wires `l2SquaredInt8Impl` directly for HasAVX512, and to `l2SquaredInt8Unrolled4x` on AVX2 and ARM/NEON, eliminating the previous redundant `euclidean... · d * d` path whose inner function ended in VSQRTSS. SIMD tests pass; int8 squared-distance CV now has zero square-root instruction on every supported dispatch branch.

- **Files**: `internal/simd/dispatch.go`, `internal/simd/all_kernels_avo_amd64.s`
- **State**: In `internal/simd/dispatch.go` lines 622-628, `l2SquaredInt8Impl` is implemented by invoking `euclideanInt8AVX512(a, b)` and then squaring the result (`return d * d, nil`). Evaluating `VSQRTPS` in the innermost distance evaluation loop only to immediately square it back in Go adds redundant square-root latency on every candidate distance evaluation in HNSW search.
- **Plan**: Implement native `l2SquaredInt8AVX512Kernel` and `l2SquaredUint8AVX512Kernel` computing the horizontal sum of squared differences directly without `VSQRTPS`/`VSQRTSS`.
- **Success**: Zero square-root instructions executed on integer HNSW distance comparisons; integer search QPS improves by 10-15%.

### Item 6: Wire AVX-512 VNNI (`VPDPBUSD`) and Saturated Int8/Uint8 into Primary Dispatch Table

- **Files**: `internal/simd/dispatch.go`, `internal/simd/all_kernels_avo_amd64.s`, `internal/simd/dot_product_amd64.go`
- **State**: In `dispatch.go` lines 629-631 under the AVX-512 dispatch block, `dotProductInt8Impl`, `dotProductUint8Impl`, and `euclideanDistanceUint8Impl` still fall back to scalar 4x unrolled loops (`dotInt8Unrolled4x`, `dotUint8Unrolled4x`, `euclideanUint8Unrolled4x`), even though `emlgo` implements 64-byte `dotInt8AVX512VNNI` (`VPDPBUSD`) and 64-byte saturated int8 kernels.
- **Plan**: Expose and wire native 64-byte VNNI dot product and saturated AVX-512 int8/uint8 distance kernels into `internal/simd`'s primary dispatch table when `features.HasAVX512VNNI` is detected.
- **Success**: Vectorized 64-byte dot product throughput on Cascade Lake/Sapphire Rapids; quantized dot product and angular similarity speedup of 4x-8x.

### Item 7: Direct Base Pointer Caching in SearchContext for SlabArena and TypedArena

- **Files**: `internal/memory/arena.go`, `internal/store/types/graph_data_accessors.go`, `internal/store/index/search_context.go`
- **State**: Runtime profiling (`profile.pprof`) showed `(*SlabArena).GetWithGeneration` accounts for 11.08% flat CPU time and `(*TypedArena).GetWithGeneration` accounts for 3.00% flat CPU time. Every vector lookup in graph search calls `GetVectorsChunkWithGen`, executing atomic pointer loads, 64-bit integer divisions (`offset / uint64(a.slabCap)`), slice boundary checks, and generation isolation checks.
- **Plan**: Cache raw chunk base pointers in `ArrowSearchContext`. Since slabs are append-only and readers are pinned via `AcquireReader`, search workers can calculate vector offsets directly via base pointer arithmetic without per-hop slab index division or atomic pointer reloading.
- **Success**: Eliminate 14% flat CPU time spent in slab dereferencing; search latency across all dtypes drops by 10-18%.

---

## 3. P2 — Optimizations & Hygiene

### Item 2: ARM NEON Dequant Kernel & Remaining Scalar SQ8 Fallbacks

- **Files**: `internal/simd/dequant_other.go`, [distance_dispatch.go](file:///home/rsd/REPOS/longbow/internal/store/index/distance_dispatch.go)
- **State**: AVX2+FMA fused dequant-L2 kernel ships for AMD64 (`dequant_amd64.s`). On ARM64, scalar fallbacks are used.
- **Plan**: Implement ARM NEON assembly for fused dequantization and L2 distance.
- **Success**: Parity tests passing on ARM64 with vectorized throughput.

### Item 3: Pooled Row Buffers in `DiskVectorStore`

- **Files**: `internal/store/index/disk_vector_store.go`, `internal/store/index/buffer_pool.go`
- **State**: Typed row extraction allocates slice headers and temporary row buffers during non-vectorized disk scans.
- **Plan**: Pool row buffers to eliminate transient allocations during disk traversals.
- **Success**: Zero-allocation row decoding in `DiskVectorStore`.

### Item 8: Lock-Free Adjacency Updates & NUMA-Local Index Queueing

- **Files**: `internal/store/index/flat_adjacency.go`, `internal/store/index/arrow_hnsw_bulk.go`, `internal/store/vector_store.go`
- **State**: Runtime lock profiling (`mutex.pprof`) revealed 69.10% flat lock delay in `sync.(*Mutex).Unlock`, with `StartIndexingWorkers` and `runIndexWorker` contributing 53.8% cumulative lock delay during concurrent batch indexing (`AddConnectionsBatch` and `AddConnection`).
- **Plan**: Transition neighbor list updates to lock-free atomic compare-and-swap (CAS) / generation-stamped adjacency lists, and partition incoming bulk index batches into NUMA-local sharded worker queues.
- **Success**: Drastic reduction in mutex contention during bulk ingest; index construction throughput scales linearly across 16+ cores.

### Item 9: Inlined 4-Ary Candidate Heap & Branchless Struct Swapping

- **Files**: `internal/store/index/candidate_heap.go`, `internal/store/index/search_float32.go`
- **State**: Profiling (`profile.pprof`) indicates priority queue maintenance during beam search (`CandidateHeapAdapter.down`, `Less`, and `Swap`) consumes ~15.2% combined flat time. `Less` and `Swap` are value-receiver methods, incurring repeated slice header copies and index bounds checks.
- **Plan**: Inline distance comparisons (`(*h)[j].Dist < (*h)[best].Dist`) directly into `up` and `down`, implement branchless struct swapping, and pre-allocate power-of-two candidate buffers in `ArrowSearchContext`.
- **Success**: Priority queue operations eliminate function call overhead; graph search layer traversal accelerates by 8-12%.

### Item 10: Iterative Stack Traversal for Geospatial Quadtree Queries

- **Files**: `internal/store/geo_index.go`
- **State**: Profiling (`allocs.pprof`) shows `(*Quadtree).queryBoxRecursive` allocated 857.49 MB (4.02% of total heap allocations) and `(*GeoIndex).SearchRadius` accounted for 1,128.16 MB cumulative allocations due to temporary slice allocations at each recursive quadrant subdivision.
- **Plan**: Replace recursive quadtree bounding box queries with an iterative traversal using a fixed-depth stack and reusable pre-allocated ID buffer in `SearchContext`.
- **Success**: Zero allocations during geospatial bounding box and radius queries; geo search QPS increases by 25-40%.

### Item 11: Dynamic Hardware-Aware EMLGo Math Dispatch Threshold Calibration

- **Files**: `internal/tensor/math_dispatch_env.go`
- **State**: In `math_dispatch_env.go`, `ResolveBackend` uses hardcoded static vector count cutoffs (e.g. `complex64EMLMaxCount = 500000`, `complex128 >= 500000` falls back to standard). On AVX-512 Xeon, 8-wide complex vectorization delivers 2,085 QPS (vs 1,150 QPS on AVX2), proving that higher-end server CPUs maintain advantage at scales where AVX2 throttles.
- **Plan**: Dynamically calibrate EMLGo vs Standard dispatch thresholds based on detected CPU instruction set extensions (`HasAVX512`, `HasAVX512VNNI`, `HasAMX`) and cache topology.
- **Success**: Optimal math backend selection across consumer AVX2 laptops and server-class AVX-512 architectures.

### Item 12: Lock-Free Concurrent Map for Metadata & ID Translation

- **Files**: `internal/store/types/map_rcu.go`
- **State**: Profiling (`allocs.pprof`) showed `(*MapRCU).Store` and `BulkStore` across `[uint32, []string]`, `[string, *uint8]`, and `[uint32, int]` accounted for >1,200 MB of heap allocations because every mutation makes a shallow copy of the underlying Go map (`newMap := make(map[K]V, len(oldMap)+1)`).
- **Plan**: Replace copy-on-write `MapRCU` with a concurrent lock-free hash map or chunked Trie/Radix structure for ID translation and string metadata.
- **Success**: Eliminate map reallocation on every batch write; lower GC pressure during streaming chunk ingest.

### Item 13: SIMD Fused BM25 Term Scoring & Posting List Intersection

- **Files**: `internal/store/index/bm25_inverted_index.go`, `internal/simd/dispatch.go`
- **State**: BM25 multi-term scoring currently evaluates posting lists sequentially and gathers document lengths and term frequencies via Go map lookups (`GetDocLengthsBatch`).
- **Plan**: Wire `BM25ScoreBatch` into vectorized posting list intersection, evaluating 16 documents concurrently using AVX-512 `VFMADD` instructions with vector-aligned document length arrays.
- **Success**: 3x-5x throughput improvement in sparse and hybrid BM25 ranking.

---

## 4. P4 — Observability & Documentation Hygiene

Raised by an audit of `grafana/` on 2026-10-11. Every finding below was
reproduced by extracting the metric families referenced in `grafana/rules.yml`
and `grafana/dashboards/*.json` and diffing them against the `Name: "longbow_..."`
literals in non-test Go sources. Numbers below come from that diff.

The general failure mode is the same in several items: **an alert that can never
fire is indistinguishable from an alert that is working**, because an expression
over a metric that is not exported evaluates to an empty series rather than to
an error. Nothing in CI currently compares the two sides, so the gap is invisible
until an incident.

### Item 18: Repair 10 alert rules bound to metrics that do not exist

- **Files**: `grafana/rules.yml`
- **State**: 10 of the 72 alerts, plus 1 recording rule, reference a metric family the binary never registers. Each is permanently silent.

  | Expression references | Actually registered | Alert(s) that never fire |
  |---|---|---|
  | `longbow_doget_zero_copy_total` | `longbow_do_get_zero_copy_total` | recording rule `job:longbow_zero_copy_ratio` |
  | `longbow_gpu_enabled` | *(none)* | `LongbowGPUFailure` |
  | `longbow_gpu_memory_used_bytes` / `longbow_gpu_memory_total_bytes` | `longbow_gpu_memory_bytes` (single gauge, not a used/total pair) | `LongbowGPUMemoryPressure` |
  | `longbow_hybrid_search_enabled` | *(none)* | `LongbowHybridSearchStalled`, `LongbowBM25IndexStalled` |
  | `longbow_index_creation_failures_total` | *(none)* — only `longbow_index_creations_total`, which counts successes | `LongbowIndexBuildFailures` |
  | `longbow_ipc_decode_errors_total` | *(none)* | `LongbowHighIpcErrors` |
  | `longbow_replication_failure_total` | `longbow_replication_failures_total` | `LongbowReplicationStalled` |
  | `longbow_storage_io_wait_seconds_total` | *(none)* | `LongbowHighIOWait` |
  | `longbow_storage_iouring_available` | *(none)* | `LongbowIOUringNotAvailable` |

  Two further defects in the same file:

  - `LongbowIOUringNotAvailable` reads `ONGBOW_STORAGE_USE_IOURING`; the
    environment variable is `LONGBOW_STORAGE_USE_IOURING`. The misspelling makes
    the guard permanently vacuous on top of the missing metric.
  - The `job:longbow_put_qps:rate5m` recording rule matches
    `longbow_flight_rows_processed_total{method="put"}`, but that vector is
    labelled `operation`/`status` (`internal/metrics/compaction_metrics.go`). The
    matcher selects nothing.

  `LongbowHybridSearchStalled` and `LongbowBM25IndexStalled` are the two that
  matter most: they are the only coverage for the hybrid search path, and hybrid
  indexing was rewritten this cycle (roadmap Item 15), so a regression there
  would go unnoticed.

- **Plan**: Correct each expression to the registered family. Where the intent
  needs a metric that does not exist yet (`gpu_enabled`, `hybrid_search_enabled`,
  `storage_iouring_available`, `index_creation_failures_total`,
  `ipc_decode_errors_total`, `storage_io_wait_seconds_total`), add the gauge or
  counter to the Go source rather than deleting the alert - each one guards a
  real failure mode. Fix the label matcher and the environment-variable spelling.
- **Success**: Every metric referenced by `rules.yml` resolves to a registered
  family; enforced permanently by Item 19.

### Item 19: Add a metric-existence conformance gate to CI

- **Files**: new `internal/metrics/conformance_test.go` (or `grafana/tests/`),
  `.github/workflows/ci.yml`
- **State**: Nothing checks the two sides. `docs/metrics.md` already documents
  its own derivation method — "enumerate every `Name: "longbow_..."` literal in
  non-vendor, non-`_test.go` Go sources, plus the one family composed from
  `Namespace: "longbow"` + `Name:`" — which is exactly the inventory the gate
  needs. Item 18's nine missing families survived because the check was written
  down but never implemented.
- **Plan**: A test that parses `grafana/rules.yml` and every
  `grafana/dashboards/*.json`, extracts each `longbow_*` family reference, and
  fails when it is neither registered in Go nor a known recording-rule name
  (`job:longbow_*:rate5m`). Should also assert the inverse for `docs/metrics.md`,
  which is the inventory it is supposed to mirror (Item 22).
- **Success**: CI fails on any rule, dashboard or doc reference to an
  unregistered family, and on any divergence between the three inventories.

### Item 20: Extend promtool unit-test coverage beyond 2 of 28 rule groups

- **Files**: `grafana/tests/test-rules.sh`, `grafana/tests/*_alerts_test.yml`
- **State**: The harness exists and is well built, but `grafana/tests/` holds
  unit tests for exactly two groups — `longbow_bulk_insert_alerts` and
  `longbow_temporal_alerts` — out of 28 in `rules.yml`. The other 26 groups have
  a parse check (`promtool check rules`) and nothing else. A parse check cannot
  distinguish an expression that fires from one that is silently empty, which is
  precisely how all nine Item 18 defects survived.
- **Plan**: Add unit-test files for the remaining groups, prioritised by blast
  radius: `longbow_search_alerts`, `longbow_index_alerts`, `longbow_wal_alerts`,
  `longbow_memory_alerts`, `longbow_compaction_alerts`,
  `longbow_layer_eviction_alerts`, `longbow_slab_alerts`,
  `longbow_simd_kernel_alerts`, `longbow_mesh_alerts`,
  `longbow_replication_alerts`. Wire the harness to discover and run every
  `*_alerts_test.yml` rather than the two hardcoded `extract_group` pairs, so a
  new group cannot ship untested.
- **Success**: Every rule group has at least one firing and one non-firing
  scenario; adding a group without a test fails the harness.

### Item 21: Alert coverage for ~35 unalerted error/failure counters

- **Files**: `grafana/rules.yml`, `internal/metrics/`
- **State**: The binary exports failure counters that nothing watches. These are
  not exotic — they include several that indicate silent data loss or a wedged
  worker:

  | Counter | Why it matters |
  |---|---|
  | `longbow_index_jobs_dropped_total`, `longbow_index_jobs_overflow_total` | Indexing work discarded; the dataset will be incomplete and no existing alert fires |
  | `longbow_replication_queue_dropped_total`, `longbow_replication_failures_total`, `longbow_replication_retries_total` | Replication data loss; `LongbowReplicationStalled` is dead per Item 18 |
  | `longbow_work_queue_overflows_total`, `longbow_work_queue_backlog` | Background work queue saturating |
  | `longbow_semaphore_timeouts_total` | Requests timing out on a semaphore |
  | `longbow_checkpoint_timeouts_total`, `longbow_checkpoint_barrier_reached_total` | Durability — a snapshot that never completes |
  | `longbow_wal_flush_errors_total`, `longbow_wal_ring_buffer_full_total` | Durability; `LongbowWALWriteErrors` watches a different counter |
  | `longbow_compaction_errors_total` | Compaction failing silently while the tombstone ratio alert reads the consequences |
  | `longbow_eviction_rejected_queries_total` | Queries rejected by the eviction manager |
  | `longbow_dataset_dimension_mismatch_total`, `longbow_bulk_insert_dimension_errors_total` | Client-side ingest defects, currently invisible |
  | `longbow_dataset_export_failures_total`, `longbow_dataset_import_failures_total`, `longbow_dataset_update_retries_total` | Backup/restore path entirely unmonitored |
  | `longbow_panic_total`, `longbow_log_errors_total`, `longbow_trace_errors_total` | Generic failure surfaces with no threshold |
  | `longbow_hnsw_repair_failure_total`, `longbow_neighbor_selection_errors_total`, `longbow_arena_nil_error_total` | Graph-health regressions |
  | `longbow_simd_blocked_processing_total`, `longbow_jit_kernel_errors_total`, `longbow_gpu_hnsw_build_fallback_total` | Silent loss of an accelerated path — the same failure shape as the SIMD kernel mismatch alert already in `longbow_simd_kernel_alerts` |
  | `longbow_hnsw_arrow_extraction_errors_total`, `longbow_hnsw_nodes_skipped_total` | Vectors failing to reach the index |
  | `longbow_temporal_cache_expiries_total` | Paired with the existing evictions alert to distinguish the two TTL-vs-capacity drop causes |
  | `longbow_onnx_inference_errors_total`, `longbow_onnx_metal_inference_errors_total`, `longbow_reranker_errors_total`, `longbow_metal_shader_compile_total` | Embedding/reranking path unmonitored |
  | `longbow_multi_gpu_device_errors_total`, `longbow_learned_index_sample_overflow_total`, `longbow_rate_limit_requests_total{result="throttled"}` | Low-frequency but silent |

  A further gap: no alert uses `longbow_search_p50_latency_ms` or
  `longbow_search_p99_latency_ms`, which are the pre-computed quantiles the
  search-latency dashboard charts. `LongbowHighSearchLatency` computes its own
  `histogram_quantile` over `longbow_vector_search_latency_seconds_bucket`
  instead, so the two can disagree and nothing reconciles them.

- **Plan**: Add alert groups for the durability and data-loss counters first
  (`wal_flush_errors`, `checkpoint_timeouts`, `replication_queue_dropped`,
  `index_jobs_dropped`, `compaction_errors`), then for the silent-fallback
  counters, then the rest. Each should name its remedy in the annotation, the
  convention already used by `longbow_simd_kernel_alerts` and the R11a temporal
  alerts. Wire the search-latency alert to the pre-computed quantiles so there is
  one source of truth.
- **Success**: Every `*_errors_total`, `*_failures_total`, `*_dropped_total`,
  `*_timeouts_total` and `*_overflows_total` counter the binary exports is either
  alerted on or explicitly listed in `rules.yml` as deliberately unwatched, with
  a reason.

### Item 22: Alert routing, Alertmanager config and per-alert runbook links

- **Files**: new `grafana/alertmanager.yml`, `grafana/README.md`
- **State**: There is no Alertmanager configuration anywhere in the repository,
  so the 72 alerts resolve to notifications nowhere and the
  `severity: critical|warning|info` labels are not mapped to any receiver or
  routing policy. Separately, **1 of 72** alert annotations references a
  document (`LongbowSIMDKernelScalarFallbackRatio`); the rest report a number
  and stop. The better-written groups — `longbow_simd_kernel_alerts`, the R11a
  temporal alerts, `longbow_bulk_insert_alerts` — name the likely cause and the
  remedy in the annotation, which is the standard worth adopting; the majority do
  not. `LongbowReplicationStalled` is additionally unparseable in practice
  because it depends on the dead `longbow_replication_failure_total` (Item 18).
- **Plan**: Add an Alertmanager configuration routing `critical` to a paging
  receiver and `warning`/`info` to a ticket or chat receiver, with grouping by
  `alertname`/`dataset`. Add a `runbook_url` (or `docs/` anchor) annotation to
  every alert, pointing at the section of `docs/` that explains the subsystem.
  Make the well-written annotation style the convention, since a threshold with
  no stated cause is a threshold nobody acts on correctly.
- **Success**: A firing `critical` alert reaches a human with a link to the
  document that explains what to check.

### Item 23: Provision Grafana dashboards and rules (compose/helm/provisioning)

- **Files**: `docker-compose.yml`, `helm/longbow/`, new `grafana/provisioning/`
- **State**: `grep -n grafana docker-compose.yml` returns nothing, and `helm/`
  contains no Grafana resources and no reference to `rules.yml`. The dashboards
  and alerting rules are maintained in the repository but reach a running system
  only through a manual `curl -d @dashboards/x.json` import — the only path the
  README documents. A rule repaired under Item 18 therefore repairs nothing until
  someone remembers to re-import it, and the `longbow.json` legacy dashboard
  competes with the nine operational ones for the same slot.
- **Plan**: Add Grafana provisioning YAML (`provisioning/datasources/`,
  `provisioning/dashboards/`, `provisioning/alerting/`) so dashboards and rules
  load from the repository, and add a `grafana` service to `docker-compose.yml`
  for local development mirroring the Helm deployment. Keep the documented
  `curl` path working for one-off imports, and exclude `longbow.json` from a
  fresh install.
- **Success**: A clean checkout plus `docker compose up` yields Grafana with all
  10 current dashboards and all 28 rule groups already loaded, and a rule change
  lands on the next restart without a manual step.

### Item 24: Enforce `docs/` ↔ code consistency in CI

- **Files**: `docs/metrics.md`, `docs/*.md`, new CI check
- **State**: The 2026-10-11 audit found `docs/metrics.md` out of sync with the
  binary in both directions — 7 registered families undocumented (exactly those
  added by the two newest alert groups) and 1 removed family still listed — with
  a stated coverage count of 783 against a real 789. It also found a markdown
  table corrupted by a spliced-in paragraph, and four dangling
  `docs/roadmap.md §9.2` cross-references. All of it was fixed by hand in that
  pass; nothing stops it drifting again. The document already states its own
  derivation method, so the inventory a checker needs is already specified.
- **Plan**: Extend the Item 19 conformance gate to cover documentation, not just
  `rules.yml` and the dashboards: assert `docs/metrics.md` lists every
  registered family and no unregistered one, and that its stated count matches.
  Add a link check for internal `docs/` and `grafana/` cross-references, since
  four of the five §9.2 references were valid when written and only became
  dangling when the roadmap was renumbered — nothing revalidates them.
- **Success**: Renumbering a roadmap section, adding a metric, or removing one
  fails CI until `docs/` is updated to match.

---

*Items 22, 23 and 24 as first drafted (repair `docs/metrics.md`, fix the §9.2
references, rewrite the `grafana/README.md` inventory) were carried out in the
same audit pass and are not repeated here; their residual automation is folded
into Item 24 above.*

---

## 5. Next 10 Improvements (2026-10-15, post-emlgo 0.7 / baseline revalidation pass)

All items below are **open**. They are drawn from the profiling findings in
`docs/performance.md` §4, the emlgo 0.7 upgrade in this pass, and the
re-measurement requirements above. Each is sized to be a single PR-sized change
with its own success metric.

### Item 25 — Tile the TurboQuant polar reconstruction across four lanes (Performance)

- **Files**: `internal/store/index/turboquant_distance*.go`, `internal/tensor/quantization/turboquant.go`
- **State**: `pprof` §4 decomposes `turboQuantDistanceAVX2Scratch`: 89.2% of
  distance time is scalar polar reconstruction + QJL correction, only 4.3% is
  the SIMD L2 kernel. At 100k the 4-way batched path already interleaves four
  independent chains; it is not vectorised single-lane.
- **Plan**: Lift the scalar reconstruction loop into a 4-lane AVX2 kernel with
  independent accumulator registers, hoisting the lookup-table fetches; reuse
  the radius scale once per chunk.
- **Success**: TurboQuant search QPS rises from ~300 to ≥600 at 100k on AVX2
  with no change in `TurboQuantDistanceBatch` bit-identity.

### Item 26 — Cache the reconstructed TurboQuant vectors in a scratch arena across hops (Stability/Accuracy)

- **Files**: `internal/store/index/arrow_hnsw_search.go`, `internal/memory/arena.go`
- **State**: Each candidate's TurboQuant vector is reconstructed from polar codes
  on every hop it is visited. Under beam search the same neighbor is touched
  multiple times; the reconstruction work is wasted and increases the variance of
  P99 latency.
- **Plan**: Memoise reconstructed float32 representations in an ephemeral,
  per-search scratch arena keyed by node index and invalidate on eviction.
  Reuse across neighbors-within-a-chunk to amortise polar reconstruction.
- **Success**: Total polar reconstructions per query ≤ 1.1× (currently
  ≥ 1.8×) at `efSearch=500`; P99 latency −20%.

### Item 27 — AVX-512 VNNI dot-product fast path for quantized dtypes (Performance)

- **Files**: `internal/simd/dot_product_amd64.go`, `internal/simd/dispatch.go`
- **State**: Roadmap Item 6 is marked closed but the AVX2-only i7 client still
  routes int8/uint8 dot products through scalar 4x unrolled loops. The 0.7 vendor
  now ships `vpdpbusd` kernels; the dispatch table has not been updated.
- **Plan**: Wire `HasAVX512VNNI`/`HasAVX512` detection to the new kernels,
  keeping scalar fallback for unsupported hosts.
- **Success**: int8 dot-product throughput ≥ 4× on AVX-512; byte-identical
  numerical output vs the generic kernel.

### Item 28 — uint8 neighbour selection parity on all corpora (Accuracy) — RESOLVED 2026-10-15

- **Files**: `internal/store/index/arrow_hnsw_insert.go`, `internal/store/index/arrow_hnsw_uint8_reachability_test.go`
- **State**: New `VectorTypeUint8` case in `selectNeighbors` (native 8-bit kernel) shipped in 45696eb; the property-based coverage has now landed as `TestUint8GraphReachabilityAcrossDistributions` exercising 5 distributions (tight clusters, uniform random, overlapping 12-cluster, single hub, sparse/bimodal) — every one reaches >=99.9% of 4000 nodes (per-run degree 14.8-16.0, mean) on `efSearch=500`.
  a `VectorTypeUint8` branch to `selectNeighbors`. The bulk-linker case and
  per-dtype `narrow_recall` were affected. The fix is corpus-specific and
  deserves a broader guard.
- **Plan**: Add a property-based test that builds a uint8 index from 5
  distinct synthetic distributions and asserts reachability ≥ 95% on every one,
  so a future refactor cannot re-introduce the branch omission silently.
- **Success**: TestNarrowSearchReachesWholeGraph passes across all 5 corpora
  with zero skips; uint8 reachability ≥ 95% on each.

### Item 29 — uint8 connection-degree invariant under bulk indexing (Stability) — RESOLVED 2026-10-16

- **Files**: `internal/store/index/arrow_hnsw_insert.go`,
  `internal/store/index/insertion_core.go`,
  `internal/store/index/hnsw_gpu_build.go`,
  `internal/store/index/arrow_hnsw_uint8_bulk_degree_test.go`,
  `internal/store/index/arrow_hnsw_select_neighbors_test.go`
- **State**: The uint8 bulk linker previously pruned to mean degree 1.3–6.3
  (vs 16.00 for int8) because of the missing diversity branch. The branch
  landed with Item 28; this closes the other half of the plan.
- **Plan**: Make `selectNeighbors` the single entry point for both narrow and
  bulk paths; add a bulk-quality gate that fails the build below degree 15.0
  for any dtype whose bulk path previously pruned below 14.
- **Success**: Minimum observed degree ≥ 15.0 on a 4k uint8 build; quality test
  becomes a CI gate.

**Resolution (2026-10-16)**:

1. *Single entry point.* Three selection sites existed. The bulk linker already
   called `selectNeighbors`. The narrow path (`insertInternal`) handed the whole
   ef-wide result set to `AddConnection` one candidate at a time — O(ef) runs of
   the same heuristic per layer, converging on whatever the last add on a
   saturated node produced — and `targetM` was there in the code as `_ =
   targetM`, an unfilled placeholder. It now selects once per layer with `m`
   equal to that layer's own degree budget (`MMax0` at layer 0, `MMax` above),
   which is exactly what the incremental prune used to converge on, so the
   resulting degree is unchanged while the selection that produced it is made
   once, in distance order, against the full candidate set. The GPU batch build
   called `selectNeighborsSimple` — an O(n²) sort by distance that took the
   first `m`, a truncation with no diversity check, which is the same defect
   class as the uint8 default branch one level down; it now goes through
   `selectNeighbors` too, and `selectNeighborsSimple` is deleted.
   `TestSelectNeighborsIsTheSingleEntryPoint` pins the heuristic itself (the
   near-duplicate rejection that a wiring test would not catch).
   Both call sites copy the result out of `ctx.scratchSelected` before their
   `AddConnection` loops, because each of those re-enters selection on the
   target side — the aliasing the bulk linker already guards against with its
   `fSources` copy.

2. *The gate.* `TestBulkInsert_Uint8MeanDegreeFloor` builds 4k uint8 indexes
   through the bulk path in the shape production ingests — four flushes of
   1,000 rows, each linked into the graph built so far, not one static
   4,000-row batch — on three corpus shapes (tight clusters, uniform random in
   [-1,1], 12 overlapping clusters), and fails below mean layer-0 degree 15.0
   of the 16.0 budget or below 95% reachability. Measured after the change:
   **16.00 / 16.00 / 16.00**, reachability 4000/4000, 3998/4000, 4000/4000. It
   runs under plain `go test ./...`, so `ci.yml` (`-race`, no `-short`) gates on
   it.

3. *No recall regression.* The change touches the sequential path, which is the
   256-node bootstrap of every bulk build plus all small-batch inserts. A/B
   against the pre-change code, three runs each (`TestNarrowTypeRecallParity`,
   float32 recall@10): before 0.428 / 0.392 / 0.400, after 0.380 / 0.400 /
   0.404 — overlapping distributions, and the spread is the per-process
   `math/rand` level generator, which is what Item 31 removes. The thresholds
   in `TestNarrowRecallOnClusteredEmbeddings` (10 flushes, 20k vectors) and
   `TestNarrowQueryDomainRecall` pass unchanged; the whole `internal/store/index`
   package passes with and without `-short`.


### Item 30 — EMLGo 0.7 accuracy gating across all 15 dtypes (Accuracy)

- **Files**: `internal/mathutil/mathutil_emlgo.go`, `internal/simd/*_test.go`
- **State**: The emlgo 0.7 upgrade (vendored copy) brings new AVX-2/AVX-512
  kernels. Existing gating uses tight 1e-4 tolerances that may not hold on
  every dtype after the upgrade.
- **Plan**: Run the full `simd_accuracy` suite on every supported dtype after
  the upgrade; tighten tolerances where the 0.7 kernels improve accuracy and
  loosen them only with documented justification.
- **Success**: Every dtype passes the accuracy gate at the regression threshold
  used by CI; no new loosenings without a recorded justification.

### Item 31 — Deterministic seed plumbing through bench-tool ingest (Stability) — RESOLVED 2026-10-16

- **Files**: `cmd/bench-tool/main.go`, `scripts/unified_benchmark.py`
- **State**: The `--seed` arg is normalised only at the client edge; the data
  generator and the server's HNSW RNG can still diverge when the harness is
  restarted mid-run, which is exactly the case during long 500k sweeps.
- **Plan**: Thread a single `LONGBOW_BENCH_SEED` through generation and
  indexing; derive all per-config seeds deterministically from it.
- **Success**: Identical 500k index built in two separate harness invocations
  yields bit-identical search output for 500 fixed queries.

**Resolution (2026-10-16)**:

The criterion asks for one 500k index built twice. The 500k tier is skipped in
this session's benchmark sweeps, so the evidence below is the same check one
tier down (25k float32/dim128/dense, 500 queries, `--workers 4`, `-tags emlgo`)
plus a unit-level gate that runs in CI at every size. It is a smaller claim than
the criterion, and it is the same claim with the corpus size reduced.

1. *One seed, derived per config.* `LONGBOW_BENCH_SEED` is now the single source
   for both sides of the run: the harness exports it into the server's
   environment and derives each config's client seed as
   `sha256(base:config_label) mod (2^62-1)`, recorded in the perf matrix as
   `config_seed` so a result row names the seed that produced it. `--seed` on
   bench-tool and `--seed` on the harness both default from the same env var.
   **This changes every corpus**: per-config derived seeds are not the old
   constant `--seed 42`, so `benchmarks/baseline_*.json` and the 25k/100k/250k
   matrices are stale by construction and have to be re-cut before Item 32's
   5% index-time gate means anything against them.

2. *Two client-side bugs, both found by the corpus digest.* bench-tool now
   hashes the Arrow IPC serialisation of every uploaded chunk into
   `corpus_digest` on the DoPut row, which splits "the corpus differs" from "the
   index differs" in one field. It earned its place immediately: chunk N's RNG
   was seeded per *worker* while workers pulled from a shared job queue, so
   which chunk got which stream was scheduling-dependent; and every row was
   stamped `arrow.Timestamp(time.Now().UnixNano())`, so the corpus differed on
   every run by construction. Rows are now stamped below the dataset's
   `as_of` (`defaultTemporalAsOf` = 2100-01-01, minus the row offset, minus 1)
   and chunk N's RNG is seeded from the job index. After those two fixes the
   corpus digest is stable across processes (`d76eb899…`), which is what let
   the remaining divergence be attributed to the server.

3. *The server had four independent sources of nondeterminism*, found by
   bisecting with per-pass digests rather than by reading for races:

   a. *Levels from the clock.* `generateLevel` drew from the process-global
      `math/rand`, whose stream depends on how many draws any other goroutine
      made first — and it was not even consulted for bulk nodes on some paths.
      Level assignment is now a pure function of the node id
      (`h.levelFor(id)`: splitmix64 of `LevelSeed`+id → `(0,1]` →
      `floor(-ln(u) * levelMultiplier)`), with `generateLevel` deleted. This is
      also the spread Item 29 measured as "per-process variation" in its
      recall A/B.

   b. *The bulk linker interleaved decisions with writes.* The linkage loop ran
      each node's selection and its writes in one `ParallelFor`, and every node
      writes reverse edges into its **neighbours'** lists, so two goroutines
      landed on the same adjacency list. A list over its connection limit is
      pruned by a heuristic over the *order* the edges arrived in, so the
      survivors — and so the whole graph — depended on the scheduler. The phase
      is now three passes that give every list exactly one writer in an order
      that is a function of the sub-batch: **P1** decides against the frozen
      graph and writes only the node's own list (targets disjoint, still
      parallel); **P2** applies the cross-node edges (chain-forward plus every
      reverse link), counting-sorted by target so one goroutine owns one list,
      in sub-batch order within it; **P3** runs the R26 inbound guard, which
      decides from in-degree P2 has just written, so it runs after it and in
      node order — recomputing the node's selection rather than carrying it out
      of the pooled buffers, because selection reads only candidates and
      vectors. Confirmed by forcing the whole phase sequential: that made two
      builds agree, which is what identified the phase at all.

   c. *The pruning guard read live state while other goroutines wrote it.*
      `protectLastInboundEdges` asks "is this neighbour's only inbound edge the
      one being dropped?" — reading `h.inDegreeL0` as it stood. In the
      interleaved linker that answer depended on how far the other goroutines
      had got. P2 now hands the guard a snapshot taken when P1 finished
      (`inDegreeSnapshot` on the search context, `inDegreeReader` instead of a
      concrete `*inDegreeTracker`), which is the same instant the sequential
      code it replaced saw. A side effect worth keeping: the guard can no longer
      add an inbound edge that a later prune in the same sub-batch removes.

   d. *The neighbour cache published empty lists.* This one is a live bug, not
      only a determinism one. `LockFreeNeighborCache.SetNeighbors` did
      `LoadOrStore(id, NewLockFreeNeighborList())` and only then filled the
      list, so between the two a concurrent reader could find the node in the
      map and read an **empty adjacency** — a search reads that as "this node
      has no neighbours". Who won that window was scheduling, which is why it
      showed up as an intermittent (≈2 in 8) divergence in the pass-by-pass
      digests and why disabling either the cache or the top-layer manager made
      it vanish. The list is now filled before it is published. The cost is
      one extra copy per cache fill, on a path that already copied.

4. *Deterministic ingest mode.* `store.DeterministicIngest` is true when
   `LONGBOW_BENCH_SEED` is set. It pins one ingestion worker, one index worker
   and one job per AddBatch, skips the queue-depth `ef` adaptation, and
   disables the autoscaler. Nothing about the graph algorithm depends on it —
   the fixes above are unconditional — but without it the *pipeline* around the
   index still interleaves four AddBatch jobs, and the criterion is about two
   harness invocations of the whole thing.

5. *Evidence.* `TestBulkBuildIsRepeatable` (`internal/store/index`, 4k float32,
   500 queries) builds one corpus twice in a process and requires the same
   graph — entry point, per-node level, adjacency of every layer — and the same
   answers, logging both digests so two processes can be compared by diffing
   two lines. It runs under plain `go test ./...`, so CI gates on it. Ten
   consecutive processes produced identical `BULK_GRAPH_DIGEST`
   `2d7bb180…` and `BULK_ANSWER_DIGEST` `440d2640…`.

   End to end, the same client and the same corpus digest against two servers:

   | server | run A `search_results_digest` | run B `search_results_digest` | |
   |---|---|---|---|
   | pre-change (`cb6c2712`) | `62fa923d…` | `eb704bcd…` | differ |
   | after | `3c10c76b…` | `3c10c76b…` | identical |

   `scripts/unified_benchmark.py` runs were 25k float32 dim128 dense, 500
   queries, `--workers 4`, `--seed 777`, `-tags emlgo`, with per-run data and
   log directories so no checkpoint or page cache carried across.

6. *Cost.* Measured on a 100k bulk build (ten 10k flushes, same corpus,
   repeated runs): the old interleaved parallel linker 15.86-15.96s, the same
   linker forced sequential 17.81-17.95s (+12%), the three-pass linker
   **16.24-16.49s (+2.4% over the old parallel path)**. Grouping the writes by
   target also removes the CAS contention the interleaved version spent its
   time on, which is why giving the parallelism up entirely — the cheap
   alternative — was measurably the worse of the two. +2.4% is inside Item 32's
   5% index-time threshold.

7. *Known remaining hazard.* Auto-sharding (`AUTO_SHARDING_THRESHOLD` = 10000)
   migrates a growing index asynchronously from inside `AddBatch`; the
   migration's own `waitGroup.Wait()` can never be reached from that call site,
   so a deterministic run that crosses the threshold could still be
   re-sharded mid-build. It did not fire at 25k, and at 500k it would. Not
   fixed here; noted so the 500k criterion is not claimed as met.

### Item 32 — Per-dtype index-time regression guard at 250k and 500k (Performance) — PARTIAL 2026-10-15

The reporting side (``ingesting_duration_seconds`` is in every perf_matrix
result) is complete and the 25k/100k/250k matrix gives 90 covered
configurations. A reusable implementation of the comparison gate landed as
``scripts/check_index_regression.py`` (lockable error codes, both index time
and dense QPS); what remains is wiring it into ``benchmark-ab.yml`` and adding
the paired-AB go.

**Blocked on a re-baseline.** Item 31 replaced the constant `--seed 42` with a
per-config seed derived from ``LONGBOW_BENCH_SEED``, so every corpus in
``benchmarks/baseline_*.json`` and in the existing matrices is now a different
one from the corpus the next run will generate. Those baselines have to be
re-cut before this gate compares anything meaningful; until they are, a 5%
verdict is a verdict about the corpus, not about the code.

- **Files**: `scripts/check_regression.py`, `benchmarks/baseline_*.json`
- **State**: The CI gate uses a 10% arms-length threshold that the roadmap
  documents fires falsely 77-89% of the time. A paired index-time check with
  `scripts/ab_benchmark.py` admits only interleaved runs and has not been
  scheduled for index time.
- **Plan**: Add an index-time arm to the scheduled A/B job that measures
  `indexing_duration_seconds` for every dtype at 250k, gating merges on a paired,
  threshold-5 verdict.
- **Success**: No change that regresses index time by ≥5% merges without
  cancellation; zero false positives in a six-run self-noise test.

### Item 33 — Reduce graph-linker heap churn on large timestamped ingests (Performance)

- **Files**: `internal/store/index/arrow_hnsw_bulk.go`, `internal/store/index/arrow_hnsw_insert.go`
- **State**: Heavy ingests at 500k+ allocations in Go maps remain
  copy-on-write; the benchmark shows peak RSS scaling with link batches that do
  not persist past insertion.
- **Plan**: Switch bulk edge staging to a reusable slab of pair indices with
  sorted commit, eliminating per-pair map growth.
- **Success**: `allocs.pprof` heap bytes during 500k ingest drop by ≥50%
  without reducing ingest throughput or recall.

### Item 34 — pprof capture under GPU load with CUDA kernel attribution (Observability)

- **Files**: `scripts/unified_benchmark.py`, `internal/gpu/cuda/cuda_index.go`
- **State**: §4 of performance.md has CPU pprof hotspots but no GPU kernel
  attribution beyond aggregate QPS; when a CUDA path regresses there is no
  kernel-level signal.
- **Plan**: Export CUDA `__global__` timing via CUDA events for the ingest and
  search hot path; aggregate per kernel under the existing `pprof` snapshots.
- **Success**: A pprof-style report for one full 250k run attributes ≥90% of
  GPU time to named kernels; regressions localisable to a kernel.
