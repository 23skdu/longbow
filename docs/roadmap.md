# Longbow Roadmap

**Last updated**: 2026-10-10  
**Status**: Canonical list of **open** work. Completed work is validated, committed, and tracked in git history.

---

## 1. Priority Overview

| Pri | # | Item | Area | Depends on |
|:---|:---|:---|:---|:---|
| **P1** | 1 | Locate complex128 GPU bottleneck & verify uint16 GPU path | GPU | — |
| **P2** | 2 | ARM NEON dequant kernel & remaining scalar SQ8 fallbacks | SIMD | — |
| **P2** | 3 | Pooled row buffers in `DiskVectorStore` | Storage | — |

```mermaid
graph TD
    I1["1: complex128 GPU Bottleneck"]
    I2["2: NEON Dequant Kernel"]
    I3["3: Pooled Disk Row Buffers"]
```

**Recently Completed (2026-10-10)**:
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

## 2. P1 — GPU Infrastructure

### Item 1: Locate complex128 GPU Bottleneck & Verify uint16 GPU Path

- **Files**: [cuda_index.go](file:///home/rsd/REPOS/longbow/internal/gpu/cuda/cuda_index.go)
- **State**: `complex128` is the slowest GPU dtype (471 dense QPS at 100k, ~5x slower than CPU). In contrast, `uint16` reports 3,934 QPS.
- **Plan**: Trace kernel execution and memory transfers using NVIDIA Nsight Systems; optimize memory coalescing and reduction for 128-bit complex elements on CUDA.
- **Success**: Eliminate GPU bottleneck for `complex128` and document actual kernel dispatch path for `uint16`.

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

