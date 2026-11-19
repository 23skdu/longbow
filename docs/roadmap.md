# Longbow Roadmap (Performance & Scale)

This document tracks the next performance and scalability work for Longbow,
informed by a 1M-vector complex128 memory profile
(`go test -memprofile` over `TestComplexRSS1M`: 4.02 GB RSS, 122 GB
allocations). Completed items from the previous roadmap (P0 performance,
OIDC federation, DevSecOps, geo/temporal/text candidate caps, TurboQuant
kernels/LUT, predicate pushdown, disk/tiered storage, spatial hash grid,
temporal sorted index, text trigram index, filtered predicate-aware entry
points, parallel filter evaluation, geo LRU result cache, and the 50k-1M
scalability tests for filtered/complex/TurboQuant/uint16) have been removed.

See `docs/performance.md` for benchmark results and baselines.

---

## Memory-Profile Findings (1M complex128)

| Hot spot | Alloc space | Notes |
|---|---|---|
| `FlatAdjacency.UpdateNeighbors` | 66.7 GB (54.6%) | Largest single source; neighbor slice growth/copy |
| `FlatAdjacency.getNeighborsWithGenInner` | 25.0 GB (20.5%) | Per-traversal neighbor materialization |
| `EuclideanDistanceComplex128Batch` | 7.2 GB (5.9%) | Per-batch scratch allocation |
| `ArrowHNSW.resolveHNSWComputer` | 6.4 GB (5.3%) | Computer resolution per batch |
| `GraphData.Clone` | 4.1 GB (3.4%) | Full graph copy on bulk insert |
| `ArrowHNSW.linkRemainingNodes` | 3.5 GB (2.9%) | Residual link work |
| `ArrowHNSW.measureBulkGraphQuality` | 1.7 GB (1.4%) | Quality sampling allocations |

These drive the priorities below.

---

## Summary of Remaining Work

| ID | Area | Priority | Status |
|---|---|---|---|
| A1 | FlatAdjacency: zero-allocation neighbor updates | CRITICAL | Done |
| A2 | FlatAdjacency: reuse neighbor buffers in traversal | CRITICAL | Done |
| A3 | Distance kernels: reuse batch scratch (complex128/float64) | HIGH | Done |
| A4 | HNSW: avoid sequential fallback in bulk insert | HIGH | Done |
| A5 | GraphData: copy-on-write Clone (zero-copy bulk insert) | HIGH | Done |
| A6 | ArrowHNSW: cache resolved distance computer | HIGH | Done |
| A7 | Memory: 1M complex128 RSS < 20 GB with disk spill (validation) | HIGH | Done |
| B1 | SIMD: AVX-512/AVX2 kernels for uint16/uint32 parity + bench | MEDIUM | Done |
| B2 | SIMD: complex128 batch distance kernel (no alloc) | MEDIUM | Done |
| B3 | SIMD: float16/int8 quantized distance kernels | MEDIUM | Done |
| B4 | SIMD: parity benchmarks across all dtypes | MEDIUM | Done |
| C1 | Concurrency: dynamic worker scaling | LOW | Done |
| C2 | Concurrency: NUMA-aware allocation | LOW | Done |
| C3 | Concurrency: request batching | LOW | Done |
| C4 | Validation: 1M-vector QPS/recall across all 21 modes & 15 dtypes | LOW | Open |
| D1 | Filtered tail: bitmap intersection for multi-predicate | MEDIUM | Done |
| D2 | HNSW quality: reachability ≥95% at 500k/1M | MEDIUM | Done |
| D3 | HNSW quality: tune addbatch_inflight + BulkChainLinks | MEDIUM | Open |
| D4 | HNSW quality: graph repair agent after bulk insert | MEDIUM | Done |
| D5 | HNSW quality: recall @10 at 1M vs brute force | MEDIUM | Done |
| E1 | Memory: TestMemoryScaling harness | LOW | Done |

---

## Detailed Work Items

### A (Allocation & Memory Efficiency)

- [x] A1 `FlatAdjacency.UpdateNeighbors`: pool/reuse neighbor slices; eliminate per-update heap growth (`TestAddConnectionsBatchReusesPooledBuffers` = 0 allocs).
- [x] A2 `getNeighborsWithGenInner`: return borrowed slices or write into caller-provided buffer (`TestGetNeighborsCombinedManualReusesCallerBuffer` = 0 allocs).
- [x] A3 `EuclideanDistanceComplex128Batch` (and float64/float32/complex64 variants): reuse scratch buffers across calls (`TestComplexBatchZeroAlloc` = 0 allocs).
- [x] A4 Bulk insert: raise descent-hop budget / improve bulk graph quality so sequential fallback is rare (`ce402715`).
- [x] A5 `GraphData.Clone`: copy-on-write / shared `Complex128Magnitudes` backing slice to avoid ~4 GB copy at 1M (`graph_data_clone.go`).
- [x] A6 `resolveHNSWComputer`: reuse pooled `ArrowSearchContext` computer slots (`TestResolveHNSWComputerReusesContextSlots` = 0 allocs).
- [x] A7 Validate 1M complex128 RSS < 20 GB with disk spill (`TestComplexRSS1M`, `TestComplexRSSDiskSpill`).

### B (SIMD & Kernels)

- [x] B1 Dedicated AVX2/AVX-512 kernels for uint16/uint32 + parity benchmark (`int16_kernels_amd64.s`, `simd_amd64.go`, `TestUint16Uint32SIMDParity`, `BenchmarkUint16Uint32Kernels`).
- [x] B2 Complex128 batch distance kernel with no allocations (`internal/simd/complex_batch.go`, `TestComplexBatchZeroAlloc`).
- [x] B3 Float16/int8 quantized distance kernels (AVX2 + AVX-512: `distance_fp16_avx512fp16_amd64.s`, `int8_kernels_avx512_amd64.s`, `TestFloat16Int8QuantizedParity`).
- [x] B4 Parity benchmarks across all dtypes (`TestSIMDAllDTypesParity`, `BenchmarkSIMDAllDTypesParity` covering float32/64, complex64/128, uint8/16/32, int8/16, float16).

### C (Scaling & Concurrency)

- [x] C1 Dynamic worker scaling based on load (`internal/autoscale/scaler.go`, `VectorStore.AdjustWorkerCounts`).
- [x] C2 NUMA-aware allocation for large heaps (`internal/memory/numa_allocator*.go`, `numa_test.go`).
- [x] C3 Request batching for small queries (`MicroBatchCoalescer` in `internal/store/batch_search.go`, `TestMicroBatchCoalescer`).
- [ ] C4 1M-vector QPS/recall validation across all 21 search modes and 15 dtypes.

### D (HNSW Quality & Filtered Tail)

- [x] D1 Filtered tail: smallest-first roaring bitmap intersection for multi-predicate (`IntersectFilterBitmaps`, `buildIntersectedFilterMask`, `applyExactFilters`, `TestIntersectFilterBitmaps_MultiPredicate`).
- [x] D2 Reachability ≥95% at 500k/1M (`TestHNSWReachabilityAtScale` in `internal/store/index/hnsw_scale_quality_test.go`).
- [ ] D3 Tune `addbatch_inflight` + `BulkChainLinks`.
- [x] D4 Graph repair agent after bulk insert (`RepairAgent.TriggerRepair` invoked post-bulk insert in `addBatchBulkInternal`, `TestRepairAgent_TriggerAfterBulkInsert`).
- [x] D5 Recall @10 at 1M vs brute force (`TestHNSWRecallAt10VsBruteForce` in `internal/store/index/hnsw_scale_quality_test.go`).

### E (Memory Testing)

- [x] E1 Add `TestMemoryScaling` harness (cap enforcement + graceful degradation + RSS guard in `internal/store/memory_scaling_test.go`).

---

## Verification Artifacts

| Gate | Command | Status |
|---|---|---|
| Build | `make build` | pass |
| Vet | `make vet` | pass |
| Unit tests | `make test-short` | pass |
| Static check | `make staticcheck` | 0 issues |
| Dead code | `make deadcode` | pre-existing only |
| Observability | `make observability-check` | pass |
| Accuracy | `make accuracy-gate` | pass |
| Oracle | `make oracle-gate` | pass |

## Baselines

- `benchmarks/baseline_cpu_20261116.json` (50k/100k/200k, dim 128 & 384)
- `benchmarks/baseline_cpu_768_20261116.json` (50k/100k/200k, dim 768)
- `benchmarks/baseline_cpu_large_20261116.json` (500k/1M, dim 128)
- `benchmarks/baseline_cpu.json`, `baseline_100k.json`, `baseline_250k.json`, `baseline_25k.json` (legacy)
