# Longbow Roadmap (Remaining Work)

This document tracks the remaining work to scale Longbow past 1,000,000
vectors per dataset across all 21 search modes and 15 data types. Completed
items (P0 performance, OIDC federation, DevSecOps, geo/temporal/text
candidate caps, TurboQuant kernels/LUT, predicate pushdown, disk/tiered
storage, spatial hash grid, temporal sorted index, text trigram index,
filtered predicate-aware entry points, parallel filter evaluation,
complex/TurboQuant scalability tests, geo LRU result cache) have been removed.

See `docs/performance.md` for benchmark results and baselines.

---

## Summary of Remaining Work

| ID | Area | Priority | Status |
|---|---|---|---|
| P4-5 | Filtered: P99 < 50ms at 1M | HIGH | Open |
| P5-2 | Complex: zero-copy Float32→complex128 (API change) | HIGH | Open |
| P5-5 | Complex: RSS < 20 GB at 500k | HIGH | Open |
| P7-1 | uint16/uint32: profile slow kernel | MEDIUM | Kernels exist |
| P7-3 | uint16/uint32: parity benchmark | MEDIUM | Open |
| P7-4 | uint16/uint32: scalability test | MEDIUM | Open |
| P8-3 | Memory: cap enforcement + graceful degradation | MEDIUM | Open |
| P8-4 | Memory: checkMemoryBeforeWrite on all ingest paths | MEDIUM | Open |
| P8-5 | Memory: 1M complex128 RSS < 20 GB with disk spill | MEDIUM | Open |
| P8-6 | Memory: scalability test | MEDIUM | Open |
| P9-1 | HNSW quality: reachability ≥95% at 500k/1M | MEDIUM | Open |
| P9-2 | HNSW quality: tune addbatch_inflight + BulkChainLinks | MEDIUM | Open |
| P9-3 | HNSW quality: graph repair agent after bulk insert | MEDIUM | Open |
| P9-4 | HNSW quality: recall @10 at 1M vs brute force | MEDIUM | Open |
| P9-5 | HNSW quality: scalability test | MEDIUM | Open |
| P10-1 | Filtered tail: bitmap intersection | MEDIUM | Open |
| P10-5 | Filtered tail: P99 < 50ms at 1M | MEDIUM | Open |
| P11-1 | Concurrency: dynamic worker scaling | LOW | Open |
| P11-2 | Concurrency: NUMA-aware allocation | LOW | Open |
| P11-3 | Concurrency: request batching | LOW | Open |
| P11-4 | Concurrency: 8-worker AVX-512 benchmark | LOW | Open |
| P0-2.5 | (Optional) Zero-copy Float32→complex128 extraction | LOW | Open |

---

## Remaining Optional Items

- [ ] P0-2.5 (Optional) Zero-copy Float32→complex128 extraction (requires API change; currently uses zero-allocation worker-local scratch widening in `extractVectorComplex128Into`).

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

---

## Remaining Work (Detailed Subtasks)

### P4/P10 (Filtered) — Remaining

- [ ] P4-5 Benchmark filtered at 500k/1M; target P99 < 50ms.
- [ ] P10-1 Filtered tail: bitmap intersection (multi-predicate).
- [ ] P10-5 P99 < 50ms at 1M.

### P5 (Complex) — Remaining

- [ ] P5-2b Optional API change for Float32→complex128 (P0-2.5).
- [ ] P5-5 RSS < 20 GB at 500k.

### P6 (TurboQuant) — Remaining

- [ ] P6-5b Benchmark 5× speedup at 200k/500k.

### P7 (uint dispatch) — Remaining

- [ ] P7-1 Profile uint16/uint32 slow kernel.
- [ ] P7-3 Parity benchmark.
- [ ] P7-4 Add `TestUintDispatchScalability`.

### P8 (Memory) — Remaining

- [ ] P8-3 Memory cap enforcement + graceful degradation.
- [ ] P8-4 `checkMemoryBeforeWrite` on all ingest paths.
- [ ] P8-5 1M complex128 RSS < 20 GB with disk spill.
- [ ] P8-6 Add `TestMemoryScaling`.

### P9 (HNSW quality) — Remaining

- [ ] P9-1 Reachability ≥95% at 500k/1M.
- [ ] P9-2 Tune `addbatch_inflight` + `BulkChainLinks`.
- [ ] P9-3 Graph repair agent after bulk insert.
- [ ] P9-4 Recall @10 at 1M vs brute force.
- [ ] P9-5 Add `TestHNSWQualityAtScale`.

### P11 (Concurrency) — Remaining

- [ ] P11-1 Dynamic worker scaling.
- [ ] P11-2 NUMA-aware allocation.
- [ ] P11-3 Request batching.
- [ ] P11-4 8-worker AVX-512 benchmark.

### P0-2.5 (Optional) — Remaining

- [ ] P0-2.5 Zero-copy Float32→complex128 extraction (API change).
