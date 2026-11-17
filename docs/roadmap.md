# Longbow Roadmap (Remaining Work)

This document tracks the remaining work to scale Longbow past 1,000,000
vectors per dataset across all 21 search modes and 15 data types. Completed
items (P0 performance, OIDC federation, DevSecOps, geo/temporal/text
candidate caps, TurboQuant kernels/LUT, predicate pushdown, disk/tiered
storage) have been removed.

See `docs/performance.md` for benchmark results and baselines.

---

## Summary of Remaining Work

| ID | Area | Priority | Status |
|---|---|---|---|
| P1-4 | Geo: spatial hash grid for radius candidates | CRITICAL | Implemented |
| P1-5 | Geo: spatial cell cache (LRU) | CRITICAL | Candidate cap covers hot path |
| P1-6b | Geo: benchmark + scalability test (≥500 QPS at 1M) | CRITICAL | Open |
| P2-4b | Temporal: benchmark + scalability test (≥1000 QPS at 1M) | CRITICAL | Open |
| P3-1 | Text: trigram inverted index for fuzzy/contains | HIGH | Open |
| P3-2 | Text: SIMD-vectorized BM25 scoring | HIGH | Open |
| P3-3 | Text: top-K heap with early termination | HIGH | Open |
| P3-4 | Text: shard corpus cache by partition | HIGH | Open |
| P3-5b | Text: benchmark + scalability test (≥500 QPS at 1M) | HIGH | Open |
| P4-1 | Filtered: bitmap intersection for multi-predicate | HIGH | Implemented (filter_mask.go) |
| P4-3 | Filtered: predicate-aware entry point selection | HIGH | Implemented (hnsw_predicate.go) |
| P4-4 | Filtered: shard filter evaluation | HIGH | Implemented (parallel_filter.go) |
| P4-5 | Filtered: P99 < 50ms at 1M | HIGH | Open |
| P4-6 | Filtered: scalability test | HIGH | Open |
| P5-1 | Complex: tiled batch kernels | HIGH | Implemented (P0-2.2) |
| P5-2 | Complex: zero-copy Float32→complex128 (API change) | HIGH | Open |
| P5-3 | Complex: verify TurboQuant auto-standardization at ≥500k | HIGH | Open |
| P5-4 | Complex: chunked loading (disk-backed) | HIGH | Implemented (mmap/disk store) |
| P5-5 | Complex: RSS < 20 GB at 500k | HIGH | Open |
| P5-6 | Complex: scalability test | HIGH | Open |
| P6-5 | TurboQuant: benchmark 5× speedup | HIGH | Implemented (kernels/LUT) |
| P6-6 | TurboQuant: scalability test | HIGH | Open |
| P7-1 | uint16/uint32: profile slow kernel | MEDIUM | Kernels exist |
| P7-2 | uint16/uint32: dedicated AVX2 kernels | MEDIUM | Implemented |
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
| P10-2 | Filtered tail: push filters into traversal | MEDIUM | Implemented (hnsw_predicate.go) |
| P10-3 | Filtered tail: predicate-aware entry point | MEDIUM | Open |
| P10-4 | Filtered tail: shard filter evaluation | MEDIUM | Open |
| P10-5 | Filtered tail: P99 < 50ms at 1M | MEDIUM | Open |
| P10-6 | Filtered tail: scalability test | MEDIUM | Open |
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
