# Longbow Roadmap (Performance & Scale)

This document tracks the **remaining** performance and scalability work for
Longbow. Completed items have been removed; their results are recorded in
`docs/performance.md`, `docs/testplan.md`, and the git history.

The removed work covered the 1M complex128 memory-profile hot spots (zero-allocation neighbor updates, batch-scratch reuse, copy-on-write graph cloning, cached distance computers), SIMD parity and zero-allocation kernels across all dtypes, dynamic worker scaling, NUMA-aware allocation, request batching, multi-predicate bitmap intersection, HNSW bulk quality (reachability, recall @10, post-bulk repair, and `addbatch_inflight` / `BulkChainLinks` tuning), memory-scaling tests, P0 performance, OIDC Federation, DevSecOps, geo/temporal/text candidate caps, TurboQuant kernels/LUT, predicate pushdown, disk/tiered storage, spatial hash grid, temporal sorted index, text trigram index, filtered predicate-aware entry points, parallel filter evaluation, geo LRU result cache, the 50k-1M scalability tests for filtered/complex/TurboQuant/uint16, and the complete 1M × 15-dtype × 21-mode validation matrix (C4, `benchmarks/baseline_cpu_1m_all15_20261121.json`, `docs/performance.md` §0e, Recall@10 = 0.8600 at 1M).

See `docs/performance.md` for benchmark results and baselines, and
`docs/testplan.md` for the full search-mode and dtype matrix.

---

## Remaining Work

All planned Performance & Scale roadmap items are **complete** (0 remaining items).

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
| Release versions | `make check-version` | pass |

## Baselines

- `benchmarks/baseline_cpu_1m_all15_20261121.json` (1M, dim 128, all 15 dtypes × 21 modes + Recall@10 = 0.8600)
- `benchmarks/baseline_cpu_20261116.json` (50k/100k/200k, dim 128 & 384)
- `benchmarks/baseline_cpu_768_20261116.json` (50k/100k/200k, dim 768)
- `benchmarks/baseline_cpu_large_20261116.json` (500k/1M, dim 128)
- `benchmarks/baseline_cpu.json`, `baseline_100k.json`, `baseline_250k.json`, `baseline_25k.json` (legacy)
