# Longbow Roadmap (Performance & Scale)

This document tracks the **remaining** performance and scalability work for
Longbow. Completed items have been removed; their results are recorded in
`docs/performance.md`, `docs/testplan.md`, and the git history.

The removed work covered the 1M complex128 memory-profile hot spots (zero-allocation neighbor updates, batch-scratch reuse, copy-on-write graph cloning, cached distance computers), SIMD parity and zero-allocation kernels across all dtypes, dynamic worker scaling, NUMA-aware allocation, request batching, multi-predicate bitmap intersection, HNSW bulk quality (reachability, recall @10, post-bulk repair, and `addbatch_inflight` / `BulkChainLinks` tuning), memory-scaling tests, P0 performance, OIDC Federation, DevSecOps, geo/temporal/text candidate caps, TurboQuant kernels/LUT, predicate pushdown, disk/tiered storage, spatial hash grid, temporal sorted index, text trigram index, filtered predicate-aware entry points, parallel filter evaluation, geo LRU result cache, and the 50k-1M scalability tests for filtered/complex/TurboQuant/uint16.

See `docs/performance.md` for benchmark results and baselines, and
`docs/testplan.md` for the full search-mode and dtype matrix.

---

## Remaining Work

| ID | Area | Priority | Status |
|---|---|---|---|
| C4 | Validation: full 1M x 15-dtype QPS/recall matrix across all 21 search modes | LOW | Partial |

### C4 — Full 1M validation matrix

- [ ] Run the complete 1M-vector matrix across all 15 dtypes and all 21 search modes.

  Already covered:
  - 1M dim-128 QPS for float32/int8/uint8 with all 21 modes
    (`benchmarks/baseline_cpu_large_20261116.json`, `docs/performance.md` §0d).
  - Recall @10 vs brute force at 1M (`TestHNSWRecallAt10VsBruteForce`).
  - All 15 dtypes x all 21 modes at 50k/100k/200k (`docs/performance.md` §0a).

  Remaining: 1M runs for the other 12 dtypes. The 14 GiB memory ceiling is the
  binding constraint; use auto-spill disk and 4 pinned workers as in §0d.

  Exit criteria: 0 failures, 0 exhausted configs, and 0 zero-QPS modes across all
  15 dtype configs at 1M, with recall @10 reported alongside QPS.

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

- `benchmarks/baseline_cpu_20261116.json` (50k/100k/200k, dim 128 & 384)
- `benchmarks/baseline_cpu_768_20261116.json` (50k/100k/200k, dim 768)
- `benchmarks/baseline_cpu_large_20261116.json` (500k/1M, dim 128)
- `benchmarks/baseline_cpu.json`, `baseline_100k.json`, `baseline_250k.json`, `baseline_25k.json` (legacy)
