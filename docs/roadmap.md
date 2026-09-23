# Longbow Roadmap & Optimization Plan

Derived from 2026-09-22 codebase audit and benchmark suite. See [performance.md](performance.md) for full benchmark data.

---

## Completed

| Date | Item | Impact |
|------|------|--------|
| 2026-09-22 | Complex64/128 ComputeBatch double-pass fix | P0 perf fix |
| 2026-09-22 | resolveInternalID O(n) → O(1) reverse index | Query latency |
| 2026-09-22 | Silent error swallowing in fallback distance path | Correctness |
| 2026-09-22 | AVX-512 Cosine/Dot batch dispatch wiring | 4-8x batch throughput |
| 2026-09-22 | Fallback distBatchComputer allocation removal | GC pressure |
| 2026-09-22 | LockFreeHNSW.Add() proper graph traversal | Recall |
| 2026-09-22 | DiskGraph header TQOffset double-read | Correctness |
| 2026-09-22 | fmt.Printf → slog in production paths | Observability |
| 2026-09-22 | TPU gated behind //go:build tpu | Build complexity |
| 2026-09-22 | Design comments moved to docs/, dead code removed | Token efficiency |
| 2026-09-22 | AdaptiveIndex delegation boilerplate reduction | Legibility |
| 2026-09-22 | simd.go split into 7 focused files (1746 → 737 + 6) | Token efficiency |
| 2026-09-22 | learned_index.go split into 6 files (1826 → 268 + 5) | Token efficiency |
| 2026-09-22 | sharded_hnsw.go split into 3 files (1596 → 867 + 2) | Token efficiency |
| 2026-09-22 | distance_resolvers.go generics (456 → 131 lines, 71% reduction) | Duplication |
| 2026-09-22 | Vector type-switch dispatch consolidation (~250 lines eliminated) | Duplication |
| 2026-09-22 | AdaptiveIndex 20 verbose delegation methods simplified | Legibility |
| 2026-09-22 | 100k/250k benchmark suite (8 configs × 8 dtypes × 5 search modes × 2 disk modes) | Baseline data |

---

## Performance Observations (2026-09-22 benchmark)

### Critical regressions to investigate

| Priority | Config | dtype | search | delta | root cause hypothesis |
|---|---|---|---|---|---|
| P0 | CPU emlgo 250k Disk | uint8 | hybrid | -82% | Memory/spill interaction — uint8 raw size + HNSW overhead may trigger aggressive spill that destroys hybrid search performance |
| P0 | CPU emlgo 100k NoDisk | complex64 | hybrid | -64% | emlgo code path regression for complex64 distance computation |
| P1 | GPU emlgo 100k Disk | complex128 | graphrag | -74% | Disk mode + emlgo interaction — complex128 benefits from emlgo NoDisk (+358%) but reverses with disk |
| P1 | CPU emlgo 100k Disk | turboquant | temporal | -69% | Temporal version indexing regression under emlgo with disk |
| P1 | GPU std 250k NoDisk | complex64 | dense | 294 QPS | Very low QPS suggests OOM or spill cliff — std build may need memory tuning at 250k |
| P2 | CPU emlgo 100k NoDisk | complex128 | graphrag | -60% | Consistent complex type regression in NoDisk mode |
| P2 | CPU emlgo 250k NoDisk | float32 | dense | -37% | float32 dense regression at scale |

### Wins to preserve

| Config | dtype | search | delta | notes |
|---|---|---|---|---|
| GPU emlgo 100k NoDisk | complex128 | graphrag | +358% | Largest single gain in the suite |
| GPU emlgo 100k Disk | uint8 | hybrid/graphrag | +197-201% | Disk + emlgo synergy for uint8 |
| CPU emlgo 100k Disk | complex128 | dense/hybrid/graphrag | +300-352% | Consistent massive complex128 speedup |
| GPU emlgo 100k NoDisk | turboquant | all | +10-26% | Broad improvement |
| CPU std 250k Disk | uint8 | dense/hybrid/graphrag | +130-164% | Disk mode enables much better uint8 at scale |

### Disk mode observations

| Finding | Detail |
|---|---|
| CPU int8/float16 at 100k | Disk hurts 20-38% — spill overhead exceeds benefit |
| CPU uint8 at 250k | Disk helps +130-164% — reduces memory pressure, better cache behavior |
| GPU complex types at 250k | Disk helps +85-1297% — prevents OOM-induced performance cliffs |
| GPU turboquant at 250k | Disk helps +33-64% — same OOM prevention pattern |
| float32 | Mostly disk-neutral (±10%) across all configs |

---

## Remaining Performance Work

### P0: Investigate uint8 disk spill regression (CPU emlgo 250k hybrid -82%)

The uint8 dtype with disk enabled at 250k shows catastrophic regression under emlgo.
Hypothesis: emlgo's memory allocator interacts poorly with auto-spill threshold for uint8
vectors, causing excessive spill/read cycles during hybrid search. Profile the spill path
under uint8 load.

### P0: Investigate complex64/128 NoDisk emlgo regression

complex64 loses 51-65% across sparse/hybrid/graphrag at 100k NoDisk under emlgo.
complex128 loses 27-60% in similar conditions. But complex128 gains +300-358% with disk.
This suggests emlgo's distance computation path for complex types is optimized for
disk-access patterns but regresses for in-memory access. Needs kernel-level profiling.

### P1: Investigate GPU std complex64/128 250k NoDisk cliff

GPU std build shows very low QPS (294-521) for complex types at 250k NoDisk, while
disk mode returns to normal (672-962). This suggests the std build hits an OOM threshold
at 250k that triggers ungraceful degradation. The emlgo build avoids this (918-1020 QPS).

### P2: CPU emlgo float32 dense regression at scale

float32 dense drops -37% under emlgo at 250k NoDisk, worse than at 100k (-5%). This
regression worsens with scale, suggesting an algorithmic overhead that compounds.

---

## Infrastructure

| Item | Status |
|------|--------|
| Dependabot auto-merge | Done |
| Tests for 5 untested packages | Done |
| Dockerfile.tpu | Done |
| docs/simd-architecture.md | Done |
| File splitting (3 largest files) | Done |
| Generics refactor (distance_resolvers) | Done |
| Vector dispatch consolidation | Done |
| AdaptiveIndex cleanup | Done |
| 100k/250k benchmark with disk modes | Done |
