# Longbow Roadmap & Optimization Plan

Derived from 2026-09-22 codebase audit. See [performance.md](performance.md) for benchmark data.

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
