# Longbow Roadmap & Optimization Plan

This document tracks open work items derived from benchmark analysis. Completed items are removed; see git history for historical context.

---

## Open Issues

None currently.

---

## Recently Resolved

| Date | Issue | Fix |
|------|-------|-----|
| 2026-09-22 | CPU complex64 dense 500k (-38% regression) | Fixed ComputeBatch double-pass; added SIMD batch dispatch via float32 reinterpret-cast |
| 2026-09-22 | CPU complex128 dense 500k (P99 75ms tail) | Fixed ComputeBatch double-pass; added per-vector SIMD dispatch via float64 reinterpret-cast |
| 2026-09-22 | resolveInternalID O(n) linear scan | Added externalID→internalID reverse index for O(1) lookup |
| 2026-09-22 | Silent error swallowing in fallback distance path | Propagate errors from entry point distance computation |
| 2026-09-22 | AVX-512 Cosine/Dot batch not wired | Connected existing AVX-512 batch implementations in dispatch tables |
| 2026-09-22 | Fallback distBatchComputer per-call allocation | Reuse caller's dst buffer instead of allocating per call |
| 2026-09-22 | LockFreeHNSW.Add() star topology | Implemented proper greedy search + select-M heuristic |
| 2026-09-22 | DiskGraph TQOffset double-read | Only read TQOffset for version >= 5; added bounds checks |
| 2026-09-22 | fmt.Printf in production paths | Replaced with log/slog structured logging |
| 2026-09-22 | TPU subsystem CGo in default builds | Gated behind //go:build tpu; added Dockerfile.tpu |
| 2026-09-22 | CandidateHeap design discussion comments | Cleaned 70-line comment block, unified with heap.Pop |
| 2026-09-22 | AdaptiveIndex delegation boilerplate | Added activeIndex() helper, simplified 10+ methods |
| 2026-09-22 | Design comments in code files | Trimmed to brief summaries, moved rationale to docs/ |
| 2026-09-22 | Dead code cleanup | Removed commented-out SQ8 code, unused vars, duplicate comments |
