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
