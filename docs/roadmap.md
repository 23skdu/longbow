# Longbow Roadmap & Optimization Plan

This document tracks open work items derived from benchmark analysis. Completed items are removed; see git history for historical context.

---

## Open Issues

Derived from 2026-09-11 A/B benchmark analysis. See [performance.md](performance.md) for raw data.

| Priority | Issue | Impact |
|----------|-------|--------|
| P0 | CPU complex64 dense 500k | -38% regression |
| P0 | CPU complex128 dense 500k | P99 75ms tail latency |
