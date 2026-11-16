# Longbow Performance Roadmap (Remaining Work)

Feature parity (Items 1–15) and BM25 arena/WAND optimizations are complete.
This document tracks the remaining performance P0 blockers identified in
`docs/performance.md` (2026-11-07 matrix review). Each task lists the
subtasks required to close it. Tasks are ordered by expected impact.

---

## P0-1: TurboQuant Index Time (429s at 500k; polar reconstruction 47.8% of distance time)

### Subtasks
- [ ] P0-1.1 Profile polar reconstruction to confirm Sincos LUT hit rate and hot loops.
- [ ] P0-1.2 Batch reconstruction across multiple candidates to amortize kernel setup.
- [ ] P0-1.3 Add Sincos LUT for angle reconstruction (if not already hit).
- [ ] P0-1.4 Validate numerical parity vs scalar reference.
- [ ] P0-1.5 Benchmark index time before/after.

## P0-2: Complex128 Throughput & Memory (951 QPS, 11.5 GB RSS at 500k/384d)

### Subtasks
- [x] P0-2.1 Early-exit diversity checks in selectNeighborsComplex128.
- [x] P0-2.2 Parallel tiled complex128 batch kernels.
- [x] P0-2.3 Float64 slab zero-copy extraction.
- [ ] P0-2.4 Reduce memory footprint (chunked loading / avoid full materialization).
- [ ] P0-2.5 (Optional) Zero-copy Float32→complex128 extraction (requires API change).

## P0-3: Float64 Index Time (433.5s at 500k/384d)

### Subtasks
- [x] P0-3.1 Early-exit diversity checks in selectNeighborsFloat64.
- [ ] P0-3.2 Batched selectNeighbors for float64.
- [ ] P0-3.3 Profile selectNeighbors and graph build.

## P0-4: Geo Search (12–35 QPS at 500k)

### Subtasks
- [ ] P0-4.1 Add annulus cell pruning to Quadtree traversal.
- [ ] P0-4.2 Add annulus cell pruning to MortonGrid traversal.
- [ ] P0-4.3 Add radius-aware QueryBox/QueryRadius methods.
- [ ] P0-4.4 Benchmark geo search before/after.

## P0-5: Uint32 Dense Search (518 QPS vs ~3,000+)

### Subtasks
- [x] P0-5.1 Add 128-dim high-dim uint32 kernel.
- [x] P0-5.2 Add AVX2 kernel benchmark.
- [ ] P0-5.3 Profile AVX2 assembly kernel.
- [ ] P0-5.4 Compare against int32/float32 dispatch.

## P0-6: Filtered String Fast Path for Complex Types (436 QPS at 500k/384d)

### Subtasks
- [ ] P0-6.1 Profile predicate evaluation during complex search.
- [ ] P0-6.2 Use dictionary codes for string filters in complex path.
- [ ] P0-6.3 Benchmark filtered search before/after.

## P0-7: Memory Cap Enforcement

### Subtasks
- [x] P0-7.1 Memory enforcement (checkMemoryBeforeWrite, eviction).
- [ ] P0-7.2 Validate cap under load tests.
