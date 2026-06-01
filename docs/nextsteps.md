# Longbow Next Steps — v0.2.1-rc5 Findings & Recommendations (2026-05-31)

## Actionable Stability & Performance Recommendations

---

### P0: Memory Budget & OOM at Scale

1. **[FIXED] 500k vectors ResourceExhausted via admission controller** — Root cause: `EnsureChunk` pre-allocated neighbor arrays (`ChunkSize * MaxNeighbors` = 2MB) for all 16 HNSW layers per chunk, but upper layers (>0) use `PackedNeighbors`/`TopLayerManager` instead. This wasted ~15GB of off-heap tracked memory at 500k nodes (489 chunks × 15 layers × 2MB = 14.7GB), hitting the admission controller's 95% memory threshold. Fix:
   - `EnsureChunk` now pre-allocates only layer 0 neighbors (always needed); upper layers skip pre-allocation
   - `MaxNeighbors` reduced from 512 to 256 (actual fan-out is MMax0=64, so 256 provides 4x headroom)
   - `neighbor_ops.go` callback now reuses `ctx.scratchPool` instead of `make([]uint32)` on every edge addition
   - Verified: 500k int8 dim=128 completes successfully with 53k vec/s ingest, 218 QPS search, no ResourceExhausted

2. **DiskVectorStore integration is incomplete** — `LONGBOW_USE_DISK=1` creates a DiskVectorStore on the Dataset but the HNSW index (`config.UseDisk`) never reads it. The `persistent` parameter in `NewGraphData` is accepted but never stored or acted upon. The disk store is write-only during `BatchAppendArrow`; search still reads from memory. To close the gap:
   - Wire `config.UseDisk` → `BackingGraph` in `NewGraphData`
   - Add `getVectorWithDiskFallback` path that first checks in-memory chunks, then falls back to DiskVectorStore.GetBatch
   - Ensure Clone/Release both reference-count the BackingGraph properly

3. **800k vectors likely still OOM** — While the EnsureChunk fix eliminates ~15GB of off-heap accounting, the admission controller's `physicalMem = heapMem + offHeapMem` formula still counts all slab allocations. At 800k (782 chunks × 1MB = 782MB for layer 0 neighbors + vectors + metadata), total may approach the 18GB limit. Profile and add disk-based neighbor swap if needed.

### P1: Performance Regressions

4. **float16 dense search is 7-15x slower than float32** — At 15k dim=128: float16 dense QPS=4935 vs float32=7347 (1.5x). At 200k dim=128: float16=549 vs float32 projected ~3000 (potentially 5-6x). float16 lacks SIMD kernel (no NEON `FMLAL` or AVX512 `VFMADD132PH`).

5. **complex128 ingest is 3-4x slower than float32** — 15k dim=128: 418k vec/s vs 1.38M vec/s. At 384d: 159k vs 510k. The generic distance path for complex types needs optimization.

6. **Ingest speed drops 20-40x from 15k to 200k** — float16 dim=128: 1.67M vec/s at 15k → 66k vec/s at 200k. This is expected (graph edge building is O(N·log N)) but creates an inconsistent UX. Consider a progress indicator or ETA in the server logs.

### P2: GPU & Platform

7. **Metal benchmarks not yet run** — This current run only covers CPU. Metal (M3 Pro GPU) needs identical parameter sweep to compare GPU vs CPU for float32/int8/uint8.

8. **ancalagon (i7-12650H + RTX 4060) unreachable** — SSH connection times out. Need to resolve network/availability to complete multi-platform matrix (CPU + CUDA).

9. **GPU integer kernels unoptimized** — Prior runs showed CUDA int8 dense QPS at 499 (vs 3742 CPU). Metal int8 similarly slow. Both need proper integer dot-product shaders.

### P3: Instrumentation & Tooling

10. **Pprof retention** — Each benchmark run generates 14 pprof files per config (7 profiles × 2 snapshots). After 48 CPU + 48 Metal runs this is ~1,344 files. Add a cleanup step or auto-aggregate into flamegraphs.

11. **Benchmark result persistence** — JSON checkpoint writes partial results but the final output file only has results from completed configs. Server crashes or OOM kills are detected but mid-run data could be lost if the script itself is killed.

12. **Generate performance report script** — Need to write `scripts/build_perf_report.py` to consume the perf_matrix JSON and produce formatted markdown tables automatically.

---

## Benchmark Run Status (2026-05-31, Commit `2b3e4f3e`)

### Localhost (M3 Pro) — CPU (IN PROGRESS)

| Stage | Status | Configs |
|-------|--------|---------|
| dim=128, count=15k (6 dtypes) | ✅ Done | float16, float32, int8, uint8, complex128, turboquant8 |
| dim=384, count=15k (6 dtypes) | ✅ Done | float16, float32, int8, uint8, complex128, turboquant8 |
| dim=128, count=200k (6 dtypes) | 🔄 Running | float16 done, 5 remaining |
| dim=384, count=200k (6 dtypes) | ⏳ Pending | |
| dim=128, count=500k (6 dtypes) | ⏳ Pending | |
| dim=384, count=500k (6 dtypes) | ⏳ Pending | |
| dim=128, count=800k (6 dtypes) | ⏳ Pending | |
| dim=384, count=800k (6 dtypes) | ⏳ Pending | |

### Localhost (M3 Pro) — Metal (NOT STARTED)

Will follow CPU run sequentially.

### Ancalagon (i7-12650H) — CPU + CUDA (SKIPPED)

Host unreachable via SSH. Will retry once network is restored.

---

## Previously Completed P0 Blockers

- `[x]` **OOM at 500k vectors — memory leak fixes** (commit `2b3e4f3e`): PreAllocate skip, initArenaSafe CAS leak fix, PackedNeighbors uses global OffHeapAllocator, GC tuner triggers SlabPool release at moderate pressure.
- `[x]` **Race condition in GraphData Clone vs Release** (commit `2b3e4f3e`): Added atomic cloneCount spin-wait to prevent use-after-free.
- `[x]` **Multi-Platform Benchmark Run (v0.2.1-rc7)**: Completed CPU + GPU benchmarks on both hosts at dim=128, counts 250k–750k.
- `[x]` **HNSW Index Passthrough**: temporal, geo, graphrag modes query HNSW directly.
- `[x]` **GraphRAG Beam Search**: O(N³) → O(B²·depth) optimization.
- `[x]` **Int16/Uint16 Kernel Fix**: 32x latency improvement.
