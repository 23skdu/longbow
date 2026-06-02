# Longbow Next Steps — Updated 2026-06-02

## Recently Completed

- **OOM at 500k vectors — EnsureChunk pre-allocation fix**: `EnsureChunk` pre-allocates only layer 0 neighbors; upper layers skip. `MaxNeighbors` reduced from 512→256. `neighbor_ops.go` reuses `ctx.scratchPool`. (commit `2b3e4f3e`)
- **Race condition in GraphData Clone vs Release** (commit `2b3e4f3e`): Added atomic cloneCount spin-wait.
- **HNSW Index Passthrough**: temporal, geo, graphrag modes query HNSW directly.
- **GraphRAG Beam Search**: O(N³) → O(B²·depth) optimization.
- **Int16/Uint16 Kernel Fix**: 32x latency improvement.
- **Auto-sharding migration deadlock fix**: `WaitForIndexing` no longer checks `migratingCount` (dataset.go). `AdmitMigration` retry loop has 30s timeout (hnsw_autoshard.go).
- **1M float64 benchmark completed**: 42,515 vec/s upload, 12.32 QPS search (with `AUTO_SHARDING_ENABLED=false`, GOGC=400, ingestion_workers=2).
- **Float64 SIMD gap fully analyzed**: Euclidean/Dot have AVX2 variable-dim but no dim-specific (384/768) float64 kernels. Cosine has zero float64 AVX2. L2Squared reuses Euclidean (wasteful sqrt). 12-step implementation plan built.

## P0: Stability & Memory

1. **800k vectors OOM hazard** — While EnsureChunk fix eliminates ~15GB off-heap, at 800k nodes total memory may approach the 18GB limit. Profile and add disk-based neighbor/vector swap if needed.

2. **DiskVectorStore integration — vector read path** — `config.UseDisk`, `BackingGraph`, and disk graph flush are wired. The missing piece: search reads vectors from memory, not disk. Add `getVectorWithDiskFallback` path (check in-memory chunks first, fall back to `DiskVectorStore.GetBatch`). Currently `DiskVectorStore` handles float32 only — float64 support needed if disk tiering is required for float64 at scale.

## P1: Performance Regressions

3. **Float64 SIMD optimization (AVX2)** — Cosine has zero float64 AVX2 kernel (uses generic `cosineFloat64Unrolled4x`). Dim-specific (384/768) Euclidean, Dot, L2Squared use generic Go loops. 24× QPS gap vs float32. Implementation plan:
   - Add `l2SquaredFloat64AVX2Kernel`, `cosineFloat64AVX2Kernel`, `ImplementSpecializedFloat64AVX2(dim)` to avo generator
   - Wire stubs, fallbacks, dispatch, and regenerate assembly
   - Estimated improvement: 3-5× dim-specific, ~2× cosine

4. **float16 dense search 1.5-6× slower than float32** — No NEON `FMLAL` or AVX512 `VFMADD132PH` kernel.

5. **complex128 ingest 3-4× slower than float32** — Generic distance path needs optimization.

6. **Ingest speed drops 20-40× from 15k to 200k vectors** — Expected (O(N·log N) graph building) but inconsistent UX. Add progress indicator or ETA in server logs.

## P2: GPU & Platform

7. **Metal benchmarks not yet run** — Identical parameter sweep needed for float32/int8/uint8 on M3 Pro GPU.

8. **ancalagon (i7-12650H + RTX 4060) unreachable** — SSH connection times out. Resolve network/availability to complete CPU + CUDA matrix.

9. **GPU integer kernels unoptimized** — CUDA int8 dense QPS at 499 (vs 3742 CPU). Metal int8 similarly slow. Both need integer dot-product shaders.

## P3: Instrumentation & Tooling

10. **Pprof retention / cleanup** — Each benchmark generates 14 pprof files per config. After ~96 runs, ~1,344 files. Add cleanup step or auto-aggregate into flamegraphs.

11. **Benchmark result persistence** — JSON checkpoint writes partial results; mid-run data could be lost if the script itself is killed.

12. **Generate performance report script** — Write `scripts/build_perf_report.py` to consume perf_matrix JSON and produce formatted markdown tables.
