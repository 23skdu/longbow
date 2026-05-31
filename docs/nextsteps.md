# Longbow Next Steps — v0.2.1-rc7 Findings & Recommendations (2026-05-31)

## Actionable Stability & Performance Recommendations

---

## Phase 0 — Cleanup & Prep (both hosts)

**Localhost:**
- Kill any lingering longbow server processes
- Delete `data/bench/` (stale snapshots)
- Delete `data/perf_logs/*.log` older than today
- Delete `bin/bench-tool`, `bin/longbow`, `bin/longbow-metal` (stale binaries)
- Rebuild: `longbow-metal` (Metal-enabled) + `bench-tool` natively

**Ancalagon (via SSH):**
- Pull latest `main` (`git pull`)
- Kill any lingering longbow processes
- Delete `data/bench/`, old perf_logs
- Delete stale `bin/` binaries
- Rebuild: `longbow-cuda` + `bench-tool` natively (amd64 Linux)

---

## Phase 1 — Localhost: CPU Run ✅ (Completed)

```
python3 scripts/unified_benchmark.py \
  --mode cpu \
  --dims 128 \
  --counts 250000,500000,750000 \
  --dtypes float16,float32,int8,uint8,complex128,turboquant8 \
  --memory 19327352832 \
  --search-modes dense,sparse \
  --queries 500 \
  --workers 8 \
  --pprof \
  --timeout 18000 \
  --label localhost_cpu
```

Results: 9/18 configs collected. 750k all ResourceExhausted. float32 500k, complex128 500k, turboquant8 500k missing from aggregate.

## Phase 2 — Localhost: Metal Run ✅ (Completed)

```
python3 scripts/unified_benchmark.py \
  --mode metal \
  --dims 128 \
  --counts 250000,500000 \
  --dtypes float32,int8,uint8 \
  --memory 19327352832 \
  --search-modes dense,sparse \
  --queries 500 \
  --workers 8 \
  --pprof \
  --timeout 14400 \
  --label localhost_metal
```

Results: 5/6 configs collected. float32 500k ResourceExhausted.

## Phase 3 — Ancalagon: CPU Run ✅ (Completed)

```
ssh ancalagon 'cd ~/longbow && python3 scripts/unified_benchmark.py \
  --mode cpu \
  --dims 128 \
  --counts 250000,500000,750000 \
  --dtypes float16,float32,int8,uint8,complex128,turboquant8 \
  --memory 15032385536 \
  --search-modes dense,sparse \
  --queries 500 \
  --workers 8 \
  --pprof \
  --timeout 18000 \
  --label ancalagon_cpu'
```

Results: 8/18 configs collected. Many 500k/750k configs missing due to ResourceExhausted cascade.

## Phase 4 — Ancalagon: CUDA Run ✅ (Completed)

```
ssh ancalagon 'cd ~/longbow && python3 scripts/unified_benchmark.py \
  --mode cuda \
  --dims 128 \
  --counts 250000,500000 \
  --dtypes float32,int8,uint8 \
  --memory 15032385536 \
  --search-modes dense,sparse \
  --queries 500 \
  --workers 8 \
  --pprof \
  --timeout 14400 \
  --label ancalagon_cuda'
```

Results: 5/6 configs collected. float32 500k ResourceExhausted.

---

## Phase 5 — Collect & Analyze

- Pull result JSON logs from ancalagon: `scp ancalagon:~/REPOS/longbow/data/perf_logs/*.json data/perf_logs/`
- Run `scripts/generate_performance_report.py` to produce markdown tables
- Update `docs/performance.md` with new section for v0.2.1-rc5
- Prepend actionable stability/performance recs to top of this file
- Commit all results + doc updates

---

## Verification Criteria

### Automated
- Monitor server logs for OOM kills, panics, or crash loops throughout each run
- Check each result JSON for `"error"` keys or missing QPS fields
- Flag any mode that regressed >20% vs v0.2.0 baselines

### Manual
- Confirm graphrag at 250k/750k completes in < 30s (new Beam Search optimization)
- Confirm temporal/geo modes return results (not empty — HNSW passthrough working)
- Confirm Metal binary uses GPU (look for `LONGBOW_GPU_ENABLED=true` in Metal server log)

---

## Memory Budget

| Host | Total RAM | Process Limit | Headroom |
|---|---|---|---|
| localhost (M3 Pro) | 18GB | 18GB (`--memory 19327352832`) | OS + Metal GPU share unified RAM |
| ancalagon | 22GB RAM + 8GB VRAM | 14GB (`--memory 15032385536`) | ~8GB OS + CUDA buffers |

---

## Previously Completed P0 Blockers

- `[x]` **Multi-Platform Benchmark Run (v0.2.1-rc7)**: Completed CPU + GPU benchmarks on localhost (M3 Pro) and ancalagon (i7-12650H + RTX 4060) at dim=128, counts 250k–750k, dtypes float16/float32/int8/uint8/complex128/turboquant8.
- `[x]` **HNSW Index Passthrough**: `temporal`, `geo_spatial`, and `graphrag` modes natively query the HNSW index via `TemporalPredicate`, `SlidingWindowPredicate`, and `GeoPredicate`. Per-hop lookup now O(log N) instead of O(N).
- `[x]` **GraphRAG Beam Search**: BFS frontier in `RankWithGraph` / `RankWithGraphDistributed` pruned to top `BeamWidth=100` nodes after each hop. Worst-case O(N³) → O(B²·depth) ≈ 30,000 ops.
- `[x]` **Explicit Edge Materialization**: `adjList`/`bwdAdjList` in `GraphStore` provide O(1) pointer dereferences under a single `adjMu.RLock()`, replacing per-edge `LockFreeMap.Get()` calls.
- `[x]` **Buffer Eviction & VRAM Management**: Segmented arenas, LRU paging, HNSW hot-node pinning, IO-aware batched distance computations.
- `[x]` **QPS Aggregation Fix** (`86b56fb7`): Sequential search modes, wall-clock QPS.
- `[x]` **Int16/Uint16 Kernel Fix**: 32x latency improvement via float64 accumulators.

## P1 Backlog

- `[ ]` **float16 SIMD Distance Kernel**: Add ARM NEON `FMLAL` and AVX512 `VFMADD132PH` float16 distance kernels. Currently 12x slower than float32.
- `[ ]` **int8/uint8 Distance Kernel Optimization**: Profile and fix int8/uint8 distance kernels to match float32 performance. Currently 5-6x slower on CPU.
- `[ ]` **GPU Integer Kernel Fix**: Metal and CUDA int8/uint8 dense search is slower than CPU fallback. Implement proper GPU integer dot product.
- `[ ]` **750k OOM Resolution**: Implement disk-based adjacency or tiered storage spill for datasets >500k vectors. HNSW overhead exceeds 18GB budget.
- `[ ]` **ResourceExhausted Cascade Fix**: Benchmark script should restart server process between configs to prevent single OOM from aborting remaining configs.
- `[ ]` **Benchmark Result Persistence Fix**: Ensure individual `result_*.json` files are saved before pprof shutdown snapshot. Currently successful configs can lose their output.
- `[ ]` **generate_performance_report.py robustness**: Add None-guard for missing baseline files. Currently crashes with TypeError.
- `[ ]` **Remote CUDA/Metal Benchmarks**: Complete higher dims (384, 768, 1536, 3072) and larger counts on GPU platforms.
