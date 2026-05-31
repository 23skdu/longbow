# Longbow Next Steps — v0.2.1-rc5 Benchmark Run

Run the complete benchmark matrix on localhost (CPU + Metal) and ancalagon (CPU + CUDA) in parallel, collect results, and update `docs/performance.md`.

## Baseline Expectations (from docs/performance.md)

| Metric | Target |
|---|---|
| Dense QPS (float32, 128d, 50k) | > 3,000 |
| Dense QPS (float32, 384d, 50k) | > 2,400 |
| Ingest (float32, 128d, 500k) | > 2,000,000 vec/s |
| Ingest (float32, 3072d, 50k) | > 100,000 vec/s |
| p50 latency (128d, 50k dense) | < 0.3ms |

New additions this run: graphrag, temporal, geo-spatial, learned_index, sparse modes at scale — first baseline after the GraphRAG O(N³)→O(B²·depth) optimization.

> [!IMPORTANT]
> **750k vectors @ dim=384**: At 384×4B = 1.5KB/vector, 750k vectors = ~1.1GB raw float32 + HNSW overhead (~3-5× raw) = ~4-6GB total. This fits within 18GB (local) and 14GB (ancalagon). Proceeding with 750k.

> [!NOTE]
> **complex128 at 384d**: complex128 is 16 bytes/element → 384d × 16B = 6KB/vector. 750k × 6KB = 4.5GB raw — borderline at 14GB on ancalagon. Will run up to 250k for complex128.

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

## Phase 1 — Localhost: CPU Run

```
python3 scripts/unified_benchmark.py \
  --mode cpu \
  --dims 128 \
  --counts 5000,15000,50000,250000,500000,600000 \
  --dtypes float16,float32,int8,uint8,uint16,uint32,uint64,complex128,turboquant2,turboquant8 \
  --memory 19327352832 \
  --search-modes hybrid,dense,sparse,filtered,byid,learned_index,geo,graphrag,temporal \
  --queries 500 \
  --workers 6 \
  --timeout 7200 \
  --label rc5_localhost_cpu
```

## Phase 2 — Localhost: Metal Run

_(starts after CPU finishes — sequential on localhost)_

```
python3 scripts/unified_benchmark.py \
  --mode metal \
  --dims 128 \
  --counts 5000,15000,50000,250000,500000,600000 \
  --dtypes float32,int8,uint8 \
  --memory 19327352832 \
  --search-modes hybrid,dense,sparse,filtered,byid \
  --queries 500 \
  --timeout 7200 \
  --label rc5_localhost_metal
```

## Phase 3 — Ancalagon: CPU Run _(parallel with localhost CPU)_

```
ssh ancalagon 'cd ~/REPOS/longbow && python3 scripts/unified_benchmark.py \
  --mode cpu \
  --dims 128 \
  --counts 5000,15000,50000,250000,500000,600000 \
  --dtypes float16,float32,int8,uint8,uint16,uint32,uint64,complex128,turboquant2,turboquant8 \
  --memory 15032385536 \
  --search-modes hybrid,dense,sparse,filtered,byid,learned_index,geo,graphrag,temporal \
  --queries 500 \
  --workers 6 \
  --timeout 7200 \
  --label rc5_ancalagon_cpu'
```

## Phase 4 — Ancalagon: CUDA Run

_(starts after ancalagon CPU finishes — sequential on ancalagon)_

```
ssh ancalagon 'cd ~/REPOS/longbow && python3 scripts/unified_benchmark.py \
  --mode cuda \
  --dims 128 \
  --counts 5000,15000,50000,250000,500000,600000 \
  --dtypes float32,int8,uint8 \
  --memory 15032385536 \
  --search-modes hybrid,dense,sparse,filtered,byid \
  --queries 500 \
  --timeout 7200 \
  --label rc5_ancalagon_cuda'
```

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

- `[x]` **HNSW Index Passthrough**: `temporal`, `geo_spatial`, and `graphrag` modes natively query the HNSW index via `TemporalPredicate`, `SlidingWindowPredicate`, and `GeoPredicate`. Per-hop lookup now O(log N) instead of O(N).
- `[x]` **GraphRAG Beam Search**: BFS frontier in `RankWithGraph` / `RankWithGraphDistributed` pruned to top `BeamWidth=100` nodes after each hop. Worst-case O(N³) → O(B²·depth) ≈ 30,000 ops.
- `[x]` **Explicit Edge Materialization**: `adjList`/`bwdAdjList` in `GraphStore` provide O(1) pointer dereferences under a single `adjMu.RLock()`, replacing per-edge `LockFreeMap.Get()` calls.
- `[x]` **Buffer Eviction & VRAM Management**: Segmented arenas, LRU paging, HNSW hot-node pinning, IO-aware batched distance computations.
- `[x]` **QPS Aggregation Fix** (`86b56fb7`): Sequential search modes, wall-clock QPS.
- `[x]` **Int16/Uint16 Kernel Fix**: 32x latency improvement via float64 accumulators.

## P1 Backlog

- `[ ]` **Rebase & Dependabot resolution**: Cleanly handle incoming PRs.
- `[ ]` **Full Benchmark Matrix**: Validate 14 types × 5 dims × 7 counts × 4 platforms once Phase 1–4 above complete.
- `[ ]` **Correct Performance Baselines in docs/performance.md**: Update with actual v0.2.1-rc5 numbers after run completes.
- `[ ]` **Review All Integer Distance Kernels**: Verify int32/uint32/int64/uint64 accumulators at count=5000+.
- `[ ]` **Remote CUDA/Metal Benchmarks**: Complete after Phase 2 and Phase 4 above.
