# HNSW Parameter Tradeoffs

Longbow uses HNSW (Hierarchical Navigable Small World) graphs for approximate nearest neighbor search. Three parameters control the accuracy/performance tradeoff: `M`, `ef_construction`, and `ef_search`.

## Quick Reference

| Parameter | Range | Low | High | Effect |
|-----------|-------|-----|------|--------|
| `M` | 8–64 | Less memory, faster build/search, lower recall | More memory, slower build/search, higher recall | Graph connectivity |
| `ef_construction` | 100–800 | 2–5x faster construction, lower recall | Higher recall, 2–5x slower construction | Build quality |
| `ef_search` | 10–1000+ | Fast queries, lower recall | Slow queries, higher recall | Search depth |

## Env Vars

| Variable | Default | Description |
|----------|---------|-------------|
| `LONGBOW_MAX_M0` | `64` (unset) | Caps max connections at layer 0; the benchmark script sets `32` |
| `LONGBOW_HNSW_EF_CONSTRUCTION` | `400` | Dynamic candidate list size during insert |
| `LONGBOW_ADAPTIVE_M_MAX_FACTOR` | — | Adaptive M scaling factor (benchmark sets `1.5`) |

Internal (Go) defaults: `M=32`, `MMax=64`, `MMax0=64`, `EfConstruction=400`, `EfSearch=50`.

## M — Max Connections Per Node

M controls the maximum number of outgoing edges per node in each graph layer.

**Construction time:**
- Doubling M roughly doubles construction memory writes (more edges to store/update per insert)
- Each new node searches ef_construction candidates and connects to the closest M of them
- Higher M also increases the number of reverse connections that must be trimmed

**Memory per node:**
- Each node stores up to `2 * M` neighbor IDs (M outgoing + up to M incoming from other nodes' reverse connections)
- Each ID is a `uint32` (4 bytes)
- Memory = `nodes * (2 * M) * 4` bytes for the graph alone
- At M=32: 50k nodes → ~12.8 MB | 1M nodes → ~256 MB

**Recommended M values:**

| M | Use Case |
|---|----------|
| 8 | Low-memory embedded devices, very large datasets (>10M), fast approximate search |
| 16 | Lighter option: fast builds, good recall (0.97-0.98), suitable for 50k+ scales |
| 32 | Default — good balance for most workloads up to 500k vectors |
| 48–64 | Research-grade recall, small datasets (<100k), or when recall >0.999 is needed |

**Dim interaction:**
- At low dimensions (64–128), distance computations are cheap — lower M works well
- At high dimensions (768–3072), each edge traversal costs more — higher M reduces the number of hops needed
- For dim=384, M=16 is usually sufficient; M=32 gives marginal recall gain at ~2x construction cost

## ef_construction

ef_construction controls how many candidates are evaluated during the search phase of each insert. It is the single largest factor in construction time.

**Construction cost model:**
Each insert at layer 0:
1. Navigates from the entry point to the insertion layer (few hops, cheap)
2. At layer 0, maintains a candidate set of size `ef_construction`
3. For each of ~ef_construction candidates, evaluates all neighbors (up to M each)
4. Total distance computations per node: roughly `ef_construction * (M/2)` at layer 0

With ef_construction=400, M=32: ~50k * 400 * 16 ≈ 320M distance computations at layer 0 alone
With ef_construction=200, M=16: ~50k * 200 * 8 ≈ 80M distance computations — **4x fewer**

**Recall saturation:**

| ef_construction | Recall@10 (dim=384, 50k) | Relative build time |
|----------------|--------------------------|---------------------|
| 100 | ~0.94 | 0.5x |
| 200 | ~0.98 | 1.0x (baseline) |
| 400 | ~0.995 | 2.5x |
| 800 | ~0.998 | 5x |

The gain from 200→400 is typically ~1.5% recall, but doubles the build time. The gain from 400→800 is even smaller.

**Recommended values:**

| ef_construction | Use Case |
|----------------|----------|
| 100 | Rapid prototyping, CI tests, benchmarks where build time matters |
| 200 | Lighter option: good recall at 2.5x faster builds than default |
| 400 | Default — maximum recall for benchmarks, production use |
| 800 | Research, marginal gain over 400 |

**Data type interaction:**
Construction time scales directly with element size:
- float32: 4 bytes/elem — baseline
- float16: 2 bytes/elem — ~40% faster than float32 (less memory bandwidth, F16C conversion overhead)
- float64: 8 bytes/elem — ~3-5x slower than float32 (half SIMD throughput on AVX2: 2 doubles per 128-bit vs 4 floats)
- int8/uint8: 1 byte/elem — ~2x faster than float32 (more elements per SIMD vector)
- int16/uint16: 2 bytes/elem — comparable to float16
- Larger types (int64, complex128): significantly slower

At ef=400 with float64 at dim=384, 50k construction: ~20-40 min on AVX2 hardware.
At ef=200 with float64 at dim=384, 50k construction: ~5-10 min on AVX2 hardware.

## ef_search

ef_search controls query-time recall. Unlike ef_construction, it can be tuned per-query without rebuilding the index.

**Cost model:**
Query time is roughly linear in ef_search:
- Each query evaluates ~ef_search * (M/2) candidates
- Doubling ef_search roughly doubles query latency
- Throughput drops inversely with ef_search

**Typical values:**

| ef_search | Use Case |
|-----------|----------|
| 32 | High-throughput serving, low-latency requirements |
| 64 | Balanced throughput/recall |
| 128 | Good recall (default) |
| 256 | High recall for benchmarks |
| 512+ | Maximum recall, offline evaluation |

## Combined Effects

### Dimension
Higher dimensions make distance computations more expensive. Mitigation strategies:
- **≤128**: Lower M (8–16) and ef_construction (100–200) suffice for >0.99 recall
- **384**: Default (M=32, ef=400) gives >0.99 recall; M=16, ef=200 is a good lighter option
- **≥768**: Use M=16–24, ef_construction=100–200; higher values give diminishing returns
- **≥1536**: Consider PQ compression or turboquant; raw HNSW is expensive

### Dataset Scale (default: M=32, ef=400)

| Scale | M | ef | Expected build time (float32, dim=384) | Expected build time (float64, dim=384) |
|-------|---|----|---------------------------------------|----------------------------------------|
| 10k | 32 | 400 | ~30s | ~2 min |
| 50k | 32 | 400 | ~5-10 min | ~20-40 min |
| 50k | 16 | 200 | ~30s | ~2-3 min |
| 500k | 16 | 200 | ~5-10 min | ~30-60 min |
| 500k | 32 | 400 | ~1-2 hours | ~4-8 hours |
| 1M | 16 | 200 | ~15-30 min | ~1-2 hours |
| 1M | 32 | 400 | ~4-8 hours | — (impractical) |

### Data Type Cost Multiplier (relative to float32 at same M/ef)

| Type | Build time multiplier | Memory multiplier |
|------|----------------------|-------------------|
| float32 | 1.0x | 1.0x |
| float16 | 0.5x | 0.5x |
| int8/uint8 | 0.4x | 0.25x |
| int16/uint16 | 0.6x | 0.5x |
| int32/uint32 | 1.1x | 1.0x |
| int64/uint64 | 2.5x | 2.0x |
| float64 | 3-5x | 2.0x |
| complex64 | 2x | 2.0x |
| complex128 | 5-8x | 4.0x |

## Adaptive ef_construction

Longbow includes an adaptive mechanism that reduces ef_construction when the indexing queue backs up:

| Queue depth | Effective ef_construction |
|-------------|--------------------------|
| < 1000 | 400 (configured value) |
| 1000–5000 | 200 |
| > 5000 | 100 |

This prevents unbounded build slowdowns during bulk loading. The adaptive value affects only new inserts, not already-queued work.

## Benchmark Configuration Recommendations

Longbow's default (M=32, ef=400) is tuned for maximum recall. For benchmark matrices covering multiple scales, use scale-adaptive settings:

| Goal | Scale | M | ef | Notes |
|------|-------|---|----|-------|
| Max recall | ≤10k | 32 | 400 | Fast at small scales |
| Balanced | 50k | 16 | 200 | 4-6x faster build than defaults |
| Large | 500k | 16 | 100 | Practical build times for CI |
| Extreme | 1M | 16 | 100 | Avoid float64/complex128 unless necessary |

The benchmark script's `start_server()` automatically applies these scale-adaptive values when `count` is provided.

## Memory Usage Breakdown

For a dataset of N vectors at dim=384, float32:

| Component | Size | Notes |
|-----------|------|-------|
| Raw vectors | N * 384 * 4 | 384 bytes per vector element × 4 bytes |
| HNSW graph (M=16) | N * 32 * 4 | 2M neighbor slots × 4 bytes |
| HNSW graph (M=32) | N * 64 * 4 | 2M neighbor slots × 4 bytes |
| Level assignments | N * 1 | uint8 per node |
| Arena/buffer overhead | ~10-20% | Internal pool allocators |

Total approximate memory for N=500k, M=16: 384MB (vectors) + 64MB (graph) = ~450MB + overhead
Total approximate memory for N=500k, M=32: 384MB (vectors) + 128MB (graph) = ~512MB + overhead

## Key Takeaways

1. **ef_construction is the dominant cost factor** in HNSW build time — far more than M
2. **float64 and large integer types** build 3-8x slower than float32 due to half SIMD throughput on AVX2
3. **M and ef are not per-table tunable via env vars** — `LONGBOW_MAX_M0` caps MMax0, `LONGBOW_HNSW_EF_CONSTRUCTION` overrides ef_construction
4. **500k+ datasets with default settings (M=32, ef=400) can take hours** — use scale-adaptive values (M=16, ef=100-200) for practical build times
5. **Adaptive ef_construction** automatically reduces ef when the indexing queue backs up (400 → 200 at depth 1000, → 100 at depth 5000)
6. **ef_search is tunable per-query** (no env var) but defaults to 50 — increase to 200+ for better recall during evaluation

## Concurrency & Lifecycle Safety

HNSW is the largest and most frequently-mutated data structure in
longbow, and it is shared across three concurrent surfaces:

1. **Bulk inserts** — `ArrowHNSW.AddBatchBulk` (called by the async
   indexing queue) and the type-switch fast path in
   `ArrowHNSW.AddBatch` (`arrow_hnsw_insert.go:996`).
2. **Search / GetNeighbors** — read-only paths that dereference
   `h.data.Load()` and walk the typed-arena fields
   (`Int8Arena`, `Float32Arena`, etc.).
3. **Entry-point lookup** — `insertInternal` reads `h.entryPoint` to
   find the graph root before navigating to a node's layer.

To make these surfaces safe in the presence of COW (copy-on-write)
publication of new `GraphData` instances, longbow uses **two
independent atomic ref-counters** that cooperate with each other.
Both are subtle; both have bitten us. The contract for each is
described below.

### `inBulkInsert` — serialises the first-node spin-wait

`ArrowHNSW.inBulkInsert` is a `atomic.Int64` (defined at
`internal/store/index/arrow_hnsw.go:152`). It is incremented for the
duration of every bulk-insert call:

```go
// arrow_hnsw_bulk.go:33-38
func (h *ArrowHNSW) AddBatchBulk(ctx context.Context, startID uint32, n int, vecs any) error {
    h.bulkMu.Lock()
    h.inBulkInsert.Add(1)
    err := h.addBatchBulkInternal(ctx, startID, n, vecs)
    h.inBulkInsert.Add(-1)
    h.bulkMu.Unlock()
    ...
}
```

and in the fast-path switch inside `AddBatch`
(`arrow_hnsw_insert.go:996-998`):

```go
h.inBulkInsert.Add(1)
err := h.addBatchBulkInternal(ctx, startID, n, vecs)
h.inBulkInsert.Add(-1)
```

The consumer is `insertInternal`
(`insertion_core.go:331-343`):

```go
ep := h.entryPoint.Load()
...
if ep == math.MaxUint32 && id > 0 && h.inBulkInsert.Load() == 0 {
    for ep == math.MaxUint32 {
        time.Sleep(1 * time.Millisecond)
        ep = h.entryPoint.Load()
    }
    maxL = int(h.maxLevel.Load())
}
```

**Why this exists.** A non-zero first node (`id > 0`) needs the entry
point to be committed before it can navigate the graph. But the entry
point is only written by `commitID(0)`, and the bulk path runs
`id=0` *and* the first wave of `id=1..n` in parallel. The
`inBulkInsert.Load() == 0` guard ensures that an `AddBatch` (single
insert) for `id > 0` will wait for any in-flight bulk that owns
`id=0`, and only then begin its own spin-wait. Without the guard,
non-bulk inserts for `id > 0` could spin forever waiting for an entry
point that another concurrent path was about to commit. The fix
landed in commit `0cddf75a`; the regression test is the existing
`TestArrowHNSW_ConcurrentAddBatch_*` family in
`concurrent_addbatch_test.go`.

**Contract.**

- Every call site that invokes `addBatchBulkInternal` MUST bracket
  the call with `inBulkInsert.Add(+1)` / `inBulkInsert.Add(-1)`.
  The current call sites are `AddBatchBulk` (line 35-37) and the
  fast-path switch in `AddBatch` (line 996-998). New call sites
  (e.g. a future `AddBatchWithCallback`) must do the same.
- The check at `insertion_core.go:336` is the only consumer. Adding
  a new consumer is fine; removing the guard without first
  proving all `id > 0` inserts are reached only through bulk paths
  will reintroduce the stall.

### `GraphData.readerCount` — pins the Slab during typed-arena reads

`GraphData.readerCount` is a `atomic.Int32` (defined at
`internal/store/types/graph_data.go:150`). It is incremented by
`AcquireReader` / decremented by `ReleaseReader`
(`graph_data.go:2422-2430`):

```go
func (g *GraphData) AcquireReader() {
    atomic.AddInt32(&g.readerCount, 1)
}
func (g *GraphData) ReleaseReader() {
    atomic.AddInt32(&g.readerCount, -1)
}
```

The consumer is `GraphData.Release()`
(`graph_data.go:3501-3523`):

```go
func (g *GraphData) Release() {
    if !atomic.CompareAndSwapUint32(&g.released, 0, 1) {
        return
    }
    // Wait for all concurrent Clone() operations to finish.
    for atomic.LoadInt32(&g.cloneCount) > 0 {
        runtime.Gosched()
    }
    // Wait for all concurrent read paths to finish.
    for atomic.LoadInt32(&g.readerCount) > 0 {
        runtime.Gosched()
    }
    // ... actually free the typed-arenas
}
```

**Why this exists.** `GraphData` is the COW publication envelope.
Reads of the typed-arena fields (`Int8Arena.AllocSlice`, etc.)
need the underlying `Slab` to stay alive for the entire read.
Two things can take the Slab away from under a reader:

1. **`compareAndSwapData`** in `ArrowHNSW` publishes a new
   `GraphData` and calls `Release()` on the old one. The old
   `Release` decrements `SlabArena.refs`; if it reaches 0, the
   Slab is freed.
2. **`TypedArena[T].Release`** used to call
   `ta.arena.Store(nil)` *before* decrementing `SlabArena.refs`
   (commit `a2f535ef` removed this). A reader that called
   `Int8Arena.AllocSlice` after the nil-out but before the
   ref-count zero would see `arena is nil` and fail with
   `errors.New("arena is nil")`. The async indexing path held a
   `*GraphData` across `AddBatch` calls, racing with the CAS that
   released the old data. With ~30× higher per-vector byte pressure
   at int8 vs float32, the race window was much wider at 50k+
   vectors, surfacing the bug deterministically (the regression
   test `TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress` fails
   3-5 of 5 batches without the fix).

**Contract.**

> **Every goroutine that dereferences a typed-arena field of a
> `*GraphData` MUST bracket the read with
> `data.AcquireReader()` / `defer data.ReleaseReader()`. The
> `GraphData.Release()` spin-wait on `readerCount==0` is what
> makes this safe.**

Current call sites (verified 2026-06-06):

| File:line | Pattern |
|---|---|
| `internal/store/index/arrow_hnsw_memory.go:40,46,60,63,73` | `ensureChunkInternalLocked` — pin the chunk returned to the caller |
| `internal/store/index/arrow_hnsw_memory.go:195-196` | `ensureChunksLocked` — defer-pin across the slice of chunks |
| `internal/store/index/arrow_hnsw_bulk.go:215-217` | `addBatchBulkInternal` — pin the original `*GraphData` across its first `Clone()` (NOT across the whole function — that would deadlock with the later `compareAndSwapData`) |
| `internal/store/index/arrow_hnsw_bulk.go:580-582` | `addBatchBulkInternal` — pin the freshly-published `*GraphData` across its `Clone()` |
| `internal/store/index/arrow_hnsw_insert.go:817-818` | zero-copy `SetZeroCopyMapping` block — defer-pin across the mapping |
| `internal/store/index/insertion_core.go:190,194` | `insertInternal` — release the pin returned by `ensureChunk` *before* reloading via `h.data.Load()+Clone()` (the pin covered the original; once we Clone, the pin is no longer needed) |

If a new read path is added that touches a typed-arena field
(`Int8Arena`, `Float32Arena`, `Float16Arena`, `Int16Arena`,
`Float64Arena`, `Complex64Arena`, `Complex128Arena`, etc.), the
author MUST add `AcquireReader`/`ReleaseReader` to that path. The
compiler will not catch the omission; the only signal will be an
intermittent `arena is nil` error at 50k+ vectors. The
`longbow_arena_nil_error_total` Prometheus counter (labelled by
`method`) is the production signal that this contract is being
violated; the counter should stay at 0 in healthy operation.

### Debugging checklist for `arena is nil` / `race detected`

If a test or production run surfaces one of these symptoms, work
through this list before opening an issue:

1. **Confirm the failure mode**: is it `"arena is nil"` (read
   pin missing) or a Go race-detector report (pin order wrong)?
2. **Run the regression test**:
   ```bash
   go test -race -timeout 240s -run 'TestArrowHNSW_ConcurrentAddBatch' \
     -count=3 -v ./internal/store/index/
   ```
   If the 50k int8 stress test fails, the reader pin is missing
   at a call site listed in the table above.
3. **Check `GraphData.Release()`** at
   `internal/store/types/graph_data.go:3501-3523` — the spin-wait
   on `readerCount` must be present. If it is missing, this is
   the root cause of every `arena is nil` failure in the field.
4. **Check the new call site** — every typed-arena read MUST be
   bracketed with `AcquireReader/ReleaseReader`. The `Grep` query
   `git grep -n 'Arena\.AllocSlice\|Arena\.AllocSliceDirty\|Arena\.AllocSliceAligned'`
   lists every read; cross-reference against the table above.
5. **Check `TypedArena.Release()`** at
   `internal/memory/typed_arena.go:37-50` — the body must NOT
   call `ta.arena.Store(nil)`. The comment at lines 38-46
   explains why.
6. **Check the Prometheus counter** —
   `longbow_arena_nil_error_total{method="..."}` should be 0.
   Any non-zero value in production means a contract violation
   is happening in the field; the `method` label points at the
   call site. Grafana alert:
   `rate(longbow_arena_nil_error_total[5m]) > 0`.
7. **For Go race-detector reports**: the failing stack trace
   points at the racing goroutines. Almost all races in
   `internal/store/index/` are COW issues on `GraphData` or its
   typed-arenas. The fix template is the reader pin pattern: one
   goroutine mutates the published `*GraphData` while another is
   reading it.

### Why both counters (and not one)

The two counters protect different invariants:

- `inBulkInsert` is a **barrier against a logical race**: a
  non-bulk `id > 0` insert needs the entry point committed by
  `id=0`, but a concurrent bulk path owns `id=0`. The counter
  signals "a bulk path is in flight that owns the first node";
  the non-bulk path waits outside that window.
- `readerCount` is a **barrier against a memory race**: a reader
  that called `Int8Arena.AllocSlice` needs the Slab to be live.
  The CAS-driven Release of an old `*GraphData` would free the
  Slab; the reader pin keeps the Slab alive until the reader is
  done.

A single counter cannot cover both — bulk inserts and typed-arena
reads happen at different granularities, with different latencies
(microseconds vs. sub-microseconds), and under different lock
regimes (`bulkMu` vs. none). The two-counter design lets each
synchronisation primitive be tuned independently.

### Regression test

`internal/store/index/concurrent_addbatch_50k_test.go::TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress`
is the canary for the `readerCount` contract. It exercises 5
concurrent 10k-int8 `AddBatch` calls (50k total, dim=384). Without
the fix, 3-5 of 5 batches fail with `arena is nil` and `nodeCount`
is well below 50000. With the fix, all 5 succeed in ~76 s.

The 50k stress test is listed as a "must run" test in
`docs/testplan.md` §3.3. Do not skip it.

