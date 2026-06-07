# Observations & Next Steps

Based on the 2026-06-06 fresh-build benchmark matrix (12 configurations: dim 128/384 × float32/int8 × 10k/50k/100k, dense + sparse search, 1,000 queries per run, pprof enabled, in-memory storage). Full results in `performance.md`. **P0 `arena is nil` fix landed in commit `a2f535ef`**; all 12 configs now pass.

## Observations

1. **float32 dense QPS holds flat across 10× scale**: 6,776 QPS at 10k dim=128 vs 6,672 QPS at 100k. The HNSW graph grows ~10× but SIMD distance work is the dominant cost and it scales linearly. Same story at dim=384 (6,273 → 6,187 QPS). This is the desired property of an HNSW+AVX2 stack.

2. **Sparse QPS improves with scale**: 7,869 QPS at 10k → 8,338 QPS at 50k dim=128. The inverted index benefits from higher posting-list density and amortizes the per-query merge-tree work across more candidates. Sparse is I/O + merge bound, not HNSW bound.

3. **int8 ingest at 50k dim=128 reaches 3.39M vec/s** — the AVX2 `euclideanInt8AVX2Kernel` plus the smaller per-vector byte footprint (128 bytes vs 512 for float32) make int8 a strong ingest performer. The same kernel sustains 1.30M vec/s at 10k and 687k vec/s at 10k dim=384. Post-fix, int8 dim=384 50k now sustains **1,510,968 vec/s** at 50k (previously crashed with "arena is nil").

4. **int8 dense QPS is ~50% slower than float32 dense** at the working scales (3,514 vs 6,777 QPS at 10k dim=128; p99 3.3 ms vs 1.5 ms). Distance work is shorter but the dispatch + dequant path costs more than the savings. This is a long-standing observation, not a regression.

5. **Lock-free / zero-alloc verification**: the three recent commits (`0cddf75a` inBulkInsert ref-counter, `cb30b97d` race+CAS-leak, `a2f535ef` arena-nil reader pins) eliminated three pre-existing races that the Go race detector had previously flagged. The full `internal/store/index` package now passes `-race` (182 s) and includes a new 50k int8 concurrent stress test (`TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress`) that would have caught the P0 bug deterministically. `go vet` clean.

6. **Goroutine and heap profile is healthy at 100k float32 dim=128**: 20–28 goroutines at steady state, no leaks, no growth over the run. Heap is dominated by gRPC buffer pools (16 MB combined) and `BloomFilter`/`FlatAdjacency` constructors (16 MB combined) — both one-shot per server lifetime, expected.

## Issues Found

### P0 — `arena is nil` failures at int8 50k+ (FIXED in `a2f535ef`)

**Symptom** (pre-fix): 4 of 12 configurations produced 0 QPS in both dense and sparse:
- int8, dim=384, count=50,000
- int8, dim=128, count=100,000
- int8, dim=384, count=100,000

In all four cases the longbow server log contained `"Async batched index add failed" error="arena is nil"`, the `Indexing` row in the result JSON took 13–63 s with `throughput=0` and `rows=0` results returned, and the `Search_Dense`/`Search_Sparse` rows showed `"rows":0`.

**Root cause**: `TypedArena[T].Release()` at `internal/memory/typed_arena.go:37` called `ta.arena.Store(nil)` after releasing the Slab. A concurrent caller that loaded the `*GraphData` via `h.data.Load()` and then called `Int8Arena.AllocSlice` would see `arena is nil` and fail. The async indexing path held a `*GraphData` across `AddBatch` calls — racing with a concurrent `compareAndSwapData` that released the old data. With ~30× higher per-vector byte pressure at int8 vs float32, the CAS race window was much wider, surfacing the bug at 50k+.

**Fix (defense in depth, commit `a2f535ef`)**:

1. **`TypedArena.Release` no longer nils the `arena` pointer** — the underlying Slab is ref-counted (`SlabArena.refs`) and stays alive as long as any Clone of a `GraphData` holds a `Retain()`. The old nil-out was making readers see "arena is nil" even though the Slab was still live. The `GraphData.Release()` spin-wait on `readerCount` (added in this commit, see #2) ensures the Slab is not freed while readers are mid-`AllocSlice`.

2. **New `GraphData.AcquireReader/ReleaseReader` pin mechanism** (atomic `int32` `readerCount`, mirrors the existing `cloneCount` pattern). Bracketed at the call sites that previously raced:
   - `ensureChunkInternalLocked` (called from `InsertWithVector` → `insertInternal`): pins and returns the pinned data; caller releases after Clone/use.
   - `ensureChunksLocked` (called from `addBatchBulkInternal` → `EnsureChunks`): pins for the duration of the function via defer.
   - `addBatchBulkInternal` line ~209 (after `EnsureChunks`): pins only across the `Clone()` call, NOT for the whole function — would deadlock with our own `compareAndSwapData` further down.
   - `addBatchBulkInternal` line ~563 refresh: same pin-across-Clone pattern.
   - `AddBatch` zero-copy `SetZeroCopyMapping` block (~line 810): pins for the block via defer.
   - `insertInternal`: releases the pin returned by `ensureChunk` before reloading via `h.data.Load()` + `Clone()` (no defer path to avoid double-release when `data` is reassigned).

3. **New regression test `TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress`** in `internal/store/index/concurrent_addbatch_50k_test.go`: 5 concurrent `AddBatch` calls of 10k int8 vectors each (50k total, dim=384) — the exact config that triggered the failure. Without the fix, 3-5 of 5 batches fail with "arena is nil" and `nodeCount` is well below 50000. With the fix, all 5 succeed (~654 vec/s, 1m16s wall-clock for the 50k stress run).

**Verified post-fix** (commit `a2f535ef`):

| Config | Pre-fix | Post-fix |
|--------|---------|----------|
| int8 384 50000 dense | 0 QPS (failed) | **549.4 QPS** (1.51M vec/s ingest) |
| int8 384 50000 sparse | 0 QPS (failed) | **7,545.6 QPS** |
| int8 384 100000 dense | 0 QPS (failed) | **283.7 QPS** |
| int8 384 100000 sparse | 0 QPS (failed) | **7,503.0 QPS** |
| int8 128 100000 dense | 0 QPS (failed) | **476.8 QPS** |
| int8 128 100000 sparse | 0 QPS (failed) | **7,499.9 QPS** |

### P1 — `Skipping invalid record in DoGet` warning is misleading

The post-failure DoGet path emits a `WARN` for every `rows=0, cols=0` record, but in the failing int8 runs the same warning is logged 5× per DoGet even though the data is otherwise present. The downstream DoGet completes with the expected 10,000 rows. This is a benign stub-record path; demote to debug or remove the warning. (Will not recur post-P0-fix because the underlying "arena is nil" failure is gone, but the warning logic is still misleading on edge cases.)

### P2 — `protoreflect` init costs 5.3 MB heap (INVESTIGATED, not actionable in longbow)

From the heap profile of 100k float32 dim=128: `protobuf/internal/impl.consumeBytesNoZero` holds 5.3 MB and `bytes.growSlice` holds 6.5 MB. These are one-shot init costs.

**Investigation (2026-06-06):** the 5.3 MB is the cost of the
Apache Arrow Flight SQL protobuf message descriptors
(`vendor/github.com/apache/arrow-go/v18/arrow/flight/gen/flight/FlightSql.pb.go`,
33 message types) plus the Flight protobuf descriptors
(`Flight.pb.go`, 34 message types) — 67 message types × ~80 KB each
of generated `messageInfo` structs with marshal/unmarshal/size/equal
function pointers. Each type is registered in protobuf's global
registry via generated `init()` functions when the binary loads.

**The original recommendation was based on a false premise:**
longbow does NOT register the gRPC reflection service
(`google.golang.org/grpc/reflection`). Verified by grep across
`cmd/` and `internal/`: no `reflection.Register` or
`grpcreflection.Register` call exists. The "trim the reflection
service" suggestion is N/A. The "lazy-load the descriptor set"
suggestion would require changes to Apache Arrow's generated
`.pb.go` files (i.e., forking the library) or to the
`google.golang.org/protobuf` runtime's eager-init contract — both
are out of scope for longbow.

**Conclusion:** P2 is recorded as INVESTIGATED, not actionable in
longbow. The 5.3 MB is the unavoidable cost of integrating with
Apache Arrow Flight SQL. Future contributors seeing this in a heap
profile should not attempt to "fix" it without first confirming
the Apache Arrow dependency has been removed or replaced.

### P3 — float16 and turboquant8 not in the matrix

User request limited the matrix to `float32` and `int8`. The previous `performance.md` had rows for `float16` and `turboquant8`. Re-run with:
```bash
python3 scripts/unified_benchmark.py \
  --dims 128,384 --dtypes float16,turboquant8 --counts 10000,50000,100000 \
  --search-modes dense,sparse --queries 1000 --memory 16 --label float16-tq
```

## Recommendations (in order)

1. **✅ DONE — Land the P0 `arena is nil` fix** (commit `a2f535ef`). The int8 path is now safe at 50k+ concurrent vectors; the 1M-vector benchmark can proceed.

2. **✅ DONE — Add a `TestArrowHNSW_ConcurrentAddBatch_50k_Int8` stress test** (`TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress` in `concurrent_addbatch_50k_test.go`). It exercises 5 concurrent 10k-int8 AddBatch calls (50k total) and asserts `nodeCount >= 50000` and `data.Int8Arena != nil`. Without the fix, 3-5 of 5 batches fail; with the fix, all 5 succeed in ~76 s.

3. **✅ DONE — Wire a Prometheus counter** `longbow_arena_nil_error_total` (counter, not gauge — the value is monotonically increasing) that increments on every `"arena is nil"` error from the typed-arena allocator family. The counter is labelled by method (`AllocSlice` / `AllocSliceDirty` / `AllocSliceAligned`) so Grafana can attribute the regression to the right call site. The counter is wired at the three error sites in `internal/memory/typed_arena.go` (lines 146, 169, 191) and the metric is defined in `internal/metrics/metrics_arena.go`. Four regression tests in `internal/memory/typed_arena_metrics_test.go` cover each label and verify that the counter stays at 0 during healthy operation. Should stay at 0 in healthy operation; non-zero values indicate a regression of the reader-pin contract introduced in commit `a2f535ef`.

4. **Re-run the full v2.2.0 matrix (13 search modes, 4 dtypes, 4 counts)** to get a complete picture post-fix. Use the unified_benchmark orchestrator with `--search-modes all` and `--dtypes float16,float32,int8,turboquant8`.

5. **✅ DONE — Document the `inBulkInsert` ref-counter AND the new `readerCount` pin** in `docs/hnsw.md` §"Concurrency & Lifecycle Safety". The new section covers: the two ref-counter mechanisms (with rationale), the contract callers must follow, the contract violation debugging checklist, the P0 bug story (commit `a2f535ef`), the call-site table for `AcquireReader`/`ReleaseReader`, the rationale for why both counters (and not one), and a reference to the regression test.

6. **✅ DONE — Add `GraphData.ShallowStructuralClone()`** to reduce `GraphData.Clone` heap pressure. The new method (in `internal/store/types/graph_data.go`) shares the per-chunk vector slice headers (`Vectors`, `VectorsFloat64`, `VectorsComplex64`, `VectorsComplex128`) with the original, while still deep-copying the structural slices and offset slices. The per-chunk data lives in the shared typed-arena Slab (already ref-counted and Retained in the new clone), so the shared slice headers are safe — the per-chunk data is read-only after publication (set once by `SetExternalVectorsChunk`/`SetZeroCopyMapping`; the modern write path uses `Vectors<Type>Offset` + `typed-arena.Get(...)`). The 3 per-batch private-clone call sites in `internal/store/index/insertion_core.go` (`ensurePrivate`, the after-grow path, and the after-`ensureChunk` path) now use `ShallowStructuralClone()`. New regression tests in `internal/store/types/graph_data_shallow_clone_test.go` verify (a) data preservation, (b) slice-header sharing via `unsafe.Pointer`, (c) heap savings vs full `Clone`, and (d) deep-copy of structural slices. The 50k int8 stress test still passes; the 2-batch int8 race test still passes; measured heap reduction in the legacy-Vectors test is 99.6% (78.9 MB → 0.3 MB per Clone). The savings at int8 50k dim=384 in the production path will be smaller than the 26.5 MB cited in the original analysis (most production data goes through the modern path, where Vectors[i] is nil), but every Clone call is now strictly cheaper.

7. **✅ DONE — Add `LONGBOW_BENCH_FAST=1` env var** to the unified_benchmark orchestrator (`scripts/unified_benchmark.py:3258-3294`) as a synonym for the `--ci` CLI flag. The env var accepts `1` / `true` / `yes` / `on` (case-insensitive, whitespace-stripped) and is OR-merged with the CLI flag — either activates the same fast-mode defaults (`dims=128`, `counts=10000,50000`, `dtypes=float32,int8`, `search_modes=dense`). `docs/testplan.md` §4.3 documents both activation paths; the `--ci` help text was updated to mention the env var.

## Status of Implemented Recommendations (from previous `nextsteps.md`)

- ✅ CLI flag consistency (`cmd/longbow/flags.go`): shipped
- ✅ Automated continuous benchmarking (`.github/workflows/ci.yml`): shipped
- ✅ **P0 `arena is nil` fix** (commit `a2f535ef`)
- ✅ **50k int8 concurrent stress test** (commit `a2f535ef`)
- ✅ **`longbow_arena_nil_error_total` Prometheus counter** (Rec #3)
- ✅ **Document `inBulkInsert` + `readerCount` contract in `docs/hnsw.md`** (Rec #5)
- ✅ **`LONGBOW_BENCH_FAST=1` env var as a synonym for `--ci`** (Rec #7)
- ✅ **`GraphData.ShallowStructuralClone()` for per-batch private clones** (Rec #6)
- ⏳ Disk-backed validation at 1M+ vectors: pending
- ⏳ CUDA execution on RTX 4060: pending
