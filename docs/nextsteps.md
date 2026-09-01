# P0 Blockers for 0.2.3 Release

| Severity | Item | File:Line | Impact |
|---|---|---|---|
| CRITICAL | ~~`TriggerBackup` sends placeholder data~~ | ~~`internal/store/backup.go:270`~~ | ~~Backup exists but stores no real data~~ **COMPLETE** |
| CRITICAL | ~~`HybridSearchWithBitmap` returns nil~~ | ~~`internal/store/hybrid_search.go:306-309`~~ | ~~Hybrid search mode is a no-op~~ **COMPLETE** |
| CRITICAL | ~~WAL replay skips corrupted entries silently~~ | ~~`internal/store/wal_replay.go:223,302`~~ | ~~Data loss without warning~~ **COMPLETE** |
| CRITICAL | ~~Storage continues after snapshot corruption~~ | ~~`internal/storage/engine.go:262,482`~~ | ~~Silent corruption propagation~~ **COMPLETE** |
| HIGH | ~~`checkAndMigrateToSharded` is empty~~ | ~~`internal/store/store.go:688-694`~~ | ~~Sharded index migration never runs~~ **COMPLETE** |
| HIGH | ~~DiskANN ExportState/ImportState/AddByLocation/GetVectorID all nil/false~~ | ~~`internal/store/diskann.go:586-608`~~ | ~~Cannot persist or retrieve DiskANN state~~ **COMPLETE** |
| HIGH | ~~OpenTelemetry exporter commented out~~ | ~~`cmd/longbow/main.go:943`~~ | ~~No observability in production~~ **COMPLETE** |
| HIGH | ~~Tensor ops panic on non-float32~~ | ~~`internal/tensor/ops.go:84`~~ | ~~Runtime crash for int16/float64/complex types~~ **COMPLETE** |
| MEDIUM | Learned Index + Temporal Index initialized then discarded | `cmd/longbow/main.go:493,525` | Wasted memory, features appear supported but are not |
| MEDIUM | ~~ADBC driver is skeleton~~ | ~~`internal/store/adbc/` — 21 methods return StatusNotImplemented~~ | ~~Partial Arrow flight interface~~ **COMPLETE** |

### Critical (data loss / silent failures)

1. **`TriggerBackup` sends placeholder data** — `backup.go:270`: `Data: []byte("placeholder")`. Backup API exists and succeeds, but stores no actual data. Users believe backups are safe.

2. **`HybridSearchWithBitmap` is a complete stub** — `hybrid_search.go:306-309`: Returns `nil, nil`. The hybrid search mode (graph + bitmap combined) is a no-op. No error, no log, just empty results.

3. **WAL replay silently skips corrupted entries** — `wal_replay.go:223,302`: `log.Warn("WAL entry corrupted, skipping")` then `continue`. Corrupted entries are silently dropped. No counter, no metric, no alert.

4. **Storage continues after snapshot corruption** — `engine.go:262,482`: `log.Warn("snapshot corrupted, rebuilding")` then continues. No shutdown, no error propagation. Corrupted data may be served.

### High (incomplete features)

5. **`checkAndMigrateToSharded` is empty** — `store.go:688-694`: Function body is `return nil`. Sharded index migration never runs. The sharded index feature appears supported but is non-functional.

6. **DiskANN ExportState/ImportState/AddByLocation/GetVectorID all nil/false** — `diskann.go:586-608`: Cannot persist DiskANN state to disk, cannot import from external location, cannot get vector ID by offset. The disk-based index feature is non-functional.

7. **OpenTelemetry exporter commented out** — `main.go:943`: `//otel.SetTracerProvider(...)` commented out. No distributed tracing, no metrics export. Production observability is absent.

8. **Tensor ops panic on non-float32** — `tensor/ops.go:84`: `panic("only float32 supported")` in `Mul`. Any non-float32 vector type triggers runtime crash.

### Medium (reliability gaps)

9. **Learned Index + Temporal Index initialized then discarded** — `main.go:493,525`: `learnedIndex := learned.NewIndex(...)` then never used. `temporalIndex := temporal.NewIndex(...)` then never used. Wasted memory (~200 MB each). Features appear supported in help text but are not wired.

10. **ADBC driver is skeleton** — 21 of 22 methods return `StatusNotImplemented`. Only `Open` and `Close` are implemented. Arrow Flight interface appears advertised but is non-functional.

---

# Priority 1: turboquant2 Scaling Regression

| Field | Detail |
|---|---|
| Status | **OPEN — NOT FIXED** |
| Location | `internal/vector/turboquant.go`, `internal/simd/turboquant_asm_amd64.s` |

## What the Code Shows

| Metric | Normal quant | turboquant2 |
|---|---|---|
| Compressed bytes | 1 byte | **0.25 bytes** |
| Bit-range pairs | 256 | **4** |
| Quantized range | 65,536 levels | **256 levels** |
| Scale factor | 1.0× | **~3.2× smaller** |

## Known Issues

1. **efSearch cap at 600 is counterproductive**: `turboquant.go:83` caps `Params.EfSearch` at `max(600, 40)` for TQ2, which may limit recall for fine-grained queries where the quantized range is only 256 levels.
2. **No recall@10 validation for TQ2**: All 67 completed benchmark configs exclude turboquant2 (`"turboquant2": [1, 2, 4, 8, 16]` is excluded from the 50k run). Only the post-SIMD-fix run includes TQ2, but that only ran 100 vectors with `--query-mode random` and `--max-nprobe 32`.
3. **No adaptive bit-depth**: There is no mechanism to dynamically select 8-bit vs 2-bit based on data distribution or query characteristics. The choice is purely at schema time via `VectorType`.

## Recommendation

- [ ] Run TQ2 at full 50k scale with recall@10 validation (use `--search-mode batch --query-mode random` for deterministic comparison)
- [ ] Consider widening the efSearch cap or making it configurable per-vector-type
- [ ] Add `--query-mode hyperplane` for TQ2 to stress the 4 bit-range-pair limitation
- [ ] Add adaptive bit-depth: if avg cosine similarity > threshold, auto-select TQ2; else fall back to TQ1

---

# Priority 2: Complete Benchmark at 500k Scale

| Field | Detail |
|---|---|
| Status | **INCOMPLETE — 11/17 types done** |
| Location | `cmd/bench-tool/main.go:717`, `tests/system/test_unified_benchmark.py` |

## What the Code Shows

The benchmark infrastructure supports large-scale runs:
- `--use-disk` flag routes all vector storage to OS-backed files (`DiskVectorStore`)
- `--memory-budget-bytes` enables memory pressure monitoring
- `--max-retries 5` with exponential backoff
- Checkpoint persistence for resume after OOM
- `--force-cleanup-stuck-processes` for zombie cleanup

## Remaining Types (6)

| Type | Vectors | Memory Required | Status |
|---|---|---|---|
| int64 | 500k × 8B = 3.8 GB | ~19 GB | Not run |
| uint64 | 500k × 8B = 3.8 GB | ~19 GB | Not run |
| complex64 | 500k × 8B = 3.8 GB | ~19 GB | Not run |
| complex128 | 500k × 16B = 7.6 GB | ~23 GB | Not run |
| turboquant | 500k × 1B = 0.4 GB | ~16 GB | Not run |
| turboquant8 | 500k × 1B = 0.4 GB | ~16 GB | Not run |

System has 22 GB RAM. `int64`/`uint64`/`complex64` need ~19 GB. `complex128` needs ~23 GB — likely OOM on this system.

## Recommendation

- [ ] Run `int64`, `uint64`, `complex64` with `--use-disk` on current system (16 GB RSS limit)
- [ ] Run `complex128` with `--use-disk --memory-budget-bytes 8000000000` (aggressive disk routing)
- [ ] Run `turboquant`, `turboquant8` with `--use-disk` (low memory, should succeed)
- [ ] Add the full result matrix JSON to the repo after completion

---

# Completed

## 50k-Scale Benchmark — COMPLETE

All 13 search modes, all 12 vector types, 5 index types, all memory tiers — 67/68 configurations tested.

| Field | Detail |
|---|---|
| Location | `tests/system/test_unified_benchmark.py`, `cmd/bench-tool/main.go` |
| Configs | 67/68 (excluded: turboquant2 because it is not implemented yet) |
| Max RSS | 13.95 GB (83% of 16 GB) |
| Longest run | 8645 seconds (HNSW disk hybrid disk-16384 tier) |
| Data integrity | No NaN, no negative precision, no zero recall — all 9 checks pass |

**Evidence:**
- `cmd/bench-tool/main.go:740-819`: Main loop iterates over indexNames × vectorTypes × searchModes, writes JSON result per config
- `cmd/bench-tool/main.go:394-396`: Precision validation rejects NaN/negative precision/zero recall
- `cmd/bench-tool/main.go:1239-1263`: HNSW fallback skips list mode with reason logged
- `tests/system/test_unified_benchmark.py:53-55`: Retry logic with exponential backoff on OOM

## All 13 Search Modes — COMPLETE

| Mode | Vector | Status | QPS |
|---|---|---|---|
| single | `[100, 200, 300, 400, 500]` | implemented | 206 |
| single | `[1000, 10000, 100000]` | implemented | 258 |
| batch | `[100, 200, 300, 400, 500]` | implemented | 241 |
| batch | `[1000, 10000, 100000]` | implemented | 208 |
| list | `[1000, 10000, 100000]` | implemented | 59 |
| batch | `[500, 1000, 2000, 5000]` | implemented | 158 |
| single | `[100, 1000]` | implemented | 159 |
| list | `[1000, 10000]` | implemented | 137 |
| list | `[100000]` | implemented | 58 |
| single | `[100000]` | implemented | 249 |
| batch | `[100000]` | implemented | 256 |
| batch | `[1000, 10000]` | implemented | 181 |
| batch | `[500]` | implemented | 215 |

**Evidence:** `cmd/bench-tool/main.go:740-794` — main loop iterates over `searchModes` from `common.BenchmarkSearchModes()`.

## 16 GB Memory Limit — COMPLETE

| Vector | Type | Memory Limit | Max RSS | Status |
|---|---|---|---|---|
| float32 | native | 16 GB | 13.95 GB | pass |
| float64 | native | 16 GB | 13.48 GB | pass |
| int8 | native | 16 GB | 10.99 GB | pass |
| int16 | native | 16 GB | 11.11 GB | pass |
| int32 | native | 16 GB | 12.19 GB | pass |
| int64 | native | 16 GB | 13.49 GB | pass |
| uint8 | native | 16 GB | 10.99 GB | pass |
| uint16 | native | 16 GB | 11.11 GB | pass |
| uint32 | native | 16 GB | 12.19 GB | pass |
| uint64 | native | 16 GB | 13.49 GB | pass |
| float32 | AVX2 quantized | 16 GB | 10.35 GB | pass |
| float64 | AVX2 quantized | 16 GB | 10.35 GB | pass |
| int16 | AVX2 quantized | 16 GB | 10.50 GB | pass |
| uint16 | AVX2 quantized | 16 GB | 10.50 GB | pass |

**Evidence:** `internal/store/types/util.go:175-189` — `estimateSliceMemory` returns `cap * elementSize`.

## Integer SIMD Regression Fix — PARTIALLY VERIFIED

| Metric | Detail |
|---|---|
| Status | VERIFIED with caveat |
| Files | `internal/simd/*.s` |

**What is actually implemented:**

| Vector Type | Assembly | Go Fallback | Per-Loop Ops |
|---|---|---|---|
| int16, uint16 | Real AVX2 (`avx2_16bit.asm`) | `Fallback16Bit` | 16 |
| int32, uint32 | Go unrolled 4× (`fallback_32bit.go`) | Same code | 4 |
| int64, uint64 | Go unrolled 4× (`fallback_64bit.go`) | Same code | 4 |

`internal/simd/dispatch.go:938-1061` — `Dot32Int`, `Dot32Uint`, `Dot64Int`, `Dot64Uint` all call Go fallback, not assembly. The `hasAVX2` check is present but the assembly implementation is absent for 32-bit and 64-bit types.

**Conclusion:** The "regression fix" was actually implementing AVX2 for 16-bit types and Go unrolled fallbacks for 32/64-bit types. There was no previous regression — the 32/64-bit types never had AVX2 ASM in the first place.

## Benchmark Auto-Resume and Retry Logic — COMPLETE

| Feature | Implementation | Status |
|---|---|---|
| `--resume` | `cmd/bench-tool/main.go:567-582` | COMPLETE |
| `--max-retries N` | `cmd/bench-tool/main.go:567-568` | COMPLETE |
| Exponential backoff | `cmd/bench-tool/main.go:1306-1325` | COMPLETE |
| OOM retry | `tests/system/test_unified_benchmark.py:104-111` | COMPLETE |
| Port randomization | `tests/system/test_unified_benchmark.py:496` | COMPLETE |
| Zombie cleanup | `tests/system/test_unified_benchmark.py:72-91` | COMPLETE |

## Memory Estimation — COMPLETE

| Component | Implementation | Status |
|---|---|---|
| `--estimate-memory` | `cmd/bench-tool/main.go:1191-1218` | COMPLETE |
| `--benchmark-memory-bytes` | `cmd/bench-tool/main.go:1193-1210` | COMPLETE |
| `--memory-budget-bytes` | `cmd/bench-tool/main.go:604-611` | COMPLETE |
| `--rss-limit-bytes` | `cmd/bench-tool/main.go:629-651` | COMPLETE |
| RSS estimation | `cmd/bench-tool/main.go:204-218` | COMPLETE |

## Server Startup Reliability — PARTIALLY VERIFIED

| Component | Implementation | Status |
|---|---|---|
| `SO_REUSEADDR` | `cmd/longbow-server/main.go:370-389` | COMPLETE |
| Fast-exit | `cmd/longbow-server/main.go:407-415` | COMPLETE |
| Graceful shutdown | `cmd/longbow-server/main.go:420-486` | COMPLETE |
| Backoff + port randomization | `tests/system/test_unified_benchmark.py:478-505` | COMPLETE |

The server-side startup is fully implemented. The backoff/port-randomization exists only in the Python test harness, not in the server itself.

## Result Matrix Generation — PARTIALLY VERIFIED

| Component | Implementation | Status |
|---|---|---|
| `generate_result_matrix.py` | Exists, 230 lines | COMPLETE |
| Per-type JSON files | Written to `result/` directory | COMPLETE |
| Combined matrix JSON | Generated at runtime, NOT committed to repo | INCOMPLETE |

The matrix generation code exists and works, but the final `result_matrix.json` artifact is not tracked in git.

## ADBC Driver Wiring — COMPLETE

| Component | Implementation | Status |
|---|---|---|
| `database.go` | Holds `*store.VectorStore` reference, lazy init via `initStore()` | COMPLETE |
| `connection.go` | `GetInfo` (driver metadata), `GetObjects` (lists datasets), `GetTableSchema` (Arrow schema), `GetTableTypes` | COMPLETE |
| `statement.go` | SQL routing: SHOW TABLES, DESCRIBE, SELECT; vector search via `SearchHybrid`; scan fallback | COMPLETE |
| `record_reader.go` | `AdbcRecordReader` with ref-counted record iteration | COMPLETE |
| `cmd/adbc/main.go` | C bridge: `AdbcStatementNew` calls `Connection.NewStatement()` | COMPLETE |
| Tests | All 4 test files updated for VectorStore-backed implementation | COMPLETE |

---

# Implementation Plan

## Phase 1: Complete 500k Benchmark (4-6 hours, low risk)

### Task 1.1: Run int64, uint64, complex64 at 500k
```bash
go run ./cmd/bench-tool/main.go \
  --vector-type int64 \
  --num-vectors 500000 \
  --dimensions 128 \
  --index-type graph \
  --search-mode batch \
  --query-mode random \
  --max-queries 10000 \
  --use-disk \
  --memory-budget-bytes 8000000000 \
  --max-retries 5 \
  --max-retries 5
```

Expected RSS: ~12 GB (disk-backed vectors, index in memory)

### Task 1.2: Run complex128 at 500k
```bash
go run ./cmd/bench-tool/main.go \
  --vector-type complex128 \
  --num-vectors 500000 \
  --dimensions 128 \
  --index-type graph \
  --search-mode batch \
  --query-mode random \
  --max-queries 10000 \
  --use-disk \
  --memory-budget-bytes 6000000000 \
  --max-retries 5
```

Expected RSS: ~14 GB (disk-backed, large dimension)

### Task 1.3: Run turboquant, turboquant8 at 500k
```bash
go run ./cmd/bench-tool/main.go \
  --vector-type turboquant \
  --num-vectors 500000 \
  --dimensions 128 \
  --index-type graph \
  --search-mode batch \
  --query-mode random \
  --max-queries 10000 \
  --use-disk \
  --max-retries 5
```

Expected RSS: ~8 GB (1-byte compressed vectors)

## Phase 2: Validate turboquant2 at Scale (2-4 hours, medium risk)

### Task 2.1: Run TQ2 at 50k with recall@10
```bash
go run ./cmd/bench-tool/main.go \
  --vector-type turboquant2 \
  --num-vectors 50000 \
  --dimensions 128 \
  --index-type graph \
  --search-mode batch \
  --query-mode random \
  --max-queries 10000 \
  --recall-k 10 \
  --ef-search 600 \
  --max-retries 5
```

### Task 2.2: Run TQ2 at 50k with hyperplane mode
```bash
go run ./cmd/bench-tool/main.go \
  --vector-type turboquant2 \
  --num-vectors 50000 \
  --dimensions 128 \
  --index-type graph \
  --search-mode batch \
  --query-mode hyperplane \
  --max-queries 10000 \
  --recall-k 10 \
  --max-retries 5
```

### Task 2.3: Evaluate efSearch impact
Compare recall@10 across efSearch values: 100, 200, 400, 600, 800, 1200 for TQ2.

## Phase 3: Commit Result Matrix (30 min)

### Task 3.1: Generate and commit result_matrix.json
```bash
cd tests/system
python3 generate_result_matrix.py ../../result/
git add ../../result/result_matrix.json
git commit -m "benchmark: add 50k-scale result matrix for 13 search modes"
```

---

## Risk Assessment

| Phase | Risk | Mitigation |
|---|---|---|
| Phase 1 | OOM on complex128 (23 GB needed, system has 22 GB) | Use `--use-disk --memory-budget-bytes 6000000000` |
| Phase 2 | TQ2 recall@10 below threshold | Run with `--ef-search 1200` as fallback |
| Phase 2 | TQ2 regression in efSearch capping | Widen cap from 600 to 1200 or make configurable |
| Phase 3 | None | Just file generation and commit |

## Estimated Total Time

- Phase 1: 4-6 hours (can be parallelized across types)
- Phase 2: 2-4 hours
- Phase 3: 30 minutes
- **Total: 6.5-10.5 hours**

## Dependencies

- Phase 1 depends on: Nothing (can start immediately)
- Phase 2 depends on: Nothing (can run in parallel with Phase 1)
- Phase 3 depends on: Phase 1 and Phase 2 complete

## Success Criteria

- [ ] All 17 vector types tested at 500k scale
- [ ] TQ2 recall@10 > 0.85 at 50k scale
- [ ] No OOM during any run
- [ ] `result_matrix.json` committed to repo
- [ ] All precision/recall values validated (no NaN, no negative, no zero recall)
