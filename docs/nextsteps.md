# 10-Part Plan: Memory Optimization & Goroutine Leak Prevention (v0.1.5)

**Goal**: Resolve `SlabArena` memory retention (59.7GB heap), implement HNSW graph compaction, prevent goroutine leaks, and achieve stable memory usage during sustained ingestion.

**Current State**: Profiling reveals 87.56% of heap (52.3GB) held by `SlabArena` allocations. Memory stabilizes but never releases back to OS.

---

## Implementation Plan

### 1. SlabArena Memory Release Audit

**Objective**: Ensure `SlabArena.Release()` returns memory to the OS via `madvise(MADV_DONTNEED)`.

**Changes**:

- **[MODIFY]** `internal/memory/slab_pool.go`: Add `madvise(MADV_DONTNEED)` syscall in `Release()` to hint OS that pages can be reclaimed.
- **[MODIFY]** `internal/memory/slab_pool.go`: Track per-slab usage metrics to identify when slabs can be fully released.
- **[NEW]** `internal/memory/slab_pool_test.go`: Unit test verifying RSS decreases after `Release()` calls.
- **[NEW]** `internal/memory/slab_pool_fuzz_test.go`: Fuzz test for concurrent allocate/release patterns.

**Verification**: Monitor RSS via `/proc/self/status` before/after release. Expect RSS to drop proportionally.

---

### 2. TypedArena Compaction

**Objective**: Implement periodic compaction for `TypedArena` to consolidate fragmented slabs.

**Changes**:

- **[NEW]** `internal/memory/arena_compaction.go`: Implement `CompactArena()` to copy live data to new slabs and release old ones.
- **[MODIFY]** `internal/memory/typed_arena.go`: Add `Compact()` method with read-write lock protection.
- **[NEW]** `internal/memory/arena_compaction_test.go`: Unit test verifying memory consolidation.
- **[NEW]** `internal/memory/arena_compaction_fuzz_test.go`: Fuzz test for concurrent compaction under load.

**Verification**: Measure slab count and RSS before/after compaction. Expect 20-30% reduction in fragmented workloads.

---

### 3. HNSW Graph Compaction

**Objective**: Implement periodic HNSW graph compaction to rebuild graphs with tighter memory layout.

**Changes**:

- **[NEW]** `internal/store/hnsw_compaction.go`: Implement `CompactGraph()` to rebuild HNSW with optimized layout.
- **[MODIFY]** `internal/store/arrow_hnsw_index.go`: Add `NeedsCompaction()` heuristic (e.g., >30% deleted nodes or >2x expected size).
- **[MODIFY]** `internal/store/compaction_worker.go`: Trigger HNSW compaction alongside record batch compaction.
- **[NEW]** `internal/store/hnsw_compaction_test.go`: Unit test verifying graph integrity post-compaction.
- **[NEW]** `internal/store/hnsw_compaction_fuzz_test.go`: Fuzz test for compaction under concurrent search/insert.

**Verification**: Compare memory usage and search latency before/after compaction. Expect 15-25% memory reduction.

---

### 4. Aggressive GC Tuner

**Objective**: Make `GCTuner` more responsive to arena retention by lowering GOGC when heap is dominated by arenas.

**Changes**:

- **[MODIFY]** `internal/memory/gc_tuner.go`: Add arena-aware tuning that sets GOGC=50 when arena usage >70% of heap.
- **[MODIFY]** `internal/memory/gc_tuner.go`: Increase tuning frequency from 2s to 500ms for faster response.
- **[MODIFY]** `internal/memory/gc_tuner_test.go`: Update tests for new aggressive thresholds.
- **[NEW]** `internal/memory/gc_tuner_integration_test.go`: Integration test with real `SlabArena` allocations.

**Verification**: Run soak test and verify GC triggers more frequently when arena usage is high. Expect heap to stabilize at lower levels.

---

### 5. Goroutine Leak Detection

**Objective**: Audit all background goroutines for proper shutdown and add leak detection tests.

**Changes**:

- **[NEW]** `internal/store/goroutine_audit.go`: Document all background goroutines with lifecycle ownership.
- **[MODIFY]** `internal/store/store_lifecycle.go`: Ensure all workers check `stopChan` and use `workerWg`.
- **[NEW]** `internal/store/goroutine_leak_test.go`: Test that verifies goroutine count returns to baseline after `Close()`.
- **[NEW]** `internal/store/goroutine_leak_fuzz_test.go`: Fuzz test for rapid open/close cycles.

**Verification**: Use `runtime.NumGoroutine()` before/after store lifecycle. Expect count to return to ±5 of baseline.

---

### 6. Context Cancellation for Long Operations

**Objective**: Add context cancellation to all long-running operations (compaction, indexing, replication).

**Changes**:

- **[MODIFY]** `internal/store/compaction_worker.go`: Accept `context.Context` and check `ctx.Done()` in compaction loop.
- **[MODIFY]** `internal/store/store_lifecycle.go`: Pass context from `stopChan` to all workers.
- **[MODIFY]** `internal/store/hnsw_batch.go`: Check context in `AddBatchBulk` inner loops.
- **[NEW]** `internal/store/context_cancellation_test.go`: Unit test verifying operations abort on context cancel.

**Verification**: Cancel context mid-operation and verify graceful shutdown within 1 second.

---

### 7. Metrics for Memory Subsystem

**Objective**: Add Prometheus metrics for arena usage, slab fragmentation, and compaction events.

**Changes**:

- **[MODIFY]** `internal/metrics/metrics.go`: Add `ArenaMemoryBytes`, `SlabFragmentationRatio`, `CompactionEventsTotal`.
- **[MODIFY]** `internal/memory/slab_pool.go`: Update metrics on allocate/release.
- **[MODIFY]** `internal/store/compaction_worker.go`: Increment `CompactionEventsTotal` on each run.
- **[NEW]** `internal/metrics/memory_metrics_test.go`: Unit test for metric registration and updates.

**Verification**: Query metrics endpoint and verify values match expected arena state.

---

### 8. Shutdown Timeout Enforcement

**Objective**: Enforce hard timeout for graceful shutdown to prevent hung workers from blocking `Close()`.

**Changes**:

- **[MODIFY]** `internal/store/store_lifecycle.go`: Add 30-second timeout for `workerWg.Wait()`.
- **[MODIFY]** `cmd/longbow/main.go`: Log warning if shutdown exceeds timeout and force-stop remaining workers.
- **[NEW]** `internal/store/shutdown_timeout_test.go`: Test that `Close()` completes within timeout even with hung workers.

**Verification**: Simulate hung worker and verify `Close()` returns within 30 seconds.

---

### 9. Memory Leak Integration Test

**Objective**: Create long-running integration test that validates memory returns to baseline after dataset drop.

**Changes**:

- **[NEW]** `internal/store/memory_leak_integration_test.go`: Test that creates/drops datasets 100 times and verifies RSS growth <10%.
- **[NEW]** `scripts/memory_leak_soak.sh`: Automated script for 1-hour memory leak detection.
- **[MODIFY]** `internal/store/store_lifecycle.go`: Add `ReleaseMemory()` method to explicitly trigger arena compaction.

**Verification**: Run for 1 hour, verify RSS growth <100MB and no goroutine leaks.

---

### 10. Documentation & Runbook

**Objective**: Document memory management architecture and create operational runbook for memory issues.

**Changes**:

- **[NEW]** `docs/memory_architecture.md`: Document `SlabArena`, `TypedArena`, and compaction strategies.
- **[NEW]** `docs/runbooks/high_memory_usage.md`: Runbook for diagnosing and resolving memory issues in production.
- **[MODIFY]** `README.md`: Add memory tuning section with recommended `GOGC` and `max_memory` values.

**Verification**: Review documentation with team and validate runbook steps in staging environment.

---

## Success Criteria

- [ ] RSS stabilizes at <10GB for 20-minute soak test (down from 60GB)
- [ ] No goroutine leaks after 100 store open/close cycles
- [ ] Compaction reduces memory by 20-30% in fragmented scenarios
- [ ] All background workers shut down within 30 seconds
- [ ] Metrics accurately reflect arena usage and compaction events
