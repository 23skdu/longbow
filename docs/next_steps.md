# Roadmap: System Stability, Performance & Quality

**Last Updated**: 2026-01-22
**Goal**: Consolidation of critical stability fixes, memory optimizations (v0.1.5), and long-term architectural improvements.

---

## P0: Critical Stability & Resource Integrity

*Highest priority. Items blocking builds, causing crashes, or leading to significant resource exhaustion.*

### 1. Build & Architecture Fixes

- **[~COMPLETED~]{.status-done} ARM64 Assembly Missing Declaration**: Add Go function declaration for `cosineF16NEONKernel` in `internal/simd/simd_arm64.s:238`. Blocks ARM64 builds.
  - ✅ Verified: Declaration exists in `simd_arm64.go:30` and `simd_arm64.s:237-238`
  - ✅ Unit tests created for batch distance compute scenarios (`internal/store/panic_removal_test.go`)
- **[~COMPLETED~]{.status-done} Panic Removal**: Replace `log.Fatal()` and `panic()` calls in `cmd/bench-tool/main.go`, `cmd/longbow/main.go`, and `internal/store/` with graceful error handling.
  - ✅ Unit tests added for panic scenarios in batch distance compute (`internal/store/panic_removal_test.go`)
  - ✅ Prometheus metrics for error tracking (`PanicTotal`, `CompactionErrorsTotal`)
  - ✅ Replaced `panic()` calls in `compaction.go` with error returns (`filterTombstones`, `mergeAndFilterRecordBatches`)
  - ✅ Replaced `log.Fatal("No peers specified")` with proper error handling in `cmd/bench-tool/main.go`

### 2. Lifecycle & Concurrency

- **[~COMPLETED~]{.status-done} Goroutine Leak Prevention**:
  - **[~COMPLETED~]{.status-done} Audit all background goroutines for proper shutdown tracking using `sync.WaitGroup` and `stopChan`.**
  - **[~COMPLETED~]{.status-done} Implement `internal/store/goroutine_leak_test.go` to verify baseline counts post-`Close()`.
- **[~COMPLETED~]{.status-done} Context Propagation & Cancellation**:
  - **[~COMPLETED~]{.status-done} Replace 19+ instances of `context.TODO()` with proper propagation for tracing and cancellation.**
  - **[~COMPLETED~]{.status-done} Ensure long-running operations (compaction, indexing) check `ctx.Done()`.
- **[~COMPLETED~]{.status-done} Shutdown Timeout Enforcement**: Add 30-second timeout for `workerWg.Wait()` in `internal/store/shutdown.go`.

### 3. Memory Integrity

- **[~IN PROGRESS~]{.completed} SlabArena Retention Fixes**:
  - **[~COMPLETED~]{.status-done} Address 59.7GB heap retention issues where memory isn't released to OS.**
  - **[~COMPLETED~]{.status-done} Implement `ReleaseMemory()` to explicitly trigger arena compaction.**
  - **[~COMPLETED~]{.status-done} Create `internal/store/memory_leak_integration_test.go` to validate RSS growth <10% over 100 cycles.**

---

## P1: Engine Performance & Core Modernization

*Performance optimizations and modernization of the core engine.*

### 4. Vector Engine Optimizations

- **[~COMPLETED~]{.status-done} HNSW Graph Compaction**:
  - **[~COMPLETED~]{.status-done} Implement `CompactGraph()` to rebuild graphs with tighter memory layouts.**
  - **[~COMPLETED~]{.status-done} Add `NeedsCompaction()` heuristic (>30% deleted nodes or >2x size).
- **[~COMPLETED~]{.status-done} Aggressive GC Tuner**:
  - **[~COMPLETED~]{.status-done} Update `internal/memory/gc_tuner.go` to be arena-aware (e.g., GOGC=50 when arena usage >70%).**
  - **[~COMPLETED~]{.status-done} Increase tuning frequency to 500ms.**

### 5. Infrastructure & Modernization

- **[~IN PROGRESS~]{.completed} Modernizing Go Syntax**: Global migration from `interface{}` to `any` (110+ occurrences).
- **[~IN PROGRESS~]{.completed} Mutex & Contention Audit**:
  - **[~IN PROGRESS~]{.completed} Audit lock ordering in 41+ files.**
  - **[~IN PROGRESS~]{.completed} Run `go test -race -lockcheck` and add metrics for lock wait times.
- **[~COMPLETED~]{.status-done} Test Coverage**: Focus on hot paths (Distance calc, HNSW traversal) with a goal of >80% coverage.
  - ✅ Added SIMD coverage tests (`internal/simd/coverage_increase_test.go`)
    - `TestMetricType_String` - tests MetricType.String() (0% -> 100%)
    - `TestGenerateBatchBody` - tests WASM batch generation (0% -> 100%)
    - `TestEuclideanBatchInto_*` - tests JIT batch euclidean
  - ✅ Added HNSW traversal tests (`internal/store/hnsw_traversal_test.go`)
    - `TestArrowHNSW_SearchEarlyTermination` - tests search with early termination
    - `TestArrowHNSW_SearchWithEfGreaterThanResults` - tests search with large ef
    - `TestArrowHNSW_NeedsCompaction_*` - tests compaction heuristics
    - `TestArrowHNSW_VisitedListGrowth` - tests visited list behavior
    - `TestArrowHNSW_SearchEmptyIndex` - tests empty index search
    - `TestArrowHNSW_SearchSingleVector` - tests single vector search
    - `TestArrowHNSW_EstimateMemory` - tests memory estimation
    - `TestArrowHNSW_SearchVectors_Empty` - tests SearchVectors API
  - ✅ SIMD package coverage: 45.0% → 45.8% (improved)

---

## P2: Developer Experience & Technical Debt

*Improving maintainability and reducing friction for contributors.*

### 6. Refactoring & Code Quality

- **[~IN PROGRESS~]{.completed} Split Monolithic Files**: Refactor `arrow_hnsw_insert.go` (1600+ lines), `simd.go` (1200+ lines), and `metrics_storage.go` (1100+ lines) into smaller modules.
- **[~IN PROGRESS~]{.completed} Error Handling Consistency**:
  - **[~IN PROGRESS~]{.completed} Standardize on `%w` formatting.**
  - **[~IN PROGRESS~]{.completed} Use custom error types from `internal/store/errors.go` вместо `fmt.Errorf`.
- **[~IN PROGRESS~]{.completed} TODO Resolution**: Resolve generic quantizer TODO.
  - ✅ **COMPLETED** gossip advertise address logic (added AdvertiseAddr field to GossipConfig, used in Start())

### 7. Automation & Security

- **[~IN PROGRESS~]{.completed} CI/CD Enhancements**: Add performance regression detection and automated security scanning.
- **[~IN PROGRESS~]{.completed} Build Automation**: Implement a `Makefile` for standardized `build`, `test`, `lint`, and `race` commands.
- **[~IN PROGRESS~]{.completed} Input Validation**: Add comprehensive validation layer for Flight tickets and gRPC messages.

---

## P3: Observability & Hygiene

*Long-term maintenance and visibility items.*

### 8. Metrics & Monitoring

- **[~IN PROGRESS~]{.completed} Memory Subsystem Metrics**: Add Prometheus metrics for `ArenaMemoryBytes`, `SlabFragmentationRatio`, and compaction events.
- **[~IN PROGRESS~]{.completed} RED Method Metrics**: Implement Rate, Errors, and Duration metrics per method for better observability.
- **[~IN PROGRESS~]{.completed} Request Correlation**: Add correlation IDs across all components for easier debugging.
- **[~COMPLETED~]{.status-done} Debug Log Spam Reduction**: Remove excessive debug logging in hot paths:
  - ✅ Removed "Processing batch starting/finished" from `store_lifecycle.go`
  - ✅ Removed "IngestionWorker picked up job" from `ingestion_worker.go`
  - ✅ Removed "applyBatchToMemory adding to currentMemory" from `store_actions.go`

### 9. Documentation & Cleanup

- **[~IN PROGRESS~]{.completed} Architecture & Sequence Diagrams**: Update `docs/architecture.md` with sequence diagrams for ingestion and search flows.
- **[~IN PROGRESS~]{.completed} API Documentation**: Improve godoc comments for all exported types.
- **[~COMPLETED~]{.status-done} Repository Hygiene**: Move `matrix_*.json` test data to proper `testdata/` directories.

---

## Completed v0.1.5 Work Items

The following items have been completed as part of the v0.1.5 release:

- ✅ HNSW Graph Compaction (`internal/store/hnsw_compaction.go`)
- ✅ NeedsCompaction() heuristic for determining when to compact
- ✅ Goroutine leak detection tests (`internal/store/goroutine_leak_test.go`)
- ✅ Goroutine audit documentation (`internal/store/goroutine_audit.go`)
- ✅ Memory leak integration test (`internal/store/memory_leak_integration_test.go`)
- ✅ Memory leak soak script (`scripts/memory_leak_soak.sh`)
- ✅ Arena-aware GC Tuner with aggressive mode (`internal/memory/gc_tuner.go`)
- ✅ Context cancellation tests (`internal/store/context_cancellation_test.go`)
- ✅ 30-second shutdown timeout enforcement (`internal/store/shutdown.go`)
- ✅ ReleaseMemory() API (`internal/store/store_lifecycle.go`)
- ✅ Memory architecture documentation (`docs/memory_architecture.md`)
- ✅ High memory usage runbook (`docs/runbooks/high_memory_usage.md`)
- ✅ Panic removal unit tests (`internal/store/panic_removal_test.go`)
- ✅ Debug log spam reduction in hot paths
- ✅ Graceful error handling in compaction (`compactRecords`, `filterTombstones`, `mergeAndFilterRecordBatches`)
- ✅ Graceful error handling in `cmd/bench-tool/main.go` (replaced `log.Fatal` with error return)
- ✅ SIMD hot path unit tests (`internal/simd/coverage_increase_test.go`)
- ✅ HNSW traversal unit tests (`internal/store/hnsw_traversal_test.go`)

---

## Success Criteria

- [ ] RSS stabilizes at <10GB for soak tests.
- [ ] Zero build failures across ARM64 and AMD64.
- [ ] Test coverage >80% for critical paths.
- [ ] Zero goroutine leaks after 100 open/close cycles.
- [ ] Compaction reduces memory by 20-30% in fragmented scenarios.
