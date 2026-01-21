# Roadmap: System Stability, Performance & Quality

**Last Updated**: 2026-01-20
**Goal**: Consolidation of critical stability fixes, memory optimizations (v0.1.5), and long-term architectural improvements.

---

## P0: Critical Stability & Resource Integrity

*Highest priority. Items blocking builds, causing crashes, or leading to significant resource exhaustion.*

### 1. Build & Architecture Fixes

- **ARM64 Assembly Missing Declaration**: Add Go function declaration for `cosineF16NEONKernel` in `internal/simd/simd_arm64.s:238`. Blocks ARM64 builds.
- **Panic Removal**: Replace `log.Fatal()` and `panic()` calls in `cmd/bench-tool/main.go`, `cmd/longbow/main.go`, and `internal/store/` with graceful error handling.

### 2. Lifecycle & Concurrency

- **Goroutine Leak Prevention**:
  - Audit all background goroutines for proper shutdown tracking using `sync.WaitGroup` and `stopChan`.
  - Implement `internal/store/goroutine_leak_test.go` to verify baseline counts post-`Close()`.
- **Context Propagation & Cancellation**:
  - Replace 19+ instances of `context.TODO()` with proper propagation for tracing and cancellation.
  - Ensure long-running operations (compaction, indexing) check `ctx.Done()`.
- **Shutdown Timeout Enforcement**: Add 30-second timeout for `workerWg.Wait()` in `internal/store/store_lifecycle.go`.

### 3. Memory Integrity

- **SlabArena Retention Fixes**:
  - Address 59.7GB heap retention issues where memory isn't released to OS.
  - Implement `ReleaseMemory()` to explicitly trigger arena compaction.
  - Create `internal/store/memory_leak_integration_test.go` to validate RSS growth <10% over 100 cycles.

---

## P1: Engine Performance & Core Modernization

*Performance optimizations and modernization of the core engine.*

### 4. Vector Engine Optimizations

- **HNSW Graph Compaction**:
  - Implement `CompactGraph()` to rebuild graphs with tighter memory layouts.
  - Add `NeedsCompaction()` heuristic (>30% deleted nodes or >2x size).
- **Aggressive GC Tuner**:
  - Update `internal/memory/gc_tuner.go` to be arena-aware (e.g., GOGC=50 when arena usage >70%).
  - Increase tuning frequency to 500ms.

### 5. Infrastructure & Modernization

- **Modernizing Go Syntax**: Global migration from `interface{}` to `any` (110+ occurrences).
- **Mutex & Contention Audit**:
  - Audit lock ordering in 41+ files.
  - Run `go test -race -lockcheck` and add metrics for lock wait times.
- **Test Coverage**: Focus on hot paths (Distance calc, HNSW traversal) with a goal of >80% coverage.

---

## P2: Developer Experience & Technical Debt

*Improving maintainability and reducing friction for contributors.*

### 6. Refactoring & Code Quality

- **Split Monolithic Files**: Refactor `arrow_hnsw_insert.go` (1600+ lines), `simd.go` (1200+ lines), and `metrics_storage.go` (1100+ lines) into smaller modules.
- **Error Handling Consistency**:
  - Standardize on `%w` formatting.
  - Use custom error types from `internal/store/errors.go` вместо `fmt.Errorf`.
- **TODO Resolution**: Resolve gossip advertise address logic and generic quantizer TODOs.

### 7. Automation & Security

- **CI/CD Enhancements**: Add performance regression detection and automated security scanning.
- **Build Automation**: Implement a `Makefile` for standardized `build`, `test`, `lint`, and `race` commands.
- **Input Validation**: Add comprehensive validation layer for Flight tickets and gRPC messages.

---

## P3: Observability & Hygiene

*Long-term maintenance and visibility items.*

### 8. Metrics & Monitoring

- **Memory Subsystem Metrics**: Add Prometheus metrics for `ArenaMemoryBytes`, `SlabFragmentationRatio`, and compaction events.
- **RED Method Metrics**: Implement Rate, Errors, and Duration metrics per method for better observability.
- **Request Correlation**: Add correlation IDs across all components for easier debugging.

### 9. Documentation & Cleanup

- **Architecture & Sequence Diagrams**: Update `docs/architecture.md` with sequence diagrams for ingestion and search flows.
- **API Documentation**: Improve godoc comments for all exported types.
- **Repository Hygiene**: Move `matrix_*.json` test data to proper `testdata/` directories.

---

## Success Criteria

- [ ] RSS stabilizes at <10GB for soak tests.
- [ ] Zero build failures across ARM64 and AMD64.
- [ ] Test coverage >80% for critical paths.
- [ ] Zero goroutine leaks after 100 open/close cycles.
- [ ] Compaction reduces memory by 20-30% in fragmented scenarios.
