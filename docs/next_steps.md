# Roadmap: System Stability, Performance & Quality

**Last Updated**: 2026-01-22
**Goal**: Consolidation of critical stability fixes, memory optimizations (v0.1.5), and long-term architectural improvements.

---

## P2: Developer Experience & Technical Debt

*Improving maintainability and reducing friction for contributors.*

### 1. Refactoring & Code Quality

- **[~IN PROGRESS~]{.completed} Split Monolithic Files**: Refactor `arrow_hnsw_insert.go` (1645 lines), `simd.go` (1429 lines), and `metrics_storage.go` (1189 lines) into smaller modules.
  - **Phase 1: Planning & Analysis (Parts 1-4)**
    - Part 1: Analyze file dependencies and coupling points
    - Part 2: Design new module boundaries and interfaces
    - Part 3: Create migration checklist and testing strategy
    - Part 4: Establish refactoring order (metrics → SIMD → HNSW)

  - **Phase 2: Metrics Storage Refactoring (Parts 5-8)**
    - Part 5: Extract WAL metrics to `wal_metrics.go`
    - Part 6: Extract compaction metrics to `compaction_metrics.go`
    - Part 7: Extract storage/index metrics to `storage_metrics.go`
    - Part 8: Extract bulk operation metrics to `bulk_metrics.go`

  - **Phase 3: SIMD Refactoring (Parts 9-12)**
    - Part 9: Extract CPU detection to `cpu_detection.go`
    - Part 10: Extract distance functions to `distance_functions.go`
    - Part 11: Extract batch operations to `batch_operations.go`
    - Part 12: Extract dispatch logic to `dispatch.go`

  - **Phase 4: HNSW Insert Refactoring (Parts 13-20)**
    - Part 13: Extract PQ training to `pq_training.go`
    - Part 14: Extract core insertion logic to `insertion_core.go`
    - Part 15: Extract connection management to `connection_management.go`
    - Part 16: Extract level generation to `level_generation.go`
    - Part 17: Extract insert search operations to `insert_search.go`
    - Part 18: Extract pruning logic to `connection_pruning.go`
    - Part 19: Extract F16-specific operations to `f16_operations.go`
    - Part 20: Update imports and validate all modules

- **[~IN PROGRESS~]{.completed} Error Handling Consistency**:
  - Standardize on `%w` formatting.
  - Use custom error types from `internal/store/errors.go` instead of `fmt.Errorf`.

### 2. Automation & Security

- **[~IN PROGRESS~]{.completed} CI/CD Enhancements**: Add performance regression detection and automated security scanning.
- **[~IN PROGRESS~]{.completed} Build Automation**: Implement a `Makefile` for standardized `build`, `test`, `lint`, and `race` commands.
- **[~IN PROGRESS~]{.completed} Input Validation**: Add comprehensive validation layer for Flight tickets and gRPC messages.

---

## P3: Observability & Hygiene

*Long-term maintenance and visibility items.*

### 3. Metrics & Monitoring

- **[~IN PROGRESS~]{.completed} RED Method Metrics**: Implement Rate, Errors, and Duration metrics per method for better observability.
- **[~IN PROGRESS~]{.completed} Request Correlation**: Add correlation IDs across all components for easier debugging.

### 4. Documentation & Cleanup

- **[~IN PROGRESS~]{.completed} Architecture & Sequence Diagrams**: Update `docs/architecture.md` with sequence diagrams for ingestion and search flows.
- **[~IN PROGRESS~]{.completed} API Documentation**: Improve godoc comments for all exported types.

---

## Success Criteria

- [ ] RSS stabilizes at <10GB for soak tests.
- [ ] Zero build failures across ARM64 and AMD64.
- [ ] Test coverage >80% for critical paths.
- [ ] Zero goroutine leaks after 100 open/close cycles.
- [ ] Compaction reduces memory by 20-30% in fragmented scenarios.
