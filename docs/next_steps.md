# Roadmap: System Stability, Performance & Quality

**Last Updated**: 2026-01-22
**Goal**: Consolidation of critical stability fixes, memory optimizations (v0.1.5), and long-term architectural improvements.

---

## P2: Developer Experience & Technical Debt

*Improving maintainability and reducing friction for contributors.*

### 1. Refactoring & Code Quality

- **[~IN PROGRESS~]{.completed} Split Monolithic Files**: Refactor `arrow_hnsw_insert.go` (1600+ lines), `simd.go` (1200+ lines), and `metrics_storage.go` (1100+ lines) into smaller modules.
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
