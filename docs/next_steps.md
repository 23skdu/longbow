# Next Steps: Code Quality & Improvements Roadmap

**Date**: 2026-01-20
**Context**: Comprehensive codebase analysis identifying critical issues, code quality improvements, and architectural enhancements.

## Critical Issues (Immediate)

1. **ARM64 Assembly Missing Declaration**
   - **File**: `internal/simd/simd_arm64.s:238`
   - **Issue**: `cosineF16NEONKernel` function is missing Go declaration
   - **Action**: Add Go function declaration to fix ARM64 compilation
   - **Priority**: P0 - Blocks ARM64 builds

2. **Panics in Production Code**
   - **Files**: `cmd/bench-tool/main.go`, `cmd/longbow/main.go`, `internal/simd/jit_runtime.go`, `internal/store/errors.go`
   - **Issue**: `log.Fatal()` and `panic()` calls that crash the service
   - **Action**: Replace with graceful error handling and return errors up the call stack
   - **Priority**: P0 - Service stability

3. **Context Propagation**
   - **Count**: 19 instances of `context.TODO()`
   - **Issue**: Missing context propagation for tracing/cancellation
   - **Action**: Replace with proper context from request chain
   - **Priority**: P0 - Observability & cancellation support

## Code Quality

4. **Interface{} to `any` Migration**
   - **Count**: 110 occurrences of `interface{}`
   - **Issue**: Using deprecated style in Go 1.24+ codebase
   - **Action**: Global find/replace `interface{}` with `any`
   - **Priority**: P1 - Code modernization

5. **time.Sleep Overuse**
   - **Count**: 53 files using `time.Sleep`
   - **Issue**: Blocks goroutines, prevents clean shutdown
   - **Action**: Replace with proper timers/context cancellation
   - **Priority**: P1 - Concurrency best practices

6. **Large File Refactoring**
   - **Files**:
     - `arrow_hnsw_insert.go`: 1639 lines
     - `simd/simd.go`: 1241 lines
     - `internal/metrics/metrics_storage.go`: 1193 lines
   - **Issue**: Monolithic files hard to maintain and test
   - **Action**: Split into smaller, focused modules (max 500-800 lines)
   - **Priority**: P2 - Maintainability

7. **Custom Error Usage**
   - **Count**: 72 files using `fmt.Errorf`
   - **Issue**: Not leveraging rich error types in `internal/store/errors.go`
   - **Action**: Use custom error types (ErrInvalidArgument, ErrPersistence, etc.)
   - **Priority**: P1 - Error handling quality

## Concurrency & Stability

8. **Mutex Audit**
   - **Count**: 41 files using `sync.Mutex`
   - **Issue**: Potential deadlocks and lock ordering issues
   - **Action**: Run `go test -race -lockcheck`, audit lock ordering
   - **Priority**: P1 - Production safety

9. **Goroutine Cleanup**
   - **Example**: `gossip.go:190` delegate notification
   - **Issue**: Goroutines launched without proper shutdown tracking
   - **Action**: Add `sync.WaitGroup` for graceful shutdown
   - **Priority**: P1 - Resource leaks

10. **Lock Contention Profiling**
    - **Action**: Run `go test -race -lockcheck` on critical paths
    - **Action**: Add metrics for lock wait times
    - **Priority**: P2 - Performance tuning

## Testing

11. **Test Coverage**
    - **Action**: Run `go test -cover ./...` to identify untested critical paths
    - **Goal**: >80% coverage for hot paths
    - **Priority**: P1 - Code quality

12. **Test Contexts**
    - **Issue**: Many tests use `context.TODO()`
    - **Action**: Use `context.Background()` or `context.WithTimeout()`
    - **Priority**: P2 - Test best practices

13. **Test Data Cleanup**
    - **Issue**: Various matrix_*.json files in root
    - **Action**: Move to `testdata/` directories or clean up
    - **Priority**: P3 - Repository hygiene

## Configuration & Debt

14. **TODO Resolution**
    - `main.go:443`: Gossip advertise addr logic incomplete
    - `arrow_hnsw_bulk.go`: Quantizer not generic yet
    - **Action**: Create GitHub issues tracking or implement
    - **Priority**: P2 - Technical debt

15. **Error Wrapping Consistency**
    - **Issue**: Mix of `%w` and `%v` in error formats
    - **Action**: Standardize on `%w` for `errors.Is()`/`errors.As()`
    - **Priority**: P2 - Error handling

16. **Silent Error Discards**
    - **Pattern**: `_ = conn.Close()` without logging
    - **Action**: Log errors in debug mode or track via metrics
    - **Priority**: P3 - Debuggability

## Documentation

17. **API Documentation**
    - **Issue**: Minimal godoc comments on exported types
    - **Example**: `// VectorStore implements flight.FlightServer with minimal logic`
    - **Action**: Add comprehensive godoc with usage examples
    - **Priority**: P2 - Developer experience

18. **Architecture Diagrams**
    - **Status**: Good foundation in `docs/architecture.md`
    - **Action**: Add sequence diagrams for critical flows (ingestion, search)
    - **Priority**: P2 - Documentation

19. **Code Comments**
    - **Issue**: Some complex algorithms lack explanatory comments
    - **Action**: Add inline comments for HNSW, SIMD, sharding logic
    - **Priority**: P3 - Knowledge transfer

## Performance

20. **Memory Allocation Profiling**
    - **Action**: Run `go test -bench=. -benchmem`
    - **Focus**: Hot paths in distance calculations, ingestion
    - **Priority**: P1 - Performance

21. **Pre-allocation Optimization**
    - **Tool**: gocritic prealloc already enabled
    - **Action**: Review and fix prealloc warnings
    - **Priority**: P2 - Performance

22. **Profiling Infrastructure**
    - **Status**: pprof endpoints exist
    - **Action**: Add automated profiling in CI load tests
    - **Action**: Add flamegraph generation scripts
    - **Priority**: P2 - Performance analysis

23. **Hot Path Analysis**
    - **Action**: Use `pprof` to identify top CPU consumers
    - **Focus**: Distance calc, filter evaluation, HNSW traversal
    - **Priority**: P1 - Performance optimization

## Security

24. **Input Validation**
    - **Area**: Flight tickets, gRPC messages, user queries
    - **Action**: Add comprehensive validation layer
    - **Priority**: P1 - Security

25. **Rate Limiting Per-IP**
    - **Current**: Global RPS limit only
    - **Action**: Add per-IP rate limiting for DoS protection
    - **Priority**: P2 - Security

26. **Dependency Scanning**
    - **Status**: Dependencies up-to-date in go.mod
    - **Action**: Add automated security scanning in CI
    - **Priority**: P1 - Security

## Infrastructure

27. **CI/CD Enhancements**
    - **Current**: Basic workflow in `.github/workflows/ci.yml`
    - **Actions**:
      - Add performance regression detection in benchmark.yml
      - Enable golangci-lint (minimal config exists)
      - Add integration tests
    - **Priority**: P1 - CI/CD quality

28. **Build Automation**
    - **Action**: Add `Makefile` for standardized build/test/lint
    - **Examples**: `make build`, `make test`, `make lint`, `make race`
    - **Priority**: P2 - Developer experience

29. **Release Automation**
    - **Action**: Add goreleaser config for multi-platform builds
    - **Priority**: P2 - Distribution

30. **Docker Optimization**
    - **Files**: `Dockerfile`, `Dockerfile.gpu`, `Dockerfile.nvidia`
    - **Action**: Consolidate or add multi-stage builds
    - **Action**: Reduce image size
    - **Priority**: P2 - Deployment

## Observability

31. **Request Correlation**
    - **Status**: Good zerolog usage
    - **Action**: Add request correlation IDs across components
    - **Priority**: P2 - Debuggability

32. **Slow Query Logging**
    - **Action**: Add configurable slow query thresholds
    - **Action**: Log queries exceeding threshold with context
    - **Priority**: P2 - Performance monitoring

33. **RED Method Metrics**
    - **Status**: Excellent Prometheus integration
    - **Action**: Add Rate, Errors, Duration metrics per method
    - **Priority**: P2 - Observability

34. **Histogram Bucket Tuning**
    - **Action**: Tune Prometheus histogram buckets based on production data
    - **Focus**: Latency percentiles (p50, p95, p99)
    - **Priority**: P3 - Metrics accuracy

## Architecture

35. **Error Type Consolidation**
    - **Status**: Good error types in `internal/store/errors.go`
    - **Action**: Consolidate error types across packages
    - **Action**: Ensure gRPC status code mapping complete
    - **Priority**: P2 - Error handling

36. **Module Boundaries**
    - **Action**: Review import cycles and tighten module boundaries
    - **Focus**: internal/store, internal/mesh, internal/simd
    - **Priority**: P3 - Architecture

37. **Interface Definition**
    - **Action**: Define clear interfaces for external dependencies
    - **Goal**: Improve testability and reduce coupling
    - **Priority**: P2 - Architecture

## Migration Path

### Phase 1: Critical Fixes (Week 1)
- Items 1-3: Fix ARM64 compilation, remove panics, add context propagation

### Phase 2: Code Quality (Week 2-3)
- Items 4-7, 14-16: Modernize code, refactor large files, improve error handling

### Phase 3: Testing & Performance (Week 4-5)
- Items 10-13, 20-23: Add coverage, profile, optimize hot paths

### Phase 4: Security & Infrastructure (Week 6-7)
- Items 24-30: Input validation, CI/CD, build automation

### Phase 5: Observability & Polish (Week 8)
- Items 31-37: Metrics, correlation, documentation

## Success Metrics

- Zero build failures across platforms
- Test coverage >80% for critical paths
- Zero `panic()` or `log.Fatal()` in production code
- All CI/CD pipelines passing with race detection
- Performance regression detection active
- Complete API documentation with examples
