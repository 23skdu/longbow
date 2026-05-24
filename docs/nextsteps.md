# Next Steps & Priorities

> [!IMPORTANT]
> **P0 Blockers: Test Suite Optimization & Context Window Refactoring**
> We must address the test execution time (especially race detection timeouts) and file sizes to ensure maintainability and agent context limits.

## 1. Test Suite Optimization
- **Parallelization vs. Serial Tests**: Identify CPU-bound index tests and prevent them from running in parallel with `t.Parallel()` during race detection, which causes excessive context switching and timeouts.
- **Test Consolidation**: Combine frivolous or overly granular tests (e.g., small individual getter/setter tests) into single table-driven tests to reduce overhead.
- **Mocking & Isolation**: Mock `mesh.Gossip` and heavy network/RPC components in `store` tests instead of spinning up full simulated clusters for basic unit tests.
- **Timeout Adjustments**: Increase timeout flags for `go test -race` specifically on heavy packages (e.g. `internal/store/index`), but prioritize optimizing the code first.

## 2. Refactoring for Context Windows
- **`navigation.go`**: Split into `navigation_search.go` (vector searching logic), `navigation_parallel.go` (parallel search host logic), and `navigation_properties.go` (getters/warmup).
- **`arrow_hnsw.go`**: Extract insertion and graph mutation logic into `arrow_hnsw_insert.go` and `arrow_hnsw_delete.go`.
- **`store.go`**: Move lifecycle methods (`Start`, `Stop`) to `store_lifecycle.go` and configuration to `store_config.go`.

---

## Other Ongoing Tasks
- Implement/integrate GPU index types for advanced hardware acceleration.
- Update `Makefile` and `Dockerfile` for `GOAMD64=v3`.
- Benchmark Execution on `ancalagon` hardware profile.

