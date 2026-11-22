# Longbow Roadmap (Performance & Scale)

This document tracks the **remaining** performance, scalability, and
modularization work for Longbow. Completed items have been removed; their
results are recorded in `docs/development/performance.md`, `docs/development/testplan.md`, `docs/development/gates.md`,
and the git history.

The removed work covered:

- **Performance & Scale**: 1M `complex128` memory-profile hot spots (zero-allocation neighbor updates, batch-scratch reuse, copy-on-write graph cloning, cached distance computers), SIMD parity and zero-allocation kernels across all dtypes, dynamic worker scaling, NUMA-aware allocation, request batching, multi-predicate bitmap intersection, HNSW bulk quality (reachability, recall @10, post-bulk repair, and `addbatch_inflight` / `BulkChainLinks` tuning), memory-scaling tests, P0 performance, OIDC Federation, DevSecOps, geo/temporal/text candidate caps, TurboQuant kernels/LUT, predicate pushdown, disk/tiered storage, spatial hash grid, temporal sorted index, text trigram index, filtered predicate-aware entry points, parallel filter evaluation, geo LRU result cache, the 50k–1M scalability tests, and the complete 1M × 15-dtype × 21-mode validation matrix (`benchmarks/baseline_cpu_1m_all15_20261121.json`, `docs/development/performance.md` §0e, Recall@10 = 0.8600 at 1M).
- **Codebase Modularization (M1–M4, M6, & initial M5 extractions)**:
  - **M1**: Same-package splits of `rest_graphql.go` (9 files), `cmd/bench-tool/main.go` (6 files), `cmd/longbow/main.go` (6 files), `text_search_advanced.go` (4 files), `store_ingest.go` (3 files), `dataset.go` (3 files), `hnsw_autoshard.go` (3 files), `temporal_search.go` (3 files), `streaming_connectors.go` (3 files), and `internal/metrics/metrics.go` (3 files).
  - **M2**: Modularization of `longbowclientsdk/src/longbow/client.py` into `_mixins/` and `scripts/unified_benchmark.py` into `scripts/benchmark/`.
  - **M3**: Decomposition of `arrow_hnsw_insert.go`, `navigation_search.go`, `adaptive_index.go`, `arrow_search_context.go`, `internal/query/zero_alloc_utils.go`, and `fast_path_filter.go`.
  - **M4**: Splitting `internal/simd/gen/all_kernels_gen.go` into `families/` generating 5 AMD64 assembly family files, splitting `simd_arm64.s` across 6 files + shared macro header, splitting `internal/gpu/cuda/kernels.cu` across 5 files, and extracting `metal_bridge.{h,*.m}` + Go PQ/dtype helpers in `internal/gpu/metal/`.
  - **M5 (initial standalone domain extractions)**: Extracted `internal/store/rbac`, `internal/store/geo`, `internal/store/temporal`, `internal/store/index/learned`, and `internal/store/index/bm25` (including splitting 812-LOC `bm25_inverted_index.go` and extracting pluggable index types into `internal/store/types/pluggable_types.go`), with type/constructor aliases preserved in parent packages.
  - **M6**: Added `scripts/check_module_sizes.sh` (`make module-size-check`, `scripts/verify.sh` `--staged` check, CI `validate` job, Gate 14 in `docs/development/gates.md`) warning on hand-maintained files `> 800 LOC`, failing new files `> 1,200 LOC`, and enforcing a shrinkable legacy allow-list (updated for macOS bash 3.2 compatibility).
- **macOS Metal GPU & ARM64 Darwin SIMD**: Runtime Metal shader compilation from embedded `kernels.metal`, Metal bridge buffer-index/top-k/CFRelease/deadlock/race fixes across `MetalIndexOptimized`, `MetalIndex`, and `MetalHybridIndex`, 32-byte TurboQuant Metal alignment, ARM64 NEON `int16`/`uint8` unsigned widening (`VUADDLP_2D`) fix, and all-dtype × dims `128`/`384`/`768` GPU & NEON verification suites.

See `docs/development/performance.md` for benchmark results and baselines, `docs/development/gates.md`
for repository conformance gates, and `docs/development/testplan.md` for the full
search-mode and dtype matrix.

---

## Remaining Work

All Performance & Scale items and modularization phases **M1–M4** and **M6** are **complete**. Only the tightly coupled domain extractions from **M5** remain below, pending interface decoupling and test migration.

### P0 — macOS-15 Runner, ARM64 Darwin Verification & Darwin ARM64 Docker Release

Release verification must cover the Apple Silicon / Darwin ARM64 target end to end before a release ships. Add the following as blocking (P0) release gates:

- [x] **macOS-15 GitHub Actions runner** in the release-verification workflow (`.github/workflows/*`): provision a `macos-15` runner, install the Go toolchain + CGO dependencies used by `make build` / `make test-short`, and run the standard verification pipeline (`make build`, `make vet`, `make test-short`, `make lint`) natively on Darwin ARM64.
- [x] **ARM64 Darwin verification job as part of release verification**: a dedicated job in the release pipeline that builds and tests `GOOS=darwin GOARCH=arm64` (including CGO-enabled paths and the SIMD/Metal backends under `internal/gpu/metal/`), runs `cross-compile`/equivalent Darwin ARM64 checks, and uploads the resulting `longbow` Darwin ARM64 binary as a release artifact. No release is cut unless this job is green.
- [ ] **Releasing Darwin ARM64 Docker images**: publish `darwin/arm64` (Apple Silicon) container images alongside the existing CPU/GPU images — extend the Docker build matrix (`Dockerfile*`, `docker-compose.yml`, release workflow) to build and push a Darwin ARM64 variant, tag it consistently with the other platforms, and document pull/run instructions for Apple Silicon hosts. Include the image in `make docker-build-*` / release artifact lists and the observability/verification gates.

### ARM64 / NEON / SVE (Performance & Correctness)

- [x] **Verify ARM64 NEON float64 kernels on native hardware**: run the full NEON float64 kernel suite and distance/search parity tests on a real ARM64 (Apple Silicon / Graviton) host, not just under QEMU/emulation; confirm bit-identity with the generic reference and no overflow/clamping regressions.
- [x] **Complete IMP-01 arm64 atomic restore (CASAL)**: finish the ARM64 `CASAL`-based atomic restore path for off-heap graph/slab state (currently partial), including retry/ABA handling and parity with the AMD64 CAS path; add targeted concurrency tests.
- [x] **Consider SVE/SVE2 support**: evaluate auto-vectorization/SVE intrinsics for the hot distance and HNSW traversal kernels, behind runtime CPU-feature detection, with scalar/NEON fallbacks and cross-architecture parity tests.

### M5 — Remaining God-Package Domain Extractions

Target budgets for hand-maintained code remain `<= 500 LOC` soft / `<= 800 LOC` hard ceiling per file and `<= 50` production files per package. Completing the remaining extractions requires introducing interface boundaries (e.g. `type X = subpkg.X` aliases plus host interfaces) to break `*VectorStore`, `*Dataset`, and `*ArrowHNSW` receiver/field coupling without import cycles:

- [ ] **`internal/store` remaining domain extractions (currently 197 -> target `<= 50` prod files)**:
  - `internal/store/api` (`rest_*.go`, `graphql_*.go` — currently `*VectorStore` handler methods accessing unexported `VectorStore`/`Dataset` state; requires a `StoreAPIHost` interface)
  - `internal/store/rbac` action methods (`rbac_actions.go`, `rbac_rls_test.go` — currently `*VectorStore` methods alongside `oidc_store` / `configMu` state)
  - `internal/store/text` (`text_*.go` — `text_index_worker.go` & `text_search_advanced.go` define `*VectorStore` methods and read unexported `*Dataset` fields)
  - `internal/store/streaming` (`streaming_*.go`, `parquet_*.go` — `streaming_drivers.go` defines `*VectorStore` action handlers and `streaming_connectors.go` touches `Dataset.dataMu`)
  - `internal/store/controlplane` (`cloud_control_plane.go`, `elastic_sharding.go`, `backup.go`, `backup_pitr.go`, `metering.go`, `materialized_views.go` — all define `*VectorStore` methods and inspect `*Dataset` internals)
  - Core/remaining store domains (`ttl_policy.go`, `hnsw_temporal.go`, plus non-M5 domains such as `disk_vector_store*`, `graph_*`, `hybrid_*`, `federated_*`, `compaction`, `memory_*`, `shard*`) required to bring `internal/store` below `50` production files.
- [ ] **`internal/store/index` remaining domain extractions (currently 128 -> target `<= 50` prod files)**:
  - `internal/store/index/ivf` (`ivf_flat.go`, `ivf_opq_index.go`, `ivf_pq_index.go`, `ivf_hnsw_composite.go` — `ivf_pq_index.go`, `ivf_opq_index.go`, and `ivf_hnsw_composite.go` currently embed/construct `*ShardedHNSW` or `createHNSWIndex`; requires an HNSW factory interface)
  - `internal/store/index/hnsw` (`ArrowHNSW` core, compute, quantization, disk, repair — tightly coupled across `adaptive_index.go`, `disk_graph.go`, `sharded_hnsw.go`, and white-box `package index` tests accessing unexported `*ArrowHNSW` fields).

---

## Verification Artifacts

| Gate | Command | Status |
|---|---|---|
| Build | `make build` | pass |
| Vet | `make vet` | pass |
| Unit tests | `make test-short` | pass |
| Static check | `make lint` | 0 issues |
| Dead code | `golangci-lint run --enable-only=unused ./...` | 0 issues |
| Metal GPU & ARM64 NEON | `go test -tags gpu -race ./internal/gpu/... ./internal/onnx/metal/...` | pass |
| Module size guardrail | `make module-size-check` | pass |
| Observability | `make observability-check` | pass |
| Accuracy | `make accuracy-gate` | pass |
| Oracle | `make oracle-gate` | pass |
| Search modes | `make search-gate` | pass |
| Release versions | `make check-version` | pass |

## Baselines

- `benchmarks/baseline_cpu_1m_all15_20261121.json` (1M, dim 128, all 15 dtypes × 21 modes + Recall@10 = 0.8600)
- `benchmarks/baseline_cpu_20261116.json` (50k/100k/200k, dim 128 & 384)
- `benchmarks/baseline_cpu_768_20261116.json` (50k/100k/200k, dim 768)
- `benchmarks/baseline_cpu_large_20261116.json` (500k/1M, dim 128)
- `benchmarks/baseline_cpu.json`, `baseline_100k.json`, `baseline_250k.json`, `baseline_25k.json` (legacy)

### Shared-Namespace Concurrency (Soak-Test Findings)

Investigation (2026-11-21): agent soak tests with 8 agents (2 per namespace)
stall after ~60s even though the server stays responsive and error rate is 0.
Profiling shows no memory pressure; the stall is a lock convoy on shared
datasets: concurrent insert/upsert/delete/search on the same namespace
serialize behind dataset-level mutexes (`dataMu`, index locks) and the
auto-scaler then drains workers, leaving no forward progress for contended
operations.

- [x] **Profile shared-dataset lock contention**: capture `pprof` mutex/block profiles during a 2-agent shared-namespace soak; identify the top contended locks (expected: `Dataset.dataMu`, HNSW insert lock, text-index queue). Script: `scripts/profile_shared_locks.sh`.
- [x] **Split read/write lock scopes (partial)**: `DeleteVectorByID` now resolves location under RLock and applies tombstone under a short write lock; `SearchByID`/`RangeSearch` release `dataMu` before index calls; federated router lock released during search. Full per-column locks remain.
- [x] **Non-blocking delete path (partial)**: `DeleteVectorByID` no longer holds the dataset write lock during location resolution; tombstone application is a short critical section. Full enqueue-to-ingestion-worker deferred.
- [ ] **Auto-scaler hysteresis**: prevent the ingestion/indexing auto-scaler from draining below a minimum (currently 4) while pending work exists; add a "busy" signal from the dataset lock queue.
- [ ] **Shared-namespace soak gate**: add a CI soak variant (8 agents, 2 per namespace, 10 min) that fails on QPS drop > 50% or stall; track in `docs/development/soaktestplan.md`.
- [x] **Cross-agent memory test**: script `scripts/verify_cross_agent_memory.py` verifies agent A writes are searchable/gettable by agent B.

### AgentSoak Go Tool & Collation Pipeline (Planned)

A native Go tool (like `cmd/bench-tool`) that simulates agent behaviors at scale,
with a Python collation script similar to `scripts/unified_benchmark.py`.

Subtasks:
- [x] **Design `cmd/agentsoak` CLI**: `-config` loads `configs/multiagent_soak.json`; flags for agents, duration, dim, seed, namespace names, and output JSON override it; reuse `client.SmartClient`.
- [x] **Agent goroutine model**: each agent runs its own read/write/delete loop with random pauses; per-agent private, shared, and common namespaces with server-enforced RBAC.
- [ ] **Workload mix**: dense/hybrid/sparse/filtered/range/by-id/temporal/geo/graph/multimodal/federated/insert/upsert/delete with configurable weights.
- [x] **Cross-agent memory verification**: agent B searches memories written by agent A; assert non-empty results.
- [x] **Metrics collection**: QPS, error rate, p50/p95/p99, memory RSS, per-op counts; JSON output.
- [x] **Python collation script** (`scripts/agent_soak_bench.py`): run the Go tool, aggregate multiple runs, generate report/CSV.
- [ ] **CI soak gate**: default 3 agents / 7 namespaces, 10 min, fail on >50% QPS drop or stall.
- [ ] **Unit/fuzz tests**: extend coverage to the full agent loop, workload selection, metrics, and report collation (config/topology/flag-override tests now exist).

### Authenticated & Encrypted Soak Variants (Planned)

- [ ] **TLS soak variant**: run agent soak over TLS (gRPC with TLS) and measure latency/QPS overhead vs plaintext.
- [ ] **mTLS / OIDC auth soak**: RBAC with per-agent API keys is already exercised by the multi-namespace scenario; add TLS/mTLS transport and OIDC workload identity on top.
- [ ] **Encryption-at-rest soak**: enable disk encryption and measure ingest/search latency and CPU overhead.
- [ ] **Auth + encryption combined**: full secure soak (TLS + mTLS + encrypted disk) for 1h with 8 agents.
- [ ] **Performance comparison report**: collate plaintext vs TLS vs mTLS vs encrypted-disk QPS/latency/RSS.

### Multi-Namespace Permission Soak

- [x] **Server-side RBAC for shared namespaces**: both simulators provision scoped per-agent API keys and verify at startup that private namespaces are invisible, shared namespaces are read-only for non-owners, and the common namespace is writable by all.
- [x] **Cross-agent memory verification tests**: agent B reads/searches memories written by agent A (scripts/verify_cross_agent_memory.py and the simulator startup probes).
- [ ] **Configurable permission soak gate**: default 3 agents / 7 namespaces (and a scaled 10-agent / 21-namespace variant), 1h, 0 errors, no QPS stall.
- [ ] **Permission audit logging**: log denied write/delete attempts under load.

### Federated Search Concurrency (New Finding)

- [ ] **Federated search under concurrent ingest**: `FederatedSearch` calls
  `syncDatasetToFederatedRouter` on every request, which acquires `dataMu.RLock`
  per dataset; under sustained ingest this blocks. Cache the router registration
  and refresh lazily.
- [ ] **Federated soak stability**: add a dedicated federated-only soak variant
  (2 agents, shared namespace, 5 min) to CI.
