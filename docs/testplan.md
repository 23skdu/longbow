# Longbow Test Plan

This document describes how to run, reproduce, and extend the Longbow test
suite. It is the single source of truth for **what to run, with which flags,
and what to look for**. Two distinct kinds of work are covered:

1. **Correctness / regression tests** — Go unit tests + the `cli_benchmark.py`
   functional suite, run in CI on every PR.
2. **Performance benchmarks** — the `unified_benchmark.py` orchestrator and
   the `bench-tool` Go client, run in CI on every PR (fast mode) and
   ad-hoc on the developer's box (full matrix).

The plan was rewritten in June 2026 to incorporate:
- The P0 `arena is nil` reader-pin fix (commit `a2f535ef`) and its regression
  test `TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress`.
- The new `cli_benchmark.py` and `unified_benchmark.py` flag surface (2026-Q2).
- The post-`faa5e546` Makefile and Go-test conventions (no `make test`,
  use `go test` directly).
- The current set of CI workflows under `.github/workflows/`.

---

## 1. Test Environment

### Hardware baseline

| Attribute | Value |
|---|---|
| CPU | Alder Lake i7-12650H, 16 cores (AVX2, FMA, F16C, **no AVX-512**) |
| RAM | 22 GB total, **16 GB available** for the longbow process |
| Swap | 15 GB |
| GPU | NVIDIA RTX 4060 Laptop (8 GB VRAM, compute 8.9) — CUDA mode optional |
| Drive | NVMe, ≥ 50 GB free for `data/` and `profiles/` |
| Kernel | Linux ≥ 5.15, `io_uring` enabled |

CI runners: GitHub Actions `ubuntu-latest` (no GPU; CPU-only). The
`benchmark.yml` workflow does not attempt CUDA.

### Toolchain

| Tool | Version |
|---|---|
| Go | 1.26.3 (`go.mod` `toolchain` directive) |
| golangci-lint | v1.64 |
| benchstat | `go install golang.org/x/perf/cmd/benchstat@latest` |
| Python | 3.10+ |
| pip deps | `scripts/requirements.txt` (`grpcio`, `numpy`, `pandas`, `pyarrow`) |

---

## 2. Build the Binaries

The plan is **binary-first** — every script assumes the Go binaries are
already built. Build them once per checkout:

```bash
# Server (the system under test)
go build -o bin/longbow ./cmd/longbow

# Load generator (the only binary the benchmark orchestrator needs)
go build -o bin/bench-tool ./cmd/bench-tool

# Functional CLI (used by cli_benchmark.py and for manual smoke tests)
go build -o bin/longbow-cli ./cmd/cli

# ADBC shared library (only needed for `make test-adbc-python`)
make build-adbc   # produces liblongbow_adbc.so + liblongbow_adbc.h

# Soak-test client (optional, used for chaos testing)
go build -o bin/soak_test ./cmd/soak_test

# Standalone disk I/O benchmark (NOT used by unified_benchmark.py — the
# script uses cmd/bench-tool. Building this as "bench-tool" will break
# benchmarks; always build as: go build ./cmd/io-bench → bin/io-bench)
go build -o bin/io-bench ./cmd/io-bench
```

> The legacy `make test` / `make build` targets still work but are no
> longer the recommended invocation. `go test ./...` and
> `go build ./...` are the canonical commands.

### `longbow` server CLI flags

`bin/longbow --help` (only flags are listed; env-var equivalents
`LONGBOW_*` take the same value):

| Flag | Default | Purpose |
|---|---|---|
| `-data-path` | `./data` | On-disk dataset directory |
| `-listen-addr` | `0.0.0.0:3000` | gRPC data-plane listen address |
| `-meta-addr` | `0.0.0.0:3001` | gRPC meta-plane listen address |
| `-metrics-addr` | `0.0.0.0:9090` | Prometheus scrape endpoint |
| `-max-memory` | `1073741824` (1 GB) | `LONGBOW_MAX_MEMORY` in bytes |
| `-gpu-enabled` | `false` | Enable CUDA dispatch |
| `-gpu-device-id` | `0` | CUDA device ordinal |
| `-gossip-enabled` | `false` | SWIM gossip for clustering |
| `-node-id` | `hostname` | Cluster node identifier |
| `-log-level` | `info` | `debug` / `info` / `warn` / `error` |
| `-log-format` | `json` | `json` or `console` |

CLI flags take precedence over `LONGBOW_*` env vars (per the
`getEnvOrFlag` helper in `cmd/longbow/main.go`).

### `bench-tool` client flags

`bin/bench-tool --help`:

| Flag | Type | Default | Purpose |
|---|---|---|---|
| `-uri` | string | `127.0.0.1:3000` | Server URI (`grpc://` or `unix://`) |
| `-dataset` | string | `bench_go` | Target dataset name |
| `-dim` | int | `128` | Vector dimensions (max 3072) |
| `-scale` | int | `1000` | Total vectors to ingest |
| `-dtype` | string | `float32` | `float32`, `int8`, `turboquant8`, etc. |
| `-tq-bits` | int | `4` | TurboQuant bits (2 / 4 / 8) |
| `-queries` | int | `1000` | Queries per search mode |
| `-workers` | int | `1` | Concurrent search workers |
| `-mode` | string | `vec` | `vec` / `kv` / `cluster` |
| `-search-modes` | string | `all` | CSV: `dense`, `hybrid`, `sparse`, `filtered`, `byid`, `graphrag`, `geo`, `temporal`, `learned_index` |
| `-fbin` | string | `""` | Read vectors from Arrow IPC binary or `.fbin` |
| `-output-fbin` / `-output-arrow` | string | `""` | Save generated vectors and exit |
| `-json` | string | `""` | Save stats as JSON to this path |
| `-drop` | bool | `false` | Drop dataset after benchmark |
| `-reset` | bool | `false` | Reset dataset in-place before benchmark |

### `longbow-cli` commands

`bin/longbow-cli <command> [options]`. Global option: `-uri` (default
`grpc://127.0.0.1:3000`). Commands: `import`, `export`, `search`,
`create-namespace`, `delete-namespace`, `list-namespaces`,
`list-datasets-in-namespace`, `stats`, `geo-search`, `recommend`,
`delete`, `snapshot`, `add-edge`, `traverse`, `get-graph-stats`,
`pagerank`, `detect-communities`, `temporal-search`, `drop`,
`download-model`.

---

## 3. Correctness Tests (run on every PR)

### 3.1 Go unit tests

```bash
# Race detector on (mandatory for any PR that touches internal/store or internal/memory)
go test -race -timeout 900s ./...

# Coverage profile (CI uploads to Codecov via the coverage.txt artifact)
go test -race -coverprofile=coverage.txt -covermode=atomic ./...

# Lint (required to pass before merge)
golangci-lint run --timeout 5m
```

The full `internal/store/index` package takes ~3 min with `-race` on the
CI runner. The full suite including ADBC, simd, and gpu packages takes
~15 min.

### 3.2 Pre-existing flakes (safe to skip on CI)

These fail intermittently on a clean `main` checkout with no source
changes. They are tracked separately and do not block PRs:

| Test | Symptom |
|---|---|
| `TestDoGetSearch_Integration` | `simd: length mismatch` or `Index should reach 3 records` (timing-dependent) |
| `TestMemoryLeak_CreateDropDataset` | empty list (finalizer timing) |
| `TestArrowHNSW_Concurrency_AddBatch` | already wrapped in `-race`-skip in the source file |

Use `-run` to skip them locally:

```bash
go test -race -timeout 900s -skip 'TestDoGetSearch_Integration|TestMemoryLeak_CreateDropDataset' ./...
```

### 3.3 The P0 regression tests (must run)

These are the tests that catch the highest-impact regressions and **must
not be skipped**:

```bash
# Reader-pin correctness (P0 arena-nil fix, commit a2f535ef)
go test -timeout 240s -run 'TestArrowHNSW_ConcurrentAddBatch' -v ./internal/store/index/

# InBulkInsert ref-counter (commit 0cddf75a)
go test -timeout 180s -run 'TestArrowHNSW_AddBatch_Concurrent' -v ./internal/store/index/

# CAS leak + race fix (commit cb30b97d)
go test -race -timeout 180s -run 'TestArrowHNSW_Concurrency_MixedReadWrite' -v ./internal/store/index/
```

The 50k int8 concurrent stress test
(`TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress`) takes ~76 s on the
CI runner and produces a clean PASS/FAIL. It exercises 5 concurrent
`AddBatch` calls of 10k int8 vectors each (50k total, dim=384) — the
exact config that triggered the original P0 bug.

### 3.4 ADBC driver (Python integration)

```bash
make build-adbc            # builds liblongbow_adbc.so
python3 scripts/verify_driver.py
```

`verify_driver.py` is a 30-line smoke test that loads the ADBC
shared library and runs `SELECT * FROM system.tables`. Used to catch
CGo / build-mode regressions.

### 3.5 Functional CLI smoke (blackbox)

```bash
python3 scripts/cli_benchmark.py
```

Spins up an **isolated** longbow server on port 3300 (separate from any
local instance), runs the full `longbow-cli` command surface against
it, and asserts every command returns success. Takes ~2 min. The
script:

- Cleans `data/cli_bench/` and starts a fresh server with
  `LONGBOW_LISTEN_ADDR=127.0.0.1:3300` etc.
- Runs import → export → search (dense/sparse/filtered/hybrid/geo/
  temporal) → graph (add-edge, traverse, pagerank, lpa) → snapshot
  → drop.
- Reports per-command PASS/FAIL with stdout/stderr on failure.

This is the closest thing to an end-to-end integration test in the
project and should be the **first** test to re-run when triaging a
"longbow is broken" bug report.

---

## 4. Performance Benchmarks

### 4.1 The orchestrator: `unified_benchmark.py`

`scripts/unified_benchmark.py` is the **only** benchmark entry point
for the longbow server. It generates test data, starts/stops the
server, runs `bench-tool` against it, captures pprof profiles, and
writes a JSON + Markdown report.

#### Modes (`--mode`)

| Mode | Backend / scenario |
|---|---|
| `cpu` | AVX2 SIMD dispatch (the default; what we run on the CI runner) |
| `cuda` | NVIDIA GPU dispatch (requires `--gpu-enabled` on the server) |
| `metal` | Apple GPU dispatch (macOS only) |
| `onnx` | ONNX Runtime embedding generation |
| `recommend` | Graph-based recommendation from seed IDs |
| `deletion` | Add → delete → re-add cycles |
| `graphrag` | Multi-hop graph spreading activation |
| `exchange` | Cross-namespace / cross-dataset exchange |
| `cluster` | Multi-node cluster behaviour |
| `temporal` | Temporal index (`as-of`, range, window) |
| `geo` | Geospatial radius search |
| `churn` | Soak test with add/delete churn (configurable cycles + payload sizes) |
| `learned_index` | Learned index traversal validation |

Multiple modes are accepted as a comma-separated list.

#### Core flags

| Flag | Type | Default | Notes |
|---|---|---|---|
| `--dims` | CSV | `128,768` | Vector dimensions to test |
| `--counts` | CSV | `1000,5000` | Vector counts to test (the script uses the first) |
| `--dtypes` | CSV | `float32,float16,int8` | Data types |
| `--search-modes` | CSV | `all` | dense / sparse / hybrid / filtered / byid / graphrag / geo / temporal / learned_index |
| `--memory` | int | `10737418240` (10 GB) | Server `LONGBOW_MAX_MEMORY` |
| `--timeout` | int | `1800` | Per-run timeout in seconds |
| `--duration` | int | `15` | Per-test duration in seconds (used by `churn` etc.) |
| `--queries` | int | `1000` | Queries per search mode |
| `--batch-size` | int | `1000` | Ingest batch size |
| `--workers` | int | `8` | Concurrent search workers |
| `--startup-timeout` | int | `120` | Server startup timeout (seconds) |
| `--addr` | string | `127.0.0.1:3000` | Server address |
| `--metrics-addr` | string | `127.0.0.1:9090` | Prometheus metrics endpoint |
| `--port` | int | `3000` | Base port (incremented per config to avoid collisions) |
| `--label` | string | `""` | Suffix for result files and pprof profiles |
| `--output-dir` | string | `data/generated` | Where to write generated `.fbin`/Arrow test data |
| `--pprof` | bool | `false` | Collect heap/allocs/block/mutex/goroutine/threadcreate profiles |
| `--max-retries` | int | `1` | Retries on transient server-startup failures |
| `--cache` | bool | `false` | Skip config if its result JSON already exists |
| `--full` | bool | `false` | Run the release-candidate matrix; overrides `--dims`/`--counts`/`--dtypes` defaults if not explicitly set |
| `--ci` | bool | `false` | **Run a reduced fast matrix** for CI (see §4.3). Equivalent to `LONGBOW_BENCH_FAST=1` env var. |
| `--numa-bind` / `--no-numa-bind` / `--numa-compare` | bool | `true` on Linux | NUMA pinning for the server process |
| `--low-mem` | bool | `false` | Sets `LONGBOW_LOW_MEM=1` |
| `--use-disk` | bool | `false` | Sets `LONGBOW_USE_DISK=1` |
| `--pq-ingest` | bool | `false` | Sets `LONGBOW_PQ_INGEST=1` |
| `--iouring` | bool | `false` | Sets `LONGBOW_STORAGE_USE_IOURING=true` (Parquet snapshots via io_uring) |
| `--rdma` | bool | `false` | Sets RDMA / RoCEv2 env flags |
| `--debug` | bool | `false` | Sets `LONGBOW_DEBUG=true` for verbose logging |
| `--fbin` / `--arrow` | string | `""` | Reuse pre-generated test data instead of generating new |
| `--generate-only` | bool | `false` | Generate test data files and exit |
| `--alpha-values` / `--graph-alpha-values` | CSV | `0.0,0.5,1.0` / `0.0,0.3,0.5,0.7,1.0` | For `recommend` / `graphrag` |
| `--k-values` | CSV | `5,10,20` | For `recommend` |
| `--num-seeds` | int | `5` | For `recommend` |
| `--max-hops` | int | `2` | For `graphrag` |
| `--decay` | float | `0.5` | Multi-hop decay for `graphrag` |
| `--delete-counts` | CSV | `100,500,1000` | For `deletion` |
| `--cluster-nodes` | int | `3` | For `cluster` |
| `--churn-payload-sizes` | CSV | `0,1,4,64,256,1024` | KB; for `churn` |
| `--churn-cycles` | int | `10` | For `churn` |
| `--churn-chunk-size` | int | `1000` | For `churn` |
| `--learned-samples` / `--learned-confidence` / `--learned-interval` | int / float / int | `0` | Tunables for `learned_index` |

#### Outputs

| Artifact | Path |
|---|---|
| Per-run JSON | `data/perf_logs/result_cpu_<dtype>_<dim>_<count>.json` |
| Matrix JSON | `data/perf_logs/perf_matrix_<label>_<timestamp>.json` |
| Matrix Markdown | `data/perf_logs/perf_matrix_<label>_<timestamp>.md` |
| Server logs | `data/perf_logs/longbow_*.log` |
| Benchmark logs | `data/perf_logs/bench_*.log` |
| pprof files | `profiles/<label>_<timestamp>_<config>_<profile>.pprof` |

### 4.2 Full matrix (developer / release run)

The 12-config matrix from `docs/performance.md` (commit `41793e13`):

```bash
mkdir -p data/perf_logs
python3 scripts/unified_benchmark.py \
  --dims 128,384 \
  --dtypes float32,int8 \
  --counts 10000,50000,100000 \
  --search-modes dense,sparse \
  --queries 1000 \
  --memory 16 \
  --label full-matrix \
  --pprof \
  --output-dir data/perf_logs
```

Wall-clock on the i7-12650H baseline: **~8 min for 12 configs**.
On the CI runner (no AVX2 dispatch, no NUMA): expect ~25–35 min.

For CUDA runs (must be on a host with an NVIDIA GPU + CUDA toolkit):

```bash
LONGBOW_GPU_ENABLED=true \
python3 scripts/unified_benchmark.py \
  --mode cuda \
  --dims 384 \
  --dtypes float32,float16,int8,turboquant8 \
  --counts 10000,50000,500000,1000000 \
  --search-modes all \
  --memory 16 \
  --iouring \
  --use-disk \
  --timeout 7200 \
  --queries 500 \
  --workers 6 \
  --max-retries 2 \
  --label rc-cuda
```

### 4.3 Fast / CI matrix (`--ci`)

The `--ci` flag is the **fast smoke matrix** for CI. It overrides
several defaults to keep the run under 15 min on a stock GitHub
runner:

```bash
# Inside .github/workflows/ci.yml "benchmark-regression" job:
python3 scripts/unified_benchmark.py --ci
```

`--ci` sets:
- `--dims=128`
- `--counts=10000,50000`
- `--dtypes=float32,int8`
- `--search-modes=dense` (only, if `--search-modes` is still the
  default `all`)

This is a **functional smoke test**, not a performance run. It
verifies the server starts, accepts ingest, returns non-zero QPS, and
does not crash. The full performance matrix is the developer's
responsibility before tagging a release.

The same fast-mode defaults can be activated via the
`LONGBOW_BENCH_FAST` environment variable:

```bash
# Equivalent to: python3 scripts/unified_benchmark.py --ci
LONGBOW_BENCH_FAST=1 python3 scripts/unified_benchmark.py
```

The env var accepts the truthy values `1`, `true`, `yes`, `on`
(case-insensitive, whitespace-stripped). The CLI flag and the env
var are OR-merged — if either is set, fast mode is on, and fast
mode overrides `--dims`/`--counts`/`--dtypes`/`--search-modes` (this
is the existing `--ci` semantics; the env var is purely a synonym
that lets CI configs set the mode via the environment without
editing the script invocation). Both are processed in
`scripts/unified_benchmark.py:3258-3294`.

### 4.4 Go micro-benchmarks (PR regression detection)

`.github/workflows/benchmark.yml` runs these on every PR and
diffs against `main` using `benchstat`:

```bash
# SIMD distance kernels
go test -bench=BenchmarkEuclidean -benchmem -count=5 ./internal/simd/...

# Search hot paths
go test -bench=BenchmarkSearch -benchmem -count=5 ./internal/store/...

# Insert / bulk insert hot paths
go test -bench=BenchmarkArrowHNSWInsert -benchmem -count=5 ./internal/store/...
```

A regression > 5% in any benchmark is flagged in the PR comment.

### 4.5 The 12-config performance matrix (release baseline)

| # | dim | dtype | count |
|---|---|---|---|
| 1 | 128 | float32 | 10,000 |
| 2 | 384 | float32 | 10,000 |
| 3 | 128 | int8 | 10,000 |
| 4 | 384 | int8 | 10,000 |
| 5 | 128 | float32 | 50,000 |
| 6 | 384 | float32 | 50,000 |
| 7 | 128 | int8 | 50,000 |
| 8 | 384 | int8 | 50,000 |
| 9 | 128 | float32 | 100,000 |
| 10 | 384 | float32 | 100,000 |
| 11 | 128 | int8 | 100,000 |
| 12 | 384 | int8 | 100,000 |

Current numbers (commit `41793e13`):

| dim | dtype | count | dense QPS | sparse QPS | note |
|---|---|---|---|---|---|
| 128 | float32 | 10,000 | 6,776.9 | 7,868.8 | |
| 384 | float32 | 10,000 | 6,272.6 | 7,794.8 | |
| 128 | int8 | 10,000 | 3,513.5 | 7,350.0 | |
| 384 | int8 | 10,000 | 3,357.9 | 6,746.5 | |
| 128 | float32 | 50,000 | 6,544.6 | 8,337.9 | |
| 384 | float32 | 50,000 | 6,256.1 | 8,319.0 | |
| 128 | int8 | 50,000 | 3,719.5 | 7,665.2 | |
| 384 | int8 | 50,000 | **549.4** | **7,545.6** | post-P0-fix (was 0.0) |
| 128 | float32 | 100,000 | 6,672.3 | 7,912.6 | |
| 384 | float32 | 100,000 | 6,186.7 | 7,995.6 | |
| 128 | int8 | 100,000 | **476.8** | **7,499.9** | post-P0-fix (was 0.0) |
| 384 | int8 | 100,000 | **283.7** | **7,503.0** | post-P0-fix (was 0.0) |

The four `int8` 50k+ rows were 0 QPS in the pre-fix matrix (commit
`cb30b97d`); the P0 fix in commit `a2f535ef` made them functional.
Dense QPS at 50k+ is 5–10× lower than the int8 10k baseline because
the in-place chunk allocator (lazy `initArenaSafe` + Slab ref-count
churn) becomes the bottleneck — see Rec #6 in `docs/nextsteps.md`.

---

## 5. Test Methodology — How to Triage a Failure

### 5.1 "arena is nil" / typed-arena errors

This was the P0 bug. If a new instance of this surfaces:

1. Run the 50k stress test:
   ```bash
   go test -race -timeout 240s -run TestArrowHNSW_ConcurrentAddBatch_Int8_50k_Stress \
     -count=3 -v ./internal/store/index/
   ```
2. Check `internal/store/types/graph_data.go:3501-3525` — the
   `Release()` spin-wait on `readerCount`. If the wait is missing
   or the pin sites in `internal/store/index/*.go` regressed, this
   is the root cause.
3. Scrape the `longbow_arena_nil_error_total` Prometheus counter
   (labelled by method: `AllocSlice` / `AllocSliceDirty` /
   `AllocSliceAligned`). A non-zero value means a regression of
   the reader-pin contract; the label tells you which allocator
   surface regressed. The metric is silent in healthy operation,
   so even a single increment in production is a strong signal.
   Grafana alert: `rate(longbow_arena_nil_error_total[5m]) > 0`.

### 5.2 "race detected" on `-race`

1. `go test -race -count=10 -run <FailingTest> ./internal/store/...`
2. The stack trace points to the racing goroutines. Almost all races
   in `internal/store/index/` are COW (copy-on-write) issues on
   `GraphData` or its typed-arenas. Pattern: one goroutine mutates
   the published `*GraphData` while another is reading it.
3. **The fix template**: bracket the read path with
   `data.AcquireReader()` / `defer data.ReleaseReader()`. This is
   the pattern added in commit `a2f535ef`.

### 5.3 Benchmark QPS regression

1. Check the most recent commits that touched
   `internal/store/index/arrow_hnsw_*` and
   `internal/store/types/graph_data.go`.
2. Run the matrix and diff against `docs/performance.md`:
   ```bash
   python3 scripts/unified_benchmark.py \
     --dims 128,384 --dtypes float32,int8 --counts 10000,50000 \
     --search-modes dense,sparse --label regression-check
   ```
3. If regression > 5% in dense QPS, the change needs a
   `go test -bench=BenchmarkArrowHNSWInsert` micro-bench to localize.

### 5.4 OOM / ResourceExhausted at 384×1M

Expected at `complex128` (6.0 GB raw data + graph overhead). Other
dtypes should fit in 16 GB. If a `float32` 1M run OOMs:

1. Check `LONGBOW_MAX_MEMORY` is set correctly: `17179869184` (16 GB).
2. Run with `--pprof` and inspect `profiles/*_heap.pprof` in
   `go tool pprof`. Top allocators are usually gRPC buffer pools and
   `GraphData.Clone` chains — Rec #6 in `nextsteps.md` targets the
   latter.
3. Try `--low-mem` mode to switch the allocators to a smaller
   regime.

### 5.5 `cli_benchmark.py` functional failure

The script prints per-command PASS/FAIL with stdout/stderr. The
first failing command is the most informative — it tells you which
API surface broke. Common causes:

- `import` failed → check `bin/longbow` starts cleanly with
  `LONGBOW_LISTEN_ADDR=127.0.0.1:3300`.
- `search dense` failed → check the gRPC server is reachable on
  the data port (default 3300 in the test).
- `pagerank` / `detect-communities` failed → graph store
  regression; check `internal/store/graphrag/...` and the
  `add-edge` path.

---

## 6. CI Workflow Summary

| Workflow | File | What it runs | When |
|---|---|---|---|
| CI | `.github/workflows/ci.yml` | `golangci-lint`, `go build ./...`, `go test -race ./...`, fast benchmark (`--ci`) | Every push to `main` and every PR |
| Continuous Benchmarks | `.github/workflows/benchmark.yml` | `go test -bench=...` on PR branch vs `main`; `benchstat` diff | Every PR + every push to `main` |
| Helm Validation | `.github/workflows/helm-validation.yml` | Helm chart linting | Every push to `main` |
| Markdown Lint | `.github/workflows/markdown-lint.yml` | Lints `docs/**/*.md` | Every push to `main` |
| Release | `.github/workflows/release.yml` | Tag-driven build + publish | Tagged releases |

The CI matrix takes ~20 min on the GitHub-hosted runner. The benchmark
job is allowed 15 min for the fast matrix.

---

## 7. Environment Variables for High-Performance Runs

These are the env vars the longbow server honours. Most are exposed
via `--use-disk`, `--iouring`, `--gpu-enabled`, etc. on the
orchestrator; the raw values are listed here for the cases where you
launch the server by hand (e.g., for `cli_benchmark.py` or
`soak_test`).

```bash
# Memory and storage
LONGBOW_MAX_MEMORY=17179869184
LONGBOW_USE_DISK=1
LONGBOW_STORAGE_USE_IOURING=true
LONGBOW_STORAGE_DOPUT_BATCH_SIZE=1000
LONGBOW_LOW_MEM=1             # via --low-mem
LONGBOW_PQ_INGEST=1           # via --pq-ingest

# Hardware
LONGBOW_GPU_ENABLED=true
LONGBOW_GPU_DEVICE_ID=0
LONGBOW_AUTO_SCALE_ENABLED=false

# Feature toggles (used by the v0.2.1-rc6 matrix; mostly stable now)
LONGBOW_TEMPORAL_ENABLED=true
LONGBOW_SPARSE_ENABLED=true
LONGBOW_GEOSPATIAL_ENABLED=true
LONGBOW_GRAPHRAG_ENABLED=true
LONGBOW_LEARNED_INDEX_ENABLED=true
LONGBOW_HYBRID_SEARCH_ENABLED=true
LONGBOW_RERANKER_ENABLED=true

# Workers
LONGBOW_INGESTION_WORKER_COUNT=6

# gRPC
LONGBOW_GRPC_MAX_RECV_MSG_SIZE=21474836470
LONGBOW_GRPC_MAX_SEND_MSG_SIZE=21474836470
LONGBOW_GRPC_MAX_CONCURRENT_STREAMS=250

# Compaction
LONGBOW_COMPACTION_ENABLED=true
LONGBOW_COMPACTION_INTERVAL=30s

# HNSW
LONGBOW_HNSW_M=16              # 32 at 10k, 16 at 100k+ (auto-tuned by --ci / --full)
LONGBOW_HNSW_EF_CONSTRUCTION=200  # 400 at 10k, 200 at 100k+

# Runtime
GODEBUG=madvdontneed=1
GOGC=200
ARROW_DISABLE_LOCKING=1
```

---

## 8. Open Items

These are tracked in `docs/nextsteps.md` and will be folded into the
test plan as they land:

- **Rec #4**: Re-run the full v2.2.0 matrix (13 modes × 4 dtypes ×
  4 counts) post-fix.
- **Disk-backed validation at 1M+ vectors** (open).
- **CUDA execution on RTX 4060** (open).

See `docs/troubleshooting.md` for issue-specific runbooks and
`docs/nextsteps.md` for the prioritized backlog.
