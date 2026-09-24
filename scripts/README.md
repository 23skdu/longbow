# Longbow Scripts Directory

Development utilities for testing, benchmarking, security scanning, and validation.
Deprecated/redundant shell scripts have been removed; testing is consolidated into
the unified suite below.

## Quick reference

| Script | Type | Purpose |
|--------|------|---------|
| `unified_benchmark.py` | Python | Primary correctness + performance benchmark matrix |
| `check_regression.py` | Python | Fail CI if results regress past a baseline threshold |
| `analyze_results.py` | Python | Aggregate `perf_matrix_*.json` logs and print summaries |
| `test_tensor_engine.py` | Python | Tensor engine test suite (9 domains, bench, fuzz) |
| `test_tensor_engine.sh` | Shell | Thin wrapper → `test_tensor_engine.py` |
| `cli_benchmark.py` | Python | Blackbox functional tests for `longbow-cli` |
| `verify_driver.py` | Python | Load/smoke-test the ADBC driver (`liblongbow_adbc.so`) |
| `check_govuln.sh` | Shell | govulncheck with allowlist; exit 1 on unexpected CVEs |
| `gpu_memcheck.sh` | Shell | CUDA memory-leak check via `compute-sanitizer` |
| `run_all_benchmarks.sh` | Shell | Master run: 4 configs × 8 dtypes × 2 sizes × 5 modes |
| `run_benchmark_full.sh` | Shell | Full run: 4 configs × disk on/off |
| `run_full_benchmark.sh` | Shell | All build variants at 200k/500k/1M counts |
| `requirements.txt` | — | Python dependencies for the scripts above |

## Benchmarking

### `unified_benchmark.py`

Primary benchmark tool. Runs ingest + search across dimensions, dtypes, and counts.
Supports `cpu`, `metal` (macOS), and `cuda` (Linux) modes. Writes
`data/perf_logs/perf_matrix_*.json`.

**Key arguments**:

- `--mode`: Backend mode (`cpu`, `metal`, `cuda`, `onnx`, `recommend`, `deletion`, `graphrag`, `exchange`, `cluster`, `temporal`)
- `--dims`: Comma-separated dimensions (default: `128,384`)
- `--counts`: Comma-separated batch sizes (default: `1000,5000,25000,100000,250000`)
- `--dtypes`: Comma-separated data types (`float32`, `int8`, `complex64`, `turboquant8`, …)
- `--search-modes`: `hybrid`, `dense`, `sparse`, `filtered`, `byid` (plus mode-specific)
- `--memory`: Max memory in bytes
- `--ci`: Compact CI matrix (dims=128, counts=10000/50000, dtypes=float32/int8, dense)
- `--runs`: Repeat count for mean/stdev
- `--save-baseline PATH`: Copy this run's results as a baseline JSON
- `--compare-baseline PATH`: Compare against baseline; fail if regression > threshold
- `--threshold`: Regression threshold % for compare (default: 10)
- `--use-disk`: Enable auto-spill (`LONGBOW_AUTO_SPILL_DISK`); auto-enabled ≥100k

**Usage**:

```bash
# Full local matrix
python3 scripts/unified_benchmark.py --mode cpu --dtypes float32,int8 \
  --dims 128,384 --counts 1000,5000 --search-modes hybrid,dense

# CI-style run and save as regression baseline
python3 scripts/unified_benchmark.py --ci --runs 3 \
  --save-baseline benchmarks/baseline_cpu.json

# Compare a new run against the committed baseline
python3 scripts/unified_benchmark.py --ci --runs 1 \
  --compare-baseline benchmarks/baseline_cpu.json --threshold 10
```

### `check_regression.py`

Standalone regression gate. Compares a results JSON against a baseline JSON and
exits non-zero if any matched config regresses beyond the threshold.
Zero-QPS baseline entries are skipped.

**Exit codes**: `0` = pass, `1` = regression, `2` = missing/invalid files.

```bash
python3 scripts/check_regression.py \
  --baseline benchmarks/baseline_cpu.json \
  --results data/perf_logs/perf_matrix_latest.json \
  --threshold 10
```

### `analyze_results.py`

Reads all `data/perf_logs/perf_matrix_*.json`, groups by config label
(CPU Standard / CPU Emlgo / GPU Standard / GPU Emlgo), and prints per-entry
peak memory and search modes. Useful for cross-run comparison after
`run_*.sh` matrices.

```bash
python3 scripts/analyze_results.py
```

### `run_all_benchmarks.sh`

Master matrix: 4 build configs (CPU std, CPU emlgo, GPU std, GPU emlgo) ×
8 dtypes × 2 sizes (50k, 500k) × 5 search modes. Sequential; 2h timeout.
Copies the appropriate server binary into `bin/` before each config.

```bash
./scripts/run_all_benchmarks.sh
```

### `run_benchmark_full.sh`

Same 4 configs but × 2 disk modes (`use_disk=yes|no`) at 100k/250k counts.
1h timeout per config. Labels results with `_disk` / `_nodisk` suffix.

```bash
./scripts/run_benchmark_full.sh
```

### `run_full_benchmark.sh`

Runs all 4 build variants (expects prebuilt binaries under
`bin/bench-variants/`) at 200k, 500k, and 1M counts across all dtypes and
search modes. Results → `data/bench-results/`. Rotates ports from 13000.

```bash
./scripts/run_full_benchmark.sh
```

## Testing

### `test_tensor_engine.py` (and `test_tensor_engine.sh`)

Comprehensive suite for the Tensor Engine (`internal/tensor`), covering 9 domains:

1. Core tensors & data types (float32/64, complex64/128, int/uint, strides, slices, clones)
2. Elementwise & transcendental math (arith, broadcast, trig, exp, log, sqrt, …)
3. Linear algebra & contractions (matmul, dot, outer, `TensorContract`)
4. Einstein summation (`Einsum`, path optimizer `OptimizePath`)
5. Computational DAG & optimizer (IR, CSE, constant folding, algebraic rewrites)
6. Relativistic & differential geometry (Levi-Civita, metric inverse, Christoffel, Riemann/Ricci, wedge)
7. Multi-dtype execution (float64, complex128, int64)
8. Hardware acceleration (AVX2 SIMD GEMM, fast-math dispatch vs pure Go)
9. Fuzzing & microbenchmarks (einsum parser, broadcast, GEMM)

```bash
# All tests
python3 scripts/test_tensor_engine.py

# With microbenchmarks and Go fuzz targets
python3 scripts/test_tensor_engine.py --bench --fuzz

# Filter by category
python3 scripts/test_tensor_engine.py --category calculus
python3 scripts/test_tensor_engine.py --category einsum

# Machine-readable report
python3 scripts/test_tensor_engine.py --json-report tensor_report.json

# Shell wrapper (forwards all args)
./scripts/test_tensor_engine.sh --category core -v
```

Other flags: `--unit-only`, `--verify-only`, `--no-color`, `-v/--verbose`.

### `cli_benchmark.py`

Blackbox functional suite for `longbow-cli`. Spins up an isolated ephemeral
server on port 3300 with a temp data dir, then exercises: namespace/dataset
management, vector + geospatial search, GraphRAG (PageRank, traversal,
community detection), temporal search, snapshot/stats/drop. Requires
`bin/longbow` and `bin/longbow-cli` to be built first.

```bash
go build -o bin/longbow ./cmd/longbow
go build -o bin/longbow-cli ./cmd/cli
python3 scripts/cli_benchmark.py
```

### `verify_driver.py`

Loads `liblongbow_adbc.so` via `adbc-driver-manager` and smoke-tests
connection, a simple query, and parametric binding (expected to be stubbed).
Used by `make` driver verification.

```bash
# Requires: pip install adbc-driver-manager
# Build liblongbow_adbc.so first, then run from repo root:
python3 scripts/verify_driver.py
```

## Security

### `check_govuln.sh`

Runs `govulncheck ./...` and fails only on vulnerabilities **outside** the
allowlist (hamba/avro GO-2026-5046/5047/5048, x/crypto openpgp GO-2026-5932 —
see `.trivyignore` and `docs/nextsteps.md`).

**Exit codes**: `0` = clean/allowlisted, `1` = unexpected vuln, `2` = tool missing/scan failed.

```bash
./scripts/check_govuln.sh
```

### `gpu_memcheck.sh`

Wraps CUDA `compute-sanitizer --tool memcheck` against a Longbow binary to
detect leaks and invalid memory accesses. Requires NVIDIA GPU + CUDA toolkit.

**Exit codes**: `0` = clean, `1` = leaks/errors, `2` = sanitizer unavailable.

```bash
./scripts/gpu_memcheck.sh ./bin/longbow [extra args...]
```

## Setup

Install Python dependencies before running any Python tool:

```bash
pip install -r scripts/requirements.txt
```

`requirements.txt` includes: pyarrow, numpy, pandas, sentence-transformers,
lorem-text, psutil, matplotlib, requests, grpcio, grpcio-tools, protobuf, pydantic.

## Binary paths

Python/shell scripts assume Go binaries are built into the project root `bin/`:

```bash
go build -o bin/longbow ./cmd/longbow
go build -o bin/bench-tool ./cmd/bench-tool
go build -o bin/longbow-cli ./cmd/cli
```

| Binary | Source | Used by |
|--------|--------|---------|
| `bin/longbow` | `cmd/longbow` | All benchmarks, `cli_benchmark.py`, `gpu_memcheck.sh` |
| `bin/bench-tool` | `cmd/bench-tool` | `unified_benchmark.py` load generation |
| `bin/longbow-cli` | `cmd/cli` | `cli_benchmark.py` |

## Output locations

| Path | Written by |
|------|------------|
| `data/perf_logs/perf_matrix_*.json` | `unified_benchmark.py`, `run_*.sh` |
| `data/bench-results/` | `run_full_benchmark.sh` |
| `data/cli_bench/` | `cli_benchmark.py` (ephemeral) |
| `benchmarks/baseline_cpu.json` | `unified_benchmark.py --save-baseline` (committed) |
