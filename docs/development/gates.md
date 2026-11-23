# Longbow Code & Quality Gates Specification

## 1. Overview & Objective

Longbow enforces a strict, defense-in-depth quality and verification architecture. Because Longbow operates as an ultra-high-throughput vector database and analytical query engine leveraging hardware-accelerated SIMD kernels (AVX2, AVX-512, ARM Neon, Metal, and CUDA), traditional unit testing alone is insufficient. Code changes can introduce subtle floating-point precision drifts, algorithmic divergence between CPU and GPU kernels, memory leaks in unmanaged memory allocations, or unbounded resource consumption.

To guarantee zero regressions in correctness, performance, security, and stability, all contributions are subject to fifteen formal **Code Gates**. These gates are codified in local developer tooling ([`Makefile`](https://github.com/23skdu/longbow/blob/main/Makefile)), automated Git pre-commit hooks ([`scripts/verify.sh`](https://github.com/23skdu/longbow/blob/main/scripts/verify.sh), invoked by [`scripts/pre-commit.sh`](https://github.com/23skdu/longbow/blob/main/scripts/pre-commit.sh)), and the consolidated CI pipeline ([`.github/workflows/ci.yml`](https://github.com/23skdu/longbow/blob/main/.github/workflows/ci.yml)).

---

## 2. Gate Architecture & Enforcement Layers

The verification system is organized across three tiers:

```text
+-------------------------------------------------------------------------------+
| Layer 1: Developer Workstation Pre-Commit Hook (scripts/verify.sh)             |
| - Fast, bounded-memory validation (<1GB RSS, executes in ~8-12 seconds)       |
| - Format/vet/lint, gosec/gitleaks, pkg presence, accuracy, oracle, search-gate|
+-------------------------------------------------------------------------------+
                                      |
                                      v
+-------------------------------------------------------------------------------+
| Layer 2: Comprehensive Local Verification Pipeline (make ci)                  |
| - Local verification of 14 quality, security, and build gates                |
| - Linters, cross-compilers, SIMD check, search-gate, observability, container |
| - Release-required bounded soaks via `make soak-quick` (separate target)      |
+-------------------------------------------------------------------------------+
                                      |
                                      v
+-------------------------------------------------------------------------------+
| Layer 3: Consolidated Server-Side Pipeline (.github/workflows/ci.yml)         |
| - Single consolidated workflow triggered on-demand via workflow_dispatch      |
| - Multi-architecture emulation (ARM64 via QEMU), container release builds     |
+-------------------------------------------------------------------------------+
```

---

## 3. Detailed Code Gate Specifications

### Gate 1: 100% Package Test Presence Gate

- **Identifier**: `test-coverage-presence`
- **Enforcement Tool**: [`scripts/pre-commit.sh`](https://github.com/23skdu/longbow/blob/main/scripts/pre-commit.sh), `make test-coverage`, CI `validate` job
- **Command**:

```bash
UNTESTED=$(go test -list '.*' ./... 2>&1 | grep '\[no test files\]' || true)
[ -z "$UNTESTED" ]
```

- **Description**: Longbow maintains a zero-tolerance policy for untested packages. Every single package across `cmd/`, `internal/`, `client/`, and `tools/` must contain at least one active test file (`*_test.go`). If a new package is created without tests, the gate immediately fails the build.

---

### Gate 2: Unit & Statement Coverage Gate

- **Identifier**: `test-unit-coverage`
- **Enforcement Tool**: `make test`, `make test-all`, `make test-coverage`, CI `validate` job
- **Command**:

```bash
go test -v -short -coverprofile=coverage.txt ./...
```

- **Description**: Executes all standard Go unit tests and collects statement coverage across the codebase.
- **Memory Safety Constraint**: Longbow unit tests default to `-short` mode during local quality checks and pre-commit validation. Tests that initialize massive in-memory graph indices (e.g. 25,000 nodes) or heavy stress benchmarks are gated with `if testing.Short() { t.Skip(...) }`. This guarantees pre-commit verification executes within a strict 1 GB memory envelope and avoids OOM crashes on development machines.

---

### Gate 3: Fuzz Testing Gate

- **Identifier**: `test-fuzz`, `fuzz-coverage`
- **Enforcement Tool**: `make test-fuzz`, `make fuzz-coverage`, [`scripts/check_fuzz_coverage.sh`](https://github.com/23skdu/longbow/blob/main/scripts/check_fuzz_coverage.sh), [`scripts/pre-commit.sh`](https://github.com/23skdu/longbow/blob/main/scripts/pre-commit.sh)
- **Commands**:

```bash
./scripts/check_fuzz_coverage.sh
go test -short -run='^Fuzz' ./...
```

- **Description**: Two complementary checks:
  1. **100% Package Fuzz Presence**: [`scripts/check_fuzz_coverage.sh`](https://github.com/23skdu/longbow/blob/main/scripts/check_fuzz_coverage.sh) enumerates every package in the module and fails if any package lacks a `func Fuzz*(f *testing.F)` target in its `*_test.go` files, so fuzz coverage cannot silently regress as packages are added.
  2. **Deterministic Fuzz Execution**: Executes deterministic fuzzing passes on input-parsing engines, dimensional validators, filter expressions, vector normalization functions, and geo/temporal range query parsers. This ensures that arbitrary or malformed user queries cannot cause memory faults, panics, or unbounded allocations.

---

### Gate 4: Numerical Precision & Accuracy Gate

- **Identifier**: `accuracy-gate`
- **Enforcement Tool**: `make accuracy-gate`, [`scripts/pre-commit.sh`](https://github.com/23skdu/longbow/blob/main/scripts/pre-commit.sh)
- **Command**:

```bash
go test -v ./internal/simd/ -run='^TestSIMDAccuracy'
```

- **Description**: Validates numerical precision tolerances across all 15 supported vector data types:
  1. `float32` (standard single precision)
  2. `float64` (double precision)
  3. `float16` (IEEE 754 half precision)
  4. `bfloat16` (Brain floating point 16)
  5. `int8` (signed 8-bit integer)
  6. `uint8` (unsigned 8-bit integer)
  7. `int16` (signed 16-bit integer)
  8. `uint16` (unsigned 16-bit integer)
  9. `int32` (signed 32-bit integer)
  10. `int64` (signed 64-bit integer)
  11. `complex64` (paired float32 complex numbers)
  12. `complex128` (paired float64 complex numbers)
  13. `turboquant_polar` (TurboQuant 1-8 bit polar coordinate quantized representation)
  14. `turboquant_lut` (lookup-table accelerated TurboQuant decoding)
  15. `turboquant_prod` (quantized inner-product accumulator)
- **Tolerance Thresholds**: Standard floating-point SIMD kernels must maintain relative epsilon errors within $10^{-5}$ for `float32` and $10^{-10}$ for `float64`. Quantized representations must strictly match theoretical LUT codebooks.

---

### Gate 5: Ground-Truth Mathematical Parity Gate (Oracle Gate)

- **Identifier**: `oracle-gate`
- **Enforcement Tool**: `make oracle-gate`, [`scripts/pre-commit.sh`](https://github.com/23skdu/longbow/blob/main/scripts/pre-commit.sh)
- **Command**:

```bash
go test -v ./internal/simd/... ./internal/tensor/... ./internal/memory/... ./internal/store/index/ -run='Parity'
```

- **Description**: Highly optimized assembly and hardware-accelerated kernels can contain subtle boundary or stride errors. The Oracle Gate executes test suites that compare the output of every optimized SIMD kernel, TurboQuant reconstruction function, memory allocator, and index traversal against pure Go "reference oracles" (unoptimized ground-truth mathematical implementations). If an optimized kernel produces even a single bit of divergence against the oracle, the gate fails.

---

### Gate 6: Static Analysis & Multi-Linter Gates

- **Identifier**: `lint`, `vet`, `staticcheck`, `deadcode`
- **Enforcement Tool**: `make lint`, `make vet`, `make staticcheck`, `make deadcode`, CI `validate` job
- **Commands**:

```bash
golangci-lint run
go vet ./...
staticcheck ./...
make deadcode
```

- **Description**:
  - `golangci-lint`: Runs multi-linter checks enforcing idiomatic Go standards, error checking, and code structure.
  - `go vet`: Performs official Go compiler structural checks (e.g. unkeyed struct literals, incorrect format strings).
  - `staticcheck`: AST-level inspection detecting unreachable code paths, misuse of standard library functions, incorrect sync.Pool pointer usage, and nil context bugs.
  - `deadcode`: Scans `cmd/` call graphs with test roots to find and reject unused or unreachable functions; the cgo-only `cmd/adbc` package is excluded because its `//export` functions are C entrypoints rather than Go `main`-reachable code.

---

### Gate 7: Source Code & Markdown Style Gates

- **Identifier**: `fmt-check`, `markdown-lint`
- **Enforcement Tool**: `make fmt-check`, `make markdown-lint`, CI `helm-and-docs` job
- **Commands**:

```bash
gofmt -l .
goimports -l .
markdownlint -c .markdownlint.json "docs/**/*.md"
```

- **Description**: Ensures that all Go source files adhere strictly to canonical `gofmt` and `goimports` formatting rules without dirty diffs. Documentation files are validated against repository markdownlint standards to guarantee consistent heading hierarchy, valid links, and clean markdown syntax.

---

### Gate 8: Security, Vulnerability & Dependency Audit Gates

- **Identifier**: `security`, `sec-gosec`, `sec-vulncheck`, `sec-oidc`, `sec-secrets`, `sec-audit`
- **Enforcement Tool**: `make security`, [`scripts/verify.sh`](https://github.com/23skdu/longbow/blob/main/scripts/verify.sh), CI `security` & `oidc-federation-security` jobs
- **Commands**:

```bash
govulncheck ./...
gosec -exclude=G115 -quiet ./...
./scripts/check_oidc_federation_security.sh
gitleaks detect --source . --no-git --exit-code 1
trivy fs --severity HIGH,CRITICAL .
```

- **Description**:
  - `govulncheck`: Scans all direct and transitive Go dependencies against the official Go Vulnerability Database (National Vulnerability Database + Go Security Team bulletins).
  - `gosec`: Inspects the Go AST for security vulnerabilities, including unhandled errors, slice bounds overflows, memory disclosure, unsafe pointer arithmetic, and insecure randomness.
  - `sec-oidc`: Runs [`scripts/check_oidc_federation_security.sh`](https://github.com/23skdu/longbow/blob/main/scripts/check_oidc_federation_security.sh) to check for hardcoded cloud credentials and run targeted OIDC/RBAC/TLS security tests.
  - `gitleaks`: Scans staged commits and workspace files for leaked credentials or API tokens.
  - `trivy`: Scans the filesystem and container build definitions for known CVEs, secrets, and misconfigurations.

---

### Gate 9: Cross-Compilation & SIMD Reproducibility Gate

- **Identifier**: `cross-compile`, `check-simd`
- **Enforcement Tool**: `make cross-compile`, `make check-simd`, CI `validate` and `emulation` jobs
- **Commands**:

```bash
GOARCH=arm64 go build ./...
GOARCH=arm64 go vet ./...
./scripts/check_simd_generation.sh
```

- **Description**:
  - **Cross-Compilation**: Longbow targets heterogeneous environments (x86_64 and ARM64). The cross-compilation gate verifies that the entire codebase and C-shared ADBC interface compile cleanly and pass `go vet` under ARM64 without architecture-specific compilation breakages.
  - **SIMD Kernel Reproducibility**: Generated SIMD assembly kernels (e.g. generated via Avo or assembly generators) are tracked in version control. The reproducibility script regenerates the assembly and verifies that the output is byte-for-byte identical to the repository version, preventing uncommitted or non-reproducible manual edits.

---

### Gate 10: Observability, Metrics & Alert Rule Conformance Gate

- **Identifier**: `observability-check`, `observability-rules`, `observability-inventory`
- **Enforcement Tool**: `make observability-check`, `make observability-rules`, CI `observability` job
- **Commands**:

```bash
make observability-inventory
git diff --exit-code -- internal/observability/testdata/registered_metrics.tsv
go test ./internal/observability/... -count=1
grafana/tests/test-rules.sh
```

- **Description**: Every metric emitted by Longbow (Prometheus counters, gauges, histograms) must be registered in the official metrics inventory. The gate ensures:
  1. `registered_metrics.tsv` matches all declared metric constants in the codebase.
  2. Metric naming conventions conform to Prometheus standards.
  3. All Grafana alert rules and recording rules under `grafana/` pass formal unit tests using Prometheus `promtool`.

---

### Gate 11: Container & Helm Packaging Conformance Gates

- **Identifier**: `helm-lint`, `test-docker`
- **Enforcement Tool**: `make helm-lint`, `make test-docker`, CI `helm-and-docs` job
- **Commands**:

```bash
helm lint helm/longbow
helm template longbow helm/longbow --debug > /dev/null
helm template longbow helm/longbow \
  --set-json 'rbac.startupNamespacePolicies=[{"namespace":"team-a","ephemeral":true}]' > /dev/null
go test -v ./tests/docker/...
python3 tests/docker/test_dockerfiles.py
```

- **Description**:
  - **Helm Chart Gate**: Validates that Kubernetes deployment manifests under `helm/longbow` are syntactically valid and can be dry-run templated across default values and the non-default startup namespace ACL configuration.
  - **Dockerfile Conformance**: Validates all supported Dockerfiles (`Dockerfile.cpu`, `Dockerfile.nvidia`, `Dockerfile.emlgo-cpu`, `Dockerfile.emlgo-nvidia`) to ensure accurate base images, multi-stage build caching, non-root user execution, and Go 1.27.4 compatibility (`Dockerfile.tpu` was removed; TPU code remains unsupported/untested).

---

### Gate 12: Benchmark Regression & Statistical Parity Gate

- **Identifier**: `bench-ci`, `test-bench-harness`
- **Enforcement Tool**: `make bench-ci`, `make test-bench-harness`, CI `benchmark-regression` job
- **Commands**:

```bash
python3 scripts/tests/test_benchmark_validation.py
python3 scripts/tests/test_benchmark_memory.py
python3 scripts/tests/test_benchmark_temporal.py
python3 scripts/tests/test_benchmark_attribution.py
python3 scripts/tests/test_ab_stats.py
python3 scripts/unified_benchmark.py --ci --runs 3 --compare-baseline benchmarks/baseline_cpu.json --threshold 10
```

- **Description**:
  - Validates that the benchmarking harness scripts themselves are statistically sound (automated Python unit tests covering Mann-Whitney U tests, bootstrap confidence intervals, P99 latency calculations, and cross-layer search mode coverage).
  - In CI mode, executes a 3-run benchmark pass comparing vector ingestion and query latency against `benchmarks/baseline_cpu.json`. Any performance degradation exceeding the 10% threshold halts the gate.

---

### Gate 13: Search Modes Accuracy & Performance Regression Gate

- **Identifier**: `search-gate`
- **Enforcement Tool**: `make search-gate`, [`scripts/verify.sh`](https://github.com/23skdu/longbow/blob/main/scripts/verify.sh), CI `validate` job
- **Commands**:

```bash
go test -v -count=1 ./internal/store -run='^TestSearchModesGate$'
go test -run='^$$' -bench='^BenchmarkSearchModesGate$' -benchtime=25ms -benchmem ./internal/store
```

- **Description**:
  - Fast, deterministic functional accuracy and latency/allocation regression gate covering all **21 search types** on a shared multi-modal Arrow corpus (`TestSearchModesGate` in `internal/store/search_modes_gate_test.go`).
  - Validates exact top-k matching, filter/geo/temporal exclusion boundaries, facet counts, and Flight `DoGet` Arrow record decoding across: `Dense`, `Hybrid`, `Filtered`, `FilteredBool`, `FilteredString`, `Sparse`, `ByID`, `Range`, `AdvancedText`, `GraphRAG`, `GlobalGraphRAG`, `Recommend`, `Geo`, `GeoBox`, `GeoHybrid`, `Temporal`, `TemporalRange`, `TemporalWindow`, `MultiModal`, `Federated`, and `LearnedIndex`.
  - Enforces per-query mean latency bounds (< 75ms CI budget per mode after warmup; typical runtime ~4–55µs per query and ~0.5s total) plus `-benchmem` allocation/throughput tracking (`BenchmarkSearchModesGate`).

---

### Gate 14: Module Size & File-Budget Guardrail

- **Identifier**: `module-size-check`
- **Enforcement Tool**: `make module-size-check`, [`scripts/check_module_sizes.sh`](https://github.com/23skdu/longbow/blob/main/scripts/check_module_sizes.sh), [`scripts/verify.sh`](https://github.com/23skdu/longbow/blob/main/scripts/verify.sh), CI `validate` job
- **Commands**:

```bash
./scripts/check_module_sizes.sh
./scripts/check_module_sizes.sh --staged
```

- **Description**:
  - Audits hand-maintained source files (`.go`, `.py`, `.cu`, `.cuh`, `.m`, `.h`, and hand-written `.s` assembly, excluding `*_test.go` and generated `kernels_*_amd64.s`) against the repository size budget (`<= 800 LOC` soft target, `<= 1,200 LOC` hard ceiling on newly created or un-allowlisted files).
  - Tracks a shrinkable legacy allow-list with baseline line counts so existing files cannot grow past their recorded baseline and stale entries (`<= 800 LOC`) are flagged for removal as modularization progresses.

---

### Gate 15: Bounded Soak & Stall Detection

- **Identifier**: `soak-quick`
- **Enforcement Tool**: `make soak-quick`, `make soak-ci`, `scripts/run_bounded_soak.sh`, `scripts/check_soak_report.py`, CI `soak-quick` matrix
- **Commands**:

```bash
make build && make build-agentsoak
make soak-quick
```

- **Description**:
  - Runs five bounded, deterministic soak variants: 8 agents with 2 agents per shared namespace, default 3 agents / 7 namespaces, scaled 10 agents / 21 namespaces, a 2-agent federated-only variant, and a 2-agent analytics/writes variant.
  - Starts a disposable RBAC-enforced server, provisions scoped per-agent API keys, verifies forbidden cross-namespace probes, and asserts that denials are queryable via `GetAuditDenialCounts`; there is no unauthenticated mode.
  - Fails when error rate exceeds 1% globally or for any operation, QPS drops more than 50% for two minutes, server RSS grows monotonically by at least 5% in the second half (with at least two minutes of samples), cross-agent verification hits are zero, or the simulator emits stall/failure records.
  - The `soak-quick` CI matrix is a required dependency of both release jobs. Secure TLS/mTLS/encryption/OIDC variants are dispatch-only and documented in [`development/soaktestplan.md`](soaktestplan.md).

---

## 4. Gate Execution Matrix

| Gate | Name | Pre-Commit Hook | Local `make ci` | CI Pipeline (`ci.yml`) | Target Makefile Rule |
| --- | --- | :---: | :---: | :---: | --- |
| **1** | 100% Package Test Presence | **Yes** | **Yes** | **Yes** | `make test-coverage` |
| **2** | Unit & Statement Coverage | **Yes** (staged) | **Yes** (all) | **Yes** (all) | `make test` / `make test-coverage` |
| **3** | 100% Fuzz Presence & Fuzz Testing | **Yes** (short) | **Yes** | **Yes** | `make fuzz-coverage`, `make test-fuzz` |
| **4** | Accuracy Gate (15 DTypes) | **Yes** | **Yes** | **Yes** | `make accuracy-gate` |
| **5** | Oracle Ground-Truth Parity | **Yes** | **Yes** | **Yes** | `make oracle-gate` |
| **6** | Static Analysis & Linters | **Yes** (vet/lint) | **Yes** | **Yes** | `make lint`, `make vet` |
| **7** | Code & Markdown Formatting | **Yes** (gofmt) | **Yes** | **Yes** | `make fmt-check`, `make markdown-lint` |
| **8** | Security & CVE Vulnerabilities | **Yes** (gosec/gitleaks) | **Yes** | **Yes** | `make security` |
| **9** | Cross-Compilation & SIMD Check | No | **Yes** | **Yes** | `make cross-compile`, `make check-simd` |
| **10** | Observability & Promtool Rules | No | **Yes** | **Yes** | `make observability-check` |
| **11** | Container & Helm Validation | No | **Yes** | **Yes** | `make test-docker`, `make helm-lint` |
| **12** | Benchmark Regression Gate | No | Optional | **Yes** | `make bench-ci`, `make test-bench-harness` |
| **13** | Search Modes Accuracy & Perf Gate | **Yes** | **Yes** | **Yes** | `make search-gate` |
| **14** | Module Size Guardrail | **Yes** (staged) | **Yes** | **Yes** | `make module-size-check` |
| **15** | Bounded Soak & Stall Detection | No | **Yes** (`make soak-quick`) | **Yes** (`soak-quick`) | `make soak-quick`, `make soak-ci` |

---

## 5. Troubleshooting & Remediation Playbooks

### Issue: Pre-Commit Hook Failed with "Found packages without test files"

- **Cause**: A new Go package was created in the repository that lacks any `*_test.go` file.
- **Remediation**: Add a unit test file (e.g. `package_test.go`) covering the exported types and functions in the new package. Re-run `make test-coverage`.

### Issue: Gate Failed with "Found packages without fuzz targets"

- **Cause**: A Go package (or its tests) defines no `func Fuzz*(f *testing.F)` target, so the 100% package fuzz coverage check fails.
- **Remediation**: Add a `<topic>_fuzz_test.go` file to the package with at least one fuzz target that seeds valid and invalid inputs via `f.Add` and asserts a real invariant (no-panic floor; prefer round-trip, error-on-invalid, or oracle-parity properties). Re-run `make fuzz-coverage`.

### Issue: Unit Tests Cause Machine Freeze or OOM Crash

- **Cause**: Running tests without `-short` triggers high-scale stress tests (e.g. 25,000 graph nodes) concurrently across multiple packages.
- **Remediation**: Always pass `-short` when running tests locally (`make test` or `go test -short ./...`). If authoring a new high-scale or long-running benchmark, guard it with:

```go
if testing.Short() {
    t.Skip("skipping heavy stress test in short mode")
}
```

### Issue: Oracle Gate Failed with Numerical Divergence

- **Cause**: An optimized SIMD kernel (AVX2, AVX-512, or Neon) or quantization accumulator produced values that diverge from the pure Go reference implementation.
- **Remediation**: Run the specific parity test with verbose logging:

```bash
go test -v ./internal/simd/ -run='TestSIMDAccuracy'
go test -v ./internal/store/index/ -run='TestTurboQuantPolarReconstructCodesParity'
```

Inspect the dimension and codebook index where divergence occurred. Verify loop bounds, unrolling remainder loops, and NaN/Inf handling.

### Issue: Search Modes Gate Failed Accuracy or Latency Checks

- **Cause**: A search backend change broke ranking/filtering/exclusion accuracy in one of the 21 supported search modes (`Dense`, `Hybrid`, `Filtered*`, `Sparse`, `ByID`, `Range`, `AdvancedText`, `GraphRAG*`, `Recommend`, `Geo*`, `Temporal*`, `MultiModal`, `Federated`, `LearnedIndex`), or caused mean query latency on the 64-doc fixture to exceed the regression budget.
- **Remediation**: Reproduce the failing subtest or benchmark directly:

```bash
go test -v -count=1 ./internal/store -run='TestSearchModesGate/<Mode>'
make search-gate
```

### Issue: Observability Gate Failed on `registered_metrics.tsv`

- **Cause**: A Prometheus metric was added, renamed, or deleted in Go code without updating the inventory file.
- **Remediation**: Run the inventory generator target to synchronize the documentation and testdata:

```bash
make observability-inventory
```

Stage the resulting changes in `internal/observability/testdata/registered_metrics.tsv`.
