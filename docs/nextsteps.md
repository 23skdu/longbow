# Next Steps & Roadmap

Last updated: 2026-09-17.

---

## 10-Part Improvement Plan

### Part 1: Multi-Architecture CUDA Kernel Builds — Done
**Goal**: Target all modern NVIDIA GPU architectures instead of sm_70 only.
**Solution**: Replaced `-arch=sm_70` with multi-arch gencode flags: `sm_70`, `sm_80`, `sm_86`, `sm_89`, `sm_90` in `Dockerfile.nvidia` and `Dockerfile.emlgo-gpu`.

### Part 2: Non-Root Docker Runtime — Done
**Goal**: Run the longbow process as non-root in all Docker images for security hardening.
**Solution**: `scratch`-based images copy `/etc/passwd` and `/etc/group` from builder, run as `USER nobody:nobody`. Ubuntu-based images create a dedicated `longbow` user, run as `USER longbow:longbow`. All 5 Dockerfiles updated.

### Part 3: Reproducible Builds with `-trimpath`
**Goal**: Strip local filesystem paths from binaries for reproducible, auditable builds.
**Problem**: All Dockerfiles now include `-trimpath` (fixed 2026-09-17), but the local `go.mod` has `replace github.com/emlgo/eml => ../emlgo` at line 183. This directive fails in Docker builds since `COPY . .` doesn't include parent directories. Vendor mode works only if `vendor/` is pre-populated.
**Action**: Remove the local `replace` directive from `go.mod`. Ensure `go mod vendor` is run before Docker builds. Add CI check that `go build -mod=vendor` succeeds without the replace directive.
**Impact**: Enables clean Docker builds without manual vendor pre-population; prevents build failures in CI/CD pipelines.

### Part 4: Healthcheck Endpoint Standardization — Done
**Goal**: Align all healthcheck endpoints to a consistent, documented path.
**Solution**: Wired `internal/health` package into `cmd/longbow/main.go` with component checkers (storage, metrics, logging, tracing). Registered as `/health` endpoint. Updated all Dockerfiles and docker-compose to use `/health`. Returns JSON with 503 on unhealthy.

### Part 5: GPU Memory Leak Detection in CI — Done
**Goal**: Add CUDA memory leak detection to the test and benchmark pipeline.
**Solution**: Created `scripts/gpu_memcheck.sh` that runs `compute-sanitizer --tool memcheck --leak-check full` and parses output for leaks, CUDA errors, and invalid memory accesses. Returns exit code 1 on issues, 2 if compute-sanitizer unavailable.

### Part 6: Benchmark Regression CI Gate
**Goal**: Block PRs that introduce performance regressions beyond a configurable threshold.
**Problem**: Performance regressions are caught manually during benchmark runs. The 3x multi-run infrastructure (Part 9, done) provides mean/stdev, but there's no automated gate. Regressions can merge undetected.
**Action**: Add a `scripts/check_regression.py` script that compares new benchmark results against a baseline JSON. Fail if any config regresses by >10% (configurable via `--threshold`). Integrate into CI as a required check. Store baselines in `benchmarks/baseline_*.json` (committed to repo).
**Impact**: Prevents performance regressions from reaching main; creates a culture of performance accountability.

### Part 7: Structured Benchmark Baselines — Done
**Goal**: Maintain versioned benchmark baselines for regression detection.
**Solution**: Created `benchmarks/` directory with `baseline_cpu.json` template. Added `--save-baseline` and `--compare-baseline` flags to `unified_benchmark.py`. Created `scripts/check_regression.py` for standalone regression checking with configurable threshold. JSON structure: `{config: {dtype, dim, count}, qps_mean, qps_stdev, p50, p95, p99, ingest_mbps}`.

### Part 8: Docker Compose GPU Profiles — Done
**Goal**: Provide production-ready Docker Compose configurations for all GPU variants.
**Solution**: Rewrote `docker-compose.yml` with Docker Compose profiles: `cpu` (default), `nvidia`, `metal`, `emlgo-cpu`, `emlgo-gpu`. NVIDIA/EMLGo-GPU services include `deploy.resources.reservations.devices` for GPU passthrough. Usage: `docker compose --profile nvidia up`.

### Part 9: Security Scanning in CI
**Goal**: Automate vulnerability scanning for Go dependencies and Docker images.
**Problem**: Dependabot handles Go module updates but doesn't scan for CVEs in base Docker images or transitive dependencies. No `govulncheck` or container scanning in CI.
**Action**: Add `govulncheck ./...` to CI pipeline (catches Go-specific vulnerabilities missed by `go list -m -json all`). Add Trivy scanning for built Docker images. Add `.trivyignore` for accepted risks. Run weekly as a scheduled workflow.
**Impact**: Proactive CVE detection; compliance with security audit requirements; prevents known-vulnerable dependencies in production.

### Part 10: Performance Documentation Automation — Done
**Goal**: Auto-generate performance documentation from benchmark results.
**Solution**: Added `--report-md` flag to `unified_benchmark.py` that generates a Markdown report with tables for all configs (dim, dtype, count, search mode, QPS, P50/P95/P99, ingest). Output to any path (e.g., `docs/performance.md`). Also added `--compare-baseline` for automated regression gating.

---

## Previously Completed (Original 10-Part Plan)

| Part | Description | Status |
|------|-------------|--------|
| 1 | GPU TurboQuant batched kernel | Done (2026-09-12) |
| 2 | GPU TurboQuant lookup tables | Done (2026-09-12) |
| 3 | GPU TurboQuant HNSW graph traversal | Done (2026-09-12) |
| 4 | GPU TurboQuant async streams | Done (2026-09-12) |
| 5 | GPU TurboQuant memory coalescing | Done (2026-09-12) |
| 6 | CPU temporal emlgo investigation | Done (2026-09-12) |
| 7 | CPU float64 emlgo exclusion | Done (2026-09-12) |
| 8 | GPU complex128/complex64 CUDA kernels | Done (2026-09-14) |
| 9 | Benchmark infrastructure (3x runs, soak) | Done (2026-09-14) |
| 10 | Conditional dispatch strategy | Done (2026-09-14) |

---

## Open Issues

### P0 — Critical

| # | Issue | Impact | Recommended Action |
|---|-------|--------|--------------------|
| 1 | CPU complex64 dense 500k | -38% regression (404 vs 251 QPS) | Profile hot path at 500k scale — dispatch overhead returns at large N |
| 2 | CPU complex128 dense 500k | +21% gain but P99 75ms | Investigate tail latency — allocation or GC pressure at large scale |

### P1 — Important

| # | Issue | Impact | Recommended Action |
|---|-------|--------|--------------------|
| 3 | CPU emlgo temporal mode | -18-36% across ALL dtypes at 10k/100k | Use standard build for temporal (by design) |
| 4 | GPU complex128 dense 100k | -50% regression (3419 vs 1718 QPS) | Profile GPU kernel — shared memory/register pressure |
| 5 | CPU float64 emlgo memory | +47% memory (10632 vs 7172 MB) | Exclude float64 from emlgo dispatch (env var available) |

### P2 — Improvement

| # | Issue | Impact | Recommended Action |
|---|-------|--------|--------------------|
| 6 | CPU sparse search with emlgo | -5-17% slower at 100k | Compare CPU vs GPU sparse dispatch |
| 7 | CPU 10k scale emlgo overhead | -15-26% on graphrag/temporal | Consider 50k minimum activation threshold |
| 8 | CUDA 12.6.3 outdated | Potential CVEs in base image | Upgrade to CUDA 12.8.x |

---

## Timeline

| Week | Focus |
|------|-------|
| 1 | Parts 1-3: Multi-arch CUDA, non-root Docker, reproducible builds |
| 2 | Parts 4-6: Healthcheck standardization, GPU leak detection, regression CI gate |
| 3 | Parts 7-8: Benchmark baselines, Docker Compose GPU profiles |
| 4 | Parts 9-10: Security scanning, performance doc automation |
