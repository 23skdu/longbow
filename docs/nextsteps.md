# Next Steps & Roadmap

Last updated: 2026-09-17.

---

## 10-Part Improvement Plan

### Part 1: Multi-Architecture CUDA Kernel Builds
**Goal**: Target all modern NVIDIA GPU architectures instead of sm_70 only.
**Problem**: `Dockerfile.nvidia:49` and `Dockerfile.emlgo-gpu:52` compile CUDA kernels with `-arch=sm_70`, targeting only Volta (V100). This wastes compute on Ampere (A100), Ada Lovelace (L40), and Hopper (H100) GPUs. Multi-arch builds add ~5s compile time but unlock architecture-specific optimizations.
**Action**: Replace `-arch=sm_70` with multi-arch gencode flags: `sm_70`, `sm_80`, `sm_86`, `sm_89`, `sm_90`. PTX forward-compatibility for future architectures.
**Impact**: 10-30% faster kernel execution on Ampere+ GPUs from architecture-specific instruction scheduling.

### Part 2: Non-Root Docker Runtime
**Goal**: Run the longbow process as non-root in all Docker images for security hardening.
**Problem**: All 5 Dockerfiles run as root (UID 0). The `scratch`-based images have no user concept. The NVIDIA/EMLGo-GPU images install `ca-certificates` and run as root. This violates the principle of least privilege and may fail security audits in production Kubernetes clusters.
**Action**: For `scratch`-based images: copy `/etc/passwd` and `/etc/group` from builder, add `USER nobody`. For Ubuntu-based images: create a dedicated `longbow` user in builder, switch to it in runtime stage.
**Impact**: Eliminates container privilege escalation risk; required for SOC2/compliance certifications.

### Part 3: Reproducible Builds with `-trimpath`
**Goal**: Strip local filesystem paths from binaries for reproducible, auditable builds.
**Problem**: All Dockerfiles now include `-trimpath` (fixed 2026-09-17), but the local `go.mod` has `replace github.com/emlgo/eml => ../emlgo` at line 183. This directive fails in Docker builds since `COPY . .` doesn't include parent directories. Vendor mode works only if `vendor/` is pre-populated.
**Action**: Remove the local `replace` directive from `go.mod`. Ensure `go mod vendor` is run before Docker builds. Add CI check that `go build -mod=vendor` succeeds without the replace directive.
**Impact**: Enables clean Docker builds without manual vendor pre-population; prevents build failures in CI/CD pipelines.

### Part 4: Healthcheck Endpoint Standardization
**Goal**: Align all healthcheck endpoints to a consistent, documented path.
**Problem**: `Dockerfile.nvidia:88` and `Dockerfile.emlgo-gpu:92` use `/metrics` for healthchecks. `docker-compose.yml:61` uses `/health`. CPU/Metal Dockerfiles have no healthcheck at all. The `/health` endpoint may not exist or may not reflect actual service readiness.
**Action**: Implement a dedicated `/health` endpoint in `cmd/longbow/main.go` that checks: (1) gRPC server is listening, (2) storage engine is initialized, (3) memory is within limits. Update all Dockerfiles and docker-compose to use `/health`. Add HEALTHCHECK to CPU and Metal Dockerfiles.
**Impact**: Reliable container orchestration; Kubernetes can detect unhealthy pods and reschedule them.

### Part 5: GPU Memory Leak Detection in CI
**Goal**: Add CUDA memory leak detection to the test and benchmark pipeline.
**Problem**: GPU memory leaks are only discovered during long-running production workloads. The race detector (`go test -race`) catches CPU data races but not GPU memory leaks. CUDA memory leaks can silently consume VRAM until OOM.
**Action**: Integrate `compute-sanitizer --tool memcheck` into CI for GPU test runs. Add a post-benchmark VRAM check in `scripts/unified_benchmark.py` that compares pre/post VRAM usage and fails if delta exceeds threshold. Add `cudaMemGetStats` logging to `internal/gpu/cuda/cuda_index.go` at index lifecycle boundaries.
**Impact**: Catches GPU memory leaks before they reach production; prevents VRAM exhaustion in multi-tenant deployments.

### Part 6: Benchmark Regression CI Gate
**Goal**: Block PRs that introduce performance regressions beyond a configurable threshold.
**Problem**: Performance regressions are caught manually during benchmark runs. The 3x multi-run infrastructure (Part 9, done) provides mean/stdev, but there's no automated gate. Regressions can merge undetected.
**Action**: Add a `scripts/check_regression.py` script that compares new benchmark results against a baseline JSON. Fail if any config regresses by >10% (configurable via `--threshold`). Integrate into CI as a required check. Store baselines in `benchmarks/baseline_*.json` (committed to repo).
**Impact**: Prevents performance regressions from reaching main; creates a culture of performance accountability.

### Part 7: Structured Benchmark Baselines
**Goal**: Maintain versioned benchmark baselines for regression detection.
**Problem**: No baseline exists to compare against. Each benchmark run is standalone. Historical results are in `docs/performance.md` but not in machine-readable format.
**Action**: Create `benchmarks/` directory with `baseline_cpu.json`, `baseline_gpu.json`. Structure: `{config: {dtype, dim, count}, qps_mean, qps_stdev, p50, p95, p99, ingest_mbps}`. Update `unified_benchmark.py` to `--save-baseline` and `--compare-baseline` flags. Commit baselines to repo; update on each full regression run.
**Impact**: Machine-readable performance history; enables automated regression detection; supports A/B comparisons across commits.

### Part 8: Docker Compose GPU Profiles
**Goal**: Provide production-ready Docker Compose configurations for all GPU variants.
**Problem**: `docker-compose.yml` only defines the CPU build. Users deploying with NVIDIA, Metal, or EMLGo variants must manually configure compose files. No profiles for GPU selection.
**Action**: Add Docker Compose profiles: `cpu` (default), `nvidia`, `emlgo-cpu`, `emlgo-gpu`, `metal`. Use `COMPOSE_PROFILES` env var for selection. Add `deploy.resources.reservations.devices` for NVIDIA GPU passthrough. Document in `docs/deploy.md`.
**Impact**: One-command deployment for any hardware configuration; reduces deployment friction for GPU users.

### Part 9: Security Scanning in CI
**Goal**: Automate vulnerability scanning for Go dependencies and Docker images.
**Problem**: Dependabot handles Go module updates but doesn't scan for CVEs in base Docker images or transitive dependencies. No `govulncheck` or container scanning in CI.
**Action**: Add `govulncheck ./...` to CI pipeline (catches Go-specific vulnerabilities missed by `go list -m -json all`). Add Trivy scanning for built Docker images. Add `.trivyignore` for accepted risks. Run weekly as a scheduled workflow.
**Impact**: Proactive CVE detection; compliance with security audit requirements; prevents known-vulnerable dependencies in production.

### Part 10: Performance Documentation Automation
**Goal**: Auto-generate performance documentation from benchmark results.
**Problem**: `docs/performance.md` is manually updated after benchmark runs. It can drift from actual results. The `unified_benchmark.py` generates JSON but doesn't produce human-readable docs.
**Action**: Add `--report-md` flag to `unified_benchmark.py` that generates a Markdown report with tables, charts (ASCII), and regression annotations. Auto-commit updated `docs/performance.md` on scheduled benchmark runs. Add a `scripts/update_docs.sh` that orchestrates benchmark → report → commit.
**Impact**: Always-current performance documentation; reduces manual toil; ensures documentation matches reality.

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
