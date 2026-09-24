# Next Steps & Roadmap

> [!NOTE]
> This document has been consolidated into [docs/roadmap.md](roadmap.md).
> Please refer to [docs/roadmap.md](roadmap.md) for the single canonical roadmap and the final list of outstanding items.

---

## Canonical Roadmap Location

All roadmap tracking, improvement plans, dispatch routing rules, performance observations, and outstanding tasks have been unified into:

👉 **[Longbow Unified Roadmap & Optimization Plan](roadmap.md)**

### Roadmap Status Summary

For full details, component owners, and targets, see [roadmap.md §1](roadmap.md#1-final-outstanding-items--next-steps):

* **[Done] P1: Benchmark Baseline Population**: `benchmarks/baseline_cpu.json` populated with empirical multi-run benchmarks; `check_regression.py` verified.
* **[Done] P1: Post-Optimization Verification Benchmarking**: Complex distance batch benchmarks and empirical dispatch routing verified on CPU.
* **[Done] P3: Continuous Package Coverage Enforcement**: `Verify 100% Package Test Coverage Gate` added to `.github/workflows/ci.yml`; all 69 packages verified passing.
* **[Open] P2: AVX-512 Product Quantization (PQ) Assembly Kernels**: Dedicated AVX-512 asymmetric distance kernels for PQ codebooks (Target: v0.2.5).
* **[Open] P2: Multi-GPU / High-VRAM Stress Profiling**: Validate `NewDoubleBufferWithHeadroom` and `CheckHeadroom` under heavy concurrent query pressure (Target: v0.2.5).

---

## Security Scanning & Accepted Risks (Reference for .trivyignore / check_govuln.sh)

As documented in Part 9 of the production hardening plan (now in [roadmap.md §4](roadmap.md#10-part-production-hardening-plan-completed)):
- `.github/workflows/security.yml` runs `govulncheck` via `scripts/check_govuln.sh`, Trivy filesystem scan, and Trivy IaC scan.
- Accepted indirect dependencies with no available upstream patch are documented in `.trivyignore` and `scripts/check_govuln.sh`:
  - `hamba/avro`: `GO-2026-5046`, `GO-2026-5047`, `GO-2026-5048` (Fixed in: N/A)
  - `golang.org/x/crypto/openpgp`: `GO-2026-5932` (Fixed in: N/A)
