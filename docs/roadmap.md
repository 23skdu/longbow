# Longbow Roadmap

This is the canonical, forward-looking roadmap. It lists **only remaining work**.
Completed milestones are not repeated here; their results are recorded in
[`development/performance.md`](development/performance.md),
[`development/gates.md`](development/gates.md),
[`development/testplan.md`](development/testplan.md), and git history.

Priorities are deliberately narrow:

- **P0** blocks a release or leaves a shipped kernel/artifact incorrect.
- **P2** is structural or test-depth work that reduces future risk.

Guardrails are not roadmap items. Always-on RBAC, namespace ACLs, OIDC
Federation, durable secret storage, 100% godoc coverage, 100% package-level
unit/fuzz coverage, the module-size guardrail, and the observability, accuracy,
oracle, and search gates must remain green while this work proceeds.

## P0 — Release correctness and target parity

### AVX-512 VBMI TurboQuant packer parity

`PackTQ2AVX512VBMI` currently falls back to the generic packer because the VBMI
kernel is one code off for every value. The fallback is safe but leaves the
specialized path unexercised and the SDE coverage gate incomplete.

Deliverables:

- Fix `packTQ2AVX512VBMIKernel` so it is bit-identical to the generic 2-bit
  packer across boundary values, all supported dimensions, and randomized
  inputs.
- Restore dispatch from `PackTQ2AVX512VBMI` to the kernel and remove the
  fallback once `TestPackTQ2AVX512VBMIMatchesGeneric` passes without tolerance.
- Wire `scripts/check_avx512_coverage.sh` into `make ci` and the release
  workflow; it exists but is not currently invoked by either. Record the result
  in [`development/performance.md`](development/performance.md).

Exit criterion: the AVX-512 VBMI packer is covered by the SDE lane, matches the
generic implementation exactly, and no `# TODO` fallback remains in
`internal/simd/turboquant_avx512_amd64.go`.

### Apple Silicon and ARM64 delivery

The native macOS ARM64 build/test job and Darwin ARM64 release binary already
exist. The remaining confusion is container terminology: OCI images are Linux
artifacts, so Apple Silicon hosts consume `linux/arm64` images, not
`darwin/arm64` images.

Deliverables:

- Extend the release matrix to publish `linux/arm64` for every image whose base
  toolchain supports it, starting with `Dockerfile.cpu` and
  `Dockerfile.emlgo-cpu`; keep GPU/TPU variants amd64-only only when their base
  images require it, and state that explicitly.
- Verify multi-arch manifests, tags, SBOMs, and signing in
  `.github/workflows/ci.yml`; the current release-container steps build with
  `push: false` and must be reconciled with the publish claim.
- Document two supported paths in [`deploy.md`](deploy.md): native Darwin
  ARM64 binaries with Metal for local development, and `linux/arm64`
  containers for Apple Silicon Docker hosts.
- Add the ARM64 variants to `make docker-build-*` and the observability gate so
  the release artifact list matches what is actually produced.

Exit criterion: a clean release produces verifiable ARM64 artifacts for both
paths, and no documentation promises a non-existent Darwin OCI image.

## P2 — Modularization

The module-size guardrail is green, but two god-packages remain far above the
`<= 50` production-file target. This work is intentionally last because it
requires interface boundaries and alias compatibility, not mechanical splits.

Deliverables:

- `internal/store`: reduce from 198 to `<= 50` production files by extracting
  API handlers, RBAC actions, text search, streaming/parquet, and control-plane
  domains behind host interfaces that avoid `*VectorStore`/`*Dataset` cycles.
- `internal/store/index`: reduce from 128 to `<= 50` production files by
  extracting IVF variants and the HNSW core behind factory interfaces, while
  preserving aliases and migrating white-box tests.
- Keep `make module-size-check` and the staged verification guardrail green
  throughout; do not raise the soft or hard file-size limits.

Exit criterion: both packages meet the file-count target with no public API
break and no new `//nolint` suppressions.

## P2 — Test depth and tooling

Package-level godoc, unit, and fuzz coverage gates are already green. The
remaining gap is behavioural coverage in the soak toolchain.

Deliverables:

- Extend `cmd/agentsoak` tests across workload selection, credential switching,
  report generation, and shutdown paths; add fuzz seeds for config and report
  parsing.
- Add unit tests for the collation script `scripts/agent_soak_bench.py`,
  including malformed run output and empty result sets.
- Keep `scripts/verify.sh` and `make ci` as the single definition of done.

Exit criterion: soak tooling failures are caught by unit/fuzz tests before a
long-running soak is started.

## Deferred and explicitly out of scope

- Publishing `darwin/arm64` OCI images: Docker images are Linux artifacts; the
  supported Apple Silicon paths are native binaries and `linux/arm64` images.
- Reintroducing RBAC bypass flags or unauthenticated soak modes.
- Raising module-size, lint, or coverage thresholds to hide regressions.
- Running the full 24-hour soak on every pull request; use bounded CI variants
  and reserve the long run for release validation.

## Verification artifacts

| Purpose | Reference |
|---|---|
| Quality gates and local verification | [`development/gates.md`](development/gates.md) |
| Performance baselines and hardware results | [`development/performance.md`](development/performance.md) |
| Search-mode and dtype matrix | [`development/testplan.md`](development/testplan.md) |
| Soak thresholds and scenario layout | [`development/soaktestplan.md`](development/soaktestplan.md) |
| Security model and operator setup | [`rbac.md`](rbac.md) |
