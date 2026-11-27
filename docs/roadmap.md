# Longbow Roadmap

This is the canonical, forward-looking roadmap. It lists **only remaining work**.
Completed milestones are not repeated here; their results are recorded in
[`development/performance.md`](development/performance.md),
[`development/gates.md`](development/gates.md),
[`development/testplan.md`](development/testplan.md), and git history.

Priorities are deliberately narrow:

The previous P0 (encrypt S3/GCS backups and snapshots at rest) is complete; see `security-compliance.md` and `deploy.md`.

- **P0** blocks a release or leaves a shipped kernel/artifact incorrect.
- **P2** is structural or test-depth work that reduces future risk.

Guardrails are not roadmap items. Always-on RBAC, namespace ACLs, OIDC
Federation, durable secret storage, 100% godoc coverage, 100% package-level
unit/fuzz coverage, the module-size guardrail, and the observability, accuracy,
oracle, search, and soak gates must remain green while this work proceeds.

## P2 — Decompose `internal/store/engine`

The root `internal/store` facade is within budget and all compatibility gates
are green, but `internal/store/engine` still holds 197 production files. The
structural risk remains until that package is split along real domain
boundaries instead of being one large implementation directory behind a
facade.

Deliverables:

- Extract API handlers, RBAC actions, text search, streaming/parquet, and
  control-plane domains into focused subpackages with explicit host interfaces
  that avoid `*VectorStore`/`*Dataset` cycles.
- Move white-box tests with the code they cover and keep the root facade as the
  single compatibility surface for external callers.
- Reduce `internal/store/engine` to the same `<= 50` production-file budget
  used by the other core packages.

Exit criterion: the domain boundaries are real, no public API breaks, and
`make module-size-check` stays green without raising any limit.

## P2 — Deepen thin-package test depth

Every package now has unit and fuzz targets, but several of the newly extracted
packages have low statement coverage: `apihandlers`, `controlplane`,
`streaming`, and `index/ivf`. Presence gates alone do not prove their behaviour.

Deliverables:

- Add behavioural tests for request validation, error mapping, and payload
  round-trips in the thin domain packages.
- Add fuzz seeds that exercise the exported wrapper surface, not just parsing
  helpers.
- Keep package-level godoc, unit, and fuzz presence gates green for every new
  subpackage.

Exit criterion: failures in thin wrappers are caught by fast tests before a
long soak or release build starts.

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
| Encryption and key management | [`security-compliance.md`](security-compliance.md) |
