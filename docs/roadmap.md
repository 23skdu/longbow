# Longbow Roadmap

This is the canonical, forward-looking roadmap. It lists **only remaining work**.
Completed milestones are not repeated here; their results are recorded in
[`development/performance.md`](development/performance.md),
[`development/gates.md`](development/gates.md),
[`development/testplan.md`](development/testplan.md), and git history.

Priorities are deliberately narrow:

- **P0** blocks a release or leaves a shipped kernel/artifact incorrect.
- **P1** protects multi-agent concurrency, security, and soak stability.
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

## P1 — Concurrency and soak reliability

### AgentSoak full workload mix

`cmd/agentsoak` currently exercises insert, dense search, cross-agent read,
delete, and a placeholder federated op. It does not yet reproduce the 21-mode
workload mix in [`development/soaktestplan.md`](development/soaktestplan.md).

Deliverables:

- Add configurable operation weights for dense, hybrid, sparse, filtered,
  range, by-id, temporal, geo, GraphRAG, multimodal, federated, text, insert,
  upsert, delete, namespace cleanup, and analytics operations.
- Replace the placeholder federated op with a real `FederatedSearch` request.
- Share the workload schema between the Go and Python simulators by extending
  `configs/multiagent_soak.json`; flags override config values.
- Record per-operation QPS and errors so stalls can be attributed to a mode.

Exit criterion: both simulators drive the same weighted mix, and the JSON
report breaks down QPS/latency/errors by operation.

### CI soak gates and stall detection

There is no CI job that runs the soak simulators. Add bounded, deterministic
variants rather than the full 24-hour plan.

Deliverables:

- Shared-namespace soak: 8 agents, 2 per namespace, 10 minutes.
- Permission soak: default 3 agents / 7 namespaces, plus a scaled 10-agent /
  21-namespace variant.
- Federated soak: 2 agents on a shared namespace, 5 minutes.
- Fail on any error rate above 1%, a QPS drop above 50% sustained for two
  minutes, monotonic RSS growth, or missing cross-agent verification hits.
- Wire the jobs into `.github/workflows/ci.yml` and record thresholds and
  artifacts in [`development/soaktestplan.md`](development/soaktestplan.md).

Exit criterion: the gates are required by release verification and reproduce
the historical lock-convoy regression before the fix is reverted.

### Auto-scaler hysteresis follow-through

Ingestion scale-down now refuses to drain while pending work exists, but the
implementation is only partially wired and the indexing side is unused.

Deliverables:

- Wire `CanScaleDownIndexing` into the indexing worker pool instead of leaving
  it test-only, or remove it if indexing scale-down is intentionally static.
- Source the busy signal from the dataset lock queue, not only
  `pendingOverflowJobs`, so a lock convoy cannot drain workers.
- Align the ingestion minimum-worker calculation with `MinIngestionWorkers`;
  today `calculateMinWorkers(2)` and the configured divisor 4 disagree.
- Add a concurrency test that holds a contended dataset lock while the
  auto-scaler evaluates a scale-down tick.

Exit criterion: neither ingestion nor indexing shrinks below its minimum while
pending jobs or contended lock waiters exist.

### Graph analytics isolation

Heavy PageRank, community detection, and GraphRAG expansion on shared
namespaces can still dominate dataset lock hold time.

Deliverables:

- Route analytics over snapshots or dedicated workers so ingest and search do
  not wait on long graph traversals.
- Add a soak variant that runs analytics concurrently with writes on the same
  namespace and asserts bounded search latency.

Exit criterion: p99 search latency stays within the existing baseline while
analytics run continuously.

### Federated search soak stability

The router registration cache and dirty-flag refresh are in place, so the
remaining work is validation under sustained ingest rather than caching.

Deliverables:

- Add a federated-only soak that mutates collection membership and vector
  counts while tag-routed searches run continuously.
- Add an explicit router removal/refresh path so deleted and recreated
  datasets do not leave stale searchers behind; the current sync only upserts
  and skips names that are already registered.
- Verify that dirty-flag refreshes do not serialise searches.

Exit criterion: no stale collection results, no lock convoy, and stable
federated QPS for the full soak window.

### Permission audit logging

Structured denial events with actor key ID, namespace, operation, and decision
reason are already emitted by the RBAC layer. What is missing is soak-level
validation and a stable query surface for those events.

Deliverables:

- Add a soak assertion that expected cross-namespace write/delete denials
  produce audit records without inflating the client-visible error rate.
- Expose denial counts as a metric or log-derived view that operators can
  query without raw log scraping.
- Keep raw API keys out of every audit record.

Exit criterion: permission denials are queryable in metrics/logs and are
distinguishable from transport or validation errors.

## P1 — Secure soak variants

Security features exist, but the soak harness does not yet measure their cost.

Deliverables:

- TLS soak: gRPC with TLS, compared against plaintext QPS and latency.
- mTLS / OIDC workload-identity soak: per-agent credentials over authenticated
  transport, without weakening namespace RBAC.
- Encryption-at-rest soak: disk encryption enabled, measuring ingest/search
  latency and CPU overhead.
- Combined secure soak: TLS + mTLS + encryption for one hour with 8 agents.
- A comparison report in [`development/performance.md`](development/performance.md)
  covering QPS, p50/p95/p99, RSS, and error rate for each variant.

Exit criterion: each variant runs in CI as a scheduled job, and the report
quantifies the overhead of every security control.

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
