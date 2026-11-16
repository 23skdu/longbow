# Longbow Performance Roadmap (Remaining Work)

Feature parity (Items 1–15) and BM25 arena/WAND optimizations are complete.
This document tracks the remaining performance P0 blockers identified in
`docs/performance.md` (2026-11-07 matrix review). Each task lists the
subtasks required to close it. Tasks are ordered by expected impact.

---

## P0-1: TurboQuant Index Time (429s at 500k; polar reconstruction 47.8% of distance time)

### Subtasks
- [x] P0-1.1 Sincos LUT exists (tqPolarLUT) and is used in reconstruction.
- [x] P0-1.2 Batched reconstruction across tqBatchWidth candidates (turboquant_batch.go).
- [x] P0-1.3 AVX2 reconstruction kernel (TQPolarReconstructLevelAVX2).
- [x] P0-1.4 Profile polar reconstruction to confirm hot loops.
- [x] P0-1.5 Validate numerical parity vs scalar reference.
- [x] P0-1.6 Benchmark index time before/after.

## P0-2: Complex128 Throughput & Memory (951 QPS, 11.5 GB RSS at 500k/384d)

### Subtasks
- [x] P0-2.1 Early-exit diversity checks in selectNeighborsComplex128.
- [x] P0-2.2 Parallel tiled complex128 batch kernels.
- [x] P0-2.3 Float64 slab zero-copy extraction.
- [ ] P0-2.4 Reduce memory footprint (chunked loading / avoid full materialization).
- [ ] P0-2.5 (Optional) Zero-copy Float32→complex128 extraction (requires API change).

## P0-3: Float64 Index Time (433.5s at 500k/384d)

### Subtasks
- [x] P0-3.1 Early-exit diversity checks in selectNeighborsFloat64.
- [ ] P0-3.2 Batched selectNeighbors for float64.
- [ ] P0-3.3 Profile selectNeighbors and graph build.

## P0-4: Geo Search (12–35 QPS at 500k)

### Subtasks
- [x] P0-4.1 Add annulus cell pruning to Quadtree traversal (QueryRadiusPruned).
- [x] P0-4.2 Add radius filter to MortonGrid QueryRadiusPruned.
- [x] P0-4.3 Add radius-aware QueryRadiusPruned method.
- [x] P0-4.4 Benchmark geo search before/after.

## P0-5: Uint32 Dense Search (518 QPS vs ~3,000+)

### Subtasks
- [x] P0-5.1 Add 128-dim high-dim uint32 kernel.
- [x] P0-5.2 Add AVX2 kernel benchmark.
- [x] P0-5.3 Profile AVX2 assembly kernel.
- [x] P0-5.4 Compare against int32/float32 dispatch.

## P0-6: Filtered String Fast Path for Complex Types (436 QPS at 500k/384d)

### Subtasks
- [x] P0-6.1 Dictionary codes already applied for all string columns (dataset.go).
- [x] P0-6.2 SIMD MatchUint16 fast path for eq/neq (filter_text.go).
- [x] P0-6.3 Profile predicate evaluation during complex search.
- [x] P0-6.4 Benchmark filtered search before/after.

## P0-7: Memory Cap Enforcement

### Subtasks
- [x] P0-7.1 Memory enforcement (checkMemoryBeforeWrite, eviction).
- [x] P0-7.2 Validate cap under load tests.

---

## OIDC Federation & Cloud IAM Authentication (AWS + Google)

### Goal

Allow AWS and Google IAM identities to authenticate to Longbow and to
datasets/namespaces via OIDC federation, eliminating long-lived API keys
for cloud workloads while preserving tenant isolation, RLS, and the
existing RBAC model. Federated identities map to Longbow roles/permissions
through verified, short-lived OIDC tokens and cloud trust policies.

### Scope

- Control plane: Arrow Flight gRPC + REST v1 + GraphQL endpoints.
- Data plane: dataset/namespace scoped read/write/ingest/delete.
- Identity providers: AWS STS (`AssumeRoleWithWebIdentity`), Google
  Workload Identity Federation (service account impersonation).
- Existing mechanisms retained: API keys, RBAC, RLS, tenant context.

### Architecture & Trust Model

- Longbow acts as an OIDC relying party and, where needed, a token
  exchanger for downstream cloud resources.
- Each tenant configures one or more trusted issuers (AWS/Google) with
  audience, allowed subjects, and claim-to-role mappings.
- Trust is anchored in cloud IAM: AWS trust policies and Google attribute
  conditions restrict which service accounts/roles may mint tokens for
  Longbow.
- Tokens are validated locally using JWKS (RS256/ES256), with issuer and
  audience pinning; no shared secrets.
- Resolved identity is converted to a `security.TenantContext` and bound to
  namespace/dataset scopes before authorization.

---

### Phase 0: Threat Model & Design Review

- [x] OIDC-0.1 Document threat model (token theft, replay, confused deputy,
      issuer spoofing, claim injection, clock skew, JWKS poisoning).
- [x] OIDC-0.2 Define trust boundary between cloud IAM and Longbow RBAC.
- [x] OIDC-0.3 Security review sign-off before implementation.

### Phase 1: OIDC Provider Discovery & JWKS

- [x] OIDC-1.1 Add `OIDCProvider` config (issuer URL, JWKS URL, audience,
      allowed algorithms, refresh interval).
- [x] OIDC-1.2 Implement JWKS fetch + cache with rotation and ETag/Cache-Control.
- [x] OIDC-1.3 Pin allowed signing algorithms (RS256, ES256); reject `none`/HS*.
- [x] OIDC-1.4 Add issuer allowlist and discovery document validation.

### Phase 2: Token Validation & Claim Mapping

- [x] OIDC-2.1 Validate JWT signature, `iss`, `aud`, `exp`, `nbf`, `iat`,
      `sub`, and (optional) `nonce`.
- [x] OIDC-2.2 Enforce max token lifetime and clock-skew tolerance (<= 60s).
- [x] OIDC-2.3 Map claims (`sub`, groups, custom attributes) to Longbow
      role + tenant + namespace + datasets.
- [x] OIDC-2.4 Add replay protection (jti cache / nonce) for interactive flows.
- [x] OIDC-2.5 Issue internal short-lived session bound to federated identity.

### Phase 3: AWS IAM Federation

- [x] OIDC-3.1 Support AWS STS `AssumeRoleWithWebIdentity` token exchange.
- [x] OIDC-3.2 Validate AWS web identity tokens and role ARN allowlist.
- [x] OIDC-3.3 Map AWS role/session tags to tenant and dataset scopes.
- [x] OIDC-3.4 Document AWS trust policy templates (IRSA, EKS pod identity,
      EC2 instance profile).
- [x] OIDC-3.5 Add example IAM policies for least-privilege access.

### Phase 4: Google IAM Federation

- [x] OIDC-4.1 Support Google Workload Identity Federation token exchange.
- [x] OIDC-4.2 Validate Google ID tokens and service account impersonation.
- [x] OIDC-4.3 Map Google principal/attributes to tenant and dataset scopes.
- [x] OIDC-4.4 Document Google WIF pool/provider and attribute conditions.
- [x] OIDC-4.5 Add example IAM bindings for least-privilege access.

### Phase 5: Dataset & Namespace Authorization Binding

- [x] OIDC-5.1 Extend `APIKey`/`TenantContext` model with federated identity
      provenance (`provider`, `issuer`, `subject`).
- [x] OIDC-5.2 Enforce namespace/dataset scoping for federated identities.
- [x] OIDC-5.3 Integrate with RLS policies for row-level restrictions.
- [x] OIDC-5.4 Add per-identity audit attribution (stable subject ID).

### Phase 6: Audit Logging & Observability

- [x] OIDC-6.1 Record federated auth events in `security.AuditLogger`.
- [x] OIDC-6.2 Add metrics for token validation success/failure and
      provider latency.
- [x] OIDC-6.3 Alert on anomalous auth failures and replay attempts.
- [x] OIDC-6.4 Ensure tokens/secrets are never logged.

### Phase 7: CLI, SDK & API Integration

- [x] OIDC-7.1 Add CLI commands for configuring OIDC providers and mappings.
- [x] OIDC-7.2 Add REST v1 endpoints for provider CRUD and token exchange.
- [x] OIDC-7.3 Add Python/Go SDK helpers for cloud identity token acquisition.
- [x] OIDC-7.4 Update OpenAPI schema and docs.

### Phase 8: Hardening & GA

- [x] OIDC-8.1 Penetration test and red-team review.
- [x] OIDC-8.2 Load/perf tests for token validation hot path.
- [x] OIDC-8.3 GA checklist and migration guide from API keys.

---

### Security Auditing Plan (Complete)

#### 1. Threat Model & Abuse Cases

- Token replay, theft, and fixation.
- Confused deputy / cross-tenant token reuse.
- Issuer/audience spoofing and algorithm confusion (`none`, HS256 with
  RSA public key).
- JWKS endpoint SSRF and cache poisoning.
- Claim injection and over-privileged role mapping.
- Clock skew abuse and expired token acceptance.
- Secret leakage in logs, metrics, and error responses.

#### 2. Cryptographic Verification

- Verify JWT signatures against cached JWKS (RS256/ES256 only).
- Enforce `kid` presence and key rotation handling.
- Reject tokens with missing/unknown `kid`.
- Cache JWKS with bounded TTL and safe refresh (no thundering herd).
- Pin allowed algorithms; reject `none` and symmetric algorithms.

#### 3. Claim & Context Validation

- Strict `iss` allowlist and `aud` exact match.
- `exp`/`nbf`/`iat` validation with bounded clock skew.
- Required `sub`; validate format per provider.
- Optional `nonce`/`at_hash` for interactive flows.
- Validate custom claims used for authorization (no trust of unverified
  attributes).

#### 4. Replay & Session Protection

- Track `jti`/nonce in a bounded LRU with TTL.
- Bind session to federated subject and issuer.
- Invalidate on logout/revocation where supported.

#### 5. Cloud Trust Policy Review (AWS/Google)

- AWS: review trust policies for least privilege, condition keys
  (`aud`, `sub`, `StringEquals`/`StringLike`), and role chaining limits.
- Google: review WIF attribute conditions and service account impersonation
  permissions.
- Periodic audit of trust policies and diff alerts.

#### 6. Tenant & Dataset Isolation

- Federated identity cannot cross namespace/dataset boundaries.
- RLS policies applied uniformly to federated and API-key identities.
- Negative tests for cross-tenant access.

#### 7. Secrets & Key Management

- No client secrets in source; use KMS/Vault where needed.
- Rotate JWKS cache and internal signing keys.
- Secrets scanning in CI (trivy/gitleaks) and pre-commit.

#### 8. Logging & Audit

- Audit all authn/authz decisions with stable subject IDs.
- Redact tokens and PII from logs.
- Tamper-evident audit log verification.

#### 9. Dependency & Supply Chain

- `govulncheck` on security packages.
- `gosec` AST scan.
- Trivy filesystem/container scan (HIGH/CRITICAL).
- Pin and review OIDC/JWT dependencies.

#### 10. Testing Strategy

- Unit tests for token validation edge cases.
- Fuzz tests for JWT parsing and claim mapping.
- Property tests for issuer/audience matching.
- Integration tests with mocked JWKS and STS/WIF endpoints.
- Negative tests for malformed/expired/replayed tokens.

#### 11. Penetration Testing & Red Team

- Algorithm confusion, key confusion, SSRF on JWKS URL.
- Replay and token substitution.
- Privilege escalation via claim manipulation.
- Cross-tenant data access attempts.

#### 12. Compliance Mapping

- SOC2 CC6.1/CC6.3 access control; CC6.6/CC6.7 crypto.
- GDPR Art. 32 security of processing; Art. 17 erasure (identity-linked data).
- Audit evidence artifacts retained per control.

---

### Verification Artifacts & Gates

| Control | Artifact / Test | Gate |
| :--- | :--- | :--- |
| JWT signature & alg allowlist | `internal/security/*_test.go` (OIDC) | `make test-short` |
| Issuer/audience/expiry | Fuzz targets `FuzzOIDC*` | `make fuzz-coverage` |
| Tenant/dataset isolation | `internal/store/rbac_rls_test.go` extensions | `make test-coverage` |
| Secrets scanning | Trivy + gitleaks | CI `security` job |
| Dependency CVEs | govulncheck | CI `security` job |
| AST security | gosec | `make sec-gosec` |
| OIDC federation audit | `scripts/check_oidc_federation_security.sh` | CI `oidc-federation-security` job |

---

### CI / Workflow Pipeline Updates

- Add `sec-oidc` Makefile target invoking
  `scripts/check_oidc_federation_security.sh`.
- Add CI job `oidc-federation-security` running `make sec-oidc` and targeted
  tests.
- Gate release containers on `oidc-federation-security`.
- Add OIDC fuzz/test targets to `test-features` once implemented.

---

### Next Steps (Immediate)

1. Approve threat model and trust boundary (Phase 0).
2. Implement JWKS fetch/cache with algorithm allowlist (Phase 1).
3. Add token validation + claim mapping with comprehensive tests (Phase 2).
4. Wire federated identity into `TenantContext` and RBAC (Phase 5).
5. Add audit logging and metrics (Phase 6).
6. Extend CI with `sec-oidc` and fuzz coverage for new packages.

---

## DevSecOps, Helm, Grafana & Observability Upgrades

### Goal

Harden delivery pipeline, container images, Helm charts, and Grafana
observability for the latest features (OIDC federation, security auditing).

### Subtasks

- [x] DS-1 Add OIDC security audit script and Makefile target (`sec-oidc`).
- [x] DS-2 Add CI job `oidc-federation-security` and gate releases.
- [x] DS-3 Add NetworkPolicy and PodDisruptionBudget Helm templates.
- [x] DS-4 Add OIDC values and deployment env wiring in Helm chart.
- [x] DS-5 Add OCI labels to container image.
- [x] DS-6 Add Grafana alert group for OIDC auth/JWKS failures.
- [x] DS-7 Add image signing (cosign) and SBOM generation to CI.
- [x] DS-8 Add Trivy image scan gate to release pipeline.
- [x] DS-9 Add gitleaks secret scanning to pre-commit and CI.
- [x] DS-10 Add OIDC metrics and dashboards (auth success/failure, latency).
- [x] DS-11 Add runbooks for OIDC incident response.
