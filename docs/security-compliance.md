# Security, Encryption & Compliance Guide (SOC2 / GDPR)

Longbow provides multi-tenant isolation, role-based access control, TLS/mTLS transport security, envelope key management, AES-256-GCM encryption at rest, and tamper-evident audit logging across Arrow Flight gRPC and HTTP endpoints.

---

## 1. Authentication, RBAC & Tenant Isolation

### Roles & Permissions

Defined in `internal/store/rbac/`:

| Role | Granted Permissions |
| :--- | :--- |
| `admin` | Full cluster and cross-tenant administration (`read`, `write`, `delete`, `backup`, `manage_keys`, `manage_tenant`, `admin`) |
| `tenant-admin` | Tenant/namespace-scoped `read`, `write`, `delete`, `backup`, `manage_keys`, `manage_tenant` |
| `writer` | Tenant/namespace-scoped `read`, `write` |
| `reader` | Tenant/namespace-scoped `read` |

### RBAC Bootstrap

RBAC is always enforced and cannot be disabled. Configure the bootstrap admin
and secret provider via environment variables (`cmd/longbow/config.go`):

```bash
export LONGBOW_RBAC_BOOTSTRAP_KEY="<initial-admin-secret>"
export LONGBOW_SECRETS_PROVIDER=file  # or vault, gcp, aws
```

When `LONGBOW_RBAC_BOOTSTRAP_KEY` is provided, the server registers it as a
bootstrap `admin` key and writes it through to the configured provider. Without
a bootstrap key, the server must load at least one admin key from that provider
or it refuses to start. The same provider stores durable namespace ACL records,
which are rehydrated with API keys at startup. Namespace objects for durable
ACLs are recreated even if they contain no replayed datasets.

Namespace ACLs can also be declared before listeners start with
`LONGBOW_STARTUP_NAMESPACE_POLICIES` (or `--startup-namespace-policies`). Durable
policies persist in the configured provider; `ephemeral: true` policies remain
runtime-only. Public namespace permissions are granted only to authenticated
keys. See [RBAC documentation](rbac.md) for provider details and the JSON schema.

### API Key Lifecycle & CLI Management

Keys use HMAC/SHA-256 digests (`internal/store/rbac/`) and support timed rotation grace periods and immediate revocation:

```bash
# Issue a tenant-scoped reader key
longbow-cli create-api-key -name analytics -role reader -tenant acme -datasets acme/docs -ttl 86400

# Rotate a key with a 1-hour overlap grace period
longbow-cli rotate-api-key -key "$OLD_KEY" -grace 3600

# Revoke a compromised key immediately by raw key or KeyID
longbow-cli revoke-api-key -key "$KEY_OR_ID"

# List key metadata (redacted; never exposes hashes or raw secrets)
longbow-cli list-api-keys

# Configure Row-Level Security (RLS) predicate for a tenant dataset
longbow-cli set-rls-policy -tenant acme -dataset acme/docs -field dept -op in -values eng,sec

# Query the structured security audit log
longbow-cli audit-log -tenant acme -limit 50
```

Equivalent REST v1 endpoints are exposed at `GET/POST/DELETE /api/v1/rbac/keys`, `POST /api/v1/rbac/keys/rotate`, `POST /api/v1/rbac/rls`, and `GET /api/v1/rbac/audit`.

### Tenant Isolation & Row-Level Security (RLS)

- **Dataset/Namespace Scoping**: `AuthorizeDatasetAccess` and `security.TenantContext.CanAccessDataset` enforce tenant, namespace-ACL, public-namespace, and allowed-dataset boundaries across `DoPut`, `DoGet`, `ListFlights`, `DoAction`, and `/api/v1/*` HTTP handlers. Namespace grants never become global permissions.
- **Row-Level Security (`security.RLSEvaluator`)**: Dynamic metadata predicates (`eq`, `neq`, `in`, `contains`) evaluated per row during query result assembly (`FilterResultsByRLS`) using the `SetRLSPolicy` Flight action, `POST /api/v1/rbac/rls`, `longbow-cli set-rls-policy`, or Python SDK `client.set_rls_policy(...)`.

---

## 2. Transport Security (TLS / mTLS & Certificate Hot Rotation)

Implemented in `internal/security/tls.go` and wired into `cmd/longbow/main.go` and `client/client.go`:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_TLS_ENABLED` | `false` | Enable TLS on gRPC/Flight servers |
| `LONGBOW_TLS_CERT_FILE` | `""` | Path to PEM server certificate chain |
| `LONGBOW_TLS_KEY_FILE` | `""` | Path to PEM server private key |
| `LONGBOW_TLS_CA_FILE` | `""` | CA bundle for verifying client certificates (mTLS) |
| `LONGBOW_TLS_REQUIRE_CLIENT_CERT` | `false` | Enforce mutual TLS (`RequireAndVerifyClientCert`) |
| `LONGBOW_TLS_MIN_VERSION` | `"1.2"` | Minimum TLS protocol (`"1.2"` or `"1.3"`) |
| `LONGBOW_TLS_RELOAD_INTERVAL` | `5m` | Periodic reload interval (`security.CertReloader`) |

`security.CertReloader` watches both `SIGHUP` signals and periodic intervals (`ReloadInterval`) to reload certificates without dropping active connections.

---

## 3. Encryption at Rest & KMS / Vault Integration

- **AES-256-GCM Cipher (`internal/storage/encryption.go`)**: Length-framed 12-byte random-nonce authenticated encryption for disk spill files, WAL segments, and snapshots (`EncryptWALRecord`, `DecryptWALRecord`, `NewEncryptedWriter`, `NewEncryptedReader`).
- **Envelope KMS (`internal/security/kms.go`)**:
  - `LocalKMSProvider`: HKDF-SHA256 key derivation (`LONGBOW_KMS_MASTER_KEY`).
  - `VaultTransitKMSProvider`: HashiCorp Vault `/v1/{mount}/datakey/plaintext/{key}` and `/decrypt/{key}` envelope key wrapping.

```bash
export LONGBOW_ENCRYPTION_AT_REST=true
export LONGBOW_KMS_PROVIDER=local
export LONGBOW_KMS_MASTER_KEY="<32-byte-hex-or-passphrase>"
```

---

## 4. SOC2 & GDPR Control Mapping

| Control Objective | Longbow Capability | Verification Artifact |
| :--- | :--- | :--- |
| **Access Control & Least Privilege** (SOC2 CC6.1, CC6.3) | RBAC roles, durable namespace ACLs, tenant scoping, RLS policies, gRPC/HTTP auth interceptors | `internal/store/rbac/`, `internal/store/rbac_rls_test.go`, `scripts/verify_rbac.py` |
| **Encryption in Transit & at Rest** (SOC2 CC6.6, CC6.7; GDPR Art. 32) | TLS 1.2/1.3, mTLS, SIGHUP hot-reload, AES-256-GCM WAL/spill, KMS envelope keys | `internal/security/tenant_tls_kms_test.go`, `internal/storage/encryption_test.go` |
| **Audit Logging & Non-Repudiation** (SOC2 CC7.2) | Structured immutable JSON audit ring + writer (`security.AuditLogger`), `GetAuditLog` | `internal/security/audit.go`, `internal/store/rbac_actions.go` |
| **Right to Erasure** (GDPR Art. 17) | Vector/dataset hard deletion (`delete-vector`, `Drop`), tombstone compaction, PITR backup exclusion | `internal/store/store_actions.go`, `internal/store/backup_pitr.go` |
| **Resilience & Recovery** (SOC2 A1.2, A1.3) | Online checksum-verified snapshots, retention policies, WAL PITR replay | `internal/store/backup_test.go` |

---

## 5. OIDC Federation & Cloud IAM Authentication

Longbow supports OIDC federation for AWS and Google IAM identities, mapping
validated, short-lived tokens to `TenantContext` with namespace/dataset scoping
and RLS.

### Components

- `internal/security/oidc.go`: `OIDCManager`, JWKS fetch/cache, token validation,
  claim mapping, replay protection.
- `internal/security/oidc_access.go`: `OIDCAccessValidator` (implements
  `AccessValidator`) enforcing permissions and dataset boundaries.
- `internal/metrics/oidc_metrics.go`: auth attempt/success/failure, JWKS fetch
  failure, and validation latency metrics.
- Grafana alert group `longbow_oidc_alerts` for auth failure rate and JWKS
  fetch failures.

### Configuration

- `OIDCConfig` / `OIDCIssuerConfig` (issuer URL, JWKS URL, audience, allowed
  algorithms, claim mappings, clock skew).
- Environment variables: `LONGBOW_OIDC_ENABLED=true`, `LONGBOW_OIDC_ISSUERS='[...]'`.
- Runtime endpoints: `/api/v1/oidc/providers`, `/api/v1/oidc/token`.
- CLI commands: `oidc-list-providers`, `oidc-add-provider`, `oidc-remove-provider`, `oidc-exchange`.
- Helpers: `AWSOIDCIssuerConfig`, `GoogleOIDCIssuerConfig`.

### Least-Privilege Cloud IAM Policy Examples

#### AWS IAM Trust Policy (IRSA / EKS Pod Identity)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE:aud": "sts.amazonaws.com",
          "oidc.eks.us-west-2.amazonaws.com/id/EXAMPLED539D4633E53DE1B71EXAMPLE:sub": "system:serviceaccount:acme-ns:longbow-reader"
        }
      }
    }
  ]
}
```

#### Google Cloud Workload Identity Federation Binding

```bash
gcloud iam service-accounts add-iam-policy-binding \
  longbow-reader@my-gcp-project.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/longbow-pool/attribute.tenant_id/acme" \
  --condition='expression=assertion.sub.startsWith("repo:23skdu/"),title=scope-to-org,description=Least-privilege WIF binding'
```

### Verification

- Unit tests: `internal/security/oidc_test.go`, `oidc_access_test.go`, `oidc_bench_test.go`.
- Store integration tests: `internal/store/oidc_store_test.go`.
- SDK tests: `client/oidc_test.go`, `longbowclientsdk/tests/test_search_modes.py`.
- Fuzz target: `FuzzOIDCValidateToken`.
- Benchmarks: `BenchmarkOIDCValidateToken`, `BenchmarkOIDCAccessValidator`.
- CI: `oidc-federation-security` job runs `make sec-oidc`.
- Grafana: `grafana/tests/oidc_alerts_test.yml`, `grafana/dashboards/oidc-federation.json`.

---

## 6. OIDC Federation GA Checklist & API Key Migration Guide

### Threat Model & Trust Boundary Sign-Off

- **Cloud IdP Trust Boundary**: Longbow accepts only asymmetric (`RS256`, `ES256`) JWTs from explicitly allowlisted HTTPS `issuer_url` / `jwks_url` providers, requires `kid`, enforces `iss`, `aud`, `exp`, `nbf`, `iat` within `<= 60s` clock skew, caps token payloads at 16 KiB, and tracks `jti` for replay prevention.
- **Authorization Boundary**: Validated claims map strictly to `TenantContext` (`tenant_id`, `namespace`, `datasets`, `row_filters`, `roles`, `permissions`). Cloud claims never bypass `OIDCAccessValidator`, `CanAccessDataset`, or `RLSEvaluator`.

### GA Readiness Checklist

- [x] Asymmetric signing pinned (`RS256`, `ES256`), `alg=none` and `HS*` rejected.
- [x] `jti` replay protection and <= 60s clock-skew tolerance enforced.
- [x] Short-lived session token exchange (`POST /api/v1/oidc/token` & `ExchangeOIDCToken`, capped <= 1h).
- [x] Namespace, dataset, and row-level security (`RowFilterClaim` + `RLSEvaluator`) enforced.
- [x] Audit logging (`security.AuditLogger`), Prometheus metrics, Grafana dashboard & promtool-tested alerts.
- [x] Helm chart OIDC config, NetworkPolicy egress (TCP 443), and incident response runbooks (`docs/runbooks.md`).
- [x] Pen-test security vector suite (`TestOIDCRedTeamSecurityVectors`), fuzzing (`FuzzOIDCValidateToken`), and benchmarks (`BenchmarkOIDC*`).

### Migrating from Static API Keys to OIDC Federation

1. **Enable OIDC alongside always-on RBAC** (`LONGBOW_OIDC_ENABLED=true`, `LONGBOW_OIDC_ISSUERS='[...]'`). Longbow's `compositeAccessValidator` evaluates both static API keys and federated OIDC JWTs concurrently during migration.
2. **Acquire & Exchange Cloud Tokens** using the Go SDK (`client.AcquireAWSWebIdentityToken()` / `client.AcquireGoogleIDToken(...)` + `sc.ExchangeOIDCToken(...)`) or Python SDK (`LongbowClient.acquire_aws_web_identity_token()` / `acquire_google_id_token(...)` + `exchange_oidc_token(...)`).
3. **Verify Audit Attribution** in `GET /api/v1/rbac/audit` (`oidc:<provider>:<sub>`) and monitor `grafana/dashboards/oidc-federation.json`.
4. **Revoke Legacy API Keys** via `longbow-cli revoke-api-key` or `POST /api/v1/rbac/keys` rotation once workloads exclusively authenticate via OIDC.
