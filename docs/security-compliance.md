# Security, Encryption & Compliance Guide (SOC2 / GDPR)

Longbow provides multi-tenant isolation, role-based access control, TLS/mTLS transport security, envelope key management, AES-256-GCM encryption at rest, and tamper-evident audit logging across Arrow Flight gRPC and HTTP endpoints.

---

## 1. Authentication, RBAC & Tenant Isolation

### Roles & Permissions

Defined in `internal/store/rbac.go`:

| Role | Granted Permissions |
| :--- | :--- |
| `admin` | Full cluster and cross-tenant administration (`read`, `write`, `delete`, `backup`, `manage_keys`, `manage_tenant`, `admin`) |
| `tenant-admin` | Tenant/namespace-scoped `read`, `write`, `delete`, `backup`, `manage_keys`, `manage_tenant` |
| `writer` | Tenant/namespace-scoped `read`, `write` |
| `reader` | Tenant/namespace-scoped `read` |

### Enabling RBAC

Configure via environment variables (`cmd/longbow/main.go`):

```bash
export LONGBOW_RBAC_ENABLED=true
export LONGBOW_RBAC_BOOTSTRAP_KEY="<initial-admin-secret>"
```

When `LONGBOW_RBAC_BOOTSTRAP_KEY` is provided at startup, the server registers it as a bootstrap `admin` key so administrators can immediately provision scoped keys.

### API Key Lifecycle & CLI Management

Keys use HMAC/SHA-256 digests (`internal/store/rbac.go`) and support timed rotation grace periods and immediate revocation:

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

- **Dataset/Namespace Scoping**: `AuthorizeDatasetAccess` and `security.TenantContext.CanAccessDataset` enforce tenant and allowed-dataset boundaries across `DoPut`, `DoGet`, `ListFlights`, `DoAction`, and `/api/v1/*` HTTP handlers.
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
| **Access Control & Least Privilege** (SOC2 CC6.1, CC6.3) | RBAC roles, tenant scoping, RLS policies, gRPC/HTTP auth interceptors | `internal/store/rbac_rls_test.go`, `internal/middleware/auth_interceptor_test.go` |
| **Encryption in Transit & at Rest** (SOC2 CC6.6, CC6.7; GDPR Art. 32) | TLS 1.2/1.3, mTLS, SIGHUP hot-reload, AES-256-GCM WAL/spill, KMS envelope keys | `internal/security/tenant_tls_kms_test.go`, `internal/storage/encryption_test.go` |
| **Audit Logging & Non-Repudiation** (SOC2 CC7.2) | Structured immutable JSON audit ring + writer (`security.AuditLogger`), `GetAuditLog` | `internal/security/audit.go`, `internal/store/rbac_actions.go` |
| **Right to Erasure** (GDPR Art. 17) | Vector/dataset hard deletion (`delete-vector`, `Drop`), tombstone compaction, PITR backup exclusion | `internal/store/store_actions.go`, `internal/store/backup_pitr.go` |
| **Resilience & Recovery** (SOC2 A1.2, A1.3) | Online checksum-verified snapshots, retention policies, WAL PITR replay | `internal/store/backup_test.go` |
