# RBAC, Namespace ACLs, API Keys & OIDC Federation

Longbow **always enforces** Role-Based Access Control (RBAC), Row-Level Security
(RLS), and OpenID Connect (OIDC) workload identity federation across **Arrow
Flight gRPC** (Data Plane `:3000` and Control Plane `:3001`), **HTTP REST v1**
(`/api/v1/*`, served on the metrics/HTTP listener), and **GraphQL**
(`/api/v1/graphql`).

There is no RBAC off switch. `LONGBOW_RBAC_ENABLED` is accepted only for
backward compatibility with old deployment manifests and is ignored. The server
refuses to start unless it can authenticate at least one server administrator.

---

## 1. Server Bootstrap

At startup Longbow:

1. Builds a durable secret store from `LONGBOW_SECRETS_*`.
2. Loads all persisted API keys into the RBAC manager.
3. Registers `LONGBOW_RBAC_BOOTSTRAP_KEY` as the `bootstrap-admin` key when set.
4. Fails startup if no enabled global admin key exists.

The startup error is explicit:

```text
RBAC is always enforced: set LONGBOW_RBAC_BOOTSTRAP_KEY or provision an admin API key in the <provider> secrets provider
```

The bootstrap key must be at least 16 characters. It is written through to the
configured secret provider, so subsequent restarts do not need to repeat the
environment variable if the provider remains available.

```bash
export LONGBOW_RBAC_BOOTSTRAP_KEY='a-long-random-admin-secret'
./bin/longbow
```

### Secret providers

| `LONGBOW_SECRETS_PROVIDER` | Durable | Required configuration | Notes |
| :--- | :--- | :--- | :--- |
| `file` (default) | yes | `LONGBOW_SECRETS_PATH` (default `data/rbac_secrets`) | Local JSON records; mount a persistent volume in Kubernetes. |
| `memory` | no | — | Intended for unit tests only; keys are lost on restart. |
| `vault` | yes | `LONGBOW_SECRETS_VAULT_ADDR`, `LONGBOW_SECRETS_VAULT_TOKEN`, optional `LONGBOW_SECRETS_VAULT_MOUNT` | HashiCorp Vault KV v2. Secrets live under `LONGBOW_SECRETS_PREFIX`. |
| `gcp` | yes | `LONGBOW_SECRETS_GCP_PROJECT_ID`, optional `LONGBOW_SECRETS_GCP_TOKEN` | Google Secret Manager. Uses Application Default Credentials unless a token is supplied. |
| `aws` | yes | `LONGBOW_SECRETS_AWS_REGION`, optional `LONGBOW_SECRETS_AWS_ENDPOINT` | AWS Secrets Manager. Uses the standard AWS credential chain; `endpoint` supports local emulators. |

Common optional configuration:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_SECRETS_PREFIX` | `longbow/rbac` | Provider-side namespace for Longbow secrets. |

Raw API secrets are returned to the operator exactly once when a key is issued
or rotated. The in-memory RBAC maps are keyed by SHA-256 hash; metadata updates
and revocations are written through to the provider without retaining raw
secrets in memory.

---

## 2. Roles & Permissions

Defined in `internal/store/rbac/` and aligned with OIDC claim mapping in
`internal/security/oidc.go`:

| Role | Aliases | Granted Permissions | Capabilities |
| :--- | :--- | :--- | :--- |
| **`admin`** | — | `read`, `write`, `delete`, `ingest`, `admin`, `backup`, `manage-keys`, `manage-tenant` | Full cluster access, OIDC provider management, audit log, backups, key/tenant administration. |
| **`tenant-admin`** | — | `read`, `write`, `delete`, `ingest`, `backup`, `manage-keys`, `manage-tenant` | Scoped to an assigned tenant/namespace; cannot use cluster-wide `admin` endpoints. |
| **`writer`** | `read-write` | `read`, `write`, `delete`, `ingest` | Search, read, ingest, upserts, graph mutations, and deletions within its scope. |
| **`ingest-only`** | — | `read`, `ingest` | Read/search and stream new vectors; cannot mutate, delete, or administer. |
| **`reader`** | `read-only` | `read` | Search, `DoGet`, `GetFlightInfo`, `GetSchema`, `ListFlights`, and read-only REST/GraphQL. |

Only server admins (`manage-tenant`) may create or delete namespaces.

---

## 3. Namespace ACLs & Public Namespaces

Every namespace can carry a `NamespacePolicy`:

```json
{
  "name": "team-a",
  "admin_key_ids": ["key_team_a_admin"],
  "writer_key_ids": ["key_team_a_writer"],
  "reader_key_ids": ["key_team_a_reader"],
  "public_permissions": ["read"]
}
```

Create it with an admin key:

```bash
curl -X POST http://127.0.0.1:9090/api/v1/namespaces \
  -H 'X-API-Key: '"$LONGBOW_RBAC_BOOTSTRAP_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"team-a","reader_key_ids":["key_team_a_reader"],"public_permissions":["read"]}'
```

Rules:

- The authenticated creator is automatically added to `admin_key_ids`.
- Namespace grants are attached to `TenantContext.NamespacePermissions` for
  that namespace only. They **never** become global permissions.
- `reader` grants allow `read`; `writer` grants allow `read`, `write`,
  `delete`, and `ingest`; `admin` grants additionally allow namespace-scoped
  `manage-keys` and `manage-tenant`.
- `public_permissions` are granted to **every authenticated key**, including
  OIDC session keys. Supported values are `read`, `write`, `delete`, and
  `ingest`; write-class permissions imply `read`.
- A public namespace is not unauthenticated. A valid API key or OIDC token is
  still required to reach it.
- Namespace ACL grants override the key's default namespace boundary and
  dataset allow-list for that namespace only. Another namespace remains
  private unless it also grants access.

REST, GraphQL, and Flight `CreateNamespace` all accept the same ACL fields and
enforce the same server-admin-only rule.

---

## 4. API Keys

Admins create scoped keys through REST:

```bash
curl -X POST http://127.0.0.1:9090/api/v1/rbac/keys \
  -H 'X-API-Key: '"$LONGBOW_RBAC_BOOTSTRAP_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
        "name": "team-a-writer",
        "role": "writer",
        "tenant_id": "home",
        "namespace": "home",
        "datasets": ["team-a/*"]
      }'
```

The response includes `key_id` and the raw `api_key`. Store the raw secret in
your secret manager; only the hash remains in the Longbow process.

- **Rotation**: `RotateAPIKey` issues a replacement key and can keep the old
  key valid for a grace period. Namespace ACLs follow the new `key_id`.
- **Revocation**: `DELETE /api/v1/rbac/keys?key_id=<id>` disables the key
  immediately and removes it from the provider.
- **Audit**: key lifecycle, auth successes/failures, RLS updates, and OIDC
  provider changes are recorded in `AuditLogger`.

---

## 5. Enforcement Layers

### Layer 1 — Transport middleware

- **Arrow Flight gRPC**: extracts `authorization: Bearer <token>` or
  `x-api-key: <token>`, plus optional `x-tenant-id` / `x-namespace` and
  `x-dataset`. Read operations require `read`; `DoPut`/`DoExchange` require
  `ingest`; `DoAction` delegates fine-grained checks to its handler.
- **HTTP REST/GraphQL**: `httpAuthWrap` and `httpMethodAuthWrap` resolve the
  access validator on every request. `GET`/`HEAD` require `read`; mutations
  require `write`, `manage-tenant`, or `admin`.
- **Token routing**: 3-segment JWTs go to the OIDC validator; opaque Longbow
  keys go to the RBAC manager (SHA-256 lookup, enabled/TTL/rotation checks).

### Layer 2 — Dataset and namespace isolation

`VectorStore.AuthorizeDatasetAccess(ctx, perm, datasetPath)` parses
`namespace/dataset`, checks global or namespace-scoped permission, then applies
the tenant boundary and any dataset allow-list. Namespace ACL and public grants
bypass only the namespace they were granted for.

### Layer 3 — Row-Level Security

Non-admin search results pass through `FilterResultsByRLS`, which applies
per-dataset RLS policies and per-key `row_filters`. Global admins bypass RLS.

---

## 6. OIDC Federation

OIDC is optional and additive:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_OIDC_ENABLED` | `false` | Enables `OIDCManager` and JWT validation. |
| `LONGBOW_OIDC_ISSUERS` | `""` | JSON array of `OIDCIssuerConfig` objects. |

Federated JWTs are validated for JWKS signature, algorithm allow-list,
`iss`/`aud`/`exp`/`nbf`, and `jti` replay. `ExchangeOIDCToken` (Flight) or
`POST /api/v1/oidc/token` (REST) issues a short-lived Longbow session key
(default 15m, max 1h, capped at JWT expiry).

| JWT Claim (Configurable) | Default | `TenantContext` Field |
| :--- | :--- | :--- |
| `tenant_claim` | `tenant_id` | `TenantID` |
| `namespace_claim` | `namespace` | `Namespace` |
| `datasets_claim` | `datasets` | `Datasets` |
| `role_claim` | `groups` | `Roles` → permissions |
| `row_filter_claim` | `row_filters` | `RowFilters` |
| `admin_claim` / `admin_value` | `role` / `""` | `IsAdmin` |

OIDC providers can be registered at runtime by an admin without restarting;
RBAC remains enforced throughout.

---

## 7. Verification

Run the RBAC-only script against a running server's HTTP listener:

```bash
LONGBOW_RBAC_BOOTSTRAP_KEY='a-long-random-admin-secret' \
  python3 scripts/verify_rbac.py --url http://127.0.0.1:9090
```

It verifies, and only verifies:

- unauthenticated requests are rejected;
- the bootstrap key can create scoped keys and namespaced policies;
- namespace reader/writer grants are scoped correctly;
- public namespaces work for any authenticated key;
- non-admins cannot create namespaces; and
- revoked keys stop working immediately.

Unit tests:

```bash
go test ./internal/security/secrets/... ./internal/store/rbac/... ./cmd/longbow ./cmd/agentsoak
python3 -m unittest scripts.tests.test_multiagent_config scripts.tests.test_verify_rbac
```

See [`configs/README.md`](../configs/README.md) and
[`docs/development/soaktestplan.md`](development/soaktestplan.md) for the
authenticated multiagent soak scenario.
