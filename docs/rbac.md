# RBAC, Namespace ACLs, API Keys & OIDC Federation

Longbow **always enforces** Role-Based Access Control (RBAC) and Row-Level
Security (RLS) across **Arrow Flight gRPC** (Data Plane `:3000` and Control
Plane `:3001`), **HTTP REST v1** (`/api/v1/*`, served on the REST/metrics
listener), and **GraphQL** (`/api/v1/graphql`). OIDC federation is optional and
additive; when enabled it supplies identities to the same authorization model.

There is no RBAC off switch. `LONGBOW_RBAC_ENABLED` is accepted only for
backward compatibility with old deployment manifests and is ignored.

---

## 1. Server Bootstrap

At startup Longbow:

1. Builds an API-key store from `LONGBOW_SECRETS_*`. The `memory` provider is
   non-durable; all other providers persist keys.
2. Loads all persisted API keys into the RBAC manager.
3. Registers `LONGBOW_RBAC_BOOTSTRAP_KEY` as the `bootstrap-admin` key when set.
4. Without a bootstrap key, requires at least one enabled, unscoped global
   `admin` key loaded from the store.

If no admin is available, the process exits with an error such as:

```text
Longbow exited with error: RBAC is always enforced: set LONGBOW_RBAC_BOOTSTRAP_KEY or provision an admin API key in the file secrets provider
```

The bootstrap key must be at least 16 characters. It is written through to the
configured provider, so later restarts do not need to repeat the environment
variable while that provider remains available. Registration is idempotent and
keyed by secret hash: use a fresh value for the bootstrap admin rather than
reusing a key that already exists with a different role.

Build the binary first, then start it:

```bash
go build -o bin/longbow ./cmd/longbow
export LONGBOW_RBAC_BOOTSTRAP_KEY='a-long-random-admin-secret'
./bin/longbow
```

The REST listener defaults to `0.0.0.0:9090`. It can be changed with
`LONGBOW_METRICS_ADDR` or `--metrics-addr`; use that same address for all REST
examples below.

### Secret providers

| `LONGBOW_SECRETS_PROVIDER` | Durable | Required configuration | Notes |
| :--- | :--- | :--- | :--- |
| `file` (default) | yes | `LONGBOW_SECRETS_PATH` (default `data/rbac_secrets`) | One `<key_id>.json` per key. Directory mode `0700`, file mode `0600`. Mount a persistent volume in Kubernetes. |
| `memory` | no | — | Unit tests only; keys are lost on restart. |
| `vault` | yes | `LONGBOW_SECRETS_VAULT_ADDR`; optional `LONGBOW_SECRETS_VAULT_TOKEN`, `LONGBOW_SECRETS_VAULT_MOUNT` (default `secret`) | HashiCorp Vault KV v2. The token header is omitted when no token is configured, so external auth such as a token helper or agent must supply it. |
| `gcp` | yes | `LONGBOW_SECRETS_GCP_PROJECT_ID`; optional `LONGBOW_SECRETS_GCP_TOKEN` | Google Secret Manager. Uses Application Default Credentials unless a token is supplied. Prefixes are lowercased with `/`, `.`, and `_` replaced by `-`. |
| `aws` | yes | `LONGBOW_SECRETS_AWS_REGION`; optional `LONGBOW_SECRETS_AWS_ENDPOINT` | AWS Secrets Manager. Uses the standard AWS credential chain; `endpoint` supports local emulators. Secrets are named `<prefix>/<key_id>`. |

Common optional configuration:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_SECRETS_PREFIX` | `longbow/rbac` | Provider-side namespace for Longbow secrets. |

The provider persists the raw secret together with its metadata; the RBAC
manager keeps only hash-keyed lookup maps in memory. The raw value is returned
to the caller when a key is created or rotated, so the provider itself must be
protected as a credential store.

**Namespace policies are not persisted by the secret provider.** Namespace ACLs
and public permissions are runtime state in the RBAC manager and must be
recreated after a process restart or redeployment. API-key creation, rotation,
and revocation metadata are persisted.

---

## 2. Roles & Permissions

Defined in `internal/store/rbac/` and aligned with OIDC claim mapping in
`internal/security/oidc.go`:

| Role | Aliases | Granted Permissions | Capabilities |
| :--- | :--- | :--- | :--- |
| **`admin`** | — | `read`, `write`, `delete`, `ingest`, `admin`, `backup`, `manage-keys`, `manage-tenant` | Full cluster access, OIDC provider management, audit log, backups, key/tenant administration. |
| **`tenant-admin`** | — | `read`, `write`, `delete`, `ingest`, `backup`, `manage-keys`, `manage-tenant` | Carries tenant-management permissions but remains subject to its tenant/namespace/dataset scope for data access. |
| **`writer`** | `read-write` | `read`, `write`, `delete`, `ingest` | Search, read, ingest, upserts, graph mutations, and deletions within its scope. |
| **`ingest-only`** | — | `read`, `ingest` | Read/search and stream new vectors; cannot mutate, delete, or administer. |
| **`reader`** | `read-only` | `read` | Search, `DoGet`, `GetFlightInfo`, `GetSchema`, `ListFlights`, and read-only REST/GraphQL. |

Namespace creation and deletion require the **global** `manage-tenant`
permission; `admin` and `tenant-admin` keys carry it. A namespace-scoped
`manage-tenant` grant from a namespace ACL does not satisfy this global check.

---

## 3. Namespace ACLs & Public Namespaces

REST and data-plane Flight `CreateNamespace` accept a wrapper payload with
`name` plus ACL fields:

```json
{
  "name": "team-a",
  "admin_key_ids": ["key_team_a_admin"],
  "writer_key_ids": ["key_team_a_writer"],
  "reader_key_ids": ["key_team_a_reader"],
  "public_permissions": ["read"]
}
```

Internally this becomes a `NamespacePolicy` whose `namespace` field is set from
`name`. Create it with a key that has global `manage-tenant`:

```bash
curl -X POST http://127.0.0.1:9090/api/v1/namespaces \
  -H 'X-API-Key: '"$LONGBOW_RBAC_BOOTSTRAP_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"team-a","reader_key_ids":["key_team_a_reader"],"public_permissions":["read"]}'
```

Rules:

- The authenticated creator is automatically added to `admin_key_ids` when the
  request has an API-key ID (REST and data-plane Flight). GraphQL and
  control-plane Flight `CreateNamespace` accept only `name` and do not attach
  ACL fields or an auto-admin grant; use REST to configure ACLs.
- Namespace grants are attached to `TenantContext.NamespacePermissions` for
  that namespace only. They **never** become global permissions.
- `admin_key_ids` grants `read`, `write`, `delete`, `ingest`, `manage-keys`, and
  namespace-scoped `manage-tenant`; `writer_key_ids` grants data mutation
  permissions; `reader_key_ids` grants `read`. An explicit key-specific grant
  takes precedence over public permissions for that namespace.
- `public_permissions` are granted to **every authenticated RBAC key**,
  including exchanged OIDC session keys. Supported values are `read`, `write`,
  `delete`, and `ingest`; write-class permissions imply `read`. Direct OIDC JWT
  validation without token exchange does not attach namespace grants.
- A public namespace is not unauthenticated. A valid API key or OIDC session is
  still required to reach it.
- Namespace grants override the key's default tenant boundary and dataset
  allow-list only for the namespace that grants them. Other namespaces remain
  private unless they also grant access.

REST, GraphQL, and Flight all enforce the global `manage-tenant` rule for
namespace creation and deletion; only REST and data-plane Flight accept the
ACL fields shown above.

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
        "datasets": ["team-a/*"],
        "row_filters": {},
        "ttl_seconds": 0
      }'
```

The response includes `key_id` and the raw `api_key`. Store the raw secret in
your own secret manager if you need to distribute it; Longbow's provider keeps
its own copy for restart rehydration.

- **Rotation**: the Flight `RotateAPIKey` action (and `RBACManager`) issues a
  replacement key and can keep the old key valid for a grace period. Namespace
  ACL key IDs are rewritten in memory; there is no REST rotation endpoint.
- **Revocation**: `DELETE /api/v1/rbac/keys?key_id=<id>` sets `Enabled=false`,
  records `RevokedAt`, and persists the disabled metadata in the provider. The
  key stops working immediately. Hard deletion is not exposed through REST or
  Flight.
- **Audit**: key lifecycle, authentication outcomes, and OIDC provider changes
  are recorded in `AuditLogger`. RLS policy updates are not currently audited.

---

## 5. Enforcement Layers

### Layer 1 — Transport middleware

- **Arrow Flight gRPC**: extracts `authorization: Bearer <token>` or
  `x-api-key: <token>`, plus optional `x-tenant-id` (then `x-namespace`) and
  `x-dataset` metadata. Read operations require `read`; `DoPut`/`DoExchange`
  require `ingest`. `DoAction` maps the action name to a required permission
  (for example, key management or tenant management) and defaults unknown
  actions to `admin`; the handler then performs its own checks.
- **HTTP REST/GraphQL**: `httpAuthWrap` and `httpMethodAuthWrap` resolve the
  access validator on every request. REST routes use route-specific
  permissions (for example, `/api/v1/rbac/keys` requires `manage-keys`, and
  backups require `backup`). GraphQL is wrapped with `read` and its mutations
  recheck permissions inside the resolver. HTTP headers are `X-API-Key` or
  `Authorization: Bearer ...`, with optional `X-Tenant-ID` and `X-Dataset`;
  `X-Namespace` is not honored on HTTP.
- **Token routing**: JWTs with three segments go to the OIDC validator only
  when an OIDC manager is configured; otherwise they are treated as opaque
  keys and fail RBAC lookup. Opaque keys are resolved by hash, raw value, or
  key ID, then checked for enabled state, TTL, and rotation grace period.

### Layer 2 — Dataset and namespace isolation

`VectorStore.AuthorizeDatasetAccess(ctx, perm, datasetPath)` parses
`namespace/dataset`, checks global or namespace-scoped permission, then applies
the tenant boundary and any dataset allow-list. Namespace ACL and public grants
apply only to the namespace that granted them.

### Layer 3 — Row-Level Security

Non-admin search results pass through `FilterResultsByRLS`, which applies
per-dataset RLS policies and per-key `row_filters`. `EvaluateMetadata` also
enforces implicit `tenant_id` metadata isolation and `expected_from_claim`
rules. The `IsAdmin` bypass applies only to unscoped global admins.

---

## 6. OIDC Federation

OIDC is optional and additive; RBAC remains enforced with or without it:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_OIDC_ENABLED` | `false` | Enables the `OIDCManager` and JWT validation. |
| `LONGBOW_OIDC_ISSUERS` | `""` | JSON array of `OIDCIssuerConfig` objects. Startup parses the JSON but does not validate issuer fields. |

Federated JWTs are validated for JWKS signature, algorithm allow-list
(default `RS256` and `ES256`), `kid`, `iss`/`aud`/`exp`/`nbf`, and `jti` replay
when a `jti` claim is present (1024-entry in-memory cache). Default clock skew
is 60 seconds. Runtime-added issuers require `issuer_url` and
`audience`, HTTPS except for localhost, and reject `HS*`/`none`; they are held
in memory only.

`ExchangeOIDCToken` (Flight) or `POST /api/v1/oidc/token` (REST) issues a
short-lived Longbow session key: default 15 minutes, maximum 1 hour, and
capped at the JWT `exp` when present. Direct JWT validation issues no session
key and does not attach namespace ACL grants.

Claim mapping is configured per `OIDCIssuerConfig`, not with global variables:

| JWT Claim (Configurable) | Default | `TenantContext` Field |
| :--- | :--- | :--- |
| `tenant_claim` | `tenant_id` | `TenantID` |
| `namespace_claim` | `namespace` | `Namespace` |
| `datasets_claim` | `datasets` | `Datasets` (default handles arrays; a configured claim also accepts a string) |
| `role_claim` | `groups` | `Roles` → permissions |
| `row_filter_claim` | `row_filters` | `RowFilters` |
| `admin_claim` / `admin_value` | `role` / empty (built-in provider presets use `admin`) | `IsAdmin`; a `groups` entry of `admin` also sets it |

Missing roles fall back to `reader` with `read` permission. OIDC providers can
be registered at runtime by a principal with global `admin` permission without
restarting; the registry is memory-only.

---

## 7. Verification

Build and start a server with a bootstrap admin, then run the RBAC-only script
against its REST/metrics address:

```bash
go build -o bin/longbow ./cmd/longbow
LONGBOW_RBAC_BOOTSTRAP_KEY='a-long-random-admin-secret' ./bin/longbow

LONGBOW_RBAC_BOOTSTRAP_KEY='a-long-random-admin-secret' \
  python3 scripts/verify_rbac.py --url http://127.0.0.1:9090
```

`--admin-key` can be used instead of `LONGBOW_RBAC_BOOTSTRAP_KEY`. The script
checks only authorization behavior: unauthenticated rejection, admin key
listing, key creation and namespace provisioning, namespace reader/writer
isolation, public namespace access, admin-only namespace creation, and
immediate revocation. It checks HTTP status codes and leaves uniquely named
test keys, namespaces, and datasets behind; run it against a disposable server
or namespace set.

Unit tests:

```bash
go test ./internal/security/secrets/... ./internal/store/rbac/... ./cmd/longbow ./cmd/agentsoak
python3 -m unittest scripts.tests.test_multiagent_config scripts.tests.test_verify_rbac
```

See [`configs/README.md`](../configs/README.md) and
[`docs/development/soaktestplan.md`](development/soaktestplan.md) for the
authenticated multiagent soak scenario.
