# RBAC, Dataset Access Control & OIDC Federation

Longbow enforces multi-tenant Role-Based Access Control (RBAC), Row-Level Security (RLS), and OpenID Connect (OIDC) workload identity federation across **Arrow Flight gRPC** (Data Plane `:3000` and Control Plane `:3001`), **HTTP REST v1** (`/api/v1/*`), and **GraphQL** (`/api/v1/graphql`).

When neither `LONGBOW_RBAC_ENABLED` nor `LONGBOW_OIDC_ENABLED` is enabled, authentication is disabled and all requests pass through unrestricted.

---

## 1. RBAC Roles & Permissions

Defined in `internal/store/rbac.go` and aligned with OIDC claim mapping in `internal/security/oidc.go`:

| Role | Aliases | Granted Permissions | Dataset & System Capabilities |
| :--- | :--- | :--- | :--- |
| **`admin`** | — | `read`, `write`, `delete`, `ingest`, `admin`, `backup`, `manage-keys`, `manage-tenant` | Full cluster access across all namespaces/datasets (unless explicitly pinned to a namespace), OIDC provider management, audit log access, backups, and key/tenant administration |
| **`tenant-admin`** | — | `read`, `write`, `delete`, `ingest`, `backup`, `manage-keys`, `manage-tenant` | Full read/write/delete/ingest, key lifecycle, backup, and RLS/quota management strictly scoped to its assigned tenant/namespace; cannot access other tenants or cluster-wide `admin` endpoints |
| **`writer`** | `read-write` | `read`, `write`, `delete`, `ingest` | Search, read, bulk `DoPut` ingest, REST/GraphQL upserts, graph edge mutations, and vector/dataset deletions within allowed namespace & dataset allow-list |
| **`ingest-only`** | — | `read`, `ingest` | Read/search and stream new vectors via Arrow Flight `DoPut` / `DoExchange`; **cannot** mutate existing vectors, delete records/datasets, or perform admin operations |
| **`reader`** | `read-only` | `read` | Search (all 21 modes), `DoGet`, `GetFlightInfo`, `GetSchema`, `ListFlights`, and read-only REST/GraphQL queries; all ingest/write/delete/admin requests are denied |

---

## 2. How Dataset Read & Write Access Is Enforced

Every authenticated request passes through three enforcement layers:

### Layer 1: Transport Middleware (Authentication & Coarse Permission)

- **Arrow Flight gRPC** (`internal/middleware/auth_interceptor.go`, attached to both Data `:3000` and Meta `:3001` via `VectorStore.DynamicAccessValidator()`):
  - Extracts credentials from `authorization: Bearer <token>` or `x-api-key: <token>`, plus optional `x-tenant-id` / `x-namespace` and `x-dataset` headers.
  - Coarse permission mapping:
    - `DoGet`, `GetFlightInfo`, `GetSchema`, `ListFlights`, `ListActions` → requires **`read`**
    - `DoPut`, `DoExchange` → requires **`ingest`**
    - `DoAction` → verifies identity in middleware, then delegates fine-grained permission checks to the action handler (`read`, `write`, `delete`, `manage-keys`, `manage-tenant`, `admin`)
- **HTTP REST v1 & GraphQL** (`internal/store/rest_graphql.go`):
  - `httpAuthWrap` / `httpMethodAuthWrap` resolves `VectorStore.AccessValidator()` dynamically on every request.
  - `GET`/`HEAD` endpoints require **`read`**; mutating REST/GraphQL operations require **`write`**, **`manage-tenant`**, or **`admin`**.
  - `/api/v1/oidc/token` is unauthenticated at the middleware layer and validates the supplied OIDC JWT directly to issue a short-lived session key.
- **Composite Token Routing**:
  - **3-segment JWTs** (`header.payload.signature`) are validated by `OIDCAccessValidator` (JWKS signature, `iss`, `aud`, `exp`, `nbf`, `jti` replay protection).
  - **Opaque Longbow API keys** are validated by `RBACManager` (SHA-256 hash lookup, enabled flag, TTL expiry, and key-rotation grace window).

### Layer 2: Dataset & Namespace Isolation (`AuthorizeDatasetAccess`)

Every dataset read (`DoGet`, vector/hybrid/range/federated/geo/temporal/text/multimodal search, `GetVector`, `ListFlights`), ingest (`DoPut`), and mutation (`POST`/`DELETE` vectors, GraphQL mutations, `delete`, `delete-dataset`) calls `VectorStore.AuthorizeDatasetAccess(ctx, perm, datasetPath)`:

1. **Path Parsing**: Splits `namespace/dataset` (e.g. `acme/docs` → namespace `acme`, dataset `docs`).
2. **Permission Check**: Verifies `tc.HasPermission(perm)`:
   - Reads/searches require `read` (`PermRead`).
   - Flight `DoPut` / `DoExchange` ingestion requires `ingest` (`PermIngest`).
   - Vector upserts, dataset creation, federated collection registration, and graph/schema writes require `write` (`PermWrite`).
   - Deletions require `delete` (`PermDelete`) or `write` depending on endpoint.
   - RLS policy & namespace quota management require `manage-tenant` (`PermManageTenant`).
   - API key CRUD requires `manage-keys` (`PermManageKeys`).
   - OIDC provider CRUD & audit log reads require `admin` (`PermAdmin`).
3. **Tenant / Namespace Boundary**: If the credential specifies a `Namespace` other than `""` or `"*"`, any request targeting another namespace is rejected with `PermissionDenied` (`ErrTenantIsolation`).
4. **Dataset Allow-List**: If `Datasets` is non-empty, only explicitly listed datasets (matching `dataset`, `namespace/dataset`, or wildcard `*`) are allowed.

### Layer 3: Row-Level Security (`FilterResultsByRLS`)

After dataset-level `read` authorization succeeds, non-admin search results pass through `FilterResultsByRLS` (`internal/store/rbac_actions.go`), which evaluates:

- Per-dataset **RLS policies** registered via `SetRLSPolicy` (`eq`, `neq`, `in`, `contains`).
- Per-API-key or per-OIDC-JWT **`row_filters`** (e.g. `{"department": "engineering"}`).

Any candidate whose metadata does not satisfy the active row filters is stripped before returning results. Global `admin` identities bypass RLS filtering.

---

## 3. Enabling RBAC & OIDC Federation on the Server

### Environment Variables (`cmd/longbow/main.go`)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `LONGBOW_RBAC_ENABLED` | `false` | Enables `RBACManager` and mints a `bootstrap-admin` API key on startup |
| `LONGBOW_RBAC_BOOTSTRAP_KEY` | `""` | Registers the supplied secret as the `bootstrap-admin` key (at least 16 chars) so callers can provision scoped keys without reading the raw key from logs |
| `LONGBOW_OIDC_ENABLED` | `false` | Enables `OIDCManager` and dynamic OIDC/RBAC gRPC & HTTP authentication |
| `LONGBOW_OIDC_ISSUERS` | `""` | JSON array of `OIDCIssuerConfig` objects loaded at startup |

The multiagent soak simulators use `LONGBOW_RBAC_BOOTSTRAP_KEY` to provision
scoped per-agent keys; see [`configs/README.md`](../configs/README.md) and
[`docs/development/soaktestplan.md`](development/soaktestplan.md) §9.

```bash
export LONGBOW_RBAC_ENABLED=true
export LONGBOW_OIDC_ENABLED=true
export LONGBOW_OIDC_ISSUERS='[
  {
    "provider": "google",
    "issuer_url": "https://accounts.google.com",
    "jwks_url": "https://www.googleapis.com/oauth2/v3/certs",
    "audience": "longbow",
    "allowed_algs": ["RS256", "ES256"],
    "tenant_claim": "tenant_id",
    "namespace_claim": "namespace",
    "datasets_claim": "datasets",
    "role_claim": "groups",
    "row_filter_claim": "row_filters",
    "admin_claim": "role",
    "admin_value": "admin"
  }
]'
./bin/longbow
```

### Dynamic Provider Registration & Token Exchange

Because gRPC servers use `VectorStore.DynamicAccessValidator()`, both of the following work without restarting the server:

- **OIDC-only startup (`LONGBOW_OIDC_ENABLED=true`, `LONGBOW_RBAC_ENABLED=false`)**: Calling `ExchangeOIDCToken` (Flight) or `POST /api/v1/oidc/token` (REST) lazily initializes `RBACManager` and mints a short-lived session API key (default 15m, max 1h, capped at JWT expiry) that is immediately accepted by gRPC and HTTP endpoints.
- **RBAC-only startup (`LONGBOW_RBAC_ENABLED=true`, `LONGBOW_OIDC_ENABLED=false`)**: An `admin` calling `AddOIDCProvider` (Flight) or `POST /api/v1/oidc/providers` (REST) immediately enables federated JWT authentication on both gRPC and HTTP.

### OIDC JWT Claim Mapping

| JWT Claim (Configurable) | Default Claim Name | Mapped `TenantContext` Field |
| :--- | :--- | :--- |
| `tenant_claim` | `tenant_id` | `TenantID` |
| `namespace_claim` | `namespace` | `Namespace` |
| `datasets_claim` | `datasets` | `Datasets` (string or string array) |
| `role_claim` | `groups` | `Roles` → expanded to `Permissions` (`admin`, `tenant-admin`, `writer`/`read-write`, `ingest-only`, `reader`/`read-only`; defaults to `reader` if absent) |
| `row_filter_claim` | `row_filters` | `RowFilters` (JSON object of `field -> value`) |
| `admin_claim` / `admin_value` | `role` / `""` | Sets `IsAdmin = true` when claim equals `admin_value` |

All validated OIDC JWTs enforce JWKS signature verification, algorithm allow-list (rejecting `alg: none` and HMAC/RSA confusion), `iss`/`aud`/`exp` checks, and `jti` replay protection.

---

## 4. API Keys, Rotation & Audit Log

- **Storage**: Only the SHA-256 hash of each API key is retained in memory (`KeyHash`); raw secrets are returned once upon creation/rotation.
- **Zero-Downtime Rotation**: `RotateAPIKey` issues a new key while keeping the old key valid for `grace_period_seconds` (default `3600s`).
- **Immediate Revocation**: `RevokeAPIKey` disables a key by secret or `KeyID` immediately.
- **Audit Logging**: All auth successes, auth failures, key lifecycle events, RLS updates, and OIDC provider changes are recorded to `AuditLogger` (`GetAuditLog` / `longbow-cli audit-log`).

---

## 5. Authentication & Authorization Error Handling

| Failure Class | gRPC Code | HTTP Status | Python SDK Exception | Common Causes & Remediation |
| :--- | :--- | :--- | :--- | :--- |
| **Authentication failure** | `Unauthenticated` | `401 Unauthorized` | `LongbowAuthenticationError` | Missing `Authorization`/`X-API-Key`, invalid/revoked/expired API key, rotation grace expired, invalid/expired/replayed OIDC JWT. Verify `api_key` / `LONGBOW_API_KEY` or exchange a fresh OIDC JWT. |
| **Authorization failure** | `PermissionDenied` | `403 Forbidden` | `LongbowAuthorizationError` (`LongbowPermissionDeniedError`) | Role lacks required permission (e.g., `reader` or `ingest-only` calling write/delete), cross-namespace `ErrTenantIsolation`, or dataset outside `datasets` allow-list. Check assigned role/OIDC groups, namespace, and dataset scope. |

See [CLI Reference](cli.md) (§5) for `longbow-cli` RBAC/OIDC commands and [Python SDK README](../longbowclientsdk/README.md) for Python client usage.
