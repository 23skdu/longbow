# Multiagent soak configuration

[`multiagent_soak.json`](multiagent_soak.json) is the shared configuration for
both multiagent simulators:

- `scripts/agent_soak_scenario.py --config scripts/configs/multiagent_soak.json`
- `bin/agentsoak -config scripts/configs/multiagent_soak.json`

Command-line flags override any value loaded from the file.

## Schema

| Field | Meaning |
| --- | --- |
| `agents` | Number of simulated agents. |
| `agents_per_namespace` | Agents sharing one namespace. `0` (default) keeps the private-per-agent `2N+1` layout; values above `1` enable group namespaces and must be smaller than `agents`. |
| `uri` / `meta_uri` | Arrow Flight data/control plane addresses. |
| `duration_hours` | Run duration. |
| `dim` | Vector dimension. |
| `seed` | Random seed. |
| `think_time_ms` | Delay between agent operations. |
| `sample_interval_s` | Metrics sampling interval (Python simulator only). |
| `namespaces.private_prefix` | Prefix for each agent's private dataset. |
| `namespaces.shared_prefix` | Prefix for each agent's shared-read dataset. |
| `namespaces.common` | Dataset writable by every agent. |
| `rbac.admin_api_key_env` | Environment variable holding the bootstrap admin key. |
| `workload` | Operation name → **relative** weight for the full soak mix (see below). |
| `workload_options.k` / `alpha` / `rrf_k` | Default result count, hybrid blend, and federated RRF constant. |
| `workload_options.federated_tags` | Routing tags passed to `federated_search`. |
| `workload_options.temporal_window_size` | Sliding-window size for `temporal_window`. |
| `workload_options.geo` | Center (`lat`/`lon`) and `radius_km` for radius/hybrid geo searches. |
| `workload_options.graph` | Numeric `start_node` and `max_hops` for graph operations. |
| `security.variant` | Security posture under test (`plaintext`, `tls`, `mtls`, `oidc`, `encrypted`, `combined`). RBAC is enforced for every variant. |
| `security.tls` | TLS/mTLS material (`enabled`, certificate/key/CA paths, `server_name`, `min_version`). No verification bypass is supported. |
| `security.oidc` | OIDC workload identity (`mode`: `none`/`aws`/`google`, `audience`, `ttl_seconds`, `token_file_env`). |
| `security.tenant_id` | Optional tenant id attached to client requests and scoped API keys. |

## Workload mix

`workload` weights are **relative**, not percentages: an operation with weight
10 runs about twice as often as one with weight 5. A zero weight disables an
operation; the sum of positive weights must be greater than zero. The shipped
mix covers dense, hybrid, sparse, filtered, range, by-id, temporal (as-of,
range, sliding window), geo (radius, box, hybrid), GraphRAG/traverse/recommend,
multimodal, federated, text, inserts/upserts/deletes, graph analytics and
vector reads. Both `scripts/agent_soak.py`,
`scripts/agent_soak_scenario.py` and `bin/agentsoak` read the same section, so
their operation names and weights stay comparable.

`delete_ns` is present in the mix but is deliberately executed as a counted
`unsupported` no-op: deleting a live soak namespace would cascade errors into
every other operation. No-op counts are reported separately from client errors.
Operations the Python SDK does not implement are also counted as `unsupported`
rather than being mislabeled or inflating the error rate.

## Default namespace layout

With the default `agents: 3`, the simulators create **7 namespaces**:

| Namespace | Owner | Owner access | Other agents |
| --- | --- | --- | --- |
| `private_0`, `private_1`, `private_2` | agent `i` | read / write / delete | no access (not even visible) |
| `shared_0`, `shared_1`, `shared_2` | agent `i` | read / write / delete | read-only |
| `master` | all agents | read / write / delete | read / write / delete |

Permissions are enforced by the server's RBAC manager, not by the simulator.
The admin client provisions the namespaces and issues per-agent API keys:

- a `writer` key scoped to `private_i`, `shared_i`, and `common`;
- a `reader` key scoped to every other agent's `shared_*` namespace.

Changing `agents` to `N` produces `2N + 1` namespaces. Changing the prefixes or
the common namespace name changes the generated dataset names; the permission
tiers stay the same.

RBAC is always enforced. Start the server with a known bootstrap admin key so
the simulators can provision scoped keys (the secrets provider defaults to a
durable local file store):

```bash
LONGBOW_RBAC_BOOTSTRAP_KEY="local-soak-admin-key-0123456789" \
./bin/longbow
```

For fixed namespaces, pre-provision ACLs with
`LONGBOW_STARTUP_NAMESPACE_POLICIES` (or `--startup-namespace-policies`); the
simulators still issue their scoped API keys. The JSON supports durable ACLs
and `ephemeral: true` runtime-only namespaces. See [`docs/rbac.md`](../docs/rbac.md)
for the schema.

The simulators require that key and always verify the permission boundaries at
startup; there is no no-RBAC mode. Neither the config nor the reports contain
raw API keys or OIDC tokens.

For the bounded variant, run `scripts/agent_soak.py --agents 8 --agents-per-ns 2`:
the eight agents share four group namespaces plus `master`, with one scoped key
set per group. Reports include per-operation QPS/latency/error counts and stall
flags (a >50% QPS drop sustained for two minutes).
