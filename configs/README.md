# Multiagent soak configuration

[`multiagent_soak.json`](multiagent_soak.json) is the shared configuration for
both multiagent simulators:

- `scripts/agent_soak_scenario.py --config configs/multiagent_soak.json`
- `bin/agentsoak -config configs/multiagent_soak.json`

Command-line flags override any value loaded from the file.

## Schema

| Field | Meaning |
| --- | --- |
| `agents` | Number of simulated agents. |
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
| `rbac.require` | Fail startup unless RBAC provisioning succeeds. |

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

The simulators require that key and always verify the permission boundaries at
startup; there is no no-RBAC mode.
