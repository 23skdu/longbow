"""Shared configuration and namespace topology for the multiagent soak tools.

Kept free of SDK imports so the layout can be validated without a running
Longbow server or installed Python client dependencies.

The workload/options/security sections mirror cmd/agentsoak's schema in
scripts/configs/multiagent_soak.json. RBAC has no off switch: there is deliberately no
"unauthenticated" security variant.
"""

import copy
import json
import math
import os

DEFAULT_CONFIG = os.path.normpath(os.path.join(os.path.dirname(__file__), "configs", "multiagent_soak.json"))

# Canonical operation names shared with the Go simulator. Keep in sync with
# cmd/agentsoak/workload.go.
KNOWN_OPS = (
    "dense", "hybrid", "sparse", "filtered", "range", "byid",
    "temporal_asof", "temporal_range", "temporal_window",
    "geo_radius", "geo_box", "geo_hybrid",
    "graphrag", "traverse", "recommend",
    "multimodal", "federated", "text",
    "insert", "upsert", "delete", "delete_ns",
    "pagerank", "communities", "get_vector", "graph_stats",
    "cross_read",
)

DEFAULT_WORKLOAD = {
    "dense": 25, "hybrid": 10, "sparse": 5, "filtered": 5, "range": 3, "byid": 3,
    "temporal_asof": 3, "temporal_range": 3, "temporal_window": 2,
    "geo_radius": 2, "geo_box": 2, "geo_hybrid": 1,
    "graphrag": 2, "traverse": 2, "recommend": 1,
    "multimodal": 2, "federated": 2, "text": 3,
    "insert": 12, "upsert": 5, "delete": 3,
    "delete_ns": 0.5, "pagerank": 0.25, "communities": 0.25,
    "get_vector": 0.5, "graph_stats": 0.5,
}

DEFAULT_WORKLOAD_OPTIONS = {
    "k": 5,
    "alpha": 0.5,
    "rrf_k": 60,
    "federated_tags": ["soak"],
    "temporal_window_size": 50,
    "geo": {"lat": 37.7, "lon": -122.4, "radius_km": 5},
    "graph": {"max_hops": 2, "start_node": 1},
}

# No credentials belong in this structure: keys and tokens come from the
# environment or explicit admin flags at runtime.
DEFAULT_SECURITY = {
    "variant": "plaintext",
    "tls": {
        "enabled": False,
        "cert_file": "",
        "key_file": "",
        "ca_file": "",
        "server_name": "",
        "min_version": "1.2",
    },
    "oidc": {
        "mode": "none",
        "audience": "longbow",
        "ttl_seconds": 900,
        "token_file_env": "AWS_WEB_IDENTITY_TOKEN_FILE",
    },
    "tenant_id": "",
}

SECURITY_VARIANTS = ("plaintext", "tls", "mtls", "oidc", "encrypted", "combined")
OIDC_MODES = ("none", "aws", "google")

BASE_DEFAULTS = {
    "agents": 3,
    "uri": "grpc://127.0.0.1:3000",
    "meta_uri": "grpc://127.0.0.1:3001",
    "duration_hours": 1.0,
    "dim": 128,
    "seed": 42,
    "think_time": 0.01,
    "sample_interval": 60,
    "private_prefix": "private_",
    "shared_prefix": "shared_",
    "common": "master",
    "admin_api_key_env": "LONGBOW_RBAC_BOOTSTRAP_KEY",
    "verbose": False,
    "workload": copy.deepcopy(DEFAULT_WORKLOAD),
    "workload_options": copy.deepcopy(DEFAULT_WORKLOAD_OPTIONS),
    "security": copy.deepcopy(DEFAULT_SECURITY),
}


def _number(value, label, positive=False, minimum=None, maximum=None):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a number")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{label} must be finite")
    if positive and value <= 0:
        raise ValueError(f"{label} must be positive")
    if minimum is not None and value < minimum:
        raise ValueError(f"{label} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{label} must be <= {maximum}")
    return value


def _merge_section(defaults, raw, label):
    """Overlay a JSON object on a defaults object, preserving unknown keys."""
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must be an object")
    out = copy.deepcopy(defaults)
    for key, value in raw.items():
        if key in out and isinstance(out[key], dict) and isinstance(value, dict):
            out[key] = _merge_section(out[key], value, f"{label}.{key}")
        else:
            out[key] = value
    return out


def parse_workload_options(raw=None):
    """Validate and normalize the shared workload_options section."""
    options = _merge_section(DEFAULT_WORKLOAD_OPTIONS, raw or {}, "workload_options")
    options["k"] = int(_number(options["k"], "workload_options.k", positive=True))
    options["alpha"] = _number(options["alpha"], "workload_options.alpha", minimum=0, maximum=1)
    options["rrf_k"] = int(_number(options["rrf_k"], "workload_options.rrf_k", positive=True))
    options["temporal_window_size"] = int(_number(
        options["temporal_window_size"], "workload_options.temporal_window_size", positive=True
    ))
    if not isinstance(options["federated_tags"], list) or not all(
        isinstance(tag, str) for tag in options["federated_tags"]
    ):
        raise ValueError("workload_options.federated_tags must be a list of strings")
    geo = options["geo"]
    for key in ("lat", "lon", "radius_km"):
        geo[key] = _number(geo[key], f"workload_options.geo.{key}")
    if geo["radius_km"] < 0:
        raise ValueError("workload_options.geo.radius_km must be non-negative")
    graph = options["graph"]
    graph["max_hops"] = int(_number(graph["max_hops"], "workload_options.graph.max_hops", positive=True))
    # Graph node ids are numeric on the server; do not allow string ids here.
    graph["start_node"] = int(_number(graph["start_node"], "workload_options.graph.start_node", minimum=0))
    return options


def workload_choices(cfg, validate_names=True):
    """Return sorted ``(name, weight)`` pairs for the configured workload.

    Weights are relative. Zero weights disable an operation; negative, NaN and
    non-numeric weights are rejected. Unknown names are rejected by default so
    the Python and Go simulators stay on the same operation vocabulary.
    """
    raw = cfg.get("workload", DEFAULT_WORKLOAD) if isinstance(cfg, dict) else cfg
    if not isinstance(raw, dict) or not raw:
        raise ValueError("workload must be a non-empty object")
    choices = []
    for name, weight in raw.items():
        if not isinstance(name, str) or not name:
            raise ValueError("workload operation names must be non-empty strings")
        if validate_names and name not in KNOWN_OPS:
            raise ValueError(f"unknown workload operation {name!r}")
        value = _number(weight, f"workload[{name!r}]")
        if value < 0:
            raise ValueError(f"workload weight for {name!r} must be non-negative")
        if value > 0:
            choices.append((name, value))
    if not choices:
        raise ValueError("workload must contain at least one positive weight")
    return sorted(choices)


def security_config(cfg=None):
    """Return a validated copy of the security section (never any secrets)."""
    raw = (cfg or {}).get("security", DEFAULT_SECURITY) if isinstance(cfg, dict) else DEFAULT_SECURITY
    security = _merge_section(DEFAULT_SECURITY, raw or {}, "security")
    if security["variant"] not in SECURITY_VARIANTS:
        raise ValueError(
            f"unknown security variant {security['variant']!r}; RBAC is always enforced"
        )
    oidc = security["oidc"]
    if oidc["mode"] not in OIDC_MODES:
        raise ValueError(f"unknown oidc mode {oidc['mode']!r}")
    oidc["ttl_seconds"] = int(_number(oidc["ttl_seconds"], "security.oidc.ttl_seconds", positive=True))
    tls = security["tls"]
    if not isinstance(tls["enabled"], bool):
        raise ValueError("security.tls.enabled must be a boolean")
    for key in ("cert_file", "key_file", "ca_file", "server_name", "min_version"):
        if not isinstance(tls[key], str):
            raise ValueError(f"security.tls.{key} must be a string")
    for key in ("audience", "token_file_env"):
        if not isinstance(oidc[key], str):
            raise ValueError(f"security.oidc.{key} must be a string")
    if not isinstance(security["tenant_id"], str):
        raise ValueError("security.tenant_id must be a string")
    return security


def load_config(path=DEFAULT_CONFIG):
    """Load the shared JSON config, falling back to built-in defaults."""
    cfg = copy.deepcopy(BASE_DEFAULTS)
    if not path:
        return cfg
    if not os.path.isfile(path):
        raise FileNotFoundError(f"config file not found: {path}")
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise ValueError("config must be a JSON object")
    for key in ("agents", "uri", "meta_uri", "duration_hours", "dim", "seed"):
        if key in raw:
            cfg[key] = raw[key]
    if "think_time_ms" in raw:
        cfg["think_time"] = _number(raw["think_time_ms"], "think_time_ms") / 1000.0
    if "sample_interval_s" in raw:
        cfg["sample_interval"] = int(_number(raw["sample_interval_s"], "sample_interval_s", positive=True))
    ns = raw.get("namespaces", {})
    for key in ("private_prefix", "shared_prefix", "common"):
        if key in ns:
            cfg[key] = ns[key]
    rbac = raw.get("rbac", {})
    if "admin_api_key_env" in rbac:
        cfg["admin_api_key_env"] = rbac["admin_api_key_env"]
    if "workload" in raw:
        cfg["workload"] = raw["workload"]
    if "workload_options" in raw:
        cfg["workload_options"] = raw["workload_options"]
    if "security" in raw:
        cfg["security"] = raw["security"]
    # Normalize/validate the new sections; failures name the offending field.
    cfg["workload_options"] = parse_workload_options(cfg["workload_options"])
    cfg["security"] = security_config(cfg)
    workload_choices(cfg)
    if int(cfg["agents"]) < 2:
        raise ValueError("scenario requires at least 2 agents for cross-agent reads")
    return cfg


class Topology:
    """Derives the 2N+1 namespace layout and per-agent permission sets."""

    def __init__(self, cfg):
        self.agents = int(cfg["agents"])
        if self.agents < 2:
            raise ValueError("scenario requires at least 2 agents")
        self.private = [f"{cfg['private_prefix']}{i}" for i in range(self.agents)]
        self.shared = [f"{cfg['shared_prefix']}{i}" for i in range(self.agents)]
        self.common = cfg["common"]
        self.namespaces = self.private + self.shared + [self.common]
        if len(set(self.namespaces)) != len(self.namespaces):
            raise ValueError("namespace names must be unique (check prefixes and common name)")

    def writable(self, i):
        """Private, shared-owned, and common datasets for agent i."""
        return [self.private[i], self.shared[i], self.common]

    def read_only(self, i):
        """Other agents' shared datasets, readable but not writable by agent i."""
        return self.shared[:i] + self.shared[i + 1:]

    def readable(self, i):
        return self.writable(i) + self.read_only(i)

    def summary(self):
        return {
            "agents": self.agents,
            "namespaces": self.namespaces,
            "per_agent": {
                str(i): {
                    "private": self.private[i],
                    "shared": self.shared[i],
                    "writable": self.writable(i),
                    "read_only": self.read_only(i),
                }
                for i in range(self.agents)
            },
        }
