"""Shared configuration and namespace topology for the multiagent soak tools.

Kept free of SDK imports so the layout can be validated without a running
Longbow server or installed Python client dependencies.
"""

import copy
import json
import os

DEFAULT_CONFIG = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "configs", "multiagent_soak.json"))

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
}


def load_config(path=DEFAULT_CONFIG):
    """Load the shared JSON config, falling back to built-in defaults."""
    cfg = copy.deepcopy(BASE_DEFAULTS)
    if not path:
        return cfg
    if not os.path.isfile(path):
        raise FileNotFoundError(f"config file not found: {path}")
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    for key in ("agents", "uri", "meta_uri", "duration_hours", "dim", "seed"):
        if key in raw:
            cfg[key] = raw[key]
    if "think_time_ms" in raw:
        cfg["think_time"] = raw["think_time_ms"] / 1000.0
    if "sample_interval_s" in raw:
        cfg["sample_interval"] = raw["sample_interval_s"]
    ns = raw.get("namespaces", {})
    for key in ("private_prefix", "shared_prefix", "common"):
        if key in ns:
            cfg[key] = ns[key]
    rbac = raw.get("rbac", {})
    if "admin_api_key_env" in rbac:
        cfg["admin_api_key_env"] = rbac["admin_api_key_env"]
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
