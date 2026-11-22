"""Tests for the shared multiagent soak configuration and namespace topology.

These guard the key invariant of the customizable simulation: for N agents the
layout is exactly 2N+1 namespaces, with private namespaces visible only to
their owner, shared namespaces readable by everyone but writable only by their
owner, and one common namespace writable by all.
"""

import importlib.util
import json
import os
import sys
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS = os.path.dirname(_HERE)
_MODULE = os.path.join(_SCRIPTS, "multiagent_config.py")
_CONFIG = os.path.join(os.path.dirname(_SCRIPTS), "configs", "multiagent_soak.json")


def _load():
    spec = importlib.util.spec_from_file_location("multiagent_config", _MODULE)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {_MODULE}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["multiagent_config"] = module
    spec.loader.exec_module(module)
    return module


mc = _load()


class TestDefaultConfig(unittest.TestCase):
    def test_built_in_defaults_are_three_agents(self):
        cfg = mc.load_config(None)
        self.assertEqual(cfg["agents"], 3)
        self.assertEqual(cfg["private_prefix"], "private_")
        self.assertEqual(cfg["shared_prefix"], "shared_")
        self.assertEqual(cfg["common"], "master")

    def test_shipped_config_matches_defaults(self):
        cfg = mc.load_config(_CONFIG)
        self.assertEqual(cfg["agents"], 3)
        self.assertEqual(cfg["dim"], 128)
        self.assertEqual(cfg["seed"], 42)
        self.assertTrue(cfg["require_rbac"])
        self.assertEqual(cfg["think_time"], 0.01)

    def test_missing_config_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            mc.load_config("/no/such/multiagent_config.json")


class TestTopologyLayout(unittest.TestCase):
    def test_default_layout_is_seven_namespaces(self):
        t = mc.Topology(mc.load_config(_CONFIG))
        self.assertEqual(t.agents, 3)
        self.assertEqual(
            t.namespaces,
            [
                "private_0", "private_1", "private_2",
                "shared_0", "shared_1", "shared_2",
                "master",
            ],
        )
        self.assertEqual(len(t.namespaces), 7)

    def test_namespace_count_is_two_n_plus_one_for_any_agent_count(self):
        for n in (2, 3, 5, 10, 50):
            t = mc.Topology({"agents": n, "private_prefix": "p_", "shared_prefix": "s_", "common": "c"})
            self.assertEqual(len(t.namespaces), 2 * n + 1)

    def test_permission_matrix(self):
        t = mc.Topology(mc.load_config(_CONFIG))
        # Private: owner-only writable and readable.
        self.assertEqual(t.writable(0), ["private_0", "shared_0", "master"])
        self.assertNotIn("private_1", t.readable(0))
        # Shared: owner writes; every other agent reads but cannot write.
        self.assertEqual(t.read_only(0), ["shared_1", "shared_2"])
        self.assertIn("shared_1", t.readable(1))
        self.assertNotIn("shared_1", t.writable(0))
        # Common: writable and readable by every agent.
        for i in range(3):
            self.assertIn("master", t.writable(i))
            self.assertIn("master", t.readable(i))

    def test_private_namespaces_are_never_in_any_other_agents_sets(self):
        t = mc.Topology({"agents": 4, "private_prefix": "p_", "shared_prefix": "s_", "common": "c"})
        for i in range(4):
            for j in range(4):
                if i != j:
                    self.assertNotIn(t.private[i], t.writable(j))
                    self.assertNotIn(t.private[i], t.read_only(j))

    def test_custom_prefixes_and_common_name(self):
        t = mc.Topology({"agents": 2, "private_prefix": "agent-", "shared_prefix": "team-", "common": "global"})
        self.assertEqual(t.namespaces, ["agent-0", "agent-1", "team-0", "team-1", "global"])

    def test_duplicate_names_rejected(self):
        with self.assertRaises(ValueError):
            mc.Topology({"agents": 2, "private_prefix": "ns_", "shared_prefix": "ns_", "common": "ns_0"})

    def test_requires_at_least_two_agents(self):
        with self.assertRaises(ValueError):
            mc.Topology({"agents": 1, "private_prefix": "p_", "shared_prefix": "s_", "common": "c"})

    def test_summary_shape(self):
        t = mc.Topology(mc.load_config(_CONFIG))
        s = t.summary()
        self.assertEqual(s["agents"], 3)
        self.assertEqual(len(s["namespaces"]), 7)
        self.assertEqual(set(s["per_agent"]), {"0", "1", "2"})
        self.assertEqual(s["per_agent"]["1"]["private"], "private_1")
        self.assertEqual(s["per_agent"]["1"]["read_only"], ["shared_0", "shared_2"])


class TestConfigOverrides(unittest.TestCase):
    def test_json_overrides_are_applied(self):
        raw = {
            "agents": 4,
            "think_time_ms": 25,
            "sample_interval_s": 15,
            "namespaces": {"private_prefix": "own_", "shared_prefix": "pool_", "common": "all"},
            "rbac": {"admin_api_key_env": "MY_ADMIN_KEY", "require": False},
        }
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(raw, f)
            path = f.name
        try:
            cfg = mc.load_config(path)
            self.assertEqual(cfg["agents"], 4)
            self.assertEqual(cfg["think_time"], 0.025)
            self.assertEqual(cfg["sample_interval"], 15)
            self.assertEqual(cfg["private_prefix"], "own_")
            self.assertEqual(cfg["shared_prefix"], "pool_")
            self.assertEqual(cfg["common"], "all")
            self.assertEqual(cfg["admin_api_key_env"], "MY_ADMIN_KEY")
            self.assertFalse(cfg["require_rbac"])
            self.assertEqual(len(mc.Topology(cfg).namespaces), 9)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
