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
_CONFIG = os.path.join(_SCRIPTS, "configs", "multiagent_soak.json")


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
        self.assertEqual(cfg["agents_per_namespace"], 0)
        self.assertEqual(cfg["private_prefix"], "private_")
        self.assertEqual(cfg["shared_prefix"], "shared_")
        self.assertEqual(cfg["common"], "master")

    def test_shipped_config_matches_defaults(self):
        cfg = mc.load_config(_CONFIG)
        self.assertEqual(cfg["agents"], 3)
        self.assertEqual(cfg["agents_per_namespace"], 0)
        self.assertEqual(cfg["dim"], 128)
        self.assertEqual(cfg["seed"], 42)
        self.assertNotIn("require_rbac", cfg)  # RBAC has no off switch
        self.assertEqual(cfg["think_time"], 0.01)

    def test_config_file_overrides_grouping_default(self):
        with open(_CONFIG, encoding="utf-8") as f:
            raw = json.load(f)
        raw["agents_per_namespace"] = 2
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "soak.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(raw, f)
            self.assertEqual(mc.load_config(path)["agents_per_namespace"], 2)

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
            "rbac": {"admin_api_key_env": "MY_ADMIN_KEY"},
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
            self.assertNotIn("require_rbac", cfg)
            self.assertEqual(len(mc.Topology(cfg).namespaces), 9)
        finally:
            os.unlink(path)


class TestSharedWorkloadSchema(unittest.TestCase):
    def test_built_in_workload_matches_shipped_config(self):
        cfg = mc.load_config(_CONFIG)
        self.assertEqual(cfg["workload"], mc.DEFAULT_WORKLOAD)
        self.assertEqual(cfg["workload"]["dense"], 25)
        self.assertEqual(cfg["workload"]["delete_ns"], 0.5)
        self.assertEqual(mc.load_config(None)["workload"], mc.DEFAULT_WORKLOAD)

    def test_workload_choices_are_sorted_and_validated(self):
        choices = mc.workload_choices(mc.load_config(_CONFIG))
        self.assertEqual([name for name, _ in choices], sorted(name for name, _ in choices))
        self.assertEqual(choices[0], ("byid", 3.0))
        self.assertNotIn("nonesuch", dict(choices))

    def test_workload_zero_disables_op_and_negative_is_rejected(self):
        choices = mc.workload_choices({"workload": {"dense": 2, "hybrid": 0}})
        self.assertEqual(choices, [("dense", 2.0)])
        for bad in ({"dense": -1}, {"dense": 0}, {}, {"bogus": 1}):
            with self.assertRaises(ValueError):
                mc.workload_choices({"workload": bad})
        with self.assertRaises(ValueError):
            mc.workload_choices({"workload": {"dense": float("nan")}})

    def test_workload_options_defaults_and_overrides(self):
        cfg = mc.load_config(_CONFIG)
        opts = cfg["workload_options"]
        self.assertEqual(opts["k"], 5)
        self.assertEqual(opts["alpha"], 0.5)
        self.assertEqual(opts["rrf_k"], 60)
        self.assertEqual(opts["federated_tags"], ["soak"])
        self.assertEqual(opts["temporal_window_size"], 50)
        self.assertEqual(opts["geo"], {"lat": 37.7, "lon": -122.4, "radius_km": 5})
        self.assertEqual(opts["graph"], {"max_hops": 2, "start_node": 1})
        raw = {"workload_options": {"k": 9, "geo": {"lat": 1.0}}}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(raw, f)
            path = f.name
        try:
            merged = mc.load_config(path)["workload_options"]
            self.assertEqual(merged["k"], 9)
            self.assertEqual(merged["geo"], {"lat": 1.0, "lon": -122.4, "radius_km": 5})
        finally:
            os.unlink(path)

    def test_invalid_workload_options_are_rejected(self):
        for bad in ({"k": 0}, {"alpha": 1.5}, {"graph": {"start_node": "one"}},
                    {"federated_tags": "soak"}):
            with self.assertRaises(ValueError):
                mc.parse_workload_options(bad)


class TestSharedSecuritySchema(unittest.TestCase):
    def test_security_defaults_have_no_credentials_or_off_switch(self):
        security = mc.security_config(mc.load_config(_CONFIG))
        self.assertEqual(security["variant"], "plaintext")
        self.assertFalse(security["tls"]["enabled"])
        self.assertEqual(security["tls"]["min_version"], "1.2")
        self.assertEqual(security["oidc"]["mode"], "none")
        self.assertEqual(security["oidc"]["ttl_seconds"], 900)
        self.assertEqual(security["tenant_id"], "")
        self.assertNotIn("api_key", json.dumps(security))
        self.assertEqual(set(security["oidc"]), {"mode", "audience", "ttl_seconds", "token_file_env"})
        self.assertEqual(set(security), {"variant", "tls", "oidc", "tenant_id"})

    def test_security_overrides_are_merged_and_validated(self):
        raw = {"security": {"variant": "mtls", "tls": {"enabled": True, "ca_file": "/tmp/ca.pem"},
                            "oidc": {"mode": "aws", "ttl_seconds": 60}, "tenant_id": "acme"}}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(raw, f)
            path = f.name
        try:
            cfg = mc.load_config(path)
        finally:
            os.unlink(path)
        security = mc.security_config(cfg)
        self.assertEqual(security["variant"], "mtls")
        self.assertTrue(security["tls"]["enabled"])
        self.assertEqual(security["tls"]["ca_file"], "/tmp/ca.pem")
        self.assertEqual(security["tls"]["key_file"], "")
        self.assertEqual(security["oidc"]["mode"], "aws")
        self.assertEqual(security["oidc"]["audience"], "longbow")
        self.assertEqual(security["tenant_id"], "acme")
        for bad in ({"variant": "none"}, {"oidc": {"mode": "off"}}, {"oidc": {"ttl_seconds": 0}}):
            with self.assertRaises(ValueError):
                mc.security_config({"security": bad})


if __name__ == "__main__":
    unittest.main()
