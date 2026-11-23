"""Tests for shared soak request payloads, per-op stats, and bench collation."""

import json
import os
import sys
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS = os.path.dirname(_HERE)
sys.path.insert(0, _SCRIPTS)

import agent_soak as soak  # noqa: E402
import agent_soak_bench as bench  # noqa: E402
import agent_soak_scenario as scenario  # noqa: E402
import multiagent_config as mc  # noqa: E402


class TestGroupedTopology(unittest.TestCase):
    def test_eight_agents_two_per_namespace_share_group_namespaces(self):
        cfg = mc.load_config(None)
        topology = soak.GroupedTopology(8, 2, cfg)
        self.assertEqual(topology.groups, 4)
        self.assertEqual(topology.namespaces, ["shared_0", "shared_1", "shared_2", "shared_3", "master"])
        self.assertEqual(topology.writable(2), ["shared_2", "master"])
        self.assertEqual(topology.read_only(2), ["shared_0", "shared_1", "shared_3"])
        keys = {i: {"writer": f"w{i}", "reader": f"r{i}"} for i in range(topology.groups)}
        metrics = scenario.Metrics()
        workload = mc.workload_choices(cfg)
        agents = [soak.Agent(i, cfg, topology, keys, metrics, workload, 2) for i in range(8)]
        self.assertEqual([a.group for a in agents], [0, 0, 1, 1, 2, 2, 3, 3])
        self.assertEqual([a.dataset for a in agents[4:6]], ["shared_2", "shared_2"])


class RecordingClient:
    def __init__(self):
        self.calls = []

    def _record(self, name, **kwargs):
        self.calls.append((name, kwargs))

    def __getattr__(self, name):
        def record(*args, **kwargs):
            self._record(name, args=args, **kwargs)
        return record

    @property
    def last(self):
        return self.calls[-1]


class TestRequestPayloads(unittest.TestCase):
    def setUp(self):
        self.client = RecordingClient()
        self.options = mc.load_config(mc.DEFAULT_CONFIG)["workload_options"]
        self.ctx = {
            "dataset": "private_0",
            "vector": [0.1] * 8,
            "options": self.options,
            "ids": ["doc-1", "doc-2"],
            "new_id": "new-doc",
            "importance": 3,
            "rng": __import__("random").Random(1),
        }

    def run_op(self, op):
        self.assertEqual(scenario.run_op(self.client, op, self.ctx), "ok")
        return self.client.last

    def test_filtered_uses_ge_operator(self):
        name, payload = self.run_op("filtered")
        self.assertEqual(name, "search")
        self.assertEqual(payload["filters"], [{"field": "importance", "operator": ">=", "value": 1}])

    def test_sparse_is_text_only_with_alpha_zero(self):
        _, payload = self.run_op("sparse")
        self.assertNotIn("vector", payload)
        self.assertEqual(payload["alpha"], 0.0)
        self.assertIn("text_query", payload)

    def test_temporal_ops_include_dataset_and_proper_search_type(self):
        expected = {
            "temporal_asof": "as_of",
            "temporal_range": "range",
            "temporal_window": "sliding_window",
        }
        for op, search_type in expected.items():
            name, payload = self.run_op(op)
            self.assertEqual(name, "temporal_search")
            self.assertEqual(payload["dataset"], "private_0")
            self.assertEqual(payload["search_type"], search_type)
            self.assertEqual(payload["k"], 5)
        self.assertIn("timestamp", self.client.calls[-3][1])
        self.assertIn("start_time", self.client.calls[-2][1])
        self.assertIn("window_size", self.client.calls[-1][1])

    def test_geo_ops_include_dataset_and_search_type(self):
        for op, search_type in (("geo_radius", "radius"), ("geo_box", "box"), ("geo_hybrid", "hybrid")):
            name, payload = self.run_op(op)
            self.assertEqual(name, "geo_search")
            self.assertEqual(payload["dataset"], "private_0")
            self.assertEqual(payload["search_type"], search_type)
        radius = self.client.calls[-3][1]
        self.assertEqual(radius["radius_km"], 5)
        box = self.client.calls[-2][1]
        self.assertEqual(set(box["box"]), {"min_lat", "max_lat", "min_lon", "max_lon"})
        hybrid = self.client.calls[-1][1]
        self.assertEqual(hybrid["query_vector"], [0.1] * 8)

    def test_graph_ids_are_numeric(self):
        _, rag = self.run_op("graphrag")
        self.assertEqual(rag["node_ids"], [1])
        self.assertIsInstance(rag["node_ids"][0], int)
        _, traverse = self.run_op("traverse")
        self.assertEqual(traverse["start"], 1)
        self.assertIsInstance(traverse["start"], int)

    def test_federated_uses_real_search_with_configured_tags(self):
        _, payload = self.run_op("federated")
        self.assertEqual(payload["collections"], ["private_0"])
        self.assertEqual(payload["tags"], ["soak"])
        self.assertEqual(payload["rrf_k"], 60)
        self.assertEqual(payload["vector"], [0.1] * 8)

    def test_delete_ns_is_counted_unsupported(self):
        self.assertEqual(scenario.run_op(self.client, "delete_ns", self.ctx), "unsupported")
        self.assertEqual(self.client.calls, [])

    def test_missing_sdk_method_is_unsupported(self):
        class Bare:
            def search(self, **kwargs):
                return None

        ctx = dict(self.ctx, options=self.options)
        self.assertEqual(scenario.run_op(Bare(), "graphrag", ctx), "unsupported")


class TestPerOpStatsAndStalls(unittest.TestCase):
    def test_unsupported_ops_do_not_inflate_error_rate(self):
        metrics = scenario.Metrics()
        metrics.record("dense", 0.01, True)
        metrics.record_unsupported("delete_ns")
        snap = metrics.snapshot()
        self.assertEqual(snap["ops"], 2)
        self.assertEqual(snap["errors"], 0)
        self.assertEqual(snap["error_rate"], 0.0)
        self.assertEqual(snap["by_op_stats"]["delete_ns"]["unsupported"], 1)
        self.assertEqual(snap["by_op_stats"]["dense"]["qps"] > 0, True)
        self.assertIn("p99_ms", snap["by_op_stats"]["dense"])

    def test_csv_keeps_old_columns_and_adds_per_op_and_stall_fields(self):
        metrics = scenario.Metrics()
        metrics.record("dense", 0.01, True)
        metrics.record("delete_ns", 0.0, True, unsupported=True)
        snap = metrics.snapshot()
        columns = scenario.csv_columns(["dense", "delete_ns"])
        self.assertEqual(columns[:9], [
            "timestamp", "elapsed_s", "ops", "errors", "error_rate",
            "qps", "p50_ms", "p95_ms", "p99_ms",
        ])
        for field in ("stalled", "stalls", "baseline_qps", "window_qps"):
            self.assertIn(field, columns)
        self.assertIn("qps_dense", columns)
        self.assertIn("p99_ms_delete_ns", columns)
        row = scenario.csv_row(snap, ["dense", "delete_ns"])
        self.assertEqual(len(row), len(columns))
        self.assertEqual(row[columns.index("unsupported_delete_ns")], 1)

    def test_stall_detector_requires_sustained_drop(self):
        detector = scenario.StallDetector()
        detector.observe(10.0, now=0)
        detector.observe(4.0, now=60)
        self.assertFalse(detector.status(60)["stalled"])
        detector.observe(4.0, now=120)
        self.assertFalse(detector.status(120)["stalled"])
        detector.observe(4.0, now=180)
        self.assertTrue(detector.status(180)["stalled"])
        self.assertEqual(detector.status(180)["stalls"], 1)
        detector.observe(4.0, now=240)
        self.assertEqual(detector.status(240)["stalls"], 1)
        detector.observe(10.0, now=300)
        self.assertFalse(detector.status(300)["stalled"])


class TestBenchReports(unittest.TestCase):
    def sample_run(self, qps=10.0, p99=12.0, stalls=None):
        return {
            "qps": qps, "error_rate": 0.01, "p99_ms": p99,
            "ops": 100, "errors": 1, "cross_verified": 3,
            "by_op_stats": {
                "dense": {"ops": 50, "errors": 0, "qps": 5.0, "p99_ms": 10.0, "error_rate": 0.0},
                "sparse": {"ops": 50, "errors": 1, "qps": 5.0, "p99_ms": 14.0, "error_rate": 0.02},
            },
            "stalls": stalls or [],
        }

    def test_empty_and_malformed_reports_fail_clearly(self):
        for bad in ([], [{"error": "cannot read report"}]):
            with self.assertRaises(ValueError):
                bench.summarize_runs(bad)
        with self.assertRaisesRegex(ValueError, "missing required field"):
            bench.summarize_runs([{"qps": 1}])
        malformed = self.sample_run()
        malformed["qps"] = "fast"
        with self.assertRaisesRegex(ValueError, "must be numeric"):
            bench.summarize_runs([malformed])
        malformed = self.sample_run()
        malformed["by_op_stats"] = ["dense"]
        with self.assertRaisesRegex(ValueError, "by_op_stats"):
            bench.summarize_runs([malformed])

    def test_summary_includes_per_op_mean_min_and_stalls(self):
        first = self.sample_run(qps=10.0, p99=12.0, stalls=[{"op": "dense", "drop_pct": 60}])
        second = self.sample_run(qps=20.0, p99=20.0)
        summary = bench.summarize_runs([first, second], agents=8, duration_s=60)
        self.assertEqual(summary["qps_mean"], 15.0)
        self.assertEqual(summary["qps_min"], 10.0)
        self.assertEqual(summary["stalls_max"], 1)
        self.assertEqual(summary["by_op"]["dense"]["qps_mean"], 5.0)
        self.assertEqual(summary["by_op"]["dense"]["stalls_total"], 1)
        self.assertIn("error_rate_mean", summary["by_op"]["sparse"])
        self.assertIn("p99_ms_min", summary["by_op"]["dense"])

    def test_csv_preserves_original_columns(self):
        runs = [self.sample_run()]
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "summary.csv")
            bench.write_csv(path, runs, ["dense", "sparse"])
            with open(path, encoding="utf-8") as f:
                lines = f.read().strip().splitlines()
        header = lines[0].split(",")
        self.assertEqual(header[:7], [
            "run", "qps", "error_rate", "p99_ms", "ops", "errors", "cross_verified",
        ])
        self.assertIn("qps_dense", header)
        self.assertIn("stalls_sparse", header)
        self.assertEqual(len(lines), 2)
        self.assertEqual(len(lines[1].split(",")), len(header))

    def test_json_output_round_trips(self):
        summary = bench.summarize_runs([self.sample_run()])
        encoded = json.dumps(summary)
        self.assertEqual(json.loads(encoded)["by_op"]["dense"]["qps_mean"], 5.0)


if __name__ == "__main__":
    unittest.main()
