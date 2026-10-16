"""Unit tests for check_index_regression.py (roadmap Item 32).

Covers:
- Self-noise tests (0% delta against canonical baseline slices -> exit 0)
- Permitted drift (< threshold -> exit 0)
- True index-time regressions (>= threshold -> exit 1)
- True search QPS regressions (<= -threshold -> exit 1)
- Setup errors (missing files, no shared configs, empty verdict -> exit 2)
- Verdict file evaluation for both 'index' and 'qps' modes
"""

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(os.path.dirname(_HERE))
_SCRIPT = os.path.join(os.path.dirname(_HERE), "check_index_regression.py")


def _run_script(*args):
    """Run check_index_regression.py in a subprocess with arguments."""
    cmd = [sys.executable, _SCRIPT] + list(args)
    res = subprocess.run(cmd, cwd=_REPO, capture_output=True, text=True)
    return res.returncode, res.stdout, res.stderr


def _create_bench_json(path, configs):
    """Write synthetic benchmark JSON file with given configs.
    
    configs is a list of tuples: (engine, scale, dtype, index_time, dense_qps)
    """
    results = []
    for engine, scale, dtype, index_time, dense_qps in configs:
        results.append({
            "mode": engine,
            "dtype": dtype,
            "count": scale,
            "dim": 128,
            "ingest": {
                "indexing_duration_seconds": index_time,
                "duration_seconds": index_time,
            },
            "peak_memory_mb": 100.0,
            "search": {
                "dense": {
                    "qps": dense_qps,
                }
            }
        })
    doc = {
        "runs": [
            {
                "mode": configs[0][0] if configs else "cpu",
                "results": results,
            }
        ]
    }
    with open(path, "w") as f:
        json.dump(doc, f)


def _create_verdict_json(path, rows):
    """Write synthetic ab_verdict.json with given rows."""
    doc = {
        "threshold_pct": 5.0,
        "reps": 6,
        "rows": rows,
    }
    with open(path, "w") as f:
        json.dump(doc, f)


class TestCheckIndexRegression(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_canonical_baseline_self_noise_zero_false_positive(self):
        """Self-noise test on actual committed baseline slices exits 0 with 0% delta."""
        baseline_path = os.path.join(_REPO, "benchmarks", "baseline_25k.json")
        self.assertTrue(os.path.exists(baseline_path), f"missing {baseline_path}")

        rc, out, _ = _run_script("--before", baseline_path, "--after", baseline_path)
        self.assertEqual(rc, 0)
        self.assertIn("NO REGRESSION", out)
        self.assertIn("offending: 0", out)

    def test_permitted_drift_within_threshold(self):
        """A +3% change in index time when threshold is 5% exits 0."""
        p1 = os.path.join(self.temp_dir.name, "before.json")
        p2 = os.path.join(self.temp_dir.name, "after.json")
        _create_bench_json(p1, [("cpu", 25000, "float32", 10.0, 500.0)])
        _create_bench_json(p2, [("cpu", 25000, "float32", 10.3, 500.0)])  # +3%

        rc, out, _ = _run_script("--before", p1, "--after", p2, "--threshold", "5.0")
        self.assertEqual(rc, 0)
        self.assertIn("NO REGRESSION", out)

    def test_index_time_regression_fails(self):
        """A +10% increase in index time when threshold is 5% exits 1."""
        p1 = os.path.join(self.temp_dir.name, "before.json")
        p2 = os.path.join(self.temp_dir.name, "after.json")
        _create_bench_json(p1, [("cpu", 25000, "float32", 10.0, 500.0)])
        _create_bench_json(p2, [("cpu", 25000, "float32", 11.0, 500.0)])  # +10%

        rc, out, _ = _run_script("--before", p1, "--after", p2, "--threshold", "5.0")
        self.assertEqual(rc, 1)
        self.assertIn("REGRESSION DETECTED", out)
        self.assertIn("offending: 1", out)

    def test_qps_regression_fails(self):
        """A -10% drop in QPS when mode=qps and threshold is 5% exits 1."""
        p1 = os.path.join(self.temp_dir.name, "before.json")
        p2 = os.path.join(self.temp_dir.name, "after.json")
        _create_bench_json(p1, [("cpu", 25000, "float32", 10.0, 500.0)])
        _create_bench_json(p2, [("cpu", 25000, "float32", 10.0, 450.0)])  # -10% QPS

        rc, out, _ = _run_script("--before", p1, "--after", p2, "--threshold", "5.0", "--mode", "qps")
        self.assertEqual(rc, 1)
        self.assertIn("REGRESSION DETECTED", out)
        self.assertIn("offending: 1", out)

    def test_setup_error_missing_args(self):
        """Calling without required arguments exits 2."""
        rc, out, _ = _run_script()
        self.assertEqual(rc, 2)
        self.assertIn("setup error", out)

    def test_setup_error_nonexistent_file(self):
        """Calling with a non-existent file exits 2."""
        rc, out, _ = _run_script("--before", "nonexistent_a.json", "--after", "nonexistent_b.json")
        self.assertEqual(rc, 2)
        self.assertIn("setup error", out)

    def test_setup_error_no_common_configs(self):
        """Calling with two files with disjoint configs exits 2."""
        p1 = os.path.join(self.temp_dir.name, "c1.json")
        p2 = os.path.join(self.temp_dir.name, "c2.json")
        _create_bench_json(p1, [("cpu", 25000, "float32", 10.0, 500.0)])
        _create_bench_json(p2, [("cuda", 100000, "int8", 10.0, 500.0)])

        rc, out, _ = _run_script("--before", p1, "--after", p2)
        self.assertEqual(rc, 2)
        self.assertIn("no shared (engine, scale, dtype) configurations", out)

    def test_verdict_clean_passes(self):
        """Verdict file with ok index_time passes with exit 0."""
        vpath = os.path.join(self.temp_dir.name, "ab_verdict.json")
        _create_verdict_json(vpath, [
            {
                "dtype": "float32",
                "count": 50000,
                "mode": "index_time",
                "median_pct": 1.2,
                "min_pct": 0.5,
                "max_pct": 2.1,
                "pairs": 6,
                "verdict": "ok",
            }
        ])

        rc, out, _ = _run_script("--verdict", vpath, "--threshold", "5.0", "--mode", "index")
        self.assertEqual(rc, 0)
        self.assertIn("NO REGRESSION", out)

    def test_verdict_regression_fails(self):
        """Verdict file with index_time regression exits 1."""
        vpath = os.path.join(self.temp_dir.name, "ab_verdict.json")
        _create_verdict_json(vpath, [
            {
                "dtype": "float32",
                "count": 50000,
                "mode": "index_time",
                "median_pct": 7.5,
                "min_pct": 6.0,
                "max_pct": 9.0,
                "pairs": 6,
                "verdict": "regression",
            }
        ])

        rc, out, _ = _run_script("--verdict", vpath, "--threshold", "5.0", "--mode", "index")
        self.assertEqual(rc, 1)
        self.assertIn("REGRESSION DETECTED", out)
        self.assertIn("offending: 1", out)

    def test_verdict_empty_rows_exits_2(self):
        """Verdict file with no rows exits 2."""
        vpath = os.path.join(self.temp_dir.name, "ab_verdict.json")
        _create_verdict_json(vpath, [])

        rc, out, _ = _run_script("--verdict", vpath)
        self.assertEqual(rc, 2)
        self.assertIn("setup error", out)

    def test_verdict_no_matching_mode_rows_exits_2(self):
        """Verdict file with only search rows when mode=index exits 2."""
        vpath = os.path.join(self.temp_dir.name, "ab_verdict.json")
        _create_verdict_json(vpath, [
            {
                "dtype": "float32",
                "count": 50000,
                "mode": "dense",
                "median_pct": 1.0,
                "pairs": 6,
                "verdict": "ok",
            }
        ])

        rc, out, _ = _run_script("--verdict", vpath, "--mode", "index")
        self.assertEqual(rc, 2)
        self.assertIn("setup error", out)


if __name__ == "__main__":
    unittest.main()
