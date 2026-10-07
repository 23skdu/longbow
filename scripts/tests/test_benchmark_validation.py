"""Tests for the benchmark provenance and self-validation helpers (roadmap R2/R3).

The invariant matters because a benchmark report is only usable as a regression
gate if its numbers are true of the parameters recorded alongside them. A row
claiming more in-flight requests than the worker count allows cannot be, so it
is refused at generation time instead of silently gating every later comparison.
"""

import importlib.util
import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT = os.path.join(os.path.dirname(_HERE), "unified_benchmark.py")


def _load():
    spec = importlib.util.spec_from_file_location("unified_benchmark", _SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["unified_benchmark"] = module
    spec.loader.exec_module(module)
    return module


ub = _load()


def _row(qps, p50, mode="dense", dim=128, count=10_000, dtype="float32"):
    return {
        "dim": dim,
        "count": count,
        "dtype": dtype,
        "search": {mode: {"qps": qps, "p50": p50, "p95": p50 * 2, "p99": p50 * 3}},
    }


def _flat_row(qps, p50):
    return {"dim": 128, "count": 10_000, "operation": "exchange_search", "qps": qps, "p50": p50}


class TestConcurrencyInvariant(unittest.TestCase):
    def test_accepts_row_consistent_with_worker_count(self):
        # 800 QPS x 1.25 ms = 1000, which is within 4 workers x 1000.
        self.assertEqual(ub.validate_concurrency_invariant([_row(800, 1.25)], 4), [])

    def test_rejects_row_exceeding_worker_cap(self):
        violations = ub.validate_concurrency_invariant([_row(2665.1, 2.522)], 4)
        self.assertEqual(len(violations), 1)
        self.assertIn("workers 4", violations[0])
        self.assertIn("6721", violations[0])

    def test_same_row_accepted_at_higher_worker_count(self):
        # The point of the check: the identical numbers are fine with 8 workers
        # and rejected with 4, so it is the provenance that is wrong, not the run.
        row = _row(2665.1, 2.522)
        self.assertEqual(ub.validate_concurrency_invariant([row], 8), [])
        self.assertEqual(len(ub.validate_concurrency_invariant([row], 4)), 1)

    def test_flags_each_offending_mode_separately(self):
        row = {
            "dim": 128,
            "count": 10_000,
            "dtype": "float32",
            "search": {
                "dense": {"qps": 5000, "p50": 5.0},
                "sparse": {"qps": 100, "p50": 1.0},
            },
        }
        violations = ub.validate_concurrency_invariant([row], 2)
        self.assertEqual(len(violations), 1)
        self.assertIn("mode=dense", violations[0])

    def test_handles_flat_top_level_rows(self):
        self.assertEqual(len(ub.validate_concurrency_invariant([_flat_row(9000, 1.0)], 4)), 1)
        self.assertEqual(ub.validate_concurrency_invariant([_flat_row(100, 1.0)], 4), [])

    def test_missing_worker_count_is_itself_a_violation(self):
        violations = ub.validate_concurrency_invariant([_row(100, 1.0)], None)
        self.assertEqual(len(violations), 1)
        self.assertIn("no worker count recorded", violations[0])
        self.assertEqual(len(ub.validate_concurrency_invariant([_row(100, 1.0)], 0)), 1)

    def test_ignores_zero_and_missing_values(self):
        for row in (
            _row(0, 1.0),
            _row(100, 0),
            {"dim": 128, "count": 10, "search": {}},
            {"dim": 128, "count": 10},
            "not-a-dict",
        ):
            self.assertEqual(ub.validate_concurrency_invariant([row], 4), [], row)

    def test_empty_results_with_valid_workers_is_clean(self):
        self.assertEqual(ub.validate_concurrency_invariant([], 4), [])
        self.assertEqual(ub.validate_concurrency_invariant(None, 4), [])


class TestProvenance(unittest.TestCase):
    def test_records_worker_count_and_affinity(self):
        class Args:
            workers = 8
            queries = 500
            cpu_affinity = "12-15"
            search_modes = "dense"
            runs = 3
            duration = 15
            numa_bind = False
            mode = "vec"
            bench_tool = None

        prov = ub.collect_provenance(Args())
        self.assertEqual(prov["workers"], 8)
        self.assertEqual(prov["queries"], 500)
        self.assertEqual(prov["cpu_affinity"], "12-15")
        self.assertEqual(prov["runs"], 3)
        self.assertIn("revision", prov)
        self.assertIn("python_version", prov)

    def test_env_capture_is_limited_to_known_keys(self):
        os.environ["LONGBOW_MAX_MEMORY"] = "18GB"
        os.environ["LONGBOW_TEST_ONLY_SHOULD_NOT_APPEAR"] = "x"
        try:
            class Args:
                workers = 4
                queries = 100
                cpu_affinity = None
                search_modes = "all"
                runs = 1
                duration = 5
                numa_bind = False
                mode = "vec"
                bench_tool = None

            env = ub.collect_provenance(Args()).get("env", {})
            self.assertEqual(env.get("LONGBOW_MAX_MEMORY"), "18GB")
            self.assertNotIn("LONGBOW_TEST_ONLY_SHOULD_NOT_APPEAR", env)
        finally:
            os.environ.pop("LONGBOW_TEST_ONLY_SHOULD_NOT_APPEAR", None)
            os.environ.pop("LONGBOW_MAX_MEMORY", None)

    def test_provenance_block_is_json_serialisable(self):
        import json

        class Args:
            workers = 4
            queries = 100
            cpu_affinity = None
            search_modes = "dense"
            runs = 1
            duration = 5
            numa_bind = False
            mode = "vec"
            bench_tool = None

        json.dumps(ub.collect_provenance(Args()))


class TestGitRevision(unittest.TestCase):
    def test_returns_revision_or_none_without_raising(self):
        rev, dirty = ub.git_revision(os.path.dirname(_HERE))
        # Running outside a repository must not raise.
        self.assertTrue(rev is None or isinstance(rev, str))
        self.assertTrue(dirty is None or isinstance(dirty, bool))

    def test_binary_provenance_tolerates_missing_file(self):
        self.assertIsNone(ub.binary_provenance(None))
        info = ub.binary_provenance("/definitely/not/here")
        self.assertEqual(info["path"], "/definitely/not/here")
        self.assertIsNone(info["size"])


if __name__ == "__main__":
    unittest.main()
