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


def _row(qps, p50, mode="dense", dim=128, count=10_000, dtype="float32", mean=None, workers=4):
    """Build a report row. `mean` defaults to the value that satisfies the
    QPS x mean ~= workers identity for `workers`, so a row that is meant to be
    self-consistent only needs its QPS and a worker count."""
    if mean is None:
        mean = (workers * 1000.0 / qps) if qps else 0.0
    return {
        "dim": dim,
        "count": count,
        "dtype": dtype,
        "search": {mode: {"qps": qps, "mean": mean, "p50": p50, "p95": p50 * 2, "p99": p50 * 3}},
    }


def _flat_row(qps, p50, workers=4, mean=None):
    if mean is None:
        mean = (workers * 1000.0 / qps) if qps else 0.0
    return {"dim": 128, "count": 10_000, "operation": "exchange_search", "qps": qps, "mean": mean, "p50": p50}


class TestConcurrencyInvariant(unittest.TestCase):
    def test_accepts_row_consistent_with_worker_count(self):
        # A self-consistent row: qps x mean == workers x 1000 exactly, so the
        # implied worker count equals the recorded one.
        self.assertEqual(ub.validate_concurrency_invariant([_row(800, 1.25)], 4), [])

    def test_rejects_row_implying_wrong_worker_count(self):
        # 2665.1 QPS with mean 2.522ms implies 6.72 workers, not 4.
        violations = ub.validate_concurrency_invariant([_row(2665.1, 2.522, mean=2.522)], 4)
        self.assertEqual(len(violations), 1)
        self.assertIn("recorded 4", violations[0])
        self.assertIn("6.72", violations[0])

    def test_same_numbers_accepted_at_the_worker_count_they_imply(self):
        # The point of the check: the identical numbers are fine against the
        # worker count they actually imply and rejected against a different one,
        # so it is the provenance that is wrong, not the run. These imply exactly
        # 8 workers: 2000 QPS x 4.0ms = 8.
        row = _row(2000, 1.0, mean=4.0)
        self.assertEqual(ub.validate_concurrency_invariant([row], 8), [])
        self.assertEqual(len(ub.validate_concurrency_invariant([row], 4)), 1)

    def test_skewed_p50_alone_does_not_trip_the_check(self):
        # Regression: the check used to be qps x p50 <= workers x 1000, which
        # fires on any right-skewed distribution. sparse at 20k recorded
        # 5424.9 QPS / P50 0.778ms (product 4221 > 4000) yet p99/p50 was 1.26 and
        # the run was sound. With the mean-based identity the same run is clean.
        row = {
            "dim": 128, "count": 20_000, "dtype": "float32",
            "search": {"sparse": {"qps": 5424.9, "mean": 4 * 1000.0 / 5424.9,
                                  "p50": 0.778, "p95": 0.867, "p99": 0.981}},
        }
        self.assertEqual(ub.validate_concurrency_invariant([row], 4), [])

    def test_partially_busy_workers_pass(self):
        # QPS x mean = sum(latencies)/duration equals W only if the workers were
        # busy the whole window; start-up and the last in-flight query leave them
        # briefly idle. Measured across 13 modes, implied ran 3.66-3.96 on 4
        # workers, so anything from ~75% to ~100% busy is a sound run.
        for implied_workers in (3.7, 3.9, 4.0):
            qps = 4000.0
            mean = implied_workers * 1000.0 / qps
            row = _row(qps, 1.0, mean=mean)
            self.assertEqual(ub.validate_concurrency_invariant([row], 4), [],
                             f"implied {implied_workers} workers should be accepted")

    def test_implausibly_idle_workers_are_flagged(self):
        # Implied far below the worker count means the numbers did not come from a
        # busy pool, i.e. wrong provenance or a truncated run.
        qps = 4000.0
        mean = 2.0 * 1000.0 / qps  # implies 2 of 4 workers -> 50%
        row = _row(qps, 1.0, mean=mean)
        violations = ub.validate_concurrency_invariant([row], 4)
        self.assertEqual(len(violations), 1)
        self.assertIn("50%", violations[0])

    def test_more_workers_in_flight_than_recorded_is_flagged(self):
        # At most W requests can be in flight, so implied above W is impossible.
        qps = 4000.0
        mean = 6.0 * 1000.0 / qps  # implies 6 of 4 workers
        row = _row(qps, 1.0, mean=mean)
        violations = ub.validate_concurrency_invariant([row], 4)
        self.assertEqual(len(violations), 1)
        self.assertIn("exceeds the recorded", violations[0])

    def test_flags_each_offending_mode_separately(self):
        row = {
            "dim": 128, "count": 10_000, "dtype": "float32",
            "search": {
                "dense": {"qps": 5000, "mean": 2 * 1000.0 / 5000, "p50": 5.0},   # implies 2 workers: OK
                "sparse": {"qps": 100, "mean": 1.0, "p50": 1.0},                 # implies 0.1 workers: bad
            },
        }
        violations = ub.validate_concurrency_invariant([row], 2)
        self.assertEqual(len(violations), 1)
        self.assertIn("mode=sparse", violations[0])

    def test_handles_flat_top_level_rows(self):
        self.assertEqual(len(ub.validate_concurrency_invariant([_flat_row(1000, 1.0, workers=4)], 4)), 0)
        self.assertEqual(len(ub.validate_concurrency_invariant([_flat_row(1000, 1.0, workers=8)], 4)), 1)

    def test_missing_worker_count_is_itself_a_violation(self):
        row = {"dim": 128, "count": 10_000, "dtype": "float32",
               "search": {"dense": {"qps": 100, "mean": 40.0, "p50": 1.0}}}
        violations = ub.validate_concurrency_invariant([row], None)
        self.assertEqual(len(violations), 1)
        self.assertIn("no worker count recorded", violations[0])
        self.assertEqual(len(ub.validate_concurrency_invariant([row], 0)), 1)

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
            seed = 42
            shuffle_modes = True

        prov = ub.collect_provenance(Args())
        self.assertEqual(prov["workers"], 8)
        self.assertEqual(prov["queries"], 500)
        self.assertEqual(prov["cpu_affinity"], "12-15")
        self.assertEqual(prov["runs"], 3)
        self.assertEqual(prov["seed"], 42)
        self.assertEqual(prov["shuffle_modes"], True)
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
