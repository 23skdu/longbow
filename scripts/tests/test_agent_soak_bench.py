"""Deterministic unit tests for the agentsoak run collation script.

The import pattern mirrors scripts/tests/test_agent_soak_reports.py so pytest
and ``python -m unittest`` both resolve the sibling scripts without installing
the repository as a package.
"""

import csv
import json
import os
import sys
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS = os.path.dirname(_HERE)
sys.path.insert(0, _SCRIPTS)

import agent_soak_bench as bench  # noqa: E402


def sample_run(qps=10.0, error_rate=0.01, p99=12.0, ops=100, errors=1,
               stalls=None, by_op=None):
    """Return a report shaped like cmd/agentsoak's JSON output."""
    run = {
        "qps": qps,
        "error_rate": error_rate,
        "p99_ms": p99,
        "ops": ops,
        "errors": errors,
        "cross_verified": 3,
        "by_op_stats": by_op if by_op is not None else {
            "dense": {"ops": 50, "errors": 0, "error_rate": 0.0,
                      "qps": 5.0, "p99_ms": 10.0},
            "sparse": {"ops": 50, "errors": 1, "error_rate": 0.02,
                       "qps": 5.0, "p99_ms": 14.0},
        },
    }
    if stalls is not None:
        run["stalls"] = stalls
    return run


class TestRunOutput(unittest.TestCase):
    def test_run_once_wraps_non_object_json_as_error_report(self):
        for payload in ("[1, 2]", '"just a string"', "42"):
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as d:
                path = os.path.join(d, "report.json")
                with open(path, "w", encoding="utf-8") as f:
                    f.write(payload)
                report = bench.run_once(
                    sys.executable, ["-c", "pass"], out_path=path, timeout=10,
                )
                self.assertIn("must be an object", report["error"])
                self.assertIn("_elapsed", report)
                self.assertEqual(report["_returncode"], 0)
                with self.assertRaisesRegex(ValueError, "error report"):
                    bench.validate_run(report, 1)

    def test_run_once_wraps_unreadable_json_as_error_report(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "report.json")
            with open(path, "w", encoding="utf-8") as f:
                f.write("not-json{")
            report = bench.run_once(
                sys.executable, ["-c", "pass"], out_path=path, timeout=10,
            )
            self.assertIn("cannot read JSON report", report["error"])
            with self.assertRaisesRegex(ValueError, "error report"):
                bench.summarize_runs([report])

    def test_validate_run_rejects_non_dict_and_error_reports(self):
        for bad in ([], "report", 7):
            with self.subTest(bad=bad):
                with self.assertRaisesRegex(ValueError, "must be a JSON object"):
                    bench.validate_run(bad, 1)
        with self.assertRaisesRegex(ValueError, "error report: cannot read"):
            bench.validate_run({"error": "cannot read report"}, 2)

    def test_validate_run_requires_every_gate_field(self):
        for field in bench.REQUIRED_FIELDS:
            with self.subTest(field=field):
                run = sample_run()
                del run[field]
                with self.assertRaisesRegex(ValueError, "missing required field"):
                    bench.validate_run(run, 1)

    def test_validate_run_rejects_invalid_numeric_fields(self):
        bad_values = ("fast", True, float("nan"), float("inf"), float("-inf"))
        for field in bench.REQUIRED_FIELDS:
            for value in bad_values:
                with self.subTest(field=field, value=value):
                    run = sample_run()
                    run[field] = value
                    with self.assertRaises(ValueError):
                        bench.validate_run(run, 1)

        for key in ("error_rate", "qps", "p99_ms", "stalls"):
            for value in bad_values:
                with self.subTest(key=key, value=value):
                    run = sample_run(by_op={"dense": {"ops": 1, key: value}})
                    with self.assertRaises(ValueError):
                        bench.validate_run(run, 1)

    def test_validate_run_accepts_plain_by_op_counts_and_missing_op_fields(self):
        run = sample_run()
        run.pop("by_op_stats")
        run["by_op"] = {"dense": 3}
        normalized = bench.validate_run(run, 1)
        self.assertEqual(normalized["by_op_stats"]["dense"]["ops"], 3.0)
        self.assertEqual(normalized["by_op_stats"]["dense"]["error_rate"], 0.0)

        run = sample_run()
        run["by_op_stats"] = None
        self.assertEqual(bench.validate_run(run, 1)["by_op_stats"], {})

    def test_validate_run_rejects_invalid_by_op_shapes(self):
        for by_op in (["dense"], {"dense": ["not", "stats"]}, {"dense": "3"}):
            with self.subTest(by_op=by_op):
                run = sample_run(by_op=by_op)
                with self.assertRaisesRegex(ValueError, "by_op_stats"):
                    bench.validate_run(run, 1)


class TestStallFormats(unittest.TestCase):
    def test_list_dict_and_integer_stalls_aggregate_together(self):
        runs = [
            sample_run(qps=10.0, stalls=[
                {"op": "dense"}, {"op": "dense"}, {"op": "federated"},
            ]),
            sample_run(qps=20.0, stalls=2),
            sample_run(qps=30.0, stalls={"dense": 1, "federated": 3}),
        ]
        summary = bench.summarize_runs(runs, agents=2, duration_s=60)
        self.assertEqual(summary["stalls_mean"], 3.0)
        self.assertEqual(summary["stalls_max"], 4)
        self.assertEqual(summary["by_op"]["dense"]["stalls_total"], 3)
        self.assertEqual(summary["by_op"]["federated"]["stalls_total"], 4)
        self.assertEqual(set(summary["by_op"]), {"dense", "sparse", "federated"})

    def test_duplicate_list_entries_count_per_occurrence(self):
        run = sample_run(stalls=[{"op": "dense"}] * 3)
        self.assertEqual(bench.validate_run(run, 1)["stall_counts"], {"dense": 3})

    def test_zero_and_missing_stalls_do_not_inflate_totals(self):
        self.assertEqual(bench.validate_run(sample_run(), 1)["stall_counts"], {})
        summary = bench.summarize_runs([sample_run(stalls=0)])
        self.assertEqual(summary["stalls_max"], 0)
        self.assertEqual(summary["by_op"]["dense"]["stalls_total"], 0)

    def test_invalid_stall_entries_fail_with_value_error(self):
        bad_entries = [
            [{"drop_pct": 50}],
            [{"op": ""}],
            [{"op": ["dense"]}],
            [{"op": 7}],
            [3],
        ]
        for stalls in bad_entries:
            with self.subTest(stalls=stalls):
                with self.assertRaises(ValueError):
                    bench.validate_run(sample_run(stalls=stalls), 1)

    def test_invalid_stall_scalars_and_dict_values_fail(self):
        for stalls in ("2", True, float("nan"), {"dense": "many"}):
            with self.subTest(stalls=stalls):
                with self.assertRaises(ValueError):
                    bench.validate_run(sample_run(stalls=stalls), 1)


class TestAggregationAndCSV(unittest.TestCase):
    def test_happy_path_summary_and_csv(self):
        first = sample_run(
            qps=10.0, error_rate=0.0, p99=10.0, ops=100, errors=0,
            stalls=[{"op": "dense"}],
            by_op={
                "dense": {"ops": 60, "errors": 0, "error_rate": 0.0,
                          "qps": 6.0, "p99_ms": 8.0},
                "federated": {"ops": 40, "errors": 0, "error_rate": 0.0,
                              "qps": 4.0, "p99_ms": 12.0},
            },
        )
        second = sample_run(
            qps=20.0, error_rate=0.02, p99=30.0, ops=200, errors=4,
            stalls={"dense": 1, "federated": 2},
            by_op={
                "dense": {"ops": 120, "errors": 2, "error_rate": 0.01,
                          "qps": 12.0, "p99_ms": 20.0},
                "federated": {"ops": 80, "errors": 2, "error_rate": 0.02,
                              "qps": 8.0, "p99_ms": 40.0},
            },
        )
        runs = [first, second]
        summary = bench.summarize_runs(runs, agents=4, duration_s=90)

        self.assertEqual(summary["runs"], 2)
        self.assertEqual((summary["agents"], summary["duration_s"]), (4, 90))
        self.assertEqual((summary["qps_mean"], summary["qps_min"], summary["qps_max"]),
                         (15.0, 10.0, 20.0))
        self.assertEqual((summary["error_rate_mean"], summary["error_rate_max"]),
                         (0.01, 0.02))
        self.assertEqual((summary["p99_ms_mean"], summary["p99_ms_max"]), (20.0, 30.0))
        self.assertEqual((summary["stalls_mean"], summary["stalls_max"]), (2.0, 3))
        self.assertEqual(len(summary["all_runs"]), 2)
        self.assertIs(summary["all_runs"][0], first)
        self.assertIs(summary["all_runs"][1], second)

        dense = summary["by_op"]["dense"]
        self.assertEqual(dense["ops_mean"], 90.0)
        self.assertEqual(dense["errors_mean"], 1.0)
        self.assertEqual(dense["qps_mean"], 9.0)
        self.assertEqual((dense["qps_min"], dense["qps_max"]), (6.0, 12.0))
        self.assertEqual(dense["error_rate_mean"], 0.005)
        self.assertEqual((dense["p99_ms_min"], dense["p99_ms_max"]), (8.0, 20.0))
        self.assertEqual(dense["stalls_total"], 2)
        self.assertEqual(summary["by_op"]["federated"]["stalls_total"], 2)

        op_names = sorted(summary["by_op"])
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "summary.csv")
            bench.write_csv(path, runs, op_names)
            with open(path, newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), 2)
        self.assertEqual(list(rows[0]), bench.csv_columns(op_names))
        self.assertEqual((rows[0]["run"], rows[1]["run"]), ("1", "2"))
        self.assertEqual(rows[0]["qps"], "10.00")
        self.assertEqual(rows[0]["error_rate"], "0.0000")
        self.assertEqual(rows[0]["p99_ms"], "10.00")
        self.assertEqual((rows[0]["ops"], rows[0]["errors"]), ("100", "0"))
        self.assertEqual(rows[0]["cross_verified"], "3")
        self.assertEqual(rows[0]["stalls"], "1")
        self.assertEqual(rows[1]["stalls"], "3")
        self.assertEqual(rows[0]["qps_dense"], "6.000")
        self.assertEqual(rows[0]["stalls_dense"], "1")
        self.assertEqual(rows[1]["stalls_federated"], "2")

        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "summary.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(summary, f)
            with open(path, encoding="utf-8") as f:
                reloaded = json.load(f)
        self.assertEqual(reloaded["by_op"]["dense"]["qps_mean"], 9.0)

    def test_empty_result_sets_raise_and_csv_writes_header_only(self):
        for empty in ([], None, ""):
            with self.subTest(empty=empty):
                with self.assertRaisesRegex(ValueError, "no runs to summarize"):
                    bench.summarize_runs(empty)
        self.assertEqual(bench.csv_columns([]), [
            "run", "qps", "error_rate", "p99_ms", "ops", "errors",
            "cross_verified", "stalls",
        ])
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "empty.csv")
            bench.write_csv(path, [], [])
            with open(path, encoding="utf-8") as f:
                self.assertEqual(f.read().strip(), ",".join(bench.csv_columns([])))

    def test_non_dict_reports_are_rejected_by_aggregation_and_csv(self):
        for bad in ([[1, 2]], ["report"]):
            with self.subTest(bad=bad):
                with self.assertRaisesRegex(ValueError, "must be a JSON object"):
                    bench.summarize_runs(bad)
                with tempfile.TemporaryDirectory() as d:
                    path = os.path.join(d, "bad.csv")
                    with self.assertRaisesRegex(ValueError, "must be a JSON object"):
                        bench.write_csv(path, bad, [])


if __name__ == "__main__":
    unittest.main()
