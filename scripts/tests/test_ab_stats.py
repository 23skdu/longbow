"""Tests for the interleaved A/B statistics (roadmap R37).

The whole point of interleaving is that the comparison is *paired*: two
measurements taken in the same time window, so the systematic component of
variation - which cores are free, what else is on the host, what the memory
ceiling resolved to - cancels in the ratio rather than having to be averaged
away. R35/R36 showed averaging over separate runs does not work.

These tests use synthetic ratios, so they test the decision rule rather than the
machine. That is deliberate: the failure mode being guarded against is the
gate declaring a verdict on noise, and that is a property of the rule.
"""

import importlib.util
import json
import os
import sys
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT = os.path.join(os.path.dirname(_HERE), "ab_benchmark.py")


def _load():
    spec = importlib.util.spec_from_file_location("ab_benchmark_stats", _SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["ab_benchmark_stats"] = module
    spec.loader.exec_module(module)
    return module


ab = _load()


def ratios(*pcts):
    """Ratios from percentage changes, e.g. ratios(-10, -12) -> 0.90, 0.88."""
    return [1.0 + p / 100.0 for p in pcts]


class TestPairedRatios(unittest.TestCase):
    def test_ratio_is_candidate_over_baseline(self):
        self.assertEqual(ab.paired_ratios([100.0], [50.0]), [0.5])

    def test_identical_measurements_give_unity(self):
        self.assertEqual(ab.paired_ratios([10.0, 20.0], [10.0, 20.0]), [1.0, 1.0])

    def test_mismatched_lengths_are_rejected_not_silently_truncated(self):
        # Truncating would pair the wrong measurements and quietly report a
        # confident verdict on unrelated data.
        with self.assertRaises(ValueError):
            ab.paired_ratios([1.0, 2.0], [1.0])

    def test_zero_and_missing_measurements_are_dropped(self):
        # A zero QPS is a failed run, not a very slow one. Folding it in would
        # produce a ratio of zero, which reads as an infinite regression.
        self.assertEqual(ab.paired_ratios([0.0, 10.0], [5.0, 5.0]), [0.5])

    def test_negative_is_impossible_but_not_a_crash(self):
        self.assertEqual(ab.paired_ratios([-1.0, 10.0], [5.0, 5.0]), [0.5])


class TestSignTest(unittest.TestCase):
    def test_all_agreeing_is_strong_evidence(self):
        # 10 of 10 in one direction: p = 2^-10.
        self.assertAlmostEqual(ab.one_sided_sign_pvalue(10, 10, "down"), 1 / 1024)

    def test_half_and_half_is_no_evidence(self):
        self.assertAlmostEqual(ab.one_sided_sign_pvalue(5, 10, "down"), 0.623046875)

    def test_boundaries(self):
        self.assertEqual(ab.one_sided_sign_pvalue(0, 10, "down"), 1.0)
        self.assertEqual(ab.one_sided_sign_pvalue(10, 10, "down"), 1 / 1024)
        self.assertEqual(ab.one_sided_sign_pvalue(0, 0, "down"), 1.0)

    def test_out_of_range_successes_are_clamped(self):
        self.assertEqual(ab.one_sided_sign_pvalue(99, 10, "down"),
                         ab.one_sided_sign_pvalue(10, 10, "down"))


class TestSummarizeVerdicts(unittest.TestCase):
    def test_consistent_large_regression_is_a_regression(self):
        s = ab.summarize(ratios(-10, -12, -11, -13, -9, -11), 5.0)
        self.assertEqual(s["verdict"], "regression")
        self.assertEqual(s["pairs"], 6)
        self.assertAlmostEqual(s["median_pct"], -11.0, places=1)

    def test_consistent_improvement_is_an_improvement(self):
        s = ab.summarize(ratios(10, 12, 11, 13, 9, 11), 5.0)
        self.assertEqual(s["verdict"], "improvement")

    def test_same_median_without_consistency_is_inconclusive(self):
        # The crux of the whole harness. Six pairs straddling zero in both
        # directions are noise, and must not be reported as a regression even
        # though half the pairs moved a long way.
        s = ab.summarize(ratios(-30, -20, -10, 10, 20, 30), 5.0)
        self.assertEqual(s["verdict"], "inconclusive")
        self.assertEqual(s["median_pct"], 0.0)
        self.assertIn("inside", s["reason"])

    def test_consistent_but_below_threshold_is_inconclusive(self):
        # -2% every pair is a real, consistent effect, but under a 5% threshold
        # it is not a regression worth gating on.
        s = ab.summarize(ratios(-2, -2, -2, -2), 5.0)
        self.assertEqual(s["verdict"], "inconclusive")

    def test_sub_threshold_reason_does_not_claim_inconsistency(self):
        """Caught on the first real hardware run, not in the synthetic suite.

        A null A/B pinned to four cores returned 100% of pairs agreeing at a
        -3.4% median. The verdict was right (inconclusive) but the reason read
        "only 100% of pairs agree; direction is not consistent", which contradicts
        itself. The synthetic tests asserted the verdict and never the message.
        """
        s = ab.summarize(ratios(-3.4, -2.0, -1.0, -5.0), 5.0)
        self.assertEqual(s["verdict"], "inconclusive")
        self.assertIn("sub-threshold", s["reason"])
        self.assertNotIn("not consistent", s["reason"])

    def test_message_never_contradicts_the_measured_agreement(self):
        """No branch may say "not consistent" when every pair agrees."""
        cases = [
            (ratios(-3, -3, -3), 5.0),
            (ratios(-30, -30, -30), 5.0),
            (ratios(30, 30, 30), 5.0),
            (ratios(-0.5, -0.5, -0.5, -0.5), 5.0),
            (ratios(50, -50, 50, -50), 5.0),
        ]
        for r, t in cases:
            s = ab.summarize(r, t)
            agree = max(s["pairs_down"], s["pairs_up"]) / s["pairs"]
            if agree == 1.0:
                self.assertNotIn(
                    "not consistent", s["reason"],
                    f"reason contradicts 100% agreement: {s['reason']}",
                )

    def test_consistent_at_exactly_threshold_is_a_regression(self):
        s = ab.summarize(ratios(-5, -5, -5, -5), 5.0)
        self.assertEqual(s["verdict"], "regression")

    def test_too_few_pairs_never_produces_a_verdict(self):
        for n in (1, 2):
            s = ab.summarize(ratios(*([-99.0] * n)), 5.0)
            self.assertEqual(s["verdict"], "inconclusive")
            self.assertIn("need 3", s["reason"])

    def test_three_agreeing_pairs_are_enough(self):
        s = ab.summarize(ratios(-10, -11, -12), 5.0)
        self.assertEqual(s["verdict"], "regression")

    def test_two_agreeing_one_dissenting_is_not_consistent_enough(self):
        # 2/3 = 0.667, below the 0.9 bar, so no verdict despite the median.
        s = ab.summarize(ratios(-30, -30, 5), 5.0)
        self.assertEqual(s["verdict"], "inconclusive")

    def test_empty_input_is_inconclusive_not_a_crash(self):
        s = ab.summarize([], 5.0)
        self.assertEqual(s["verdict"], "inconclusive")
        self.assertEqual(s["pairs"], 0)

    def test_spread_is_reported(self):
        s = ab.summarize(ratios(-5, 0, 5), 5.0)
        self.assertAlmostEqual(s["min_pct"], -5.0, places=6)
        self.assertAlmostEqual(s["max_pct"], 5.0, places=6)
        self.assertAlmostEqual(s["median_pct"], 0.0, places=6)

    def test_verdict_never_triggers_on_symmetric_noise(self):
        """The property that matters most, over many synthetic noise draws."""
        import random

        rng = random.Random(20260926)
        false_positives = 0
        trials = 400
        for _ in range(trials):
            # Same binary both arms, so the true ratio is exactly 1.0. Inject
            # log-normal multiplicative noise of the magnitude R35 measured.
            noise = [rng.lognormvariate(0.0, 0.30) for _ in range(6)]
            base = [100.0 * rng.uniform(0.5, 1.5) for _ in range(6)]
            cand = [b * n for b, n in zip(base, noise)]
            s = ab.summarize(ab.paired_ratios(base, cand), 5.0)
            if s["verdict"] == "regression":
                false_positives += 1
        rate = false_positives / trials
        self.assertLess(
            rate, 0.05,
            f"regression declared on {rate:.1%} of null cases; the gate is not sound",
        )


class TestBuildOrder(unittest.TestCase):
    def test_balanced_alternation(self):
        order = ab.build_order(4)
        self.assertEqual(
            order,
            ["baseline", "candidate", "candidate", "baseline",
             "baseline", "candidate", "candidate", "baseline"],
        )

    def test_each_arm_appears_once_per_rep(self):
        order = ab.build_order(7)
        self.assertEqual(order.count("baseline"), 7)
        self.assertEqual(order.count("candidate"), 7)

    def test_order_starts_baseline(self):
        self.assertEqual(ab.build_order(1), ["baseline", "candidate"])

    def test_order_is_not_plain_abab(self):
        # ABAB would bias every rep the same way; the runner alternates the
        # order each rep so within-rep drift hits both arms equally.
        order = ab.build_order(2)
        self.assertNotEqual(order, ["baseline", "candidate",
                                    "baseline", "candidate"])


class TestLoadQps(unittest.TestCase):
    """Regression tests for a bug that made the gate report a clean pass.

    `load_qps` iterates its argument, and `main()` passed a single path string.
    Iterating a string yields characters, `os.path.exists('d')` is false for every
    one, and the function returned an empty mapping. The report table was then
    empty and the process exited 0 - a passing verdict carrying no measurements.
    """

    def _write(self, tmp, name, qps=4563.5):
        path = os.path.join(tmp, name)
        with open(path, "w") as f:
            json.dump(
                {"results": [{"dtype": "float32", "count": 20000,
                              "search": {"dense": {"qps": qps}}}]},
                f,
            )
        return path

    def test_bare_string_path_is_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, "r.json")
            self.assertEqual(
                ab.load_qps(path),
                {("float32", 20000, "dense"): 4563.5},
            )

    def test_list_of_paths_is_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, "r.json")
            self.assertEqual(ab.load_qps([path]), {("float32", 20000, "dense"): 4563.5})

    def test_missing_file_is_skipped_not_fatal(self):
        self.assertEqual(ab.load_qps("/nonexistent/nope.json"), {})

    def test_malformed_file_raises_instead_of_being_swallowed(self):
        # Previously this was caught and turned into an empty mapping, so a
        # corrupt result file looked identical to a successful empty run.
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "bad.json")
            with open(path, "w") as f:
                f.write("{not json")
            with self.assertRaises(json.JSONDecodeError):
                ab.load_qps(path)

    def test_zero_qps_is_dropped(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, "r.json", qps=0)
            self.assertEqual(ab.load_qps(path), {})


class TestEmptyReportIsNotAPass(unittest.TestCase):
    def test_no_rows_returns_setup_error_not_success(self):
        """An empty report must never exit 0.

        This is the property whose absence let the load_qps bug report a pass.
        It is enforced by invoking the harness with a mocked runner that produces
        no usable measurements.
        """
        import subprocess
        with tempfile.TemporaryDirectory() as tmp:
            base = os.path.join(tmp, "base")
            cand = os.path.join(tmp, "cand")
            for p in (base, cand):
                with open(p, "w") as f:
                    f.write("")
                os.chmod(p, 0o755)
            proc = subprocess.run(
                [sys.executable, _SCRIPT,
                 "--baseline-binary", base, "--candidate-binary", cand,
                 "--counts", "1000", "--dtypes", "float32",
                 "--reps", "3", "--duration", "1", "--timeout", "5",
                 "--port", "6391",
                 "--log-dir", tmp, "--label", "empty_probe",
                 "--out", os.path.join(tmp, "v.json")],
                capture_output=True, text=True,
            )
            # The arms fail to start (they are empty files), so either path must
            # be a non-zero exit. 0 would mean "no regression" on no data.
            self.assertNotEqual(
                proc.returncode, 0,
                "harness reported success despite producing no measurements",
            )
            self.assertIn("arm failed", proc.stdout + proc.stderr)


class TestSourceInvariants(unittest.TestCase):
    def test_does_not_delegate_to_the_single_run_gate(self):
        # The harness must never fall back to comparing one run against another.
        # The name appears in the docstring because that gate is what this
        # replaces, so check for an actual import rather than the string.
        import ast

        tree = ast.parse(open(_SCRIPT).read(), filename=_SCRIPT)
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for a in node.names:
                    imported.add(a.name)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
            elif isinstance(node, ast.Call):
                f = node.func
                name = getattr(f, "attr", None) or getattr(f, "id", None)
                if name:
                    imported.add(name)
        self.assertNotIn("check_regression", imported)
        self.assertNotIn("check_regressions", imported)

    def test_docstring_records_the_measured_false_positive_rate(self):
        src = open(_SCRIPT).read()
        self.assertIn("77-89%", src)

    def test_reps_below_three_are_refused(self):
        src = open(_SCRIPT).read()
        self.assertIn("--reps must be at least 3", src)


if __name__ == "__main__":
    unittest.main()