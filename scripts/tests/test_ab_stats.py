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
import os
import sys
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
        # The crux of the whole harness. Six pairs with a median of -11% but the
        # direction split 3/3 is noise, and must not be reported as a regression.
        s = ab.summarize(ratios(-30, -20, -10, 10, 20, 30), 5.0)
        self.assertEqual(s["verdict"], "inconclusive")
        self.assertIn("not consistent", s["reason"])

    def test_consistent_but_below_threshold_is_inconclusive(self):
        # -2% every pair is a real, consistent effect, but under a 5% threshold
        # it is not a regression worth gating on.
        s = ab.summarize(ratios(-2, -2, -2, -2), 5.0)
        self.assertEqual(s["verdict"], "inconclusive")

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