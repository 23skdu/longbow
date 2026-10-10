"""Tests for R15 (tq_bits identity), R16 (determinism plumbing) and R17
(child exit signals).

Three defects made matrix numbers unattributable or undiagnosable:

R15/H5: the harness reassigned `dtype`, so turboquant4 and turboquant8 were both
recorded as "turboquant" and every "turboquant" row was two configurations. Only
tq_bits told them apart, and the comparison key did not include it - so half the
TurboQuant matrix could not be attributed, and a 4-bit baseline could be paired
with an 8-bit result and the difference called a regression.

R16/H7: corpus, query vectors and ByID ids were all drawn from the clock or an
auto-seeded global rand, so two runs of the same binary were not comparable.

R17/H8: a SIGKILLed client, a timeout and a crash were all "FAILED" with an empty
error, which is exactly why H1's collateral damage went unnoticed.
"""

import importlib.util
import os
import subprocess
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(_HERE))
_SCRIPT = os.path.join(_ROOT, "scripts", "unified_benchmark.py")
_REGRESSION = os.path.join(_ROOT, "scripts", "check_regression.py")


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


ub = _load("unified_benchmark_r15", _SCRIPT)
cr = _load("check_regression_r15", _REGRESSION)


def _read_file(path):
    with open(path) as f:
        return f.read()


class TestTurboQuantIdentity(unittest.TestCase):
    """R15 / H5."""

    def test_dtype_is_not_reassigned(self):
        # The exact defect: `dtype = "turboquant"` inside the bit-pack branch.
        src = _read_file(_SCRIPT)
        self.assertNotIn('if dtype == "turboquant4":\n            dtype = "turboquant"', src)

    def test_wire_and_requested_dtype_are_separate_variables(self):
        src = _read_file(_SCRIPT)
        self.assertIn("requested_dtype = dtype", src)
        self.assertIn("wire_dtype = dtype", src)
        self.assertIn('wire_dtype = "turboquant"', src)

    def test_client_receives_wire_dtype_not_the_label(self):
        src = _read_file(_SCRIPT)
        self.assertIn("-dtype {wire_dtype}", src)

    def test_result_row_records_requested_dtype_and_bits(self):
        src = _read_file(_SCRIPT)
        self.assertIn('"requested_dtype": requested_dtype', src)
        self.assertIn('"tq_bits": tq_bits', src)

    def test_match_config_separates_bit_depths(self):
        base4 = {"dim": 128, "dtype": "turboquant", "count": 10000, "tq_bits": 4}
        res8 = {"dim": 128, "dtype": "turboquant", "count": 10000, "tq_bits": 8}
        res4 = {"dim": 128, "dtype": "turboquant", "count": 10000, "tq_bits": 4}
        self.assertFalse(cr.match_config(base4, res8), "4-bit baseline must not match an 8-bit result")
        self.assertTrue(cr.match_config(base4, res4))

    def test_bit_depth_recovered_from_dtype_string_when_absent(self):
        # Reports written before R15 carry no tq_bits; the dtype string is the
        # only thing distinguishing them, so it is used as a fallback.
        self.assertEqual(cr._tq_bits({"dtype": "turboquant4"}), 4)
        self.assertEqual(cr._tq_bits({"dtype": "turboquant8"}), 8)
        self.assertEqual(cr._tq_bits({"dtype": "turboquant"}), 0)
        self.assertEqual(cr._tq_bits({"dtype": "float32"}), 0)

    def test_explicit_tq_bits_wins_over_the_string(self):
        self.assertEqual(cr._tq_bits({"dtype": "turboquant", "tq_bits": 8}), 8)

    def test_non_turboquant_configs_still_match_each_other(self):
        a = {"dim": 128, "dtype": "float32", "count": 10000}
        b = {"dim": 128, "dtype": "float32", "count": 10000}
        self.assertTrue(cr.match_config(a, b))


class TestSeedPlumbing(unittest.TestCase):
    """R16 at the harness boundary."""

    def test_harness_exposes_a_seed_flag(self):
        src = _read_file(_SCRIPT)
        self.assertIn('"--seed"', src)

    def test_seed_is_passed_to_the_client_when_set(self):
        src = _read_file(_SCRIPT)
        self.assertIn("seed_arg = f\" -seed {self.args.seed}\"", src)
        self.assertIn("{seed_arg}", src)

    def test_seed_is_recorded_on_the_result_row(self):
        src = _read_file(_SCRIPT)
        self.assertIn('"seed": self.args.seed', src)

    def test_seed_flag_absent_by_default(self):
        src = _read_file(_SCRIPT)
        block = src[src.index('"--seed"'):src.index('"--memory-default-fraction"')]
        self.assertIn("default=None", block)

    def test_client_source_has_no_clock_seeded_corpus(self):
        src = _read_file(os.path.join(_ROOT, "cmd", "bench-tool", "main.go"))
        # The corpus worker seeds from the fixed run seed now.
        self.assertIn("workerSeed := *seed + int64(w)*10007", src)
        self.assertNotIn("workerSeed := time.Now().UnixNano()", src)

    def test_client_source_has_no_global_rand_for_queries(self):
        src = _read_file(os.path.join(_ROOT, "cmd", "bench-tool", "main.go"))
        # Go seeds the global math/rand per process, so a query drawn from it is
        # different on every run.
        self.assertNotIn("s.vector[i] = rand.Float32()", src)
        self.assertIn("qRng := rand.New(rand.NewSource(RunSeed", src)

    def test_client_defaults_to_a_fixed_seed(self):
        src = _read_file(os.path.join(_ROOT, "cmd", "bench-tool", "main.go"))
        self.assertIn("const defaultSeed int64 = 42", src)


class TestFailureReasons(unittest.TestCase):
    """R17 / H8."""

    def _reason(self, rc):
        import signal as _signal

        sig = None
        if rc is not None and rc < 0:
            sig = -rc
        elif rc is not None and rc > 128:
            sig = rc - 128
        if sig is not None:
            try:
                return _signal.Signals(sig).name
            except (ValueError, AttributeError):
                return f"SIG{sig}"
        return None

    def test_sigkill_is_recognisable_from_both_conventions(self):
        # Python's subprocess reports -N; a shell reports 128+N.
        self.assertEqual(self._reason(-9), "SIGKILL")
        self.assertEqual(self._reason(137), "SIGKILL")

    def test_source_records_failure_reasons(self):
        src = _read_file(_SCRIPT)
        self.assertIn("last_failure_reason", src)
        self.assertIn("self.failure_reasons[config_key] = reason", src)
        self.assertIn('"failure_reasons"', src)

    def test_timeout_is_distinguished_from_a_crash(self):
        src = _read_file(_SCRIPT)
        self.assertIn('reason = "client timed out"', src)

    def test_empty_output_with_zero_exit_is_its_own_reason(self):
        # bench-tool can exit non-zero on success, so exit 0 with no metrics must
        # not be reported as a crash.
        src = _read_file(_SCRIPT)
        self.assertIn("client exited 0 but wrote no parsable metrics", src)


class TestGoClientStillPasses(unittest.TestCase):
    def test_go_test_bench_tool(self):
        go = None
        for d in os.environ.get("PATH", "").split(os.pathsep):
            candidate = os.path.join(d, "go")
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                go = candidate
                break
        if go is None:
            self.skipTest("go not available")
        proc = subprocess.run(
            [go, "test", "./cmd/bench-tool/", "-count", "1", "-run",
             "TestTicketDeterminism|TestByIDUsesQueryIndex|TestCorpusGenerationIsDeterministic|TestTemporalAsOfIsFixed"],
            cwd=_ROOT, capture_output=True, text=True, timeout=900,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)




class TestMultiRunMerge(unittest.TestCase):
    """R30/R36: multiple files merge by minimum, and that is not a noise fix."""

    def _run(self, qps_by_mode, dim=128, dtype="float32", count=10000):
        return {
            "results": [{
                "dim": dim, "dtype": dtype, "count": count,
                "ingest": {"vec_per_sec": 1000.0},
                "search": {m: {"qps": q} for m, q in qps_by_mode.items()},
            }],
        }

    def test_minimum_is_taken_per_mode(self):
        merged = cr.merge_runs_minimum(None, [
            self._run({"dense": 100.0}),
            self._run({"dense": 500.0}),
            self._run({"dense": 300.0}),
        ])
        self.assertEqual(merged["results"][0]["search"]["dense"]["qps"], 100.0)

    def test_modes_are_merged_independently(self):
        merged = cr.merge_runs_minimum(None, [
            self._run({"dense": 100.0, "sparse": 900.0}),
            self._run({"dense": 800.0, "sparse": 200.0}),
        ])
        search = merged["results"][0]["search"]
        self.assertEqual(search["dense"]["qps"], 100.0)
        self.assertEqual(search["sparse"]["qps"], 200.0)

    def test_config_key_includes_bit_depth(self):
        merged = cr.merge_runs_minimum(None, [
            {"results": [{"dim": 128, "dtype": "turboquant", "count": 1000,
                          "tq_bits": 4, "search": {"dense": {"qps": 10.0}}}]},
            {"results": [{"dim": 128, "dtype": "turboquant", "count": 1000,
                          "tq_bits": 8, "search": {"dense": {"qps": 20.0}}}]},
        ])
        # Two distinct configurations, not one merged row.
        self.assertEqual(len(merged["results"]), 2)

    def test_merge_is_not_order_dependent(self):
        runs = [self._run({"dense": 100.0}), self._run({"dense": 500.0})]
        a = cr.merge_runs_minimum(None, runs)
        b = cr.merge_runs_minimum(None, list(reversed(runs)))
        self.assertEqual(a["results"][0]["search"]["dense"]["qps"],
                         b["results"][0]["search"]["dense"]["qps"])

    def test_help_text_does_not_claim_min_is_a_noise_fix(self):
        # R36 measured that combining runs makes the gate worse. Anyone reading
        # --help must not be told otherwise.
        src = _read_file(_REGRESSION)
        self.assertIn("does NOT make the gate reliable", src)
        self.assertIn("Interleaving", src)


if __name__ == "__main__":
    unittest.main()
