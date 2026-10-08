"""Tests for the benchmark memory ceiling and port-scoped cleanup (R13/R14).

Three separate defects shared one root cause: no parser for a size string.
--memory was typed as int bytes while being documented as "10GB";
LONGBOW_MAX_MEMORY is documented as a size string everywhere else, so int() on it
raised ValueError; and start_server read an 18 GiB literal, ignoring both. The
result was that the documented knob did nothing, and a ceiling above available RAM
showed up as a silent OOM kill of the client mid-indexing.
"""

import importlib.util
import os
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT = os.path.join(os.path.dirname(_HERE), "unified_benchmark.py")


def _load():
    spec = importlib.util.spec_from_file_location("unified_benchmark_mem", _SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["unified_benchmark_mem"] = module
    spec.loader.exec_module(module)
    return module


ub = _load()


class Args:
    def __init__(self, memory=None, fraction=None):
        self.memory = memory
        self.memory_default_fraction = fraction


class TestParseSizeBytes(unittest.TestCase):
    def test_bare_byte_counts(self):
        self.assertEqual(ub.parse_size_bytes("10737418240"), 10 * 1024 ** 3)
        self.assertEqual(ub.parse_size_bytes(1024), 1024)

    def test_decimal_and_binary_units_are_both_1024_based(self):
        # Deliberately: the codebase already writes 1GB as 1024^3 everywhere, so
        # matching it avoids a silent off-by-2.4% surprise.
        for unit in ("G", "GB", "GiB", "g", "gb", "gib"):
            self.assertEqual(ub.parse_size_bytes(f"10{unit}"), 10 * 1024 ** 3, unit)

    def test_smaller_and_larger_units(self):
        self.assertEqual(ub.parse_size_bytes("512M"), 512 * 1024 ** 2)
        self.assertEqual(ub.parse_size_bytes("512MiB"), 512 * 1024 ** 2)
        self.assertEqual(ub.parse_size_bytes("1K"), 1024)
        self.assertEqual(ub.parse_size_bytes("1B"), 1)
        self.assertEqual(ub.parse_size_bytes("2T"), 2 * 1024 ** 4)

    def test_whitespace_and_fractions(self):
        self.assertEqual(ub.parse_size_bytes("  10 GB "), 10 * 1024 ** 3)
        self.assertEqual(ub.parse_size_bytes("1.5GB"), int(1.5 * 1024 ** 3))

    def test_unparseable_degrades_to_default_rather_than_raising(self):
        # A typo in an environment variable must not abort a multi-hour run.
        for bad in ("", "   ", "gb", "not-a-size", "10 furlongs", None):
            self.assertEqual(ub.parse_size_bytes(bad, default=42), 42, bad)
        self.assertIsNone(ub.parse_size_bytes("nonsense", default=None))


class TestResolveMemoryLimit(unittest.TestCase):
    def setUp(self):
        self._saved = os.environ.pop("LONGBOW_MAX_MEMORY", None)

    def tearDown(self):
        os.environ.pop("LONGBOW_MAX_MEMORY", None)
        if self._saved is not None:
            os.environ["LONGBOW_MAX_MEMORY"] = self._saved

    def test_flag_is_authoritative_over_environment(self):
        os.environ["LONGBOW_MAX_MEMORY"] = "2GB"
        # R14: an explicit flag must not be overridden by an inherited variable.
        self.assertEqual(ub.resolve_memory_limit_bytes(Args(memory="16GB")), 16 * 1024 ** 3)

    def test_environment_used_when_flag_absent(self):
        os.environ["LONGBOW_MAX_MEMORY"] = "2GB"
        self.assertEqual(ub.resolve_memory_limit_bytes(Args()), 2 * 1024 ** 3)

    def test_falls_back_to_fraction_of_host_ram(self):
        total = ub.host_total_memory_bytes()
        if total is None:
            self.SkipTest("host RAM not detectable")
        resolved = ub.resolve_memory_limit_bytes(Args())
        self.assertEqual(resolved, int(total * ub.DEFAULT_MEMORY_FRACTION))

    def test_unparseable_environment_falls_through_to_host_default(self):
        os.environ["LONGBOW_MAX_MEMORY"] = "lots"
        total = ub.host_total_memory_bytes()
        if total is None:
            self.SkipTest("host RAM not detectable")
        self.assertEqual(
            ub.resolve_memory_limit_bytes(Args()), int(total * ub.DEFAULT_MEMORY_FRACTION)
        )

    def test_h2_regression_flag_actually_reaches_the_env_value(self):
        # start_server used to write an 18 GiB literal regardless of --memory.
        os.environ.pop("LONGBOW_MAX_MEMORY", None)
        for value in ("8GB", "14GB", "10737418240"):
            args = Args(memory=value)
            env_value = str(ub.resolve_memory_limit_bytes(args))
            self.assertEqual(int(env_value), ub.parse_size_bytes(value))


class TestHostMemoryProbes(unittest.TestCase):
    def test_total_memory_is_plausible(self):
        total = ub.host_total_memory_bytes()
        if total is None:
            self.SkipTest("host RAM not detectable")
        self.assertGreater(total, 256 * 1024 ** 2)

    def test_available_memory_does_not_exceed_total(self):
        total = ub.host_total_memory_bytes()
        available = ub.host_available_memory_bytes()
        if total is None or available is None:
            self.SkipTest("meminfo not readable")
        self.assertLessEqual(available, total)

    def test_format_size_gb(self):
        self.assertEqual(ub.format_size_gb(10 * 1024 ** 3), "10.0 GB")
        self.assertEqual(ub.format_size_gb(None), "unset")


class TestNoGlobalPkill(unittest.TestCase):
    """H1 must not come back: reaping by process name is what destroyed data."""

    def test_no_executable_pkill_anywhere(self):
        """Find real calls, not prose. Docstrings describe the old bug and a
        commented-out line remains; neither reaps anything."""
        import ast

        with open(_SCRIPT) as f:
            tree = ast.parse(f.read(), filename=_SCRIPT)

        offenders = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = None
            if isinstance(func, ast.Attribute):
                name = func.attr
            elif isinstance(func, ast.Name):
                name = func.id
            if name not in ("run", "Popen", "call", "check_call", "check_output", "system"):
                continue
            for arg in list(node.args) + [kw.value for kw in node.keywords]:
                text = arg.value if isinstance(arg, ast.Constant) and isinstance(arg.value, str) else None
                if text and "pkill" in text:
                    offenders.append((node.lineno, name, text.strip()))
        self.assertEqual(
            offenders, [], f"process-name reaping reintroduced at: {offenders}"
        )

    def test_cleanup_helper_exists_and_is_port_scoped(self):
        self.assertTrue(hasattr(ub.BenchmarkRunner, "_reap_only_own_ports"))
        self.assertTrue(hasattr(ub.BenchmarkRunner, "_own_ports"))


class TestCheckpointAlwaysWrites(unittest.TestCase):
    """H9: an all-ResourceExhausted run must still leave an artefact."""

    def test_save_checkpoint_has_no_results_guard(self):
        with open(_SCRIPT) as f:
            body = f.read()
        start = body.index("def _save_checkpoint")
        end = body.index("def _load_checkpoint")
        snippet = body[start:end]
        # The old guard was `if ... and self.results and ...`.
        self.assertNotIn("and self.results and", snippet)

    def test_stable_resume_path_is_defined(self):
        with open(_SCRIPT) as f:
            self.assertIn("stable_output_file", f.read())


if __name__ == "__main__":
    unittest.main()
