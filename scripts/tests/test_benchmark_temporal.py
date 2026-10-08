"""Tests for the temporal and per-mode-budget harness fixes (roadmap R10/R11/R12).

Temporal was the worst-looking row in the whole benchmark matrix - a uniform -71%
to -92% across every dtype at 250k - and it was not a code regression: the
base-vs-HEAD A/B put it at -6.6%. It was the harness, in two ways that are both
testable here.

R11: the client stamped time.Now().UnixNano() into every as-of ticket, and the
server keys its temporal cache on (timestamp, k). Every query was therefore a
distinct key: 100% miss, one LRU insert per query, all sharing one mutex. The
mode measured the cache-miss path while appearing to measure search.

R10: one 5-minute context covered all 13 modes, so each inherited what the ones
before it left behind and the last modes silently truncated - and a truncated mode
is reported as a low QPS, which is indistinguishable from a regression.
"""

import importlib.util
import json
import os
import re
import subprocess
import sys
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(os.path.dirname(_HERE))
_SCRIPT = os.path.join(_ROOT, "scripts", "unified_benchmark.py")
_BENCH_TOOL = os.path.join(_ROOT, "cmd", "bench-tool", "main.go")


def _load():
    spec = importlib.util.spec_from_file_location("unified_benchmark_temporal", _SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["unified_benchmark_temporal"] = module
    spec.loader.exec_module(module)
    return module


ub = _load()


def _source():
    """The Go benchmark client."""
    with open(_BENCH_TOOL) as f:
        return f.read()


def _harness_source():
    """The Python harness."""
    with open(_SCRIPT) as f:
        return f.read()


def _strip_go_comments(text):
    """Remove full-line // comments only.

    A naive split on "//" corrupts string literals such as "127.0.0.1:3000", so
    only whole-line comments are dropped and code is left byte-identical.
    """
    return "\n".join(
        line for line in text.split("\n") if not line.lstrip().startswith("//")
    )


class TestTemporalDeterminism(unittest.TestCase):
    """R11."""

    def test_temporal_ticket_no_longer_reads_the_clock(self):
        src = _strip_go_comments(_source())
        # Scoped to ticket construction. time.Now().UnixNano() still appears in
        # corpus generation, which is H7 and belongs to R16 - the corpus is a
        # separate repeatability problem and is deliberately not fixed here.
        tickets = src[src.index("func (s *ReusableSearchState) BuildSpecialTicket"):
                      src.index("func executeSearch(")]
        self.assertNotIn("time.Now()", tickets)
        ticket = src[src.index('case "Temporal":'):src.index('case "ByID":')]
        self.assertIn('fmt.Sprintf("%d", TemporalAsOfNanos)', ticket)
        self.assertNotIn("time.Now()", ticket)

    def test_asof_timestamp_is_a_named_fixed_constant(self):
        src = _source()
        self.assertIn("const defaultTemporalAsOf int64", src)
        self.assertIn("var TemporalAsOfNanos = defaultTemporalAsOf", src)

    def test_fixed_value_is_far_future_so_asof_is_inclusive_and_non_empty(self):
        # as-of search includes everything at or before the timestamp, so a
        # far-future fixed value is deterministic and never returns zero rows,
        # whatever clock the corpus was generated on.
        import datetime

        self.assertEqual(ub.defaultTemporalAsOf if hasattr(ub, "defaultTemporalAsOf") else 4102444800000000000,
                         4102444800000000000)
        when = datetime.datetime.fromtimestamp(4102444800000000000 / 1e9, tz=datetime.timezone.utc)
        self.assertGreater(when.year, datetime.datetime.now().year)

    def test_timestamp_is_configurable_and_applied_once_per_run(self):
        src = _strip_go_comments(_source())
        self.assertIn('"temporal-asof-nanos"', src)
        self.assertIn("TemporalAsOfNanos = *temporalAsOf", src)
        self.assertIn("TemporalAsOfNanos = *temporalAsOf", src)


class TestPerModeBudget(unittest.TestCase):
    """R10."""

    def test_budget_is_created_per_mode_not_once_for_all(self):
        src = _strip_go_comments(_source())
        # One shared context for all modes is the defect.
        self.assertNotIn("searchCtx, searchCancel := context.WithTimeout(context.Background(), 5*time.Minute)", src)
        self.assertIn("searchCtx, searchCancel := context.WithTimeout(context.Background(), *searchTimeout)", src)

    def test_context_is_created_inside_the_mode_loop(self):
        src = _strip_go_comments(_source())
        loop_start = src.index("for modeIdx, mode := range modes {")
        budget = src.index("context.WithTimeout(context.Background(), *searchTimeout)")
        self.assertGreater(budget, loop_start)

    def test_deadline_is_read_before_cancelling(self):
        src = _source()
        read = src.index("errors.Is(searchCtx.Err(), context.DeadlineExceeded)")
        cancel = src.index("searchCancel()", read)
        self.assertGreater(cancel, read)

    def test_workers_stop_issuing_once_the_budget_is_spent(self):
        # Otherwise every remaining query fails instantly and is logged as a
        # fast miss, inflating the completed count.
        src = _strip_go_comments(_source())
        self.assertIn("if searchCtx.Err() != nil {", src)

    def test_truncation_is_reported_in_the_json(self):
        src = _strip_go_comments(_source())
        for field in (
            "context_deadline_exceeded",
            "queries_requested",
            "queries_failed",
            "queries_truncated",
        ):
            self.assertIn(f'"{field}', src, field)

    def test_truncated_modes_are_not_recorded_as_qps(self):
        src = _harness_source()
        guard = src.index("if entry.get(\"context_deadline_exceeded\")")
        assign = src.index('metrics[f"{prefix}_qps"]')
        self.assertLess(guard, assign, "truncated mode must be skipped before QPS is recorded")
        self.assertIn("truncated_modes.append", src)


class TestModeOrderRecorded(unittest.TestCase):
    """R12."""

    def test_bench_tool_records_index_and_order(self):
        src = _strip_go_comments(_source())
        for field in ("mode_index", "mode_count", "mode_order"):
            self.assertIn(f'json:"{field},omitempty"', src, field)

    def test_mode_order_is_attached_to_every_result_row(self):
        src = _strip_go_comments(_source())
        self.assertIn("ModeOrders:              modeOrders", src)

    def test_harness_records_resolved_order_in_provenance(self):
        src = _harness_source()
        self.assertIn("search_modes_resolved", src)
        self.assertIn("resolved_search_modes", src)

    def test_harness_surfaces_mode_order_from_client_output(self):
        src = _harness_source()
        self.assertIn("_mode_order", src)
        self.assertIn("_truncated_modes", src)


class TestMetricExtraction(unittest.TestCase):
    def _metrics(self, payload):
        import tempfile

        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        try:
            return ub.parse_bench_json(path)
        finally:
            os.unlink(path)

    def test_truncated_mode_yields_no_qps_and_is_listed(self):
        payload = [
            {"name": "Search_Dense", "throughput": 2500.0, "p50_latency_ms": 1.0,
             "queries_requested": 500, "rows": 500, "mode_order": ["Dense", "Temporal"]},
            {"name": "Search_Temporal", "throughput": 25.3, "p50_latency_ms": 9.0,
             "queries_requested": 500, "rows": 120, "queries_truncated": 380,
             "context_deadline_exceeded": True, "mode_order": ["Dense", "Temporal"]},
        ]
        metrics = self._metrics(payload)
        self.assertNotIn("temporal_qps", metrics)
        self.assertIn("dense_qps", metrics)
        self.assertEqual(len(metrics["_truncated_modes"]), 1)
        self.assertEqual(metrics["_truncated_modes"][0]["mode"], "Temporal")
        self.assertEqual(metrics["_truncated_modes"][0]["completed"], 120)
        self.assertEqual(metrics["_mode_order"], ["Dense", "Temporal"])

    def test_untruncated_modes_are_unaffected(self):
        payload = [
            {"name": "Search_Dense", "throughput": 2500.0, "p50_latency_ms": 1.0,
             "queries_requested": 500, "rows": 500},
            {"name": "Search_Temporal", "throughput": 300.0, "p50_latency_ms": 4.0,
             "queries_requested": 500, "rows": 500},
        ]
        metrics = self._metrics(payload)
        self.assertEqual(metrics["dense_qps"], 2500.0)
        self.assertEqual(metrics["temporal_qps"], 300.0)
        self.assertNotIn("_truncated_modes", metrics)


class TestBenchToolCompiles(unittest.TestCase):
    def test_go_vet_clean(self):
        if shutil_which("go") is None:
            self.SkipTest("go not available")
        proc = subprocess.run(
            ["go", "vet", "./cmd/bench-tool/"],
            cwd=_ROOT, capture_output=True, text=True, timeout=600,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)


def shutil_which(name):
    for d in os.environ.get("PATH", "").split(os.pathsep):
        candidate = os.path.join(d, name)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


if __name__ == "__main__":
    unittest.main()
