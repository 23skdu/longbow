"""Tests for the bounded soak report gate checker."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import check_soak_report as checker  # noqa: E402


def _report(**overrides):
    report = {
        "rbac_enforced": True,
        "ops": 1000,
        "errors": 0,
        "error_rate": 0.0,
        "cross_verified": 12,
    }
    report.update(overrides)
    return report


class CheckSoakReportTests(unittest.TestCase):
    def check(self, report):
        return checker.check_report(
            report, max_error_rate=0.01, qps_drop_pct=50, qps_window_s=120
        )

    def test_healthy_report_passes(self):
        self.assertEqual(self.check(_report()), [])

    def test_error_rate_gate(self):
        failures = self.check(_report(error_rate=0.02))
        self.assertTrue(any("error_rate" in item for item in failures))

    def test_stall_and_cross_verification_gates(self):
        failures = self.check(_report(stalls=[{"op": "dense"}], cross_verified=0))
        self.assertTrue(any("stall" in item for item in failures))
        self.assertTrue(any("cross-agent verification" in item for item in failures))

    def test_qps_drop_and_rss_growth_gates(self):
        samples = [
            {"t_s": 0, "qps": 100, "rss_bytes": 1000},
            {"t_s": 60, "qps": 100, "rss_bytes": 1100},
            {"t_s": 120, "qps": 100, "rss_bytes": 1200},
            {"t_s": 180, "qps": 20, "rss_bytes": 1500},
            {"t_s": 240, "qps": 20, "rss_bytes": 1800},
            {"t_s": 300, "qps": 20, "rss_bytes": 2200},
        ]
        failures = self.check(_report(samples=samples))
        self.assertTrue(any("QPS dropped" in item for item in failures))
        self.assertTrue(any("RSS grew monotonically" in item for item in failures))

    def test_merge_external_rss_samples(self):
        report = checker.merge_rss_samples(
            _report(), [{"t_s": 0, "rss_bytes": 10}, {"t_s": 60, "rss_bytes": 20}]
        )
        self.assertEqual(report["samples"][1]["rss_bytes"], 20)

    def test_load_reports_rejects_malformed_json(self):
        with tempfile.TemporaryDirectory() as directory:
            bad = Path(directory) / "bad.json"
            bad.write_text("{not json", encoding="utf-8")
            with self.assertRaises(SystemExit):
                checker.load_reports([str(bad)])

            good = Path(directory) / "good.json"
            good.write_text(json.dumps(_report()), encoding="utf-8")
            self.assertEqual(checker.load_reports([str(good)])[0][1]["ops"], 1000)


if __name__ == "__main__":
    unittest.main()
