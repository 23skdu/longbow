#!/usr/bin/env python3
"""Validate bounded soak reports against CI failure criteria.

The soak simulators emit JSON reports with slightly different field names. This
checker normalizes the common fields and fails on:
  * error rate above 1%;
  * >50% QPS drop sustained for two minutes;
  * monotonic RSS growth over the second half of a run;
  * missing cross-agent verification hits.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _first(report: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in report:
            return report[key]
    return None


def _samples(report: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("samples", "timeline", "metrics"):
        value = report.get(key)
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return value
    return []


def _sample_time(sample: dict[str, Any], index: int) -> float:
    value = _first(sample, "t_s", "elapsed_s", "timestamp_s", "time_s", "elapsed")
    number = _number(value)
    if number is not None:
        return number
    return float(index)


def _sample_qps(sample: dict[str, Any]) -> float | None:
    return _number(_first(sample, "qps", "ops_per_second", "throughput"))


def _sample_rss(sample: dict[str, Any]) -> float | None:
    return _number(_first(sample, "rss_bytes", "rss", "memory_bytes", "memory_rss_bytes"))


def _qps_stall(samples: list[dict[str, Any]], window_s: float, drop_pct: float) -> str | None:
    """Detect a sustained QPS drop using at least two samples in the window."""
    if len(samples) < 3:
        return None
    points = [(_sample_time(s, i), _sample_qps(s)) for i, s in enumerate(samples)]
    points = [(t, q) for t, q in points if q is not None]
    if len(points) < 3:
        return None
    for i, (t, qps) in enumerate(points):
        if qps <= 0:
            continue
        prior = [p for p in points[:i] if 0 <= t - p[0] <= window_s * 2]
        if len(prior) < 2:
            continue
        baseline = max(p[1] for p in prior)
        if baseline <= 0:
            continue
        later = [p for p in points[i:] if 0 <= p[0] - t <= window_s]
        if len(later) < 2:
            continue
        if all(p[1] <= baseline * (1 - drop_pct / 100.0) for p in later):
            return (
                f"QPS dropped from {baseline:.2f} to {qps:.2f} "
                f"({(1 - qps / baseline) * 100:.1f}%) for at least {window_s:.0f}s"
            )
    return None


def _rss_growth(samples: list[dict[str, Any]]) -> str | None:
    """Detect steady RSS growth over the second half of the run."""
    values = [_sample_rss(s) for s in samples]
    points = [v for v in values if v is not None]
    if len(points) < 4:
        return None
    tail = points[len(points) // 2 :]
    if len(tail) < 3 or tail[0] <= 0:
        return None
    if all(b >= a for a, b in zip(tail, tail[1:])) and tail[-1] > tail[0] * 1.2:
        return f"RSS grew monotonically in the second half: {tail[0]:.0f} -> {tail[-1]:.0f} bytes"
    return None


def _cross_verified(report: dict[str, Any]) -> int:
    for key in ("cross_verified", "cross_verification_hits", "verified_cross_reads"):
        value = _number(report.get(key))
        if value is not None:
            return int(value)
    by_op = report.get("by_op")
    if isinstance(by_op, dict):
        for key in ("cross_read", "cross_read_verified"):
            value = _number(by_op.get(key))
            if value is not None:
                return int(value)
    return -1


def check_report(
    report: dict[str, Any],
    *,
    max_error_rate: float,
    qps_drop_pct: float,
    qps_window_s: float,
) -> list[str]:
    failures: list[str] = []
    declared = report.get("failures")
    if isinstance(declared, list):
        for item in declared:
            failures.append(f"simulator reported failure: {item}")

    error_rate = _number(report.get("error_rate"))
    if error_rate is None:
        ops = _number(report.get("ops"))
        errors = _number(report.get("errors"))
        if ops and errors is not None:
            error_rate = errors / ops
    if error_rate is None:
        failures.append("report has no error_rate or ops/errors fields")
    elif error_rate > max_error_rate:
        failures.append(f"error_rate {error_rate:.4f} exceeds {max_error_rate:.4f}")

    by_op_stats = report.get("by_op_stats")
    if isinstance(by_op_stats, dict):
        for op, stats in by_op_stats.items():
            if not isinstance(stats, dict):
                continue
            if stats.get("stall"):
                failures.append(f"operation {op} reported a stall")
            op_rate = _number(stats.get("error_rate"))
            if op_rate is not None and op_rate > max_error_rate:
                failures.append(f"operation {op} error_rate {op_rate:.4f} exceeds {max_error_rate:.4f}")

    stalls = report.get("stalls")
    if isinstance(stalls, list) and stalls:
        failures.append(f"{len(stalls)} QPS stall record(s) in report")

    qps_stall = _qps_stall(_samples(report), qps_window_s, qps_drop_pct)
    if qps_stall:
        failures.append(qps_stall)

    rss_growth = _rss_growth(_samples(report))
    if rss_growth:
        failures.append(rss_growth)

    verified = _cross_verified(report)
    if verified == 0:
        failures.append("no cross-agent verification hits")
    elif verified < 0 and report.get("rbac_enforced"):
        failures.append("RBAC report has no cross-agent verification field")

    return failures


def load_rss_samples(path: str) -> list[dict[str, Any]]:
    """Load RSS samples emitted as JSONL by the CI soak wrapper."""
    samples: list[dict[str, Any]] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"invalid RSS sample in {path}: {exc}") from exc
        if isinstance(payload, dict):
            samples.append(payload)
        else:
            number = _number(payload)
            if number is not None:
                samples.append({"rss_bytes": number})
    return samples


def merge_rss_samples(report: dict[str, Any], rss_samples: list[dict[str, Any]]) -> dict[str, Any]:
    """Attach externally sampled RSS values to a soak report for validation."""
    if not rss_samples:
        return report
    merged = dict(report)
    existing = _samples(report)
    if existing:
        combined = []
        for index, sample in enumerate(existing):
            item = dict(sample)
            if index < len(rss_samples) and _sample_rss(item) is None:
                item["rss_bytes"] = _sample_rss(rss_samples[index])
            combined.append(item)
        merged["samples"] = combined
    else:
        merged["samples"] = rss_samples
    return merged


def load_reports(paths: Iterable[str]) -> list[tuple[str, dict[str, Any]]]:
    reports: list[tuple[str, dict[str, Any]]] = []
    for raw in paths:
        path = Path(raw)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SystemExit(f"invalid soak report {path}: {exc}") from exc
        if isinstance(payload, list):
            for index, item in enumerate(payload):
                if not isinstance(item, dict):
                    raise SystemExit(f"invalid soak report {path}: entry {index} is not an object")
                reports.append((f"{path}#{index}", item))
        elif isinstance(payload, dict):
            reports.append((str(path), payload))
        else:
            raise SystemExit(f"invalid soak report {path}: expected object or array")
    return reports


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", help="JSON soak report paths")
    parser.add_argument("--max-error-rate", type=float, default=0.01)
    parser.add_argument("--qps-drop-pct", type=float, default=50.0)
    parser.add_argument("--qps-window-s", type=float, default=120.0)
    parser.add_argument(
        "--rss-samples",
        help="Optional JSONL file of server RSS samples collected during the soak",
    )
    args = parser.parse_args()

    extra_samples = load_rss_samples(args.rss_samples) if args.rss_samples else []
    failed = False
    for name, report in load_reports(args.reports):
        failures = check_report(
            merge_rss_samples(report, extra_samples),
            max_error_rate=args.max_error_rate,
            qps_drop_pct=args.qps_drop_pct,
            qps_window_s=args.qps_window_s,
        )
        if failures:
            failed = True
            print(f"FAIL {name}")
            for failure in failures:
                print(f"  - {failure}")
        else:
            print(f"PASS {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
