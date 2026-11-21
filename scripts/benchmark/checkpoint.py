"""Checkpoint persistence, resume handling, result saving, and baseline diffing."""

import json
import os
import platform
import shutil
import sys

from .telemetry import collect_provenance


def save_baseline_file(output_file: str, baseline_path: str) -> None:
    """Copy a completed benchmark JSON report to a baseline path."""
    if output_file and os.path.exists(output_file):
        os.makedirs(os.path.dirname(baseline_path) or ".", exist_ok=True)
        shutil.copy2(output_file, baseline_path)
        print(f"\n  [baseline] Saved baseline to {baseline_path}")
    else:
        print("\n  [baseline] WARNING: no results file to save as baseline")


def compare_baseline_file(output_file: str, baseline_path: str, threshold: float) -> None:
    """Compare current results against a baseline JSON file and exit non-zero on regression."""
    if not os.path.exists(baseline_path):
        print(f"\n  [regression] ERROR: baseline file not found: {baseline_path}")
        sys.exit(1)
    if not (output_file and os.path.exists(output_file)):
        return
    try:
        with open(baseline_path) as f:
            baseline = json.load(f)
        with open(output_file) as f:
            results = json.load(f)
        baseline_cfgs = baseline.get("configs", baseline.get("results", []))
        result_cfgs = results.get("configs", results.get("results", []))
        regressions = []
        for b_cfg in baseline_cfgs:
            for r_cfg in result_cfgs:
                if (
                    b_cfg.get("dim") != r_cfg.get("dim")
                    or b_cfg.get("dtype") != r_cfg.get("dtype")
                    or b_cfg.get("count") != r_cfg.get("count")
                ):
                    continue
                for mode, b_data in b_cfg.get("search", {}).items():
                    r_data = r_cfg.get("search", {}).get(mode)
                    if not r_data:
                        continue
                    b_qps = b_data.get("qps", 0)
                    r_qps = r_data.get("qps", 0)
                    if b_qps <= 0:
                        continue
                    change_pct = ((r_qps - b_qps) / b_qps) * 100
                    if change_pct < -threshold:
                        regressions.append(
                            f"  {b_cfg['dim']}d/{b_cfg['dtype']}/{b_cfg['count']}v "
                            f"{mode}: {b_qps:.1f} -> {r_qps:.1f} QPS ({change_pct:+.1f}%)"
                        )
                b_idx = b_cfg.get("ingest", {}).get("indexing_duration_seconds", 0)
                r_idx = r_cfg.get("ingest", {}).get("indexing_duration_seconds", 0)
                if b_idx > 0 and r_idx > 0:
                    idx_change_pct = ((r_idx - b_idx) / b_idx) * 100
                    if idx_change_pct > threshold:
                        regressions.append(
                            f"  {b_cfg['dim']}d/{b_cfg['dtype']}/{b_cfg['count']}v "
                            f"index_time: {b_idx:.2f}s -> {r_idx:.2f}s ({idx_change_pct:+.1f}%)"
                        )
        if regressions:
            print(f"\n  [regression] FAIL: {len(regressions)} regression(s) beyond {threshold}%:")
            for r in regressions:
                print(r)
            sys.exit(1)
        print(f"\n  [regression] PASS: no regressions beyond {threshold}% threshold")
    except Exception as e:
        print(f"\n  [regression] ERROR: {e}")
        sys.exit(1)


class CheckpointMixin:
    """Checkpoint persistence and per-run JSON serialization."""

    def _save_checkpoint(self):
        """Save the partial results checkpoint.

        R14b (H9): this used to be a no-op when self.results was empty, so a run
        in which every config was ResourceExhausted produced a 9,194-line log and
        zero result files - the exhaustion was never recorded anywhere. An empty
        result set is a result: it is written, along with the exhausted/failed
        config lists, so the run is legible without re-running it.
        """
        if hasattr(self, "results") and hasattr(self, "output_file"):
            try:
                dims = [int(d) for d in self.args.dims.split(",")]
                counts = [int(c) for c in self.args.counts.split(",")]
                dtypes = self.args.dtypes.split(",")
                with open(self.output_file, "w") as f:
                    json.dump({
                        "mode": getattr(self, 'current_mode', self.args.mode),
                        "timestamp": self.timestamp,
                        "platform": f"{platform.system()} {platform.machine()}",
                        "config": {
                            "dims": dims,
                            "counts": counts,
                            "dtypes": dtypes,
                            "duration": self.args.duration,
                        },
                        "provenance": collect_provenance(self.args),
                        "results": self.results,
                        "completed_configs": [list(c) for c in getattr(self, 'completed_configs', set())],
                        "failed_configs": [list(c) for c in getattr(self, 'failed_configs', set())],
                        "exhausted_configs": [list(c) for c in getattr(self, 'exhausted_configs', set())],
                        "failure_reasons": {str(k): v for k, v in getattr(self, 'failure_reasons', {}).items()},
                    }, f, indent=2)
                # Mirror to the stable path so a later --resume in a fresh
                # invocation finds it.
                try:
                    with open(self.stable_output_file, "w") as f:
                        json.dump(json.load(open(self.output_file)), f, indent=2)
                except Exception as e:
                    print(f"\n  [checkpoint] Could not mirror to {self.stable_output_file}: {e}")
                suffix = " (no results)" if not self.results else ""
                print(f"\n  [checkpoint] Partial results saved to {self.output_file}{suffix}")
            except Exception as e:
                print(f"\n  [checkpoint] Failed to save partial results: {e}")
    def _load_checkpoint(self):
        """Load checkpoint file and extract completed/failed configs for resume.

        Returns list of previously collected results (empty if no checkpoint).
        """
        if not hasattr(self.args, 'resume') or not self.args.resume:
            return []
        if not hasattr(self, 'output_file') or not os.path.exists(self.output_file):
            print("  [resume] No checkpoint file found, starting fresh")
            return []
        try:
            with open(self.output_file) as f:
                data = json.load(f)
            completed = data.get("completed_configs", [])
            if completed:
                self.completed_configs = set(tuple(c) for c in completed)
                print(f"  [resume] Loaded {len(completed)} completed configs from checkpoint, will skip them")
            failed = data.get("failed_configs", [])
            if failed:
                self.failed_configs = set(tuple(c) for c in failed)
                print(f"  [resume] Loaded {len(failed)} previously failed configs, will retry")
            exhausted = data.get("exhausted_configs", [])
            if exhausted:
                self.exhausted_configs = set(tuple(c) for c in exhausted)
                print(f"  [resume] Loaded {len(exhausted)} exhausted (OOM) configs, will skip")
            return data.get("results", [])
        except Exception as e:
            print(f"  [resume] Failed to load checkpoint: {e}")
            return []
    def check_cache(self, dim, dtype, count, label):
        if not self.args.cache:
            return False
        json_file = os.path.join(self.log_dir, f"result_{label}.json")
        return os.path.exists(json_file)
    def save_results(self):
        """Save current results to JSON matrix file."""
        with open(self.output_file, "w") as f:
            json.dump(
                {
                    "mode": self.args.mode,
                    "timestamp": self.timestamp,
                    "platform": f"{platform.system()} {platform.machine()}",
                    "config": {
                        "dims": [int(d) for d in self.args.dims.split(",")],
                        "counts": [int(c) for c in self.args.counts.split(",")],
                        "dtypes": self.args.dtypes.split(","),
                        "duration": self.args.duration,
                    },
                    "results": self.results,
                },
                f,
                indent=2,
            )
