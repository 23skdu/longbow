#!/usr/bin/env python3
"""
Unified Benchmark Orchestrator for Longbow Vector Database.
Thin CLI entrypoint and compatibility facade delegating implementation to scripts/benchmark/.
"""

import atexit
import os
import signal
import sys
from datetime import datetime

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_SDK_SRC = os.path.join(os.path.dirname(_SCRIPTS_DIR), "longbowclientsdk", "src")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)
if os.path.isdir(_SDK_SRC) and _SDK_SRC not in sys.path:
    sys.path.append(_SDK_SRC)

from benchmark import (
    CheckpointMixin,
    ConfigRunnerMixin,
    MatrixRunnerMixin,
    MemoryExceededException,
    MemoryGuardMixin,
    ReportingMixin,
    ScenarioMixin,
    ServerLifecycleMixin,
)
from benchmark.cli import build_arg_parser, main
from benchmark.config_runner import HAS_ANALYSIS_LIBS
from benchmark.memory_guard import (
    DEFAULT_MEMORY_FRACTION,
    _SIZE_UNITS,
    format_size_gb,
    host_available_memory_bytes,
    host_total_memory_bytes,
    parse_size_bytes,
    resolve_memory_limit_bytes,
)
from benchmark.process import (
    ALL_DTYPES,
    DTYPE_BYTES,
    ResourceExhaustedException,
    _kill_port,
    parse_bench_json,
    run_command,
)
from benchmark.server_lifecycle import HAS_LONGBOW_SDK
from benchmark.telemetry import (
    PROVENANCE_ENV_VARS,
    binary_provenance,
    collect_provenance,
    derive_config_seed,
    env_seed,
    git_revision,
    validate_concurrency_invariant,
)


class BenchmarkRunner(
    ServerLifecycleMixin,
    MemoryGuardMixin,
    CheckpointMixin,
    ConfigRunnerMixin,
    MatrixRunnerMixin,
    ReportingMixin,
    ScenarioMixin,
):
    """Unified benchmark runner composed from modular domain mixins."""

    def __init__(self, args):
        self.args = args
        self.server_addr = os.environ.get("LONGBOW_ADDR", args.addr)
        self.node_id = os.environ.get("LONGBOW_NODE_ID", "bench1")
        self.data_dir = os.environ.get(
            "LONGBOW_DATA_PATH", os.path.join(os.getcwd(), "data/bench")
        )
        self.log_dir = os.environ.get("LONGBOW_PERF_LOGS", os.path.join(os.getcwd(), "data/perf_logs"))
        print(f"DEBUG: data_dir={self.data_dir}")
        print(f"DEBUG: log_dir={self.log_dir}")
        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)

        self.bin_dir = os.environ.get("LONGBOW_BIN_PATH", os.path.join(os.getcwd(), "bin"))
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        label_suffix = f"_{args.label}" if args.label else ""
        # R14b (H10): the timestamped name meant --resume only ever found a
        # checkpoint from the same invocation, so an interrupted multi-hour tier
        # could not be resumed by a fresh one. The stable path is now the
        # resume target and the timestamped file is written alongside it, so
        # per-run artefacts are still available.
        self.stable_output_file = os.path.join(
            self.log_dir, f"perf_matrix_{args.mode}{label_suffix}_latest.json"
        )
        self.output_file = os.path.join(
            self.log_dir, f"perf_matrix_{args.mode}{label_suffix}_{self.timestamp}.json"
        )
        self.results = []
        self.exhausted_configs = set()
        self.completed_configs = set()
        self.failed_configs = set()
        # R17: failure reason per config key, so a report distinguishes a
        # SIGKILLed client from a timeout from a server crash.
        self.failure_reasons = {}
        self.last_failure_reason = None
        self.server_pid = None
        self.test_counter = 0

        # Register cleanup to prevent zombie longbow processes on any exit path.
        # _force_cleanup uses port-scoped lsof so it is safe for parallel benchmark runs.
        atexit.register(self._force_cleanup)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)


if __name__ == "__main__":
    main(BenchmarkRunner)
