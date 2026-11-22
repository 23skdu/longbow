"""CLI argument parser and entrypoint helper for unified_benchmark.py."""

import argparse
import os
import platform

from .checkpoint import compare_baseline_file, save_baseline_file
from .memory_guard import DEFAULT_MEMORY_FRACTION
from .reporting import write_cli_markdown_report
from .telemetry import env_seed


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for Longbow Unified Benchmark Orchestrator."""
    parser = argparse.ArgumentParser(description="Longbow Unified Benchmark Orchestrator")
    parser.add_argument(
        "--mode",
        default="cpu",
        help="Benchmark mode(s), comma-separated: cpu, metal, cuda, onnx, recommend, deletion, graphrag, exchange, cluster, temporal, geo, range, federated, text, multimodal, churn, learned_index",
    )
    parser.add_argument(
        "--dims", default="128,768", help="Comma-separated dimensions"
    )
    parser.add_argument(
        "--counts",
        default="1000,5000",
        help="Comma-separated vector counts (uses first)",
    )
    parser.add_argument(
        "--dtypes", default="float32,float16,int8", help="Comma-separated datatypes"
    )
    parser.add_argument(
        "--memory",
        type=str,
        default=None,
        help=(
            "Server memory ceiling, as a size string or a byte count. Accepts "
            "10GB, 10GiB, 512M, or 10737418240. This is authoritative: it takes "
            "precedence over LONGBOW_MAX_MEMORY. When neither is given the ceiling "
            f"defaults to {int(DEFAULT_MEMORY_FRACTION * 100)}%% of detected host RAM "
            "rather than a fixed constant."
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=env_seed(),
        help=(
            "Seed for the benchmark run. It seeds both ends: each config's "
            "corpus, queries and ByID ids (derived from this and the config "
            "label, so a restart mid-run reproduces the same data), and the "
            "server's graph level assignment via LONGBOW_BENCH_SEED, which "
            "also pins the ingest worker pools so the index itself is "
            "reproducible (roadmap Item 31). Defaults to $LONGBOW_BENCH_SEED "
            "when set; otherwise the client uses its own fixed default."
        ),
    )
    parser.add_argument(
        "--memory-default-fraction",
        type=float,
        default=DEFAULT_MEMORY_FRACTION,
        help=(
            "Fraction of host RAM to use as the memory ceiling when neither "
            f"--memory nor LONGBOW_MAX_MEMORY is set (default: {DEFAULT_MEMORY_FRACTION})"
        ),
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1800,
        help="Command timeout in seconds (default 1800)",
    )
    parser.add_argument(
        "--duration", type=int, default=15, help="Duration in seconds per test"
    )
    parser.add_argument(
        "--queries", type=int, default=1000, help="Number of search queries"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for ingest"
    )
    parser.add_argument(
        "--startup-timeout", type=int, default=120, help="Server startup timeout"
    )
    parser.add_argument("--addr", default="127.0.0.1:3000", help="Server address")
    parser.add_argument(
        "--server-binary",
        default=None,
        help="Explicit path to server binary (e.g. bin/longbow_main, bin/longbow_emlgo)",
    )
    parser.add_argument(
        "--emlgo",
        action="store_true",
        help="Enable EMLGo math backend (sets LONGBOW_MATH_DISPATCH=emlgo)",
    )
    parser.add_argument(
        "--metrics-addr",
        default="127.0.0.1:9090",
        help="Prometheus metrics endpoint for learned_index mode (default 127.0.0.1:9090)",
    )
    # Recommend mode specific parameters
    parser.add_argument(
        "--alpha-values",
        default="0.0,0.5,1.0",
        help="Comma-separated alpha values to test (0.0=graph, 1.0=ANN, 0.5=hybrid)",
    )
    parser.add_argument(
        "--k-values",
        default="5,10,20",
        help="Comma-separated k values (number of recommendations)",
    )
    parser.add_argument(
        "--num-seeds",
        type=int,
        default=5,
        help="Number of seed IDs to use for recommendations",
    )
    parser.add_argument(
        "--max-hops",
        type=int,
        default=2,
        help="Maximum BFS hops for graph connectivity",
    )
    parser.add_argument(
        "--decay",
        type=float,
        default=0.5,
        help="Multi-hop connectivity decay factor",
    )
    # Deletion mode parameters
    parser.add_argument(
        "--delete-counts",
        default="100,500,1000",
        help="Comma-separated delete counts to test (for deletion mode)",
    )
    # GraphRAG mode parameters
    parser.add_argument(
        "--graph-alpha-values",
        default="0.0,0.3,0.5,0.7,1.0",
        help="Comma-separated graph_alpha values to test (0.0=disabled, 1.0=full graph)",
    )
    # Cluster mode parameters
    parser.add_argument(
        "--cluster-nodes",
        type=int,
        default=3,
        help="Number of nodes in cluster for cluster mode",
    )
    # Churn (soak test) mode parameters
    parser.add_argument(
        "--churn-payload-sizes",
        default="0,1,4,64,256,1024",
        help="Comma-separated lorem-ipsum payload sizes in KB (0=minimal metadata)",
    )
    parser.add_argument(
        "--churn-cycles",
        default="10",
        help="Number of add/delete cycles per churn test (default 10)",
    )
    parser.add_argument(
        "--churn-chunk-size",
        default="1000",
        help="Number of vectors added/deleted per churn cycle (default 1000)",
    )
    parser.add_argument(
        "--label",
        default="",
        help="Custom label for result files and pprof profiles",
    )
    # Benchmark feature flags
    parser.add_argument(
        "--low-mem",
        action="store_true",
        help="Enable LONGBOW_LOW_MEM=1 for low memory mode",
    )
    parser.add_argument(
        "--use-disk",
        action="store_true",
        help="Enable auto-disk spillover (LONGBOW_AUTO_SPILL_DISK) with 60%% threshold. "
             "Vectors spill to disk when memory pressure exceeds 60%% of LONGBOW_MAX_MEMORY. "
             "IMPORTANT: This does NOT use LONGBOW_USE_DISK=1 (forced disk mode) because "
             "forced disk mode makes HNSW graph construction extremely slow — every distance "
             "computation during indexing requires a disk read. Auto-spill lets HNSW build "
             "in-memory and only spills vectors to disk after indexing is complete.",
    )
    parser.add_argument(
        "--pq-ingest",
        action="store_true",
        help="Enable LONGBOW_PQ_INGEST=1 for PQ encoding during ingest",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable LONGBOW_DEBUG=true for verbose logging",
    )
    parser.add_argument(
        "--learned-samples",
        type=int,
        default=0,
        help="Set LONGBOW_LEARNED_MIN_SAMPLES for learned index",
    )
    parser.add_argument(
        "--learned-confidence",
        type=float,
        default=0.0,
        help="Set LONGBOW_LEARNED_CONFIDENCE_THRESHOLD for learned index",
    )
    parser.add_argument(
        "--learned-interval",
        type=int,
        default=0,
        help="Set LONGBOW_LEARNED_UPDATE_INTERVAL for learned index",
    )
    # Hardware acceleration flags
    parser.add_argument(
        "--rdma",
        action="store_true",
        help="Enable RDMA/RoCEv2 zero-copy ingest",
    )
    parser.add_argument(
        "--iouring",
        action="store_true",
        help="Enable io_uring optimized Parquet snapshots",
    )
    # Benchmarking Infrastructure (Large Scale)
    parser.add_argument(
        "--fbin",
        help="Path to an .fbin file for ingestion benchmarks",
    )
    parser.add_argument(
        "--arrow",
        help="Path to an Arrow IPC file for ingestion benchmarks",
    )
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Only generate the test data files and exit",
    )
    parser.add_argument(
        "--output-dir",
        default="data/generated",
        help="Directory to save generated data files",
    )
    parser.add_argument(
        "--pprof",
        action="store_true",
        help="Enable pprof collection during benchmarks",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=1,
        help="Max retries for benchmark execution (default 1)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=3000,
        help="Base port for server instances (default 3000)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of concurrent search workers (default 8)",
    )
    parser.add_argument(
        "--search-modes",
        type=str,
        default="all",
        help="Comma-separated search modes for cpu/metal/cuda matrix (default: all -> dense, hybrid, sparse, filtered, filteredbool, filteredstring, byid, range, advancedtext, graphrag, globalgraphrag, recommend, geo, geobox, geohybrid, temporal, temporalrange, temporalwindow, multimodal, federated, learnedindex)",
    )
    parser.add_argument(
        "--shuffle-modes",
        action="store_true",
        help="Randomize search mode execution order per run using seed to break mode-order coupling (roadmap R12a)",
    )
    parser.add_argument(
        "--full", action="store_true", help="Run the full release candidate matrix (overrides dims/counts/dtypes if not explicitly set)"
    )
    parser.add_argument(
        "--cache", action="store_true", help="Skip unchanged code paths if result json already exists"
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from checkpoint file, auto-skipping completed configs"
    )
    parser.add_argument(
        "--estimate-memory", action="store_true",
        help="Estimate peak memory per config and skip those exceeding the limit"
    )
    parser.add_argument(
        "--random-port-fallback", action="store_true",
        help="Fall back to a random port if the calculated port is stuck in TIME_WAIT"
    )
    numa_default = False
    try:
        if platform.system() == "Linux":
            numa_default = True
    except Exception:
        pass

    parser.add_argument(
        "--numa-bind",
        dest="numa_bind",
        action="store_true",
        default=numa_default,
        help=f"Enable NUMA binding in benchmarks (default: {numa_default})"
    )
    parser.add_argument(
        "--no-numa-bind",
        dest="numa_bind",
        action="store_false",
        help="Disable NUMA binding in benchmarks"
    )
    parser.add_argument(
        "--numa-compare", action="store_true", help="Run benchmarks with and without NUMA binding to compare"
    )
    parser.add_argument(
        "--cpu-affinity",
        dest="cpu_affinity",
        default=os.environ.get("LONGBOW_CPU_AFFINITY"),
        help="Pin server and benchmark workers to specific CPU cores via taskset (e.g. '12-15')",
    )
    parser.add_argument(
        "--runs", type=int, default=1,
        help="Number of runs per configuration for mean/stdev reporting (default: 1)"
    )
    parser.add_argument(
        "--memory-soak", action="store_true",
        help="Run memory soak test: insert vectors and monitor RSS over time"
    )
    parser.add_argument(
        "--soak-duration", type=int, default=3600,
        help="Duration in seconds for memory soak test (default: 3600 = 1 hour)"
    )
    parser.add_argument(
        "--ci", action="store_true",
        help="Run a reduced 'fast' matrix for CI environments. Equivalent to setting the LONGBOW_BENCH_FAST=1 env var; both activate the same fast-mode defaults (dims=128, counts=10000,50000, dtypes=float32,int8, search_modes=dense)."
    )
    parser.add_argument(
        "--save-baseline", type=str, default=None,
        help="Save benchmark results as a baseline JSON file (e.g., benchmarks/baseline_cpu.json)"
    )
    parser.add_argument(
        "--compare-baseline", type=str, default=None,
        help="Compare results against a baseline JSON file; fail if regression exceeds --threshold"
    )
    parser.add_argument(
        "--threshold", type=float, default=10.0,
        help="Regression threshold percentage for --compare-baseline (default: 10)"
    )
    parser.add_argument(
        "--report-md", type=str, default=None,
        help="Generate a Markdown performance report at the given path (e.g., docs/development/performance.md)"
    )
    return parser


def main(runner_cls) -> None:
    """Parse CLI arguments, execute benchmarks, and write requested outputs."""
    parser = build_arg_parser()
    args = parser.parse_args()

    # LONGBOW_BENCH_FAST=1 is a synonym for --ci, so CI configs can set the
    # env var without editing the script invocation. Accepts 1/true/yes/on
    # (case-insensitive); any other value (including unset) leaves --ci in
    # whatever state argparse produced. The CLI flag and the env var are
    # OR-merged: if either is set, fast mode is on, and fast mode overrides
    # --dims/--counts/--dtypes/--search-modes (mirroring the --ci semantics
    # above).
    if os.environ.get("LONGBOW_BENCH_FAST", "").strip().lower() in ("1", "true", "yes", "on"):
        args.ci = True

    if args.ci:
        args.dims = "128"
        args.counts = "10000,50000"
        args.dtypes = "float32,int8"
        if args.search_modes == "all":
            args.search_modes = "dense"

    runner = runner_cls(args)
    runner.execute()

    output_file = getattr(runner, "output_file", None)
    if args.save_baseline:
        save_baseline_file(output_file, args.save_baseline)
    if args.compare_baseline:
        compare_baseline_file(output_file, args.compare_baseline, args.threshold)
    if args.report_md:
        write_cli_markdown_report(output_file, args.report_md, args.mode)
