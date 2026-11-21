"""Benchmark provenance collection and concurrency self-validation."""

import hashlib
import os
import platform
import subprocess

# Provenance and self-validation (roadmap R2/R3).
#
# A QPS number without the parameters it was measured under cannot gate a
# regression, because a change in any of them moves the number as much as a
# change in the code does. The harness previously recorded only dims, counts,
# dtypes and duration, so two runs of the same binary on differently-affinitised
# cores or with a different worker count were indistinguishable from a code
# regression. collect_provenance records the rest.

# Environment variables that change server behaviour and must therefore be
# recorded with every number.
PROVENANCE_ENV_VARS = (
    "LONGBOW_CPU_AFFINITY",
    "LONGBOW_MAX_MEMORY",
    "LONGBOW_MAX_MEMORY_HARD",
    "LONGBOW_AUTO_SPILL_DISK",
    "LONGBOW_SPILL_THRESHOLD_RATIO",
    "LONGBOW_USE_DISK",
    "LONGBOW_HNSW_M",
    "LONGBOW_HNSW_MMAX",
    "LONGBOW_HNSW_MMAX0",
    "LONGBOW_HNSW_EF_CONSTRUCTION",
    "LONGBOW_HNSW_BULK_CHAIN_LINKS",
    "LONGBOW_HNSW_ENSURE_INBOUND_EDGE",
    "LONGBOW_MATH_DISPATCH",
    "LONGBOW_GPU_ENABLED",
    "GOMAXPROCS",
)


def env_seed():
    """Read LONGBOW_BENCH_SEED, or None when it is unset or not an integer.

    One seed for the whole benchmark: the harness passes it to the client as
    the per-config corpus/query seed and to the server as the graph level seed.
    (roadmap Item 31)
    """
    v = os.environ.get("LONGBOW_BENCH_SEED")
    if not v:
        return None
    try:
        return int(v)
    except ValueError:
        print(f"Warning: LONGBOW_BENCH_SEED={v!r} is not an integer, ignoring")
        return None


def derive_config_seed(base_seed, label):
    """Derive the per-config seed from the run seed and the config label.

    A pure function of the two, so a config's corpus depends on what the config
    is rather than on where it sat in the matrix or which configs ran before it.
    That is what makes a harness restart mid-run reproduce the same corpus
    instead of shifting every later config's data. (roadmap Item 31)
    """
    digest = hashlib.sha256(f"{base_seed}:{label}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % (2**62 - 1)


def git_revision(repo_dir=None):
    """Return (revision, dirty) for the working tree, or (None, None)."""
    try:
        rev = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_dir, capture_output=True, text=True, timeout=15,
        )
        if rev.returncode != 0:
            return None, None
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo_dir, capture_output=True, text=True, timeout=15,
        )
        return rev.stdout.strip(), bool(status.stdout.strip())
    except Exception:
        return None, None


def binary_provenance(path):
    """Return size and mtime for a binary, so two builds are distinguishable."""
    if not path:
        return None
    try:
        st = os.stat(path)
        return {"path": str(path), "size": st.st_size, "mtime": int(st.st_mtime)}
    except OSError:
        return {"path": str(path), "size": None, "mtime": None}


def collect_provenance(args):
    """Build the provenance block recorded next to every benchmark result.

    Anything recorded here can change the numbers without any code change, so it
    is what makes a later revision-to-revision A/B meaningful (roadmap R1).
    """
    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    rev, dirty = git_revision(repo)

    affinity = getattr(args, "cpu_affinity", None) or os.environ.get("LONGBOW_CPU_AFFINITY")

    prov = {
        "revision": rev,
        "revision_dirty": dirty,
        "binary": binary_provenance(getattr(args, "bench_tool", None) or os.environ.get("LONGBOW_BENCH_TOOL")),
        "workers": getattr(args, "workers", None),
        "queries": getattr(args, "queries", None),
        "cpu_affinity": affinity,
        "search_modes_requested": getattr(args, "search_modes", None),
        # R12: the resolved order matters as much as the set, because the modes
        # before the one under test decide its cache and GC state. A 9-mode
        # baseline and a 13-mode run are not comparable.
        "search_modes_resolved": getattr(args, "resolved_search_modes", None),
        "runs": getattr(args, "runs", None),
        "seed": getattr(args, "seed", None),
        "seed_env": os.environ.get("LONGBOW_BENCH_SEED"),
        "shuffle_modes": getattr(args, "shuffle_modes", False),
        "duration": getattr(args, "duration", None),
        "numa_bind": getattr(args, "numa_bind", None),
        "mode": getattr(args, "mode", None),
        "python_version": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "env": {k: os.environ[k] for k in PROVENANCE_ENV_VARS if k in os.environ},
    }
    return prov


def validate_concurrency_invariant(results, workers):
    """Check QPS x mean_latency ~= workers for every search result row.

    QPS and latency are only meaningful together with the worker count that
    produced them: with W concurrent workers, throughput is completed/duration and
    mean latency is sum/completed, so

        QPS x mean_latency == W

    holds by construction. That identity is what makes this a useful provenance
    check: a row violating it means the worker count was recorded wrongly, or the
    QPS and the latency came from different runs - both of which silently
    invalidate every regression decision made against the file (roadmap R3).

    It previously checked QPS x P50 <= W x 1000 instead, which was wrong. The
    physical bound applies to the mean; P50 is a percentile and carries no such
    bound. The consequence was that the check fired on any right-skewed latency
    distribution - the normal case - and rejected valid reports. A real example
    from the 13-mode matrix: `sparse` at 20k, 4 workers, recorded 5424.9 QPS with
    P50 0.778ms, giving 4221 > 4000 and refusing the report, while its p99/p50
    ratio was 1.26 and the run was entirely sound.

    The identity is a band, not an equality, because QPS x mean = sum(latencies)
    / wall_duration, which equals W only if the workers were busy for the entire
    window. They never quite are: goroutine start-up, and the last in-flight query
    straddling the end of the window, both leave the workers briefly idle. Measured
    across all 13 search modes at 20k on 4 workers, the implied worker count ran
    3.66-3.96 against a recorded 4 - consistently 92-99% of it, never above.

    So the upper bound is hard (implied cannot exceed W: at most W requests are in
    flight) and the lower bound is a floor, set well below the observed floor of
    92% to absorb run-to-run variation while still catching the failure the check
    exists for - a row whose worker count was mis-recorded, or whose QPS and
    latency came from different runs.

    Rows recorded before mean existed fall back to P50 and are reported as a note
    rather than silently passed on a weaker check.

    Returns a list of human-readable violations; empty means the file is sound.
    """
    if not workers or workers <= 0:
        return [f"no worker count recorded, cannot validate the QPS x mean ~= workers {workers} identity"]

    limit = float(workers)
    busy_floor = 0.75  # implied worker count must be at least this fraction of W
    violations = []
    stale = []

    def check(where, mode, qps, mean, p50):
        if not qps or qps <= 0:
            return
        latency = mean if mean and mean > 0 else None
        source = "mean"
        if latency is None:
            # No mean recorded. Fall back to P50, which cannot support a bound;
            # only flag it if even P50 is implausible.
            latency, source = p50, "p50(fallback)"
            if not latency or latency <= 0:
                return
            stale.append(f"{where} mode={mode}")
        implied = qps * latency / 1000.0  # workers actually kept busy
        if implied > limit * 1.02:
            violations.append(
                f"{where} mode={mode}: QPS {qps:.1f} x {source} {latency:.3f}ms "
                f"= {implied:.2f} workers, which exceeds the recorded {limit:.0f}; "
                f"more requests were in flight than there were workers"
            )
        elif implied < limit * busy_floor:
            violations.append(
                f"{where} mode={mode}: QPS {qps:.1f} x {source} {latency:.3f}ms "
                f"= {implied:.2f} workers, only {implied / limit * 100:.0f}% of the recorded "
                f"{limit:.0f}; the numbers are too low to come from a fully busy worker pool, "
                f"so they were not produced by the run the provenance claims"
            )

    for row in results or []:
        if not isinstance(row, dict):
            continue
        where = f"dim={row.get('dim')} count={row.get('count')} dtype={row.get('dtype')}"

        # Rows shaped {"search": {mode: {...}}} - the vector matrix.
        search = row.get("search")
        if isinstance(search, dict):
            for mode, m in search.items():
                if not isinstance(m, dict):
                    continue
                check(where, mode, m.get("qps"), m.get("mean"), m.get("p50"))
            continue

        # Rows with top-level qps/p50, e.g. exchange_search.
        check(where, row.get("operation", "?"), row.get("qps"), row.get("mean"), row.get("p50"))

    if stale and not violations:
        violations.append(
            f"NOTE: {len(stale)} row(s) predate mean_latency and were checked with the "
            f"weaker P50 fallback: {', '.join(stale[:5])}"
            + (" ..." if len(stale) > 5 else "")
        )
    return violations


