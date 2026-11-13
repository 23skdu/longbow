"""Distance-threshold range search benchmark scenario."""

import time

from ._mode_runner import (
    HAS_ANALYSIS_LIBS,
    run_scenario_matrix,
    summarize_latencies,
)

if HAS_ANALYSIS_LIBS:
    import numpy as np


def _run_range_queries(client, orchestrator, dim, count, label):
    rng = np.random.default_rng(42)
    vectors = rng.random((count, dim), dtype=np.float32).tolist()
    dataset = f"range_bench_{dim}_{count}"
    client.insert(
        dataset,
        [{"id": str(i), "vector": vectors[i]} for i in range(count)],
    )
    time.sleep(2)

    num_queries = min(getattr(orchestrator.args, "queries", 100), 100)
    threshold = float(dim) * 0.25
    latencies = []
    total_hits = 0
    start_all = time.perf_counter()
    for q_idx in range(num_queries):
        q = vectors[q_idx % count]
        t0 = time.perf_counter()
        df = client.range_search(dataset, q, threshold=threshold, max_results=20)
        latencies.append((time.perf_counter() - t0) * 1000.0)
        total_hits += len(df)
    elapsed = time.perf_counter() - start_all
    stats = summarize_latencies(latencies, elapsed)
    return {
        "scenario": "range",
        "dim": dim,
        "count": count,
        "dtype": "float32",
        "threshold": threshold,
        "avg_hits": total_hits / max(num_queries, 1),
        **stats,
    }


class RangeScenarioMixin:
    """Scenario mixin for Distance-Threshold Range Search."""

    def execute_range(self):
        run_scenario_matrix(
            self,
            scenario_name="range",
            title="DISTANCE-THRESHOLD RANGE SEARCH BENCHMARK",
            bench_mode="range",
            query_runner=_run_range_queries,
        )
