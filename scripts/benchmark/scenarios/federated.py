"""Federated cross-collection RRF search benchmark scenario."""

import time

from ._mode_runner import (
    HAS_ANALYSIS_LIBS,
    run_scenario_matrix,
    summarize_latencies,
)

if HAS_ANALYSIS_LIBS:
    import numpy as np


def _run_federated_queries(client, orchestrator, dim, count, label):
    rng = np.random.default_rng(42)
    half = max(count // 2, 10)
    vecs_a = rng.random((half, dim), dtype=np.float32).tolist()
    vecs_b = rng.random((half, dim), dtype=np.float32).tolist()
    ds_a = f"fed_alpha_{dim}_{count}"
    ds_b = f"fed_beta_{dim}_{count}"
    client.insert(ds_a, [{"id": f"a_{i}", "vector": vecs_a[i]} for i in range(half)])
    client.insert(ds_b, [{"id": f"b_{i}", "vector": vecs_b[i]} for i in range(half)])
    client.register_federated_collection(ds_a, description="alpha shard", tags=["bench"])
    client.register_federated_collection(ds_b, description="beta shard", tags=["bench"])
    time.sleep(2)

    num_queries = min(getattr(orchestrator.args, "queries", 100), 100)
    latencies = []
    start_all = time.perf_counter()
    for q_idx in range(num_queries):
        q = vecs_a[q_idx % half]
        t0 = time.perf_counter()
        _ = client.federated_search(q, collections=[ds_a, ds_b], k=10, rrf_k=60)
        latencies.append((time.perf_counter() - t0) * 1000.0)
    elapsed = time.perf_counter() - start_all
    stats = summarize_latencies(latencies, elapsed)
    return {
        "scenario": "federated",
        "dim": dim,
        "count": count,
        "dtype": "float32",
        "collections": [ds_a, ds_b],
        **stats,
    }


class FederatedScenarioMixin:
    """Scenario mixin for Federated Cross-Collection Search."""

    def execute_federated(self):
        run_scenario_matrix(
            self,
            scenario_name="federated",
            title="FEDERATED CROSS-COLLECTION SEARCH BENCHMARK",
            bench_mode="federated",
            query_runner=_run_federated_queries,
        )

