"""Advanced BM25 full-text search benchmark scenario."""

import json
import time

from ._mode_runner import (
    HAS_ANALYSIS_LIBS,
    run_scenario_matrix,
    summarize_latencies,
)

if HAS_ANALYSIS_LIBS:
    import numpy as np


def _run_text_queries(client, orchestrator, dim, count, label):
    rng = np.random.default_rng(42)
    sample_count = min(count, 5000)
    vectors = rng.random((sample_count, dim), dtype=np.float32).tolist()
    dataset = f"text_bench_{dim}_{count}"
    docs = [
        {
            "id": str(i),
            "vector": vectors[i],
            "text": (
                "vector database hnsw quantized semantic retrieval"
                if i % 2 == 0
                else "apache arrow columnar full text bm25 faceting"
            ),
            "metadata": json.dumps({"category": "electronics" if i % 2 == 0 else "books"}),
        }
        for i in range(sample_count)
    ]
    client.insert(dataset, docs)
    time.sleep(2)

    queries = [
        'vector AND "hnsw"',
        "quantized OR columnar",
        "vectr~1 NOT legacy",
    ]
    num_queries = min(getattr(orchestrator.args, "queries", 50), 50)
    latencies = []
    start_all = time.perf_counter()
    for q_idx in range(num_queries):
        q = queries[q_idx % len(queries)]
        t0 = time.perf_counter()
        _ = client.advanced_text_search(
            dataset,
            q,
            k=10,
            fuzzy_max_dist=1,
            facet_fields=["category"],
        )
        latencies.append((time.perf_counter() - t0) * 1000.0)
    elapsed = time.perf_counter() - start_all
    stats = summarize_latencies(latencies, elapsed)
    return {
        "scenario": "text",
        "dim": dim,
        "count": sample_count,
        "dtype": "float32",
        **stats,
    }


class TextScenarioMixin:
    """Scenario mixin for Advanced BM25 Full-Text Search (Boolean AST, phrase, fuzzy, facets)."""

    def execute_text(self):
        run_scenario_matrix(
            self,
            scenario_name="text",
            title="ADVANCED FULL-TEXT SEARCH (BM25 / FUZZY / FACETS) BENCHMARK",
            bench_mode="advancedtext",
            query_runner=_run_text_queries,
        )

