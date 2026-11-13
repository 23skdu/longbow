"""Multi-modal cross-modal ingest and search benchmark scenario."""

import time

from ._mode_runner import (
    run_scenario_matrix,
    summarize_latencies,
)


def _run_multimodal_queries(client, orchestrator, dim, count, label):
    dataset = f"mm_bench_{dim}_{count}"
    items = [
        {"id": "mm_0", "modality": "text", "caption": "red sneaker product catalog photo"},
        {"id": "mm_1", "modality": "text", "caption": "wireless audio headphones studio"},
        {"id": "mm_2", "modality": "image", "mime_type": "image/png", "caption": "ocean sunset beach landscape", "blob": b"\x89PNG\r\n\x1a\n" + b"\x01\x02\x03\x04" * 16},
        {"id": "mm_3", "modality": "audio", "mime_type": "audio/wav", "caption": "speech transcription query sample", "blob": b"RIFF\x24\x00\x00\x00WAVEfmt " + b"\x10\x00" * 16},
    ]
    client.ingest_media(dataset, items)
    time.sleep(2)

    num_queries = min(getattr(orchestrator.args, "queries", 100), 100)
    latencies = []
    start_all = time.perf_counter()
    for q_idx in range(num_queries):
        q = "red sneaker photo" if q_idx % 2 == 0 else "ocean sunset beach"
        t0 = time.perf_counter()
        _ = client.multimodal_search(dataset, text_query=q, k=5)
        latencies.append((time.perf_counter() - t0) * 1000.0)
    elapsed = time.perf_counter() - start_all
    stats = summarize_latencies(latencies, elapsed)
    return {
        "scenario": "multimodal",
        "dim": dim,
        "count": len(items),
        "dtype": "float32",
        **stats,
    }


class MultiModalScenarioMixin:
    """Scenario mixin for Multi-Modal / Cross-Modal Search."""

    def execute_multimodal(self):
        run_scenario_matrix(
            self,
            scenario_name="multimodal",
            title="MULTI-MODAL / CROSS-MODAL SEARCH BENCHMARK",
            bench_mode="multimodal",
            query_runner=_run_multimodal_queries,
        )

