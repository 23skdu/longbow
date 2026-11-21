"""GraphQL, REST search endpoints, rerank evaluation, and provenance helpers."""

from typing import Union, List, Dict, Any, Optional


class RestSearchMixin:
    # ---- Item 2: First-Class REST/HTTP v1 & GraphQL APIs ----

    def graphql(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        operation_name: str = "",
    ) -> Dict[str, Any]:
        """Execute a GraphQL query or mutation via Flight DoAction."""
        return self._do_action_json(
            "GraphQLQuery",
            {
                "query": query,
                "variables": variables or {},
                "operationName": operation_name,
            },
            default={},
        )
    def rest_search(
        self,
        dataset: str,
        vector: List[float],
        k: int = 10,
        http_endpoint: str = "http://127.0.0.1:6000",
        filters: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """Execute a REST v1 JSON vector search against /api/v1/datasets/{dataset}/search."""
        import urllib.parse

        encoded_ds = urllib.parse.quote(dataset, safe="")
        data = self._rest_request(
            "POST",
            f"/api/v1/datasets/{encoded_ds}/search",
            {"vector": vector, "k": k, "filters": filters or []},
            http_endpoint=http_endpoint,
        )
        return data.get("results", [])
    def rest_hybrid_search(
        self,
        dataset: str,
        vector: Optional[List[float]] = None,
        text_query: str = "",
        k: int = 10,
        alpha: float = 0.5,
        rrf_k: int = 60,
        graph_alpha: float = 0.0,
        graph_depth: int = 0,
        raw_hybrid: bool = False,
        filters: Optional[List[Dict[str, Any]]] = None,
        http_endpoint: str = "http://127.0.0.1:6000",
    ) -> List[Dict[str, Any]]:
        """Execute a REST v1 hybrid search against /api/v1/datasets/{dataset}/hybrid."""
        import urllib.parse

        encoded_ds = urllib.parse.quote(dataset, safe="")
        payload: Dict[str, Any] = {
            "text_query": text_query,
            "k": k,
            "alpha": alpha,
            "rrf_k": rrf_k,
            "graph_alpha": graph_alpha,
            "graph_depth": graph_depth,
            "raw_hybrid": raw_hybrid,
        }
        if vector is not None:
            payload["vector"] = vector
        if filters:
            payload["filters"] = filters
        return self._rest_request(
            "POST",
            f"/api/v1/datasets/{encoded_ds}/hybrid",
            payload,
            http_endpoint=http_endpoint,
        ).get("results", [])
    def rest_range_search(
        self,
        dataset: str,
        vector: List[float],
        threshold: float,
        max_results: int = 100,
        filters: Optional[List[Dict[str, Any]]] = None,
        http_endpoint: str = "http://127.0.0.1:6000",
    ) -> List[Dict[str, Any]]:
        """Execute a REST v1 range search against /api/v1/datasets/{dataset}/range-search."""
        import urllib.parse

        encoded_ds = urllib.parse.quote(dataset, safe="")
        return self._rest_request(
            "POST",
            f"/api/v1/datasets/{encoded_ds}/range-search",
            {
                "vector": vector,
                "threshold": threshold,
                "max_results": max_results,
                "filters": filters or [],
            },
            http_endpoint=http_endpoint,
        ).get("results", [])
    def rest_search_by_id(
        self,
        dataset: str,
        id: Union[int, str],
        k: int = 10,
        filters: Optional[List[Dict[str, Any]]] = None,
        http_endpoint: str = "http://127.0.0.1:6000",
    ) -> List[Dict[str, Any]]:
        """Execute a REST v1 search-by-id against /api/v1/datasets/{dataset}/search/by-id."""
        import urllib.parse

        encoded_ds = urllib.parse.quote(dataset, safe="")
        return self._rest_request(
            "POST",
            f"/api/v1/datasets/{encoded_ds}/search/by-id",
            {"id": str(id), "k": k, "filters": filters or []},
            http_endpoint=http_endpoint,
        ).get("results", [])
    def rest_text_search(
        self,
        dataset: str,
        query: str,
        k: int = 10,
        facet_fields: Optional[List[str]] = None,
        fuzzy_max_dist: int = 1,
        http_endpoint: str = "http://127.0.0.1:6000",
    ) -> Dict[str, Any]:
        """Execute a REST v1 full-text search against /api/v1/datasets/{dataset}/text-search."""
        import urllib.parse

        encoded_ds = urllib.parse.quote(dataset, safe="")
        payload: Dict[str, Any] = {
            "query": query,
            "k": k,
            "fuzzy_max_dist": fuzzy_max_dist,
        }
        if facet_fields:
            payload["facet_fields"] = facet_fields
        return self._rest_request(
            "POST",
            f"/api/v1/datasets/{encoded_ds}/text-search",
            payload,
            http_endpoint=http_endpoint,
        )
    def rest_geo_search(
        self,
        dataset: str,
        search_type: str = "radius",
        center: Optional[Dict[str, float]] = None,
        radius_km: float = 1.0,
        box: Optional[Dict[str, float]] = None,
        query_vector: Optional[List[float]] = None,
        k: int = 10,
        http_endpoint: str = "http://127.0.0.1:6000",
    ) -> List[Dict[str, Any]]:
        """Execute a REST v1 geospatial search against /api/v1/datasets/{dataset}/geo-search."""
        import urllib.parse

        encoded_ds = urllib.parse.quote(dataset, safe="")
        payload: Dict[str, Any] = {"search_type": search_type, "radius_km": radius_km, "k": k}
        if center:
            payload["center"] = center
        if box:
            payload["box"] = box
        if query_vector is not None:
            payload["query_vector"] = query_vector
        return self._rest_request(
            "POST",
            f"/api/v1/datasets/{encoded_ds}/geo-search",
            payload,
            http_endpoint=http_endpoint,
        ).get("results", [])
    def rest_temporal_search(
        self,
        dataset: str,
        search_type: str = "as_of",
        timestamp: int = 0,
        start_time: int = 0,
        end_time: int = 0,
        window_size: int = 0,
        duration: Optional[Union[str, int]] = None,
        k: int = 10,
        http_endpoint: str = "http://127.0.0.1:6000",
    ) -> List[Dict[str, Any]]:
        """Execute a REST v1 temporal search against /api/v1/datasets/{dataset}/temporal-search."""
        import urllib.parse

        encoded_ds = urllib.parse.quote(dataset, safe="")
        payload: Dict[str, Any] = {
            "search_type": search_type,
            "timestamp": timestamp,
            "start_time": start_time,
            "end_time": end_time,
            "window_size": window_size,
            "k": k,
        }
        if duration is not None:
            payload["duration"] = duration
        return self._rest_request(
            "POST",
            f"/api/v1/datasets/{encoded_ds}/temporal-search",
            payload,
            http_endpoint=http_endpoint,
        ).get("results", [])
    def rest_multimodal_search(
        self,
        dataset: str,
        modality: str = "text",
        text_query: str = "",
        image_base64: str = "",
        audio_base64: str = "",
        k: int = 10,
        http_endpoint: str = "http://127.0.0.1:6000",
    ) -> List[Dict[str, Any]]:
        """Execute a REST v1 multi-modal search against /api/v1/datasets/{dataset}/multimodal-search."""
        import urllib.parse

        encoded_ds = urllib.parse.quote(dataset, safe="")
        return self._rest_request(
            "POST",
            f"/api/v1/datasets/{encoded_ds}/multimodal-search",
            {
                "modality": modality,
                "text_query": text_query,
                "image_base64": image_base64,
                "audio_base64": audio_base64,
                "k": k,
            },
            http_endpoint=http_endpoint,
        ).get("results", [])
    def rest_federated_search(
        self,
        vector: List[float],
        collections: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        k: int = 10,
        rrf_k: int = 60,
        http_endpoint: str = "http://127.0.0.1:6000",
    ) -> List[Dict[str, Any]]:
        """Execute a REST v1 federated cross-collection search against /api/v1/search/federated."""
        payload: Dict[str, Any] = {"vector": vector, "k": k, "rrf_k": rrf_k}
        if collections:
            payload["collections"] = collections
        if tags:
            payload["tags"] = tags
        return self._rest_request(
            "POST",
            "/api/v1/search/federated",
            payload,
            http_endpoint=http_endpoint,
        ).get("results", [])
    def ingest_media_parquet(self, dataset: str, parquet_path: str) -> Dict[str, Any]:
        """Bulk-ingest multi-modal items from a Parquet file."""
        return self._do_action_json(
            "MultiModalIngestParquet",
            {"dataset": dataset, "parquet_path": parquet_path},
            default={},
        )
    def evaluate_reranker(
        self,
        dataset: str,
        queries: List[Dict[str, Any]],
        reranker_type: str = "cross_encoder",
        top_k: int = 10,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Evaluate reranker NDCG@10, MRR@10, and Recall@10 over relevance-labeled queries."""
        payload: Dict[str, Any] = {
            "dataset": dataset,
            "type": reranker_type,
            "top_k": top_k,
            "queries": queries,
        }
        payload.update(kwargs)
        return self._do_action_json("EvaluateReranker", payload, default={})
    def record_provenance(
        self,
        dataset: str,
        vector_id: str,
        source_document_id: str = "",
        source_uri: str = "",
        chunk_index: int = 0,
        embedding_model: str = "",
        model_version: str = "",
        extra: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Record lineage/provenance metadata for a stored vector."""
        return self._do_action_json(
            "RecordProvenance",
            {
                "dataset": dataset,
                "vector_id": vector_id,
                "source_document_id": source_document_id,
                "source_uri": source_uri,
                "chunk_index": chunk_index,
                "embedding_model": embedding_model,
                "model_version": model_version,
                "extra": extra or {},
            },
            default={},
        )
    def debug_search_path(
        self,
        dataset: str,
        vector: Optional[List[float]] = None,
        text_query: str = "",
        k: int = 5,
    ) -> Dict[str, Any]:
        """Return detailed HNSW traversal hop trace and score breakdown via DebugSearchPath."""
        return self._do_action_json(
            "DebugSearchPath",
            {
                "dataset": dataset,
                "vector": vector or [],
                "text_query": text_query,
                "k": k,
                "trace_hops": True,
            },
            default={},
        )
    @staticmethod
    def plot_search_explanation(explanation: Dict[str, Any]) -> str:
        """Render an HTML/SVG score breakdown and HNSW hop-trace visualization from explain_search."""
        hits = explanation.get("hits", [])
        hops = explanation.get("hops", [])
        dataset = explanation.get("dataset", "")
        bars_svg = []
        for idx, hit in enumerate(hits):
            vid = hit.get("id", str(idx))
            bd = hit.get("breakdown", {})
            v_score = float(bd.get("vector_score", 0.0))
            b_score = float(bd.get("bm25_score", 0.0))
            g_score = float(bd.get("graph_boost", 0.0))
            y = 25 + idx * 32
            w_v = max(0, min(200, int(v_score * 160)))
            w_b = max(0, min(200, int(b_score * 40)))
            w_g = max(0, min(200, int(g_score * 80)))
            bars_svg.append(
                f'<g transform="translate(0,{y})">'
                f'<text x="8" y="16" font-family="monospace" font-size="12">{vid}</text>'
                f'<rect x="90" y="4" width="{w_v}" height="16" fill="#3b82f6" />'
                f'<rect x="{90 + w_v}" y="4" width="{w_b}" height="16" fill="#10b981" />'
                f'<rect x="{90 + w_v + w_b}" y="4" width="{w_g}" height="16" fill="#f59e0b" />'
                f"</g>"
            )
        hop_items = "".join(
            f"<li>Step {h.get('step', i)}: node <code>{h.get('node_id', '')}</code> "
            f"(dist={h.get('distance', 0):.4f}, improved={h.get('improved', False)})</li>"
            for i, h in enumerate(hops)
        )
        svg_height = max(80, 40 + len(hits) * 32)
        return (
            f'<div class="longbow-explain-viz" data-dataset="{dataset}">'
            f'<svg width="480" height="{svg_height}" xmlns="http://www.w3.org/2000/svg">'
            + "".join(bars_svg)
            + "</svg>"
            + f"<ul class=\"longbow-hop-trace\">{hop_items}</ul></div>"
        )
