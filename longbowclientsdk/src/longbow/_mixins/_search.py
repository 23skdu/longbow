"""Vector, hybrid, text, geo, temporal, multimodal, and explainability search methods."""

import pandas as pd
from typing import Union, List, Dict, Any, Optional
from ..exceptions import (
    LongbowNotFoundError,
)


class SearchMixin:
    def search(
        self,
        dataset: str,
        vector: Any = None,
        k: int = 10,
        filters: Optional[List[Dict]] = None,
        projection: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Perform a dense, sparse (alpha=0 + text_query), filtered, or hybrid search.

        Args:
            dataset: Target dataset.
            vector: Query vector (optional for pure sparse BM25 search with alpha=0.0).
            k: Number of results.
            filters: Optional filter criteria.
            projection: Optional list of columns to return (reduces bandwidth).
            **kwargs: Extra arguments (e.g. 'alpha', 'text_query', 'rrf_k', 'graph_alpha', 'graph_depth', 'raw_hybrid').

        Returns:
            pandas.DataFrame: Dataframe containing search results.
        """
        query_vec = self._serialize_vector(vector)

        req: Dict[str, Any] = {
            "dataset": dataset,
            "k": k,
        }
        if query_vec is not None:
            req["vector"] = query_vec

        if filters:
            req["filters"] = filters

        if projection:
            req["projection"] = projection

        # Merge extra args (e.g. alpha, text_query, rrf_k, graph_alpha, graph_depth)
        for kw, val in kwargs.items():
            if val is not None:
                req[kw] = val

        # Handle efSearch PID tuning if requested
        if kwargs.get("ef_search_pid", False):
            req["ef_search_pid"] = True

        return self._do_get_df({"search": req}, "Search")
    def geo_search(
        self,
        dataset: str,
        center: Optional[Dict[str, float]] = None,
        radius_km: Optional[float] = None,
        box: Optional[Dict[str, float]] = None,
        search_type: str = "radius",
        k: int = 10,
        filters: Optional[List[Dict]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Perform a Geospatial search (radius, bounding box, or geo+vector hybrid).
        """
        geo_req: Dict[str, Any] = {
            "dataset": dataset,
            "k": k,
            "search_type": search_type,
        }
        if center:
            geo_req["center"] = center
        if radius_km is not None:
            geo_req["radius_km"] = radius_km
        if box:
            geo_req["box"] = box
        if filters:
            geo_req["filters"] = filters
        geo_req.update(kwargs)

        return self._do_get_df({"geo_search": geo_req}, "Geo-Search")
    def get_vector(
        self,
        dataset: str,
        id: Union[int, str],
        http_endpoint: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch a stored vector record by document ID via REST v1 or Arrow Flight filter."""
        if http_endpoint:
            import urllib.parse

            ds_q = urllib.parse.quote(dataset, safe="")
            id_q = urllib.parse.quote(str(id), safe="")
            return self._rest_request(
                "GET", f"/api/v1/datasets/{ds_q}/vectors/{id_q}", http_endpoint=http_endpoint
            )

        table = self.download_arrow(
            dataset, filter=[{"field": "id", "operator": "=", "value": str(id)}]
        )
        rows = table.to_pylist()
        if not rows:
            raise LongbowNotFoundError(f"Vector {id!r} not found in dataset {dataset!r}")
        return rows[0]
    def search_by_id(
        self,
        dataset: str,
        id: Union[int, str],
        k: int = 10,
        filters: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Search for similar vectors by ID."""
        req: Dict[str, Any] = {"dataset": dataset, "id": id, "k": k}
        if filters:
            req["filters"] = filters
        return self._do_action_json("VectorSearchByID", req, default={})
    def search_by_id_df(
        self,
        dataset: str,
        id: Union[int, str],
        k: int = 10,
        filters: Optional[List[Dict[str, Any]]] = None,
    ) -> pd.DataFrame:
        """Search for similar vectors by ID via Arrow Flight DoGet returning a DataFrame."""
        req: Dict[str, Any] = {"dataset": dataset, "id": id, "k": k}
        if filters:
            req["filters"] = filters
        return self._do_get_df({"search_by_id": req}, "SearchByID")
    def range_search(
        self,
        dataset: str,
        vector: Any,
        threshold: float,
        max_results: int = 100,
        filters: Optional[List[Dict[str, Any]]] = None,
        filter_expr: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """Find all vectors within `threshold` distance of `vector`."""
        req: Dict[str, Any] = {
            "dataset": dataset,
            "vector": self._serialize_vector(vector) or [],
            "threshold": float(threshold),
            "max_results": max_results,
        }
        if filters:
            req["filters"] = filters
        if filter_expr:
            req["filter_expr"] = filter_expr
        return self._do_get_df({"range_search": req}, "RangeSearch")
    def federated_search(
        self,
        vector: Any,
        collections: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        k: int = 10,
        rrf_k: int = 60,
    ) -> pd.DataFrame:
        """Search across multiple collections (or routed tags) and merge via RRF."""
        req: Dict[str, Any] = {
            "vector": self._serialize_vector(vector) or [],
            "k": k,
            "rrf_k": rrf_k,
        }
        if collections:
            req["collections"] = collections
        if tags:
            req["tags"] = tags
        return self._do_get_df({"federated_search": req}, "FederatedSearch")
    def list_federated_collections(self) -> List[Dict[str, Any]]:
        """List federated collections registered with the server."""
        return self._do_action_json("ListFederatedCollections", {}, default={}).get(
            "collections", []
        )
    def register_federated_collection(
        self,
        name: str,
        description: str = "",
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Register a dataset and routing tags in the federated query router."""
        payload: Dict[str, Any] = {"dataset": name, "name": name}
        if description:
            payload["description"] = description
        if tags:
            payload["tags"] = tags
        if metadata:
            payload["metadata"] = metadata
        return self._do_action_json("RegisterFederatedCollection", payload, default={})
    def advanced_text_search(
        self,
        dataset: str,
        query: str,
        k: int = 10,
        fuzzy_max_dist: int = 1,
        highlight_tag: str = "<mark>",
        snippet_chars: int = 160,
        facet_fields: Optional[List[str]] = None,
        bm25: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """Execute advanced BM25 full-text search (boolean AST, phrases, fuzzy, snippets, facets)."""
        payload: Dict[str, Any] = {
            "dataset": dataset,
            "query": query,
            "k": k,
            "fuzzy_max_dist": fuzzy_max_dist,
            "highlight_tag": highlight_tag,
            "snippet_chars": snippet_chars,
        }
        if facet_fields:
            payload["facet_fields"] = facet_fields
        if bm25:
            payload["bm25"] = bm25
        return self._do_action_json("AdvancedTextSearch", payload, default={})
    def search_facets(
        self,
        dataset: str,
        query: str,
        facet_fields: Optional[List[str]] = None,
        k: int = 10,
    ) -> Dict[str, Any]:
        """Compute faceted metadata counts matching an advanced full-text query."""
        payload: Dict[str, Any] = {"dataset": dataset, "query": query, "k": k}
        if facet_fields:
            payload["facet_fields"] = facet_fields
        return self._do_action_json("SearchFacets", payload, default={})
    def search_analytics(self, limit: int = 20) -> Dict[str, Any]:
        """Retrieve query popularity and zero-result search analytics."""
        return self._do_action_json("SearchAnalytics", {"limit": limit}, default={})
    def tune_bm25(self, dataset: str, k1: float = 1.2, b: float = 0.75) -> Dict[str, Any]:
        """Tune per-dataset BM25 scoring parameters k1 and b."""
        return self._do_action_json(
            "TuneBM25", {"dataset": dataset, "k1": k1, "b": b}, default={}
        )
    def temporal_search(
        self,
        search_type: str,
        timestamp: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        window_size: Optional[int] = None,
        duration: Optional[str] = None,
        k: int = 10,
        dataset: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Perform a temporal search on the temporal index.

        Args:
            search_type: Type of temporal search - "as_of", "range", "sliding_window", "sliding_window_time"
            timestamp: Timestamp for as_of search (unix nanoseconds)
            start_time: Start time for range search (unix nanoseconds)
            end_time: End time for range search (unix nanoseconds)
            window_size: Window size for sliding_window
            duration: Duration for sliding_window_time (e.g., "1h", "30m")
            k: Number of results
            dataset: Target dataset (optional, auto-inferred if omitted)

        Returns:
            List of search results with id, distance, and score
        """
        if dataset is None:
            namespaces = self.list_namespaces()
            dataset = namespaces[0] if namespaces else ""

        req: Dict[str, Any] = {
            "dataset": dataset,
            "search_type": search_type,
            "k": k,
        }

        if timestamp is not None:
            req["timestamp"] = timestamp
        if start_time is not None:
            req["start_time"] = start_time
        if end_time is not None:
            req["end_time"] = end_time
        if window_size is not None:
            req["window_size"] = window_size
        if duration is not None:
            req["duration"] = duration

        return self._do_action_json("TemporalSearch", req, default=[])
    def temporal_version_history(self, vector_id: int, dataset: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get version history for a vector.

        Args:
            vector_id: ID of the vector
            dataset: Target dataset (optional, auto-inferred if omitted)

        Returns:
            List of versioned vectors with timestamps
        """
        if dataset is None:
            namespaces = self.list_namespaces()
            dataset = namespaces[0] if namespaces else ""

        return self._do_action_json(
            "TemporalVersionHistory",
            {"dataset": dataset, "vector_id": vector_id},
            default=[],
        )
    def temporal_aggregation(
        self,
        aggregation_type: str,
        start_time: int,
        end_time: int,
        interval: Optional[int] = None,
        dataset: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Perform temporal aggregation (count, min, max, mean) over time buckets.

        Args:
            aggregation_type: Type of aggregation - "count", "min", "max", "mean"
            start_time: Start time (unix nanoseconds)
            end_time: End time (unix nanoseconds)
            interval: Bucket interval in nanoseconds (default: 1 hour)
            dataset: Target dataset (optional, auto-inferred if omitted)

        Returns:
            Dictionary with buckets and total_count
        """
        if dataset is None:
            namespaces = self.list_namespaces()
            dataset = namespaces[0] if namespaces else ""

        req: Dict[str, Any] = {
            "dataset": dataset,
            "aggregation_type": aggregation_type,
            "start_time": start_time,
            "end_time": end_time,
        }

        if interval is not None:
            req["interval"] = interval

        return self._do_action_json("TemporalAggregation", req, default={})
    def ingest_media(
        self,
        dataset: str,
        items: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Ingest multi-modal media items (image/audio bytes, paths, URIs, captions)."""
        import base64
        from pathlib import Path

        encoded_items = []
        for item in items:
            copy_item = dict(item)
            blob = copy_item.pop("blob", None)
            if isinstance(blob, (bytes, bytearray)):
                copy_item["blob_base64"] = base64.b64encode(bytes(blob)).decode("ascii")
            elif isinstance(blob, Path):
                copy_item["blob_base64"] = base64.b64encode(blob.read_bytes()).decode("ascii")
            encoded_items.append(copy_item)
        return self._do_action_json(
            "MultiModalIngest",
            {"dataset": dataset, "items": encoded_items},
            default={},
        )
    def multimodal_search(
        self,
        dataset: str,
        text_query: str = "",
        image_bytes: Optional[bytes] = None,
        audio_bytes: Optional[bytes] = None,
        modality: str = "",
        k: int = 10,
    ) -> List[Dict[str, Any]]:
        """Perform cross-modal text-to-image, image-to-text, or audio search."""
        import base64

        payload: Dict[str, Any] = {
            "dataset": dataset,
            "text_query": text_query,
            "modality": modality,
            "k": k,
        }
        if image_bytes is not None:
            payload["image_base64"] = base64.b64encode(image_bytes).decode("ascii")
        if audio_bytes is not None:
            payload["audio_base64"] = base64.b64encode(audio_bytes).decode("ascii")
        return self._do_action_json("MultiModalSearch", payload, default={}).get(
            "results", []
        )
    def rerank(
        self,
        query: str,
        results: List[Dict[str, Any]],
        strategy: str = "hybrid",
        top_k: int = 10,
    ) -> Dict[str, Any]:
        """Rerank search results with hybrid/cross-encoder/heuristic strategy and caching."""
        return self._do_action_json(
            "Rerank",
            {"query": query, "results": results, "strategy": strategy, "top_k": top_k},
            default={},
        )
    def rewrite_query(self, query: str, mode: str = "synonyms") -> List[str]:
        """Expand or rewrite a search query via synonyms, multi_query, or hyde."""
        return self._do_action_json(
            "RewriteQuery", {"query": query, "mode": mode}, default={}
        ).get("queries", [])
    def explain_search(
        self,
        dataset: str,
        vector: Optional[List[float]] = None,
        text_query: str = "",
        k: int = 10,
        alpha: float = 0.7,
        graph_alpha: float = 0.0,
        max_hops: int = 16,
    ) -> Dict[str, Any]:
        """Run a search with score breakdowns, HNSW traversal hops, and vector provenance."""
        return self._do_action_json(
            "ExplainSearch",
            {
                "dataset": dataset,
                "vector": vector or [],
                "text_query": text_query,
                "k": k,
                "alpha": alpha,
                "graph_alpha": graph_alpha,
                "max_hops": max_hops,
            },
            default={},
        )
    def get_provenance(
        self, dataset: str, vector_id: str = "", limit: int = 100
    ) -> Dict[str, Any]:
        """Retrieve vector provenance (model, version, source URI, timestamps, lineage)."""
        return self._do_action_json(
            "GetProvenance",
            {"dataset": dataset, "vector_id": vector_id, "limit": limit},
            default={},
        )
    @staticmethod
    def format_search_explanation(explanation: Dict[str, Any]) -> str:
        """Format an ExplainSearch response as a readable table + HNSW hop trace."""
        lines = [
            f"Dataset: {explanation.get('dataset')} | Index: {explanation.get('index_type')} | "
            f"Entry: {explanation.get('entry_point')} | Hops: {explanation.get('total_hops')} | "
            f"Elapsed: {explanation.get('elapsed_ms', 0):.2f} ms"
        ]
        for hop in explanation.get("hops", []):
            lines.append(
                f"  Hop {hop.get('step')}: node={hop.get('node_id')} neighbors={hop.get('neighbor_count')} "
                f"dist={hop.get('estimated_dist', 0):.4f} improved={hop.get('improved_best')}"
            )
        for cand in explanation.get("candidates", []):
            bd = cand.get("breakdown", {})
            prov = cand.get("provenance") or {}
            lines.append(
                f"  Hit id={cand.get('user_id', cand.get('id'))}: final={bd.get('final_score', 0):.4f} "
                f"(dense={bd.get('dense_score', 0):.4f}, sparse={bd.get('sparse_score', 0):.4f}) "
                f"model={prov.get('model', 'n/a')} src={prov.get('source_uri', 'n/a')}"
            )
        return "\n".join(lines)
