import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd
import json
import logging
import math
import warnings
from typing import Union, List, Dict, Any, Optional, Iterator

from .exceptions import LongbowConnectionError, LongbowNotFoundError, LongbowQueryError
from .ingest import to_arrow_table

__all__ = ["LongbowClient"]

logger = logging.getLogger(__name__)


class LongbowClient:
    """Client for interacting with the Longbow Vector Database."""

    def __init__(
        self,
        uri: str = "grpc://localhost:3000",
        meta_uri: Optional[str] = None,
        api_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        tls_root_certs: Optional[bytes] = None,
        tls_cert_chain: Optional[bytes] = None,
        tls_private_key: Optional[bytes] = None,
    ):
        """
        Initialize the Longbow Client.

        Args:
            uri: gRPC URI for the Data Plane (DoPut/DoGet).
            meta_uri: gRPC URI for the Control Plane (DoAction/Info). Defaults to uri.
            api_key: Optional API key for authentication.
            tenant_id: Optional tenant identifier injected as x-tenant-id header.
            tls_root_certs: Optional PEM root CA certificates for TLS/mTLS.
            tls_cert_chain: Optional client certificate chain PEM for mTLS.
            tls_private_key: Optional client private key PEM for mTLS.
        """
        self.uri = uri
        self.meta_uri = meta_uri or uri
        self.api_key = api_key
        self.tenant_id = tenant_id
        self.headers = headers or {}
        self.tls_root_certs = tls_root_certs
        self.tls_cert_chain = tls_cert_chain
        self.tls_private_key = tls_private_key

        self._data_client: Any = None
        self._meta_client: Any = None

    def _require_data_client(self) -> Any:
        if self._data_client is None:
            self.connect()
        if self._data_client is None:
            raise LongbowConnectionError("Failed to initialize data client")
        return self._data_client

    def _require_meta_client(self) -> Any:
        if self._meta_client is None:
            self.connect()
        if self._meta_client is None:
            raise LongbowConnectionError("Failed to initialize meta client")
        return self._meta_client

    def _do_action_json(
        self, action_type: str, payload: Dict[str, Any], default: Any = None
    ) -> Any:
        meta = self._require_meta_client()
        action = flight.Action(action_type, json.dumps(payload).encode("utf-8"))
        try:
            results = list(meta.do_action(action, options=self._get_call_options()))
            if results:
                return json.loads(results[0].body.to_pybytes())
            return {} if default is None else default
        except Exception as e:
            raise LongbowQueryError(f"{action_type} failed: {e}")

    def _do_get_df(self, ticket_payload: Dict[str, Any], label: str) -> pd.DataFrame:
        if self._data_client is None:
            self.connect()
        ticket = flight.Ticket(json.dumps(ticket_payload).encode("utf-8"))
        try:
            reader = self._require_data_client().do_get(
                ticket, options=self._get_call_options()
            )
            return reader.read_all().to_pandas()
        except Exception as e:
            raise LongbowQueryError(f"{label} failed: {e}")

    @staticmethod
    def _serialize_vector(vector: Any) -> Optional[List[float]]:
        if vector is None:
            return None
        np_mod: Any = None
        try:
            import numpy as np

            np_mod = np
        except ImportError:
            pass

        query_vec: Any = vector
        if np_mod is not None and hasattr(query_vec, "dtype"):
            if np_mod.issubdtype(query_vec.dtype, np_mod.complexfloating):
                target_dtype = (
                    np_mod.float32
                    if query_vec.dtype == np_mod.complex64
                    else np_mod.float64
                )
                query_vec = query_vec.view(target_dtype).flatten().tolist()
            elif isinstance(query_vec, np_mod.ndarray):
                query_vec = query_vec.tolist()

        if (
            isinstance(query_vec, list)
            and len(query_vec) > 0
            and isinstance(query_vec[0], complex)
        ):
            flat_vector = []
            for v in query_vec:
                flat_vector.append(float(v.real))
                flat_vector.append(float(v.imag))
            query_vec = flat_vector

        for i, v in enumerate(query_vec):
            if isinstance(v, (int, float)) and not math.isfinite(v):
                raise ValueError(
                    f"Query vector contains invalid value (NaN or Inf) at index {i}"
                )
        return list(query_vec)

    def _rest_request(
        self,
        method: str,
        path: str,
        body: Optional[Dict[str, Any]] = None,
        http_endpoint: str = "http://127.0.0.1:6000",
    ) -> Dict[str, Any]:
        import urllib.request

        if not (
            http_endpoint.startswith("http://") or http_endpoint.startswith("https://")
        ):
            raise ValueError(f"Unsupported http_endpoint scheme: {http_endpoint}")
        url = f"{http_endpoint.rstrip('/')}{path}"
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if self.tenant_id:
            headers["X-Tenant-ID"] = self.tenant_id
        data_bytes = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=15) as resp:  # nosec B310
            return json.loads(resp.read().decode("utf-8"))

    def connect(self):
        """Establish connections to the server."""
        try:
            # Set high limits (1GB) to support large batch transfers
            options = [
                ("grpc.max_receive_message_length", 1024 * 1024 * 1024),
                ("grpc.max_send_message_length", 1024 * 1024 * 1024),
            ]
            kwargs: Dict[str, Any] = {"generic_options": options}
            if self.tls_root_certs is not None:
                kwargs["tls_root_certs"] = self.tls_root_certs
            if self.tls_cert_chain is not None and self.tls_private_key is not None:
                kwargs["cert_chain"] = self.tls_cert_chain
                kwargs["private_key"] = self.tls_private_key
            self._data_client = flight.FlightClient(self.uri, **kwargs)
            self._meta_client = flight.FlightClient(
                self.meta_uri, **kwargs
            )
        except Exception as e:
            raise LongbowConnectionError(f"Failed to connect: {e}")

    def close(self):
        """Close connections."""
        # FlightClient doesn't have an explicit close in older versions, but good to have hook.
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _get_call_options(self, timeout: Optional[float] = None):
        call_headers = []
        for k, v in self.headers.items():
            call_headers.append((k.encode("utf-8"), v.encode("utf-8")))

        if self.tenant_id:
            call_headers.append((b"x-tenant-id", self.tenant_id.encode("utf-8")))

        if self.api_key:
            call_headers.append(
                (b"authorization", f"Bearer {self.api_key}".encode("utf-8"))
            )

        if timeout is not None:
            return flight.FlightCallOptions(headers=call_headers, timeout=timeout)
        return flight.FlightCallOptions(headers=call_headers)

    def insert(
        self,
        dataset: str,
        data: Union[pd.DataFrame, List[Dict]],
        batch_size: int = 10000,
        timeout: float = 180.0,
    ) -> None:
        """
        Insert vectors into a dataset.

        Args:
            dataset: Name of the target dataset.
            data: Data to insert (Pandas DataFrame or List of Dicts).
            batch_size: Batch size for upload chunks.
            timeout: Timeout in seconds for the upload operation (default: 180.0).
        """
        # Handle other types
        table = to_arrow_table(data)
        self._upload_batch(dataset, table, batch_size=batch_size, timeout=timeout)

    def _upload_batch(
        self,
        dataset: str,
        data: Union[pd.DataFrame, List[Dict], pa.Table],
        batch_size: int = 10000,
        timeout: float = 180.0,
    ):
        """Internal helper to upload a materialized batch in streaming chunks with timeout."""
        table: Any = data if isinstance(data, pa.Table) else to_arrow_table(data)
        descriptor = flight.FlightDescriptor.for_path(dataset)
        call_opts = self._get_call_options(timeout=timeout)
        writer, reader = self._require_data_client().do_put(
            descriptor, table.schema, options=call_opts
        )
        for batch in table.to_batches(max_chunksize=batch_size):
            writer.write_batch(batch)
        writer.done_writing()  # Signal completion without blocking

        # Read server acknowledgment if available
        try:
            # FlightMetadataReader.read() returns metadata, not iterable
            _ = reader.read()
        except (StopIteration, AttributeError):
            pass  # No response or reader doesn't support read()
        except Exception as e:
            # Log but don't fail - data was already sent
            logger.debug(f"Could not read server response (non-critical): {e}")

        logger.debug(f"Uploaded batch of {table.num_rows} rows to {dataset}")

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

    def recommend(
        self,
        dataset: str,
        seed_ids: List[str],
        k: int = 10,
        alpha: float = 0.5,
        max_hops: int = 2,
        decay: float = 0.5,
    ) -> "pd.DataFrame":
        """
        Produce a list of recommendations based on seed IDs (hybrid vector-graph closeness).

        Args:
            dataset: The dataset name.
            seed_ids: List of source IDs to use as seeds.
            k: Number of recommendations to return.
            alpha: Hybrid blend (1.0 = pure vector similarity, 0.0 = pure graph connectivity).
            max_hops: BFS depth for graph connectivity.
            decay: Multi-hop connectivity decay factor.

        Returns:
            Pandas DataFrame with 'id' and 'score'.
        """
        if self._data_client is None:
            self.connect()

        req = {
            "dataset": dataset,
            "seed_ids": seed_ids,
            "k": k,
            "alpha": float(alpha),
            "max_hops": int(max_hops),
            "decay": float(decay),
        }

        ticket_json = {"recommend": req}
        ticket = flight.Ticket(json.dumps(ticket_json).encode("utf-8"))

        try:
            reader = self._require_data_client().do_get(
                ticket, options=self._get_call_options()
            )
            table = reader.read_all()
            return table.to_pandas()
        except Exception as e:
            raise LongbowQueryError(f"Recommendation failed: {e}")

    def create_namespace(
        self,
        name: str,
        dims: int = 128,
        data_type: str = "float32",
        force: bool = False,
        **hnsw_config,
    ):
        """
        Create a new dataset/namespace.

        Args:
            name: Name of the namespace.
            dims: Vector dimensions.
            data_type: Storage type (float32, int8, turboquant, etc.).
            force: If True, overwrite if exists.
            **hnsw_config: HNSW parameters (m, ef_construction).
        """
        req: Dict[str, Any] = {
            "name": name,
            "dims": dims,
            "data_type": data_type,
            "overwrite": force,
            "hnsw_config": hnsw_config,
        }
        action_body = json.dumps(req).encode("utf-8")
        action = flight.Action("CreateNamespace", action_body)
        list(self._require_meta_client().do_action(action, options=self._get_call_options()))

    def create_dataset(
        self,
        name: str,
        dimensions: int,
        vector_type: str = "float32",
        turboquant_bits: int = 4,
        geo_enabled: bool = False,
        disk_enabled: bool = False,
        metric: str = "cosine",
    ):
        """
        Create a new dataset with specific feature configurations.

        Args:
            name: Name of the dataset (e.g. "tenant/vectors").
            dimensions: Vector dimensionality.
            vector_type: "float32", "turboquant" (tq), "int8", "float16".
            turboquant_bits: Bit depth for TQ (4 or 8; 1-3 encode but lose the vector
                direction). Defaults to 4, matching the server-side default.
            geo_enabled: Enable geospatial indexing (Quadtree).
            disk_enabled: Enable Disk-ANN offloading.
            metric: Distance metric ("cosine", "l2", "ip").
        """
        req: Dict[str, Any] = {
            "name": name,
            "dimension": dimensions,
            "vector_type": vector_type,
            "turboquant_bits": turboquant_bits,
            "geo_enabled": geo_enabled,
            "disk_enabled": disk_enabled,
            "metric": metric,
        }
        action_body = json.dumps(req).encode("utf-8")
        action = flight.Action("CreateDataset", action_body)
        list(self._require_meta_client().do_action(action, options=self._get_call_options()))
        # Check results if needed

    def list_namespaces(self) -> List[str]:
        """List all available datasets."""
        return [
            f.descriptor.path[0].decode("utf-8")
            for f in self._require_meta_client().list_flights()
        ]

    def list_datasets_in_namespace(self, namespace: str = "default") -> List[str]:
        req = {"name": namespace}
        action_body = json.dumps(req).encode("utf-8")
        action = flight.Action("ListDatasetsInNamespace", action_body)
        results = list(
            self._require_meta_client().do_action(
                action, options=self._get_call_options()
            )
        )

        if not results:
            return []

        resp = json.loads(results[0].body.to_py())
        return resp.get("datasets", [])

    def download_arrow(
        self, dataset: str, filter: Optional[List[Dict]] = None
    ) -> pa.Table:
        """Download dataset as Arrow Table (zero-copy, high performance).

        Args:
            dataset: Name of the dataset to download
            filter: Optional list of filter dictionaries [{"field": "...", "op": "...", "value": "..."}]

        Returns:
            pyarrow.Table: The complete dataset as an Arrow Table

        Example:
            >>> table = client.download_arrow("my_dataset")
            >>> print(f"Downloaded {table.num_rows} rows")
        """
        data_client = self._require_data_client()

        req: Dict[str, Any] = {"name": dataset}
        if filter:
            req["filters"] = filter

        ticket_bytes = json.dumps(req).encode("utf-8")
        ticket = flight.Ticket(ticket_bytes)

        try:
            reader = data_client.do_get(ticket, options=self._get_call_options())
            # Zero-copy: read all batches into single Arrow Table
            return reader.read_all()
        except Exception as e:
            raise LongbowQueryError(f"Download failed: {e}")

    def download_stream(
        self, dataset: str, filter: Optional[List[Dict]] = None
    ) -> Iterator[pa.RecordBatch]:
        """Stream dataset as Arrow RecordBatches (memory-efficient for large datasets).

        Args:
            dataset: Name of the dataset to download
            filter: Optional list of filter dictionaries

        Yields:
            pyarrow.RecordBatch: Individual batches of data

        Example:
            >>> for batch in client.download_stream("large_dataset"):
            ...     print(f"Processing batch with {batch.num_rows} rows")
            ...     # Process batch without loading entire dataset into memory
        """
        data_client = self._require_data_client()

        req: Dict[str, Any] = {"name": dataset}
        if filter:
            req["filters"] = filter

        ticket_bytes = json.dumps(req).encode("utf-8")
        ticket = flight.Ticket(ticket_bytes)

        try:
            reader = data_client.do_get(ticket, options=self._get_call_options())
            # Stream batches one at a time (memory-efficient)
            for chunk in reader:
                yield chunk.data
        except Exception as e:
            raise LongbowQueryError(f"Download stream failed: {e}")

    def download(self, dataset: str, filter: Optional[List[Dict]] = None) -> pa.Table:
        """Download dataset as Arrow Table.

        DEPRECATED: This method now returns pa.Table instead of dd.DataFrame.
        Use download_arrow() for explicit Arrow Table return.
        Use download_stream() for memory-efficient streaming.

        Args:
            dataset: Name of the dataset to download
            filter: Optional list of filter dictionaries

        Returns:
            pyarrow.Table: The complete dataset (changed from dd.DataFrame)
        """
        warnings.warn(
            "download() now returns pa.Table instead of dd.DataFrame. "
            "Use download_arrow() explicitly or download_stream() for streaming.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.download_arrow(dataset, filter)

    def delete(self, dataset: str, ids: Optional[List[int]] = None):
        """Delete specific IDs from a dataset."""
        meta_client = self._require_meta_client()

        req = {"dataset": dataset}
        if ids:
            # Server "delete" action takes "id" (string) and "dataset".
            # It processes one ID at a time.
            # We iterate here.
            # Convert all IDs to string as server expects string IDs (currently).
            for i in ids:
                single_req = {"dataset": dataset, "id": str(i)}
                action_body = json.dumps(single_req).encode("utf-8")
                # ignore result for now, just best effort
                try:
                    action = flight.Action("delete", action_body)
                    list(
                        meta_client.do_action(
                            action, options=self._get_call_options()
                        )
                    )
                except Exception:
                    # Log or warn? For batch delete, partial failure is tricky.
                    # We continue.
                    pass
        else:
            # Delete entire namespace
            action_body = json.dumps(req).encode("utf-8")
            action = flight.Action("DeleteNamespace", action_body)
            list(meta_client.do_action(action, options=self._get_call_options()))

    def delete_namespace(self, dataset: str):
        """Delete an entire dataset."""
        self.delete(dataset)

    def drop_dataset(self, dataset: str):
        """Drop (delete) an entire dataset."""
        req = {"dataset": dataset}
        action_body = json.dumps(req).encode("utf-8")
        action = flight.Action("delete-dataset", action_body)
        list(self._require_meta_client().do_action(action, options=self._get_call_options()))

    def snapshot(self):
        """Trigger a manual snapshot of the database."""
        action = flight.Action("ForceSnapshot", b"")
        list(self._require_meta_client().do_action(action, options=self._get_call_options()))

    def get_info(self, dataset: str) -> Dict[str, Any]:
        """Get information about a dataset."""
        descriptor = flight.FlightDescriptor.for_path(dataset)
        info = self._require_meta_client().get_flight_info(
            descriptor, options=self._get_call_options()
        )
        return {
            "schema": str(info.schema),
            "total_records": info.total_records,
            "total_bytes": info.total_bytes,
        }

    def add_edge(
        self,
        dataset: str,
        subject: int,
        predicate: str,
        object: int,
        weight: float = 1.0,
    ) -> None:
        """Add a directed edge to the graph."""
        req: Dict[str, Any] = {
            "dataset": dataset,
            "subject": subject,
            "predicate": predicate,
            "object": object,
            "weight": weight,
        }
        action = flight.Action("add-edge", json.dumps(req).encode("utf-8"))
        try:
            list(
                self._require_meta_client().do_action(
                    action, options=self._get_call_options()
                )
            )
        except Exception as e:
            raise LongbowQueryError(f"Add edge failed: {e}")

    def traverse(
        self,
        dataset: str,
        start: int,
        max_hops: int = 2,
        incoming: bool = False,
        decay: float = 0.0,
        weighted: bool = True,
    ) -> List[Dict]:
        """Traverse the graph from a start node."""
        req: Dict[str, Any] = {
            "dataset": dataset,
            "start": start,
            "max_hops": max_hops,
            "incoming": incoming,
            "weighted": weighted,
            "decay": decay,
        }
        action = flight.Action("traverse-graph", json.dumps(req).encode("utf-8"))
        try:
            results = []
            for res in self._require_meta_client().do_action(
                action, options=self._get_call_options()
            ):
                results.append(json.loads(res.body.to_pybytes()))
            return results
        except Exception as e:
            raise LongbowQueryError(f"Traversal failed: {e}")

    def get_graph_stats(self, dataset: str) -> Dict[str, Any]:
        """Get graph statistics."""
        return self._do_action_json("GetGraphStats", {"dataset": dataset}, default={})

    def calculate_pagerank(
        self,
        dataset: str,
        damping_factor: float = 0.85,
        max_iterations: int = 20,
        tolerance: float = 1e-4,
    ) -> Dict[str, float]:
        """
        Calculate PageRank centrality for all nodes in the dataset's HNSW graph.

        Args:
            dataset: Name of the dataset.
            damping_factor: Damping coefficient (default 0.85).
            max_iterations: Maximum iterations (default 20).
            tolerance: Convergence tolerance (default 1e-4).

        Returns:
            Dict[str, float]: Mapping of node IDs to PageRank scores.
        """
        raw_res = self._do_action_json(
            "calculate-pagerank",
            {
                "dataset": dataset,
                "damping_factor": damping_factor,
                "max_iterations": max_iterations,
                "tolerance": tolerance,
            },
            default={},
        )
        return raw_res.get("Scores", {})

    def detect_communities(self, dataset: str, max_iterations: int = 10) -> Dict[str, Any]:
        """
        Run community detection (LPA) on the dataset's HNSW graph.

        Args:
            dataset: Name of the dataset.
            max_iterations: Maximum iterations for propagation (default 10).

        Returns:
            Dict[str, Any]: Result containing 'CommunityCount' and 'Labels' mapping.
        """
        return self._do_action_json(
            "detect-communities",
            {"dataset": dataset, "max_iterations": max_iterations},
            default={},
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

    def graph_rag_expand(self, dataset: str, node_ids: List[int]) -> Dict[int, List[int]]:
        """
        Perform a distributed GraphRAG expansion (get neighbors for multiple nodes).
        
        Args:
            dataset: Name of the dataset.
            node_ids: List of node IDs to expand.
            
        Returns:
            Dict[int, List[int]]: Mapping of node IDs to their neighbor lists.
        """
        return self._do_action_json(
            "GraphRAGExpand", {"dataset": dataset, "node_ids": node_ids}, default={}
        )

    def get_flight_info_metadata(self, dataset: str) -> Dict[str, Any]:
        """
        Retrieve FlightInfo for a dataset via the MetaServer.
        
        Args:
            dataset: Name of the dataset.
            
        Returns:
            Dict: Dataset statistics and metadata.
        """
        descriptor = flight.FlightDescriptor.for_path(dataset)
        info = self._require_meta_client().get_flight_info(
            descriptor, options=self._get_call_options()
        )
        return {
            "total_records": info.total_records,
            "total_bytes": info.total_bytes,
            "schema": str(info.schema),
            "endpoints": [str(e.locations) for e in info.endpoints],
        }

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

    def create_api_key(
        self,
        name: str,
        role: str = "reader",
        tenant_id: str = "",
        namespace: str = "",
        datasets: Optional[List[str]] = None,
        row_filters: Optional[Dict[str, str]] = None,
        ttl_seconds: int = 0,
    ) -> Dict[str, Any]:
        """Issue a new RBAC API key scoped to a tenant/namespace."""
        payload = {
            "name": name,
            "role": role,
            "tenant_id": tenant_id or namespace,
            "namespace": namespace or tenant_id,
            "datasets": datasets or [],
            "row_filters": row_filters or {},
            "ttl_seconds": ttl_seconds,
        }
        return self._do_action_json("CreateAPIKey", payload, default={})

    def rotate_api_key(self, api_key: str, grace_period_seconds: int = 3600) -> Dict[str, Any]:
        """Rotate an existing API key with an optional grace period."""
        return self._do_action_json(
            "RotateAPIKey",
            {"api_key": api_key, "grace_period_seconds": grace_period_seconds},
            default={},
        )

    def revoke_api_key(self, key_or_id: str) -> Dict[str, Any]:
        """Revoke an API key by secret or KeyID."""
        return self._do_action_json("RevokeAPIKey", {"api_key": key_or_id}, default={})

    def set_rls_policy(
        self,
        name: str,
        namespace: str,
        dataset: str,
        rules: List[Dict[str, Any]],
        enabled: bool = True,
    ) -> Dict[str, Any]:
        """Configure a row-level security (RLS) policy for a namespace/dataset."""
        return self._do_action_json(
            "SetRLSPolicy",
            {
                "name": name,
                "namespace": namespace,
                "dataset": dataset,
                "rules": rules,
                "enabled": enabled,
            },
            default={},
        )

    def trigger_backup(self, dataset: str, destination: str = "") -> Dict[str, Any]:
        """Trigger an online snapshot backup for a dataset."""
        return self._do_action_json(
            "TriggerBackup",
            {"dataset": dataset, "destination": destination},
            default={},
        )

    def restore_backup(
        self, backup_id: str, dataset: str, pitr_timestamp: int = 0
    ) -> Dict[str, Any]:
        """Restore a dataset from a backup or point-in-time WAL timestamp (Unix ns)."""
        return self._do_action_json(
            "RestoreBackup",
            {
                "backup_id": backup_id,
                "dataset": dataset,
                "pitr_timestamp": pitr_timestamp,
            },
            default={},
        )

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

    # ---- Item 1: Managed Cloud Control Plane & Multi-Region Replication ----

    def cloud_create_cluster(
        self,
        name: str,
        primary_region: str,
        tier: str = "dedicated",
        tenant_id: str = "default",
        nodes: int = 3,
        replica_regions: Optional[Dict[str, str]] = None,
        replication_mode: str = "semi_sync",
        cluster_id: str = "",
    ) -> Dict[str, Any]:
        """Provision a managed multi-region Longbow Cloud cluster with tier SLA defaults."""
        return self._do_action_json(
            "CreateCloudCluster",
            {
                "id": cluster_id,
                "name": name,
                "tenant_id": tenant_id,
                "tier": tier,
                "primary_region": primary_region,
                "replica_regions": replica_regions or {},
                "nodes": nodes,
                "replication_mode": replication_mode,
            },
            default={},
        )

    def cloud_list_clusters(self, tenant_id: str = "") -> List[Dict[str, Any]]:
        """List managed cloud clusters, optionally filtered by tenant ID."""
        return self._do_action_json(
            "ListCloudClusters", {"tenant_id": tenant_id}, default={}
        ).get("clusters", [])

    def cloud_scale_cluster(
        self, cluster_id: str, nodes: int = 0, tier: str = ""
    ) -> Dict[str, Any]:
        """Scale node count and/or tier on an existing managed cloud cluster."""
        return self._do_action_json(
            "ScaleCloudCluster",
            {"id": cluster_id, "nodes": nodes, "tier": tier},
            default={},
        )

    def cloud_add_replica_region(
        self, cluster_id: str, region: str, endpoint: str = "local"
    ) -> Dict[str, Any]:
        """Attach a new geographic replica region to a managed cloud cluster."""
        return self._do_action_json(
            "AddReplicaRegion",
            {"id": cluster_id, "region": region, "endpoint": endpoint},
            default={},
        )

    def cloud_promote_region(
        self, cluster_id: str, target_region: str = "", reason: str = "failover"
    ) -> Dict[string, Any]:
        """Promote a replica region to primary during manual or automated failover."""
        return self._do_action_json(
            "PromoteClusterRegion",
            {
                "cluster_id": cluster_id,
                "target_region": target_region,
                "reason": reason,
            },
            default={},
        )

    def cloud_get_sla(
        self, cluster_id: str, observed_p99_ms: float = 0.0
    ) -> Dict[str, Any]:
        """Evaluate live SLA compliance (RPO/RTO/availability) for a managed cloud cluster."""
        return self._do_action_json(
            "GetCloudSLAReport",
            {"cluster_id": cluster_id, "observed_p99_ms": observed_p99_ms},
            default={},
        )

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

    def list_api_keys(self) -> List[Dict[str, Any]]:
        """List registered RBAC API keys."""
        return self._do_action_json("ListAPIKeys", {}, default={}).get("keys", [])

    def get_audit_log(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch recent RBAC security audit log entries."""
        return self._do_action_json("GetAuditLog", {"limit": limit}, default={}).get(
            "entries", []
        )

    def list_backups(self, dataset: str = "") -> List[Dict[str, Any]]:
        """List online backups (optionally filtered by dataset)."""
        return self._do_action_json(
            "ListBackups", {"dataset": dataset}, default={}
        ).get("backups", [])

    def verify_backup(self, backup_id: str, dataset: str = "") -> Dict[str, Any]:
        """Verify backup integrity via dry-run PITR replay."""
        return self._do_action_json(
            "VerifyBackup", {"backup_id": backup_id, "dataset": dataset}, default={}
        )

    def create_stream_connector(
        self,
        name: str,
        dataset: str,
        source_type: str = "memory",
        brokers: str = "",
        topic: str = "",
        format: str = "json",
        start: bool = False,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Create a streaming CDC connector (Kafka, Pulsar, or Memory)."""
        payload: Dict[str, Any] = {
            "name": name,
            "dataset": dataset,
            "source_type": source_type,
            "brokers": brokers,
            "topic": topic,
            "format": format,
            "start": start,
        }
        payload.update(kwargs)
        return self._do_action_json("CreateStreamConnector", payload, default={})

    def stop_stream_connector(self, name: str) -> Dict[str, Any]:
        """Stop and remove a streaming CDC connector."""
        return self._do_action_json("StopStreamConnector", {"name": name}, default={})

    def list_stream_connectors(self) -> List[Dict[str, Any]]:
        """List streaming CDC connectors and their runtime stats."""
        return self._do_action_json("ListStreamConnectors", {}, default={}).get(
            "connectors", []
        )

    def get_stream_stats(self) -> List[Dict[str, Any]]:
        """Alias for list_stream_connectors returning connector throughput/lag stats."""
        return self._do_action_json("GetStreamStats", {}, default={}).get(
            "connectors", []
        )

    def ingest_stream_event(
        self, connector: str, envelopes: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Ingest streaming envelopes into a named connector."""
        return self._do_action_json(
            "IngestStreamEvent",
            {"connector": connector, "envelopes": envelopes},
            default={},
        )

    def register_stream_schema(
        self, subject: str, format: str = "json", fields: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Register a local streaming schema subject."""
        return self._do_action_json(
            "RegisterStreamSchema",
            {"subject": subject, "format": format, "fields": fields or {}},
            default={},
        )

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

    def get_tenant_usage(self, tenant_id: str = "") -> Dict[str, Any]:
        """Retrieve per-tenant or global usage and billing metrics."""
        return self._do_action_json(
            "GetTenantUsage", {"tenant_id": tenant_id}, default={}
        )

    def set_tenant_quota(
        self,
        tenant_id: str,
        max_queries: int = 0,
        max_vectors: int = 0,
        max_storage_bytes: int = 0,
        soft_limit_ratio: float = 0.8,
        enforcement_mode: str = "hard",
    ) -> Dict[str, Any]:
        """Configure tenant usage quotas and enforcement mode."""
        return self._do_action_json(
            "SetTenantQuota",
            {
                "tenant_id": tenant_id,
                "max_queries": max_queries,
                "max_vectors": max_vectors,
                "max_storage_bytes": max_storage_bytes,
                "soft_limit_ratio": soft_limit_ratio,
                "enforcement_mode": enforcement_mode,
            },
            default={},
        )

    def export_billing_report(
        self, format: str = "json", tenant_id: str = ""
    ) -> Union[Dict[str, Any], str]:
        """Export tenant billing report in json, csv, or stripe format."""
        action = flight.Action(
            "ExportBillingReport",
            json.dumps({"format": format, "tenant_id": tenant_id}).encode("utf-8"),
        )
        results = list(
            self._require_meta_client().do_action(
                action, options=self._get_call_options()
            )
        )
        if not results:
            return {} if format == "json" else ""
        raw = results[0].body.to_pybytes().decode("utf-8")
        if format == "csv":
            return raw
        try:
            return json.loads(raw)
        except Exception:
            return raw

    def get_cluster_topology(self) -> Dict[str, Any]:
        """Fetch the elastic sharding ring topology and shard assignment snapshot."""
        return self._do_action_json("GetClusterTopology", {}, default={})

    def get_autoscale_config(self) -> Dict[str, Any]:
        """Fetch current elastic sharding & auto-scaling policy."""
        return self._do_action_json("GetAutoScaleConfig", {}, default={})

    def set_autoscale_config(self, policy: Dict[str, Any]) -> Dict[str, Any]:
        """Update elastic sharding & auto-scaling policy."""
        return self._do_action_json("SetAutoScaleConfig", policy, default={})

    def trigger_shard_split(self, dataset: str, shard_id: str) -> Dict[str, Any]:
        """Split a hot shard into left and right child shards."""
        return self._do_action_json(
            "TriggerShardSplit", {"dataset": dataset, "shard_id": shard_id}, default={}
        )

    def trigger_shard_merge(
        self, dataset: str, left_shard_id: str, right_shard_id: str
    ) -> Dict[str, Any]:
        """Merge two adjacent shards."""
        return self._do_action_json(
            "TriggerShardMerge",
            {
                "dataset": dataset,
                "left_shard_id": left_shard_id,
                "right_shard_id": right_shard_id,
            },
            default={},
        )

    def trigger_rebalance(self, dataset: str = "") -> Dict[str, Any]:
        """Trigger shard rebalancing across cluster nodes."""
        return self._do_action_json(
            "TriggerRebalance", {"dataset": dataset}, default={}
        )

    def register_schema(
        self,
        dataset: str,
        constraints: List[Dict[str, Any]],
        compatibility: str = "BACKWARD",
        metadata_types: Optional[Dict[str, str]] = None,
        strict_metadata: bool = False,
    ) -> Dict[str, Any]:
        """Register versioned field constraints and compatibility rules for a dataset."""
        return self._do_action_json(
            "RegisterSchema",
            {
                "dataset": dataset,
                "compatibility": compatibility,
                "constraints": constraints,
                "metadata_types": metadata_types or {},
                "strict_metadata": strict_metadata,
            },
            default={},
        )

    def get_schema_registry(self, dataset: str) -> Dict[str, Any]:
        """Get the latest registered schema specification for a dataset."""
        return self._do_action_json(
            "GetSchemaRegistry", {"dataset": dataset}, default={}
        )

    def validate_schema(
        self, dataset: str, vectors: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Dry-run validate a list of vector records against the dataset's registered schema."""
        return self._do_action_json(
            "ValidateSchema", {"dataset": dataset, "vectors": vectors}, default={}
        )

    def migrate_schema(
        self,
        dataset: str,
        target_schema: Dict[str, Any],
        add_string_cols: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Migrate dataset schema and backfill default values for newly added columns."""
        target = dict(target_schema)
        target["dataset"] = dataset
        return self._do_action_json(
            "MigrateSchema",
            {"target": target, "add_string_cols": add_string_cols or {}},
            default={},
        )

    def create_materialized_view(
        self,
        name: str,
        source_dataset: str,
        group_by: List[str],
        aggregations: Optional[List[Dict[str, str]]] = None,
        refresh_mode: str = "streaming",
        filter_equals: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Create a pre-aggregated materialized view over a dataset."""
        return self._do_action_json(
            "CreateMaterializedView",
            {
                "name": name,
                "source_dataset": source_dataset,
                "group_by": group_by,
                "aggregations": aggregations or [{"alias": "count", "op": "COUNT"}],
                "refresh_mode": refresh_mode,
                "filter_equals": filter_equals or {},
            },
            default={},
        )

    def refresh_materialized_view(self, name: str) -> Dict[str, Any]:
        """Refresh a materialized view on demand."""
        return self._do_action_json(
            "RefreshMaterializedView", {"name": name}, default={}
        )

    def query_materialized_view(
        self, name: str, group_filter: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Query pre-aggregated rows from a materialized view."""
        return self._do_action_json(
            "QueryMaterializedView",
            {"name": name, "group_filter": group_filter or {}},
            default={},
        )

    def list_materialized_views(self) -> List[Dict[str, Any]]:
        """List all materialized views and their snapshots."""
        return self._do_action_json("ListMaterializedViews", {}, default={}).get(
            "views", []
        )

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
