import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd
import json
import logging
import math
import warnings
from typing import Union, List, Dict, Any, Optional, Iterator

from .exceptions import LongbowConnectionError, LongbowQueryError
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
        vector: Any,
        k: int = 10,
        filters: Optional[List[Dict]] = None,
        projection: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Perform a K-Nearest Neighbor search.

        Args:
            dataset: Target dataset.
            vector: Query vector.
            k: Number of results.
            filters: Optional filter criteria.
            projection: Optional list of columns to return (reduces bandwidth).
            **kwargs: Additional arguments passed to the search query (e.g. 'alpha', 'text_query', 'include_vectors').

        Returns:
            pandas.DataFrame: Dataframe containing search results.
        """
        # Handle complex query vectors and sanitization for JSON
        np_mod: Any = None
        try:
            import numpy as np

            np_mod = np
        except ImportError:
            pass

        query_vec: Any = vector
        if np_mod is not None and hasattr(query_vec, "dtype"):
            if np_mod.issubdtype(query_vec.dtype, np_mod.complexfloating):
                # Flatten complex to [real, imag, real, imag...]
                # Use correct float type matching the complex precision
                target_dtype = (
                    np_mod.float32
                    if query_vec.dtype == np_mod.complex64
                    else np_mod.float64
                )
                query_vec = query_vec.view(target_dtype).flatten().tolist()
            elif isinstance(query_vec, np_mod.ndarray):
                query_vec = query_vec.tolist()

        # Handle Python list of complex numbers (e.g. [1+1j, 2+2j])
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

        # Sanitize: Ensure no NaN/Inf which breaks server JSON parsing
        for i, v in enumerate(query_vec):
            if isinstance(v, (int, float)) and not math.isfinite(v):
                raise ValueError(
                    f"Query vector contains invalid value (NaN or Inf) at index {i}"
                )

        req: Dict[str, Any] = {
            "dataset": dataset,
            "vector": query_vec,
            "k": k,
        }

        if filters:
            req["filters"] = filters

        if projection:
            req["projection"] = projection

        # Merge extra args (e.g. alpha, text_query)
        for kw, val in kwargs.items():
            if val is not None:
                req[kw] = val

        # Handle efSearch PID tuning if requested
        if kwargs.get("ef_search_pid", False):
            req["ef_search_pid"] = True

        ticket_bytes = json.dumps({"search": req}).encode("utf-8")
        ticket = flight.Ticket(ticket_bytes)

        try:
            reader = self._require_data_client().do_get(
                ticket, options=self._get_call_options()
            )
            table = reader.read_all()
            return table.to_pandas()  # Convert to Pandas

        except Exception as e:
            raise LongbowQueryError(f"Search failed: {e}")

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
        Perform a Geospatial search (radius or bounding box).

        Args:
            dataset: Target dataset (must have geospatial index enabled).
            center: Center point {"lat": 1.2, "lon": 3.4}. Required for radius/hybrid.
            radius_km: Search radius in kilometers. Required for radius/hybrid.
            box: Bounding box {"min_lat": ..., "max_lat": ..., "min_lon": ..., "max_lon": ...}. Required for box search.
            search_type: "radius", "box", or "hybrid".
            k: Number of results.
            filters: Optional metadata filters.
            **kwargs: Extra parameters.

        Returns:
            pandas.DataFrame: Search results with 'id' and 'distance'.
        """
        if self._data_client is None:
            self.connect()

        geo_req = {
            "dataset": dataset,
            "k": k,
            "search_type": search_type,
        }

        if center:
            geo_req["center"] = center
        if radius_km:
            geo_req["radius_km"] = radius_km
        if box:
            geo_req["box"] = box
        if filters:
            geo_req["filters"] = filters

        geo_req.update(kwargs)

        ticket_bytes = json.dumps({"geo_search": geo_req}).encode("utf-8")
        ticket = flight.Ticket(ticket_bytes)

        try:
            reader = self._require_data_client().do_get(
                ticket, options=self._get_call_options()
            )
            table = reader.read_all()
            return table.to_pandas()
        except Exception as e:
            raise LongbowQueryError(f"Geo-Search failed: {e}")

    def search_by_id(
        self, dataset: str, id: Union[int, str], k: int = 10
    ) -> Dict[str, Any]:
        """Search for similar vectors by ID."""
        req: Dict[str, Any] = {"dataset": dataset, "id": id, "k": k}
        return self._do_action_json("VectorSearchByID", req, default={})

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
