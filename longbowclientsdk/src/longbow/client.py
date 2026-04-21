import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd
import json
import logging
import warnings
from typing import Union, List, Dict, Any, Optional, Iterator

# from .models import Vector, SearchResult, IndexStats # Unused internally for now
from .exceptions import LongbowConnectionError, LongbowQueryError
from .ingest import to_arrow_table

logger = logging.getLogger(__name__)


class LongbowClient:
    """Client for interacting with the Longbow Vector Database."""

    def __init__(
        self,
        uri: str = "grpc://localhost:3000",
        meta_uri: Optional[str] = None,
        api_key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the Longbow Client.

        Args:
            uri: gRPC URI for the Data Plane (DoPut/DoGet).
            meta_uri: gRPC URI for the Control Plane (DoAction/Info). Defaults to uri.
            api_key: Optional API key for authentication.
        """
        self.uri = uri
        self.meta_uri = meta_uri or uri
        self.api_key = api_key
        self.headers = headers or {}

        self._data_client = None
        self._meta_client = None

    def connect(self):
        """Establish connections to the server."""
        try:
            # Set high limits (1GB) to support large batch transfers
            options = [
                ("grpc.max_receive_message_length", 1024 * 1024 * 1024),
                ("grpc.max_send_message_length", 1024 * 1024 * 1024),
            ]
            self._data_client = flight.FlightClient(self.uri, generic_options=options)
            self._meta_client = flight.FlightClient(
                self.meta_uri, generic_options=options
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
        if self._data_client is None:
            self.connect()

        # Handle other types
        table = to_arrow_table(data)
        self._upload_batch(dataset, table, timeout=timeout)

    def _upload_batch(
        self,
        dataset: str,
        data: Union[pd.DataFrame, List[Dict], pa.Table],
        timeout: float = 180.0,
    ):
        """Internal helper to upload a materialized batch with timeout."""
        if isinstance(data, pa.Table):
            table = data
        else:
            table = to_arrow_table(data)
        descriptor = flight.FlightDescriptor.for_path(dataset)
        call_opts = self._get_call_options(timeout=timeout)
        writer, reader = self._data_client.do_put(
            descriptor, table.schema, options=call_opts
        )
        writer.write_table(table)
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
        vector: List[float],
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
        if self._data_client is None:
            self.connect()

        # Handle complex query vectors and sanitization for JSON
        try:
            import numpy as np

            has_numpy = True
        except ImportError:
            has_numpy = False

        if has_numpy and hasattr(vector, "dtype"):
            if np.issubdtype(vector.dtype, np.complexfloating):
                # Flatten complex to [real, imag, real, imag...]
                # Use correct float type matching the complex precision
                target_dtype = (
                    np.float32 if vector.dtype == np.complex64 else np.float64
                )
                vector = vector.view(target_dtype).flatten().tolist()
            elif isinstance(vector, np.ndarray):
                vector = vector.tolist()

        # Handle Python list of complex numbers (e.g. [1+1j, 2+2j])
        if (
            isinstance(vector, list)
            and len(vector) > 0
            and isinstance(vector[0], complex)
        ):
            flat_vector = []
            for v in vector:
                flat_vector.append(float(v.real))
                flat_vector.append(float(v.imag))
            vector = flat_vector

        # Sanitize: Ensure no NaN/Inf which breaks server JSON parsing
        import math

        for i, v in enumerate(vector):
            if isinstance(v, (int, float)) and not math.isfinite(v):
                raise ValueError(
                    f"Query vector contains invalid value (NaN or Inf) at index {i}"
                )

        req = {
            "dataset": dataset,
            "vector": vector,
            "k": k,
        }

        if filters:
            req["filters"] = filters

        if projection:
            req["projection"] = projection

        # Merge extra args (e.g. alpha, text_query)
        for k, v in kwargs.items():
            if v is not None:
                req[k] = v

        ticket_bytes = json.dumps({"search": req}).encode("utf-8")
        ticket = flight.Ticket(ticket_bytes)

        try:
            reader = self._data_client.do_get(ticket, options=self._get_call_options())
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
            reader = self._data_client.do_get(ticket, options=self._get_call_options())
            table = reader.read_all()
            return table.to_pandas()
        except Exception as e:
            raise LongbowQueryError(f"Geo-Search failed: {e}")

    def search_by_id(
        self, dataset: str, id: Union[int, str], k: int = 10
    ) -> Dict[str, Any]:
        """Search for similar vectors by ID."""
        if self._data_client is None:
            self.connect()

        req = {"dataset": dataset, "id": id, "k": k}
        action = flight.Action("VectorSearchByID", json.dumps(req).encode("utf-8"))
        try:
            # We assume single result batch for this action
            results = list(
                self._meta_client.do_action(action, options=self._get_call_options())
            )
            if results:
                return json.loads(results[0].body.to_pybytes())
            return {}
        except Exception as e:
            raise LongbowQueryError(f"SearchByID failed: {e}")

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
            reader = self._data_client.do_get(ticket, options=self._get_call_options())
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
        if self._meta_client is None:
            self.connect()

        req = {
            "name": name,
            "dims": dims,
            "data_type": data_type,
            "overwrite": force,
            "hnsw_config": hnsw_config,
        }
        action_body = json.dumps(req).encode("utf-8")
        action = flight.Action("CreateNamespace", action_body)
        list(self._meta_client.do_action(action, options=self._get_call_options()))

    def create_dataset(
        self,
        name: str,
        dimensions: int,
        vector_type: str = "float32",
        turboquant_bits: int = 8,
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
            turboquant_bits: Bit depth for TQ (4 or 8).
            geo_enabled: Enable geospatial indexing (Quadtree).
            disk_enabled: Enable Disk-ANN offloading.
            metric: Distance metric ("cosine", "l2", "ip").
        """
        if self._meta_client is None:
            self.connect()

        req = {
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
        list(self._meta_client.do_action(action, options=self._get_call_options()))
        # Check results if needed

    def list_namespaces(self) -> List[str]:
        """List all available datasets."""
        if self._meta_client is None:
            self.connect()
        return [
            f.descriptor.path[0].decode("utf-8")
            for f in self._meta_client.list_flights()
        ]

    def list_datasets_in_namespace(self, namespace: str = "default") -> List[str]:
        if self._meta_client is None:
            self.connect()

        req = {"name": namespace}
        action_body = json.dumps(req).encode("utf-8")
        action = flight.Action("ListDatasetsInNamespace", action_body)
        results = list(
            self._meta_client.do_action(action, options=self._get_call_options())
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
        if self._data_client is None:
            self.connect()

        req = {"name": dataset}
        if filter:
            req["filters"] = filter

        ticket_bytes = json.dumps(req).encode("utf-8")
        ticket = flight.Ticket(ticket_bytes)

        try:
            reader = self._data_client.do_get(ticket, options=self._get_call_options())
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
        if self._data_client is None:
            self.connect()

        req = {"name": dataset}
        if filter:
            req["filters"] = filter

        ticket_bytes = json.dumps(req).encode("utf-8")
        ticket = flight.Ticket(ticket_bytes)

        try:
            reader = self._data_client.do_get(ticket, options=self._get_call_options())
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
        if self._meta_client is None:
            self.connect()

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
                        self._meta_client.do_action(
                            action, options=self._get_call_options()
                        )
                    )
                except Exception as e:
                    # Log or warn? For batch delete, partial failure is tricky.
                    # We continue.
                    pass
        else:
            # Delete entire namespace
            action_body = json.dumps(req).encode("utf-8")
            action = flight.Action("DeleteNamespace", action_body)
            list(self._meta_client.do_action(action, options=self._get_call_options()))

    def delete_namespace(self, dataset: str):
        """Delete an entire dataset."""
        self.delete(dataset)

    def drop_dataset(self, dataset: str):
        """Drop (delete) an entire dataset. Alias for delete_namespace."""
        self.delete_namespace(dataset)

    def snapshot(self):
        """Trigger a manual snapshot of the database."""
        if self._meta_client is None:
            self.connect()

        action = flight.Action("ForceSnapshot", b"")
        list(self._meta_client.do_action(action, options=self._get_call_options()))

    def get_info(self, dataset: str) -> Dict[str, Any]:
        """Get information about a dataset."""
        if self._meta_client is None:
            self.connect()

        descriptor = flight.FlightDescriptor.for_path(dataset)
        info = self._meta_client.get_flight_info(
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
        if self._meta_client is None:
            self.connect()

        req = {
            "dataset": dataset,
            "subject": subject,
            "predicate": predicate,
            "object": object,
            "weight": weight,
        }
        action = flight.Action("add-edge", json.dumps(req).encode("utf-8"))
        try:
            list(self._meta_client.do_action(action, options=self._get_call_options()))
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
        if self._meta_client is None:
            self.connect()

        req = {
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
            for res in self._meta_client.do_action(
                action, options=self._get_call_options()
            ):
                results.append(json.loads(res.body.to_pybytes()))
            return results
        except Exception as e:
            raise LongbowQueryError(f"Traversal failed: {e}")

    def get_graph_stats(self, dataset: str) -> Dict[str, Any]:
        """Get graph statistics."""
        if self._meta_client is None:
            self.connect()

        req = {"dataset": dataset}
        action = flight.Action("GetGraphStats", json.dumps(req).encode("utf-8"))
        try:
            results = list(
                self._meta_client.do_action(action, options=self._get_call_options())
            )
            if results:
                return json.loads(results[0].body.to_pybytes())
            return {}
        except Exception as e:
            raise LongbowQueryError(f"GetGraphStats failed: {e}")

    def temporal_search(
        self,
        search_type: str,
        timestamp: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        window_size: Optional[int] = None,
        duration: Optional[str] = None,
        k: int = 10,
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

        Returns:
            List of search results with id, distance, and score
        """
        if self._meta_client is None:
            self.connect()

        req = {
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

        action = flight.Action("TemporalSearch", json.dumps(req).encode("utf-8"))
        try:
            results = list(
                self._meta_client.do_action(action, options=self._get_call_options())
            )
            if results:
                return json.loads(results[0].body.to_pybytes())
            return []
        except Exception as e:
            raise LongbowQueryError(f"Temporal search failed: {e}")

    def temporal_version_history(self, vector_id: int) -> List[Dict[str, Any]]:
        """
        Get version history for a vector.

        Args:
            vector_id: ID of the vector

        Returns:
            List of versioned vectors with timestamps
        """
        if self._meta_client is None:
            self.connect()

        req = {"vector_id": vector_id}
        action = flight.Action(
            "TemporalVersionHistory", json.dumps(req).encode("utf-8")
        )
        try:
            results = list(
                self._meta_client.do_action(action, options=self._get_call_options())
            )
            if results:
                return json.loads(results[0].body.to_pybytes())
            return []
        except Exception as e:
            raise LongbowQueryError(f"Version history failed: {e}")

    def temporal_aggregation(
        self,
        aggregation_type: str,
        start_time: int,
        end_time: int,
        interval: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Perform temporal aggregation (count, min, max, mean) over time buckets.

        Args:
            aggregation_type: Type of aggregation - "count", "min", "max", "mean"
            start_time: Start time (unix nanoseconds)
            end_time: End time (unix nanoseconds)
            interval: Bucket interval in nanoseconds (default: 1 hour)

        Returns:
            Dictionary with buckets and total_count
        """
        if self._meta_client is None:
            self.connect()

        req = {
            "aggregation_type": aggregation_type,
            "start_time": start_time,
            "end_time": end_time,
        }

        if interval is not None:
            req["interval"] = interval

        action = flight.Action("TemporalAggregation", json.dumps(req).encode("utf-8"))
        try:
            results = list(
                self._meta_client.do_action(action, options=self._get_call_options())
            )
            if results:
                return json.loads(results[0].body.to_pybytes())
            return {}
        except Exception as e:
            raise LongbowQueryError(f"Temporal aggregation failed: {e}")
