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

    def __init__(self, uri: str = "grpc://localhost:3000", meta_uri: Optional[str] = None, api_key: Optional[str] = None, headers: Optional[Dict[str, str]] = None):
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
            self._meta_client = flight.FlightClient(self.meta_uri, generic_options=options)
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
            call_headers.append((k.encode('utf-8'), v.encode('utf-8')))
        
        if self.api_key:
            call_headers.append((b"authorization", f"Bearer {self.api_key}".encode('utf-8')))
        
        if timeout is not None:
            return flight.FlightCallOptions(headers=call_headers, timeout=timeout)
        return flight.FlightCallOptions(headers=call_headers)

    def insert(self, dataset: str, data: Union[pd.DataFrame, List[Dict]], batch_size: int = 10000, timeout: float = 180.0) -> None:
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

    def _upload_batch(self, dataset: str, data: Union[pd.DataFrame, List[Dict], pa.Table], timeout: float = 180.0):
        """Internal helper to upload a materialized batch with timeout."""
        if isinstance(data, pa.Table):
            table = data
        else:
            table = to_arrow_table(data)
        descriptor = flight.FlightDescriptor.for_path(dataset)
        call_opts = self._get_call_options(timeout=timeout)
        writer, reader = self._data_client.do_put(descriptor, table.schema, options=call_opts)
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
        **kwargs
    ) -> pd.DataFrame:
        """
        Perform a K-Nearest Neighbor search.

        Args:
            dataset: Target dataset.
            vector: Query vector.
            k: Number of results.
            filter: Optional filter criteria.
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
                target_dtype = np.float32 if vector.dtype == np.complex64 else np.float64
                vector = vector.view(target_dtype).flatten().tolist()
            elif isinstance(vector, np.ndarray):
                vector = vector.tolist()
        
        # Handle Python list of complex numbers (e.g. [1+1j, 2+2j])
        if isinstance(vector, list) and len(vector) > 0 and isinstance(vector[0], complex):
            flat_vector = []
            for v in vector:
                flat_vector.append(float(v.real))
                flat_vector.append(float(v.imag))
            vector = flat_vector

        # Sanitize: Ensure no NaN/Inf which breaks server JSON parsing
        import math
        for i, v in enumerate(vector):
            if isinstance(v, (int, float)) and not math.isfinite(v):
                raise ValueError(f"Query vector contains invalid value (NaN or Inf) at index {i}")

        req = {
            "dataset": dataset,
            "vector": vector,
            "k": k,
        }

        if filters:
            req["filters"] = filters
        
        # Merge extra args (e.g. alpha, text_query)
        for k, v in kwargs.items():
            if v is not None:
                req[k] = v

        ticket_bytes = json.dumps({"search": req}).encode("utf-8")
        ticket = flight.Ticket(ticket_bytes)
        
        try:
            reader = self._data_client.do_get(ticket, options=self._get_call_options())
            table = reader.read_all()
            return table.to_pandas() # Convert to Pandas
                
        except Exception as e:
            raise LongbowQueryError(f"Search failed: {e}")

    def search_by_id(self, dataset: str, id: Union[int, str], k: int = 10) -> Dict[str, Any]:
        """Search for similar vectors by ID."""
        if self._data_client is None:
            self.connect()
            
        req = {
            "dataset": dataset,
            "id": id,
            "k": k
        }
        action = flight.Action("VectorSearchByID", json.dumps(req).encode("utf-8"))
        try:
            # We assume single result batch for this action
            results = list(self._meta_client.do_action(action, options=self._get_call_options()))
            if results:
                return json.loads(results[0].body.to_pybytes())
            return {}
        except Exception as e:
            raise LongbowQueryError(f"SearchByID failed: {e}")

    def create_namespace(self, name: str, force: bool = False):
        """Create a new dataset/namespace."""
        if self._meta_client is None:
            self.connect()
            
        action_body = json.dumps({"name": name, "overwrite": force}).encode("utf-8")
        action = flight.Action("CreateNamespace", action_body)
        list(self._meta_client.do_action(action, options=self._get_call_options()))
        # Check results if needed

    def list_namespaces(self) -> List[str]:
        """List all available datasets."""
        if self._meta_client is None:
            self.connect()
        return [f.descriptor.path[0].decode("utf-8") for f in self._meta_client.list_flights()]

    def download_arrow(self, dataset: str, filter: Optional[List[Dict]] = None) -> pa.Table:
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
    
    def download_stream(self, dataset: str, filter: Optional[List[Dict]] = None) -> Iterator[pa.RecordBatch]:
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
            stacklevel=2
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
                single_req = {
                    "dataset": dataset,
                    "id": str(i)
                }
                action_body = json.dumps(single_req).encode("utf-8")
                # ignore result for now, just best effort
                try:
                    action = flight.Action("delete", action_body)
                    list(self._meta_client.do_action(action, options=self._get_call_options()))
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
        info = self._meta_client.get_flight_info(descriptor, options=self._get_call_options())
        return {
            "schema": str(info.schema),
            "total_records": info.total_records,
            "total_bytes": info.total_bytes
        }

    def add_edge(self, dataset: str, subject: int, predicate: str, object: int, weight: float = 1.0) -> None:
        """Add a directed edge to the graph."""
        if self._meta_client is None:
            self.connect()

        req = {
            "dataset": dataset,
            "subject": subject,
            "predicate": predicate,
            "object": object,
            "weight": weight
        }
        action = flight.Action("add-edge", json.dumps(req).encode("utf-8"))
        try:
            list(self._meta_client.do_action(action, options=self._get_call_options()))
        except Exception as e:
            raise LongbowQueryError(f"Add edge failed: {e}")

    def traverse(self, dataset: str, start: int, max_hops: int = 2, incoming: bool = False, decay: float = 0.0, weighted: bool = True) -> List[Dict]:
        """Traverse the graph from a start node."""
        if self._meta_client is None:
            self.connect()

        req = {
            "dataset": dataset,
            "start": start,
            "max_hops": max_hops,
            "incoming": incoming,
            "weighted": weighted,
            "decay": decay
        }
        action = flight.Action("traverse-graph", json.dumps(req).encode("utf-8"))
        try:
            results = []
            for res in self._meta_client.do_action(action, options=self._get_call_options()):
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
            results = list(self._meta_client.do_action(action, options=self._get_call_options()))
            if results:
                return json.loads(results[0].body.to_pybytes())
            return {}
        except Exception as e:
            raise LongbowQueryError(f"GetGraphStats failed: {e}")
