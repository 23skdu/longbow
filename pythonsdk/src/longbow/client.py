import pyarrow.flight as flight
import dask.dataframe as dd
import pandas as pd
import json
import logging
from typing import Union, List, Dict, Any, Optional

from .models import Vector, SearchResult, IndexStats
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
            self._data_client = flight.FlightClient(self.uri)
            self._meta_client = flight.FlightClient(self.meta_uri)
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

    def _get_call_options(self):
        call_headers = []
        for k, v in self.headers.items():
            call_headers.append((k.encode('utf-8'), v.encode('utf-8')))
        
        if self.api_key:
            call_headers.append((b"authorization", f"Bearer {self.api_key}".encode('utf-8')))
            
        return flight.FlightCallOptions(headers=call_headers)

    def insert(self, dataset: str, data: Union[dd.DataFrame, pd.DataFrame, List[Dict]], batch_size: int = 10000) -> None:
        """
        Insert vectors into a dataset.
        
        Args:
            dataset: Name of the target dataset.
            data: Data to insert (Dask DataFrame, Pandas DataFrame, or List of Dicts).
            batch_size: Batch size for upload chunks.
        """
        if self._data_client is None:
            self.connect()

        # Handle Dask DataFrame by iterating partitions
        if isinstance(data, dd.DataFrame):
            # Process partitions sequentially (client-side) to avoid overwhelming server
            # or parallel if we implement parallel connections.
            # Simple approach: map_partitions with a custom function that computes and calls internal _upload
            # But we can't pickle the client easily for distributed workers.
            # Best pattern for Dask: iterate partitions on the driver (client) if data is local-ish, 
            # OR assume client is running on driver and use `.partitions` iterator.
            
            logger.info("Processing Dask DataFrame partitions...")
            for partition in data.partitions:
                # Compute partition to Pandas
                df_part = partition.compute()
                if not df_part.empty:
                    self._upload_batch(dataset, df_part)
            return

        # Handle other types
        self._upload_batch(dataset, data)

    def _upload_batch(self, dataset: str, data: Union[pd.DataFrame, List[Dict]]):
        """Internal helper to upload a materialized batch."""
        table = to_arrow_table(data)
        descriptor = flight.FlightDescriptor.for_path(dataset)
        writer, _ = self._data_client.do_put(descriptor, table.schema, options=self._get_call_options())
        writer.write_table(table)
        writer.close()
        logger.debug(f"Uploaded batch of {table.num_rows} rows to {dataset}")

    def search(
        self, 
        dataset: str, 
        vector: List[float], 
        k: int = 10, 
        filters: Optional[List[Dict]] = None,
        **kwargs
    ) -> dd.DataFrame:
        """
        Perform a K-Nearest Neighbor search.

        Args:
            dataset: Target dataset.
            vector: Query vector.
            k: Number of results.
            filter: Optional filter criteria.
            **kwargs: Additional arguments passed to the search query (e.g. 'alpha', 'text_query', 'include_vectors').

        Returns:
            dask.dataframe.DataFrame: Lazy dataframe containing search results. 
            (Currently materialized immediately, wrapped in Dask for API consistency)
        """
        if self._meta_client is None:
            self.connect()

        req = {
            "dataset": dataset,
            "vector": vector,
            "k": k,
        }

        if filters:
            req["filters"] = filters
        
        # Merge extra args (e.g. alpha, text_query)
        req.update(kwargs)

        ticket_bytes = json.dumps({"search": req}).encode("utf-8")
        ticket = flight.Ticket(ticket_bytes)
        
        try:
            reader = self._meta_client.do_get(ticket, options=self._get_call_options())
            table = reader.read_all()
            df = table.to_pandas() # Convert to Pandas
            
            # Wrap in Dask for consistency
            if not df.empty:
                return dd.from_pandas(df, npartitions=1)
            else:
                # Empty structure
                return dd.from_pandas(pd.DataFrame(columns=["id", "score", "vector", "metadata"]), npartitions=1)
                
        except Exception as e:
            raise LongbowQueryError(f"Search failed: {e}")

    def search_by_id(self, dataset: str, id: Union[int, str], k: int = 10) -> Dict[str, Any]:
        """Search for similar vectors by ID."""
        if self._meta_client is None:
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
        results = list(self._meta_client.do_action(action, options=self._get_call_options()))
        # Check results if needed

    def list_namespaces(self) -> List[str]:
        """List all available datasets."""
        if self._meta_client is None:
            self.connect()
        return [f.descriptor.path[0].decode("utf-8") for f in self._meta_client.list_flights()]

    def download(self, dataset: str, filter: Optional[Dict] = None) -> dd.DataFrame:
        """Download the entire dataset, optionally filtered."""
        if self._data_client is None:
            self.connect()
        
        req = {"name": dataset}
        if filter:
            req["filters"] = filter  # Note: Server uses "filters" (plural) or "filter"? verify ops_test.
            # ops_test lines 103-104: filters.append(...); query["filters"] = filters.
            # So "filters" is correct for data plane query? 
            # But search uses "filter"? 
            # ops_test lines 240: request["filters"] = filters (for meta search)
            # client.search uses "filter". 
            # Wait, ops_test line 240 says request["filters"]. 
            # My client.search uses req["filter"] = filter. 
            # Is there a mismatch? 
            # Let's check ops_test line 240 again.
            # Line 240: request["filters"] = filters.
            # My client.py line 121: req["filter"] = filter.
            # I might have introduced a bug in client.py "filter" key.
            # I should verify against what the server expects.
            # ops_test uses "filters" list. 
            # My client.py search takes 'filter' dict? 
            # ops_test parser takes --filter field:op:val and constructs a list of dicts.
            # So 'filter' arg in SDK should likely be 'List[Dict]' or just passed through.
            # I will assume "filters" is the key.
            
        ticket_bytes = json.dumps(req).encode("utf-8")
        ticket = flight.Ticket(ticket_bytes)

        
        try:
            reader = self._data_client.do_get(ticket, options=self._get_call_options())
            table = reader.read_all()
            df = table.to_pandas()
            if not df.empty:
                return dd.from_pandas(df, npartitions=1)
            return dd.from_pandas(pd.DataFrame(columns=["id", "vector", "timestamp", "metadata"]), npartitions=1)
        except Exception as e:
            raise LongbowQueryError(f"Download failed: {e}")

    def delete(self, dataset: str, ids: Optional[List[int]] = None):
        """Delete specific IDs from a dataset."""
        if self._meta_client is None:
            self.connect()
            
        req = {"name": dataset}
        if ids:
            req["ids"] = ids
            
        action_body = json.dumps(req).encode("utf-8")
        action = flight.Action("DeleteNamespace", action_body)
        list(self._meta_client.do_action(action, options=self._get_call_options()))

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
