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

    def __init__(self, uri: str = "grpc://localhost:3000", meta_uri: Optional[str] = None, api_key: Optional[str] = None):
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
        # TODO: Add headers for auth
        return flight.FlightCallOptions()

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
        filter: Optional[Dict] = None
    ) -> dd.DataFrame:
        """
        Perform a K-Nearest Neighbor search.

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
        if filter:
            req["filter"] = filter

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
