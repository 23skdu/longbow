"""Transport, connection lifecycle, batch ingest, and download helpers."""

import logging
import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd
import json
import math
import warnings
from typing import Union, List, Dict, Any, Optional, Iterator
from ..exceptions import (
    LongbowAuthenticationError,
    LongbowAuthorizationError,
    LongbowConnectionError,
    LongbowNotFoundError,
    LongbowQueryError,
)
from ..ingest import to_arrow_table

logger = logging.getLogger("longbow.client")


class TransportMixin:
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
    @staticmethod
    def _raise_mapped_flight_error(operation: str, exc: Exception) -> None:
        if isinstance(exc, (LongbowAuthenticationError, LongbowConnectionError, LongbowNotFoundError, LongbowQueryError)):
            raise exc
        exc_type = type(exc).__name__.lower()
        msg = str(exc)
        low = msg.lower()

        is_forbidden = (
            "unauthorized" in exc_type
            or "permissiondenied" in low
            or "permission_denied" in low
            or "permission denied" in low
            or "tenant isolation" in low
            or "namespace mismatch" in low
            or "dataset not authorized" in low
            or "forbidden" in low
        )
        if is_forbidden:
            raise LongbowAuthorizationError(
                f"{operation} denied: {msg}",
                operation=operation,
                hint="Verify your RBAC role/OIDC groups, target namespace, and dataset allow-list.",
            ) from exc

        is_unauth = (
            "unauthenticated" in exc_type
            or "unauthenticated" in low
            or "missing authentication" in low
            or "missing credentials" in low
            or "invalid api key" in low
            or "api key disabled" in low
            or "api key expired" in low
            or "grace period expired" in low
            or "oidc" in low
            and any(k in low for k in ("expired", "replay", "invalid", "untrusted", "signature", "issuer", "audience"))
        )
        if is_unauth:
            raise LongbowAuthenticationError(
                f"{operation} authentication failed: {msg}",
                operation=operation,
                hint="Check api_key / LONGBOW_API_KEY or refresh/exchange your federated OIDC JWT.",
            ) from exc

        if "notfound" in low or "not_found" in low or "not found" in low:
            raise LongbowNotFoundError(f"{operation} failed: {msg}") from exc

        raise LongbowQueryError(f"{operation} failed: {msg}") from exc
    def _do_action_json(
        self,
        action_type: str,
        payload: Dict[str, Any],
        default: Any = None,
        bearer_override: Optional[str] = None,
    ) -> Any:
        meta = self._require_meta_client()
        action = flight.Action(action_type, json.dumps(payload).encode("utf-8"))
        try:
            results = list(
                meta.do_action(
                    action,
                    options=self._get_call_options(bearer_override=bearer_override),
                )
            )
            if results:
                return json.loads(results[0].body.to_pybytes())
            return {} if default is None else default
        except Exception as e:
            self._raise_mapped_flight_error(action_type, e)
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
            self._raise_mapped_flight_error(label, e)
            return pd.DataFrame()  # unreachable; satisfies type checkers
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
        import urllib.error
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
        if self.namespace:
            headers["X-Namespace"] = self.namespace
        data_bytes = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:  # nosec B310
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                raw_err = e.read().decode("utf-8", errors="replace")
                parsed = json.loads(raw_err)
                detail = str(parsed.get("error") or raw_err)
            except Exception:
                detail = str(e.reason or e)
            op = f"{method} {path}"
            if e.code == 401:
                raise LongbowAuthenticationError(
                    f"{op} authentication failed (HTTP 401): {detail}",
                    status_code=401,
                    code="UNAUTHENTICATED",
                    operation=op,
                    hint="Provide a valid api_key / Bearer token or exchange a fresh OIDC JWT via exchange_oidc_token().",
                ) from e
            if e.code == 403:
                raise LongbowAuthorizationError(
                    f"{op} denied (HTTP 403): {detail}",
                    status_code=403,
                    code="PERMISSION_DENIED",
                    operation=op,
                    hint="Verify your RBAC role/OIDC groups, target namespace, and allowed datasets.",
                ) from e
            if e.code == 404:
                raise LongbowNotFoundError(f"{op} not found (HTTP 404): {detail}") from e
            raise LongbowQueryError(f"{op} failed (HTTP {e.code}): {detail}") from e
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
    def _get_call_options(
        self,
        timeout: Optional[float] = None,
        bearer_override: Optional[str] = None,
    ):
        call_headers = []
        for k, v in self.headers.items():
            call_headers.append((k.encode("utf-8"), v.encode("utf-8")))

        if self.tenant_id:
            call_headers.append((b"x-tenant-id", self.tenant_id.encode("utf-8")))

        if self.namespace:
            call_headers.append((b"x-namespace", self.namespace.encode("utf-8")))

        token = bearer_override if bearer_override is not None else self.api_key
        if token:
            call_headers.append(
                (b"authorization", f"Bearer {token}".encode("utf-8"))
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
        try:
            writer, reader = self._require_data_client().do_put(
                descriptor, table.schema, options=call_opts
            )
            for batch in table.to_batches(max_chunksize=batch_size):
                writer.write_batch(batch)
            writer.done_writing()  # Signal completion without blocking
        except Exception as e:
            self._raise_mapped_flight_error(f"DoPut({dataset})", e)
            return

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
