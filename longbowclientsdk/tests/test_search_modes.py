"""Offline unit tests for LongbowClient search modes and payload serialization."""

from __future__ import annotations

import json
import sys
import types
import unittest
from typing import Any, Dict, List
from unittest.mock import MagicMock


def _ensure_optional_test_deps() -> bool:
    """Provide minimal in-memory stubs when numpy/pyarrow/pandas/pydantic are absent."""
    try:
        import numpy  # noqa: F401
        import pandas  # noqa: F401
        import pyarrow  # noqa: F401
        import pyarrow.flight  # noqa: F401
        import pydantic  # noqa: F401

        return True
    except ImportError:
        np_mod = types.ModuleType("numpy")
        np_mod.ndarray = list  # type: ignore[attr-defined]
        np_mod.float32 = float  # type: ignore[attr-defined]
        np_mod.float64 = float  # type: ignore[attr-defined]
        np_mod.complex64 = complex  # type: ignore[attr-defined]
        np_mod.complexfloating = ()  # type: ignore[attr-defined]
        np_mod.issubdtype = lambda *a: False  # type: ignore[attr-defined]
        sys.modules.setdefault("numpy", np_mod)
        pa_mod = types.ModuleType("pyarrow")
        flight_mod = types.ModuleType("pyarrow.flight")

        class _Ticket:
            def __init__(self, data: bytes):
                self.ticket = data

        class _Action:
            def __init__(self, typ: str, body: bytes):
                self.type = typ
                self.body = body

        class _Table:
            def __init__(self, rows: List[Dict[str, Any]]):
                self._rows = rows

            def to_pandas(self):
                return self._rows

            def to_pylist(self):
                return list(self._rows)

            @classmethod
            def from_pandas(cls, df):
                return cls(list(df))

        pa_mod.Table = _Table  # type: ignore[attr-defined]
        pa_mod.Schema = object  # type: ignore[attr-defined]
        pa_mod.schema = lambda *a, **kw: None  # type: ignore[attr-defined]
        pa_mod.field = lambda *a, **kw: None  # type: ignore[attr-defined]
        pa_mod.list_ = lambda *a, **kw: None  # type: ignore[attr-defined]
        pa_mod.float32 = lambda: None  # type: ignore[attr-defined]
        pa_mod.int64 = lambda: None  # type: ignore[attr-defined]
        pa_mod.string = lambda: None  # type: ignore[attr-defined]
        flight_mod.Ticket = _Ticket  # type: ignore[attr-defined]
        flight_mod.Action = _Action  # type: ignore[attr-defined]
        flight_mod.FlightCallOptions = lambda **kw: kw  # type: ignore[attr-defined]
        pa_mod.flight = flight_mod  # type: ignore[attr-defined]

        pd_mod = types.ModuleType("pandas")
        pd_mod.DataFrame = lambda rows: rows  # type: ignore[attr-defined]

        pydantic_mod = types.ModuleType("pydantic")

        class _BaseModel:
            pass

        pydantic_mod.BaseModel = _BaseModel  # type: ignore[attr-defined]
        pydantic_mod.Field = lambda **kw: None  # type: ignore[attr-defined]

        sys.modules.setdefault("pyarrow", pa_mod)
        sys.modules.setdefault("pyarrow.flight", flight_mod)
        sys.modules.setdefault("pandas", pd_mod)
        sys.modules.setdefault("pydantic", pydantic_mod)
        return False


_HAS_NATIVE_DEPS = _ensure_optional_test_deps()

import pandas as pd  # noqa: E402
import pyarrow as pa  # noqa: E402

from longbow.client import LongbowClient  # noqa: E402


def _make_mock_client(rows: List[Dict[str, Any]] | None = None):
    client = LongbowClient(uri="grpc://localhost:3000")
    data_client = MagicMock()
    meta_client = MagicMock()

    sample_rows = rows or [{"id": "doc-0", "distance": 0.0, "score": 1.0}]
    df = pd.DataFrame(sample_rows)
    table = pa.Table.from_pandas(df) if hasattr(pa.Table, "from_pandas") else df
    reader = MagicMock()
    reader.read_all.return_value = table
    data_client.do_get.return_value = reader

    action_res = MagicMock()
    action_res.body.to_pybytes.return_value = json.dumps(
        {"results": sample_rows, "collections": []}
    ).encode("utf-8")
    meta_client.do_action.return_value = [action_res]

    client._data_client = data_client
    client._meta_client = meta_client
    return client, data_client, meta_client


class TestLongbowClientSearchModes(unittest.TestCase):
    def test_dense_and_pure_sparse_search_tickets(self):
        client, data_client, _ = _make_mock_client()

        df_sparse = client.search("kb", None, k=5, alpha=0.0, text_query="quantum")
        self.assertEqual(len(df_sparse), 1)
        ticket = json.loads(data_client.do_get.call_args[0][0].ticket.decode("utf-8"))
        self.assertIn("search", ticket)
        self.assertEqual(ticket["search"]["alpha"], 0.0)
        self.assertEqual(ticket["search"]["text_query"], "quantum")
        self.assertNotIn("vector", ticket["search"])

        client.search(
            "kb",
            [1.0, 0.0],
            k=3,
            filters=[{"field": "cat", "operator": "=", "value": "sec"}],
        )
        ticket_dense = json.loads(
            data_client.do_get.call_args[0][0].ticket.decode("utf-8")
        )
        self.assertEqual(ticket_dense["search"]["vector"], [1.0, 0.0])
        self.assertEqual(ticket_dense["search"]["filters"][0]["field"], "cat")

    def test_range_and_federated_and_by_id_tickets(self):
        client, data_client, meta_client = _make_mock_client()

        client.range_search("kb", [1.0, 0.0], threshold=0.25, max_results=20)
        t_range = json.loads(data_client.do_get.call_args[0][0].ticket.decode("utf-8"))
        self.assertEqual(t_range["range_search"]["threshold"], 0.25)
        self.assertEqual(t_range["range_search"]["max_results"], 20)

        client.federated_search(
            [1.0, 0.0], collections=["c1", "c2"], tags=["prod"], k=5, rrf_k=30
        )
        t_fed = json.loads(data_client.do_get.call_args[0][0].ticket.decode("utf-8"))
        self.assertEqual(t_fed["federated_search"]["collections"], ["c1", "c2"])
        self.assertEqual(t_fed["federated_search"]["tags"], ["prod"])
        self.assertEqual(t_fed["federated_search"]["rrf_k"], 30)

        client.search_by_id_df("kb", "doc-0", k=4)
        t_id = json.loads(data_client.do_get.call_args[0][0].ticket.decode("utf-8"))
        self.assertEqual(t_id["search_by_id"]["id"], "doc-0")

        client.list_federated_collections()
        self.assertEqual(
            meta_client.do_action.call_args[0][0].type, "ListFederatedCollections"
        )

    def test_advanced_text_and_bm25_actions(self):
        client, _, meta_client = _make_mock_client()

        client.advanced_text_search(
            "kb", "enc AND mesh", facet_fields=["category"], fuzzy_max_dist=1
        )
        act = meta_client.do_action.call_args[0][0]
        self.assertEqual(act.type, "AdvancedTextSearch")
        payload = json.loads(act.body.decode("utf-8"))
        self.assertEqual(payload["query"], "enc AND mesh")
        self.assertEqual(payload["facet_fields"], ["category"])

        client.search_facets("kb", "mesh", facet_fields=["category"])
        self.assertEqual(meta_client.do_action.call_args[0][0].type, "SearchFacets")

        client.search_analytics(limit=15)
        self.assertEqual(meta_client.do_action.call_args[0][0].type, "SearchAnalytics")

        client.tune_bm25("kb", k1=1.5, b=0.8)
        self.assertEqual(meta_client.do_action.call_args[0][0].type, "TuneBM25")

    def test_get_vector_flight_and_rest_dispatch(self):
        client, _, _ = _make_mock_client([{"id": "doc-0", "vector": [1.0, 0.0]}])
        rec = client.get_vector("kb", "doc-0")
        self.assertEqual(rec["id"], "doc-0")

        calls: List[Dict[str, Any]] = []

        def fake_rest(method, path, body=None, http_endpoint="http://127.0.0.1:6000"):
            calls.append({"method": method, "path": path, "body": body})
            return {"results": [{"id": "doc-0"}], "hits": [], "total_hits": 1}

        client._rest_request = fake_rest  # type: ignore[method-assign]
        self.assertEqual(len(client.rest_range_search("kb", [1.0, 0.0], threshold=0.5)), 1)
        self.assertEqual(len(client.rest_search_by_id("kb", "doc-0", k=2)), 1)
        self.assertEqual(
            len(
                client.rest_geo_search(
                    "kb",
                    search_type="box",
                    box={"min_lat": 1, "max_lat": 2, "min_lon": 3, "max_lon": 4},
                )
            ),
            1,
        )
        self.assertEqual(
            len(
                client.rest_temporal_search(
                    "kb", search_type="sliding_window_time", duration="5m"
                )
            ),
            1,
        )
        self.assertEqual(
            len(
                client.rest_multimodal_search(
                    "media", modality="text", text_query="quantum"
                )
            ),
            1,
        )
        self.assertEqual(
            len(client.rest_federated_search([1.0, 0.0], collections=["c1", "c2"])), 1
        )
        self.assertEqual(
            len(
                client.rest_hybrid_search(
                    "kb", [1.0, 0.0], text_query="quantum", graph_alpha=0.2
                )
            ),
            1,
        )
        self.assertEqual(client.rest_text_search("kb", "quantum")["total_hits"], 1)
        self.assertEqual(
            [c["path"] for c in calls],
            [
                "/api/v1/datasets/kb/range-search",
                "/api/v1/datasets/kb/search/by-id",
                "/api/v1/datasets/kb/geo-search",
                "/api/v1/datasets/kb/temporal-search",
                "/api/v1/datasets/media/multimodal-search",
                "/api/v1/search/federated",
                "/api/v1/datasets/kb/hybrid",
                "/api/v1/datasets/kb/text-search",
            ],
        )


if __name__ == "__main__":
    unittest.main()
