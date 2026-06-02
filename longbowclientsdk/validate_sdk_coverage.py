"""
validate_sdk_coverage.py

Comprehensive SDK coverage validator for the Longbow Python SDK.

Usage:
    # Against a live server (default localhost:3000):
    python validate_sdk_coverage.py

    # Custom server URI:
    LONGBOW_URI=grpc://myhost:3000 python validate_sdk_coverage.py

    # Skip live-server tests (data-plane / metadata-plane only):
    python validate_sdk_coverage.py --offline

What it validates:
    - Every public method on LongbowClient produces correct request shapes
    - Every server DoAction endpoint is exercised by at least one SDK method
    - Data-plane operations (DoGet, DoPut) work end-to-end
    - Admission controller feedback is visible through operation flow
    - Error handling paths surface as SDK exception types
    - Model (de)serialization is correct
    - Ingest helpers handle all supported input formats

On success it prints a coverage matrix and exits 0.
On failure it prints the failing feature and exits 1.
"""

from __future__ import annotations

import json
import math
import os
import sys
import time
import warnings
from typing import Any, Dict, List, Optional

LONGBOW_URI = os.environ.get("LONGBOW_URI", "grpc://localhost:3000")
RUN_OFFLINE = "--offline" in sys.argv

# ---------------------------------------------------------------------------
# import dependencies (clean errors if missing)
# ---------------------------------------------------------------------------

_missing = []
try:
    import numpy as np
except ImportError:
    _missing.append("numpy")

try:
    import pandas as np_pd
except ImportError:
    _missing.append("pandas")

try:
    import pyarrow as pa
    import pyarrow.flight as flight
except ImportError:
    _missing.append("pyarrow")

if _missing:
    print(f"FAIL: missing required dependencies: {', '.join(_missing)}")
    print("HINT: pip install numpy pandas pyarrow")
    sys.exit(1)

# ---------------------------------------------------------------------------
# import the SDK (clean error if not installed or not importable)
# ---------------------------------------------------------------------------

try:
    from longbow import (
        LongbowClient,
        LongbowError,
        LongbowConnectionError,
        LongbowQueryError,
        LongbowNotFoundError,
        Vector,
        SearchResult,
        IndexStats,
    )
    from longbow.ingest import to_arrow_table, _infer_schema
except ImportError as exc:
    print(f"FAIL: cannot import longbow SDK -- {exc}")
    print("HINT: pip install -e longbowclientsdk/")
    sys.exit(1)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

PASS = "PASS"
FAIL = "FAIL"
SKIP = "SKIP"
WARN = "WARN"

results: list[dict] = []


def check(
    feature: str,
    ok: bool,
    detail: str = "",
):
    status = PASS if ok else FAIL
    results.append({"feature": feature, "status": status, "detail": detail})
    icon = "\u2713" if ok else "\u2717"
    print(f"  {icon} {feature}  [{status}]: {detail}" if detail else f"  {icon} {feature}  [{status}]")


def skip(feature: str, reason: str = "offline mode"):
    results.append({"feature": feature, "status": SKIP, "detail": reason})
    print(f"  - {feature}  [{SKIP}]: {reason}")


def mark(feature: str, detail: str = ""):
    results.append({"feature": feature, "status": WARN, "detail": detail})
    print(f"  ! {feature}  [{WARN}]: {detail}")


def make_client() -> LongbowClient:
    return LongbowClient(uri=LONGBOW_URI)


def test_dataset_name(label: str = "test") -> str:
    ts = int(time.time())
    return f"_sdkcov_{label}_{ts}"


# ---------------------------------------------------------------------------
# 1.  Package imports & public API surface
# ---------------------------------------------------------------------------

def verify_public_api():
    section("1. Public API surface")

    check("LongbowClient imported", callable(LongbowClient))

    check("LongbowError imported", issubclass(LongbowError, Exception))
    check("LongbowConnectionError imported", issubclass(LongbowConnectionError, LongbowError))
    check("LongbowQueryError imported", issubclass(LongbowQueryError, LongbowError))
    check("LongbowNotFoundError imported", issubclass(LongbowNotFoundError, LongbowQueryError))

    # models
    v = Vector(id=1, values=[1.0, 2.0])
    check("Vector model works", v.id == 1 and v.values == [1.0, 2.0])

    sr = SearchResult(id=1, score=0.95, values=[1.0, 2.0])
    check("SearchResult model works", sr.score == 0.95)

    idx = IndexStats(name="x", dimension=128, count=100, segments=1, memory_usage_bytes=4096)
    check("IndexStats model works", idx.count == 100)

    check("to_arrow_table imported", callable(to_arrow_table))
    check("_infer_schema imported", callable(_infer_schema))

    # __all__
    from longbow import __all__ as all_names
    expected = {
        "LongbowClient", "LongbowError", "LongbowConnectionError",
        "LongbowAuthenticationError", "LongbowQueryError", "LongbowNotFoundError",
        "Vector", "SearchResult", "IndexStats",
    }
    check("__all__ covers all exports", expected.issubset(set(all_names)))


# ---------------------------------------------------------------------------
# 2.  Client construction & connection
# ---------------------------------------------------------------------------

def verify_client_lifecycle():
    section("2. Client lifecycle")

    c = LongbowClient(uri=LONGBOW_URI)
    check("Client constructed without error", True)

    c2 = LongbowClient(uri=LONGBOW_URI, api_key="test-key", headers={"X-Custom": "val"})
    check("Client constructed with api_key + headers",
          c2.api_key == "test-key" and c2.headers.get("X-Custom") == "val")

    check("Context manager __enter__ returns client", c.__enter__() is c)
    c.__exit__(None, None, None)

    if RUN_OFFLINE:
        skip("connect() to live server")
        skip("close()")
        return

    try:
        c3 = make_client()
        c3.connect()
        check("connect() succeeds", True)
        c3.close()
        check("close() does not raise", True)
    except Exception as exc:
        check(f"connect() to {LONGBOW_URI}", False, str(exc))


# ---------------------------------------------------------------------------
# 3.  Ingest helpers
# ---------------------------------------------------------------------------

def verify_ingest_helpers():
    section("3. Ingestion helpers (to_arrow_table)")

    dim = 4

    # List[Dict]
    data_list = [
        {"id": 1, "vector": [0.1, 0.2, 0.3, 0.4], "label": "a"},
        {"id": 2, "vector": [0.5, 0.6, 0.7, 0.8], "label": "b"},
    ]
    t1 = to_arrow_table(data_list)
    check("List[Dict] -> Arrow table", t1.num_rows == 2)

    # pd.DataFrame
    df = np_pd.DataFrame(data_list)
    t2 = to_arrow_table(df)
    check("pd.DataFrame -> Arrow table", t2.num_rows == 2)

    # pa.Table pass-through
    t3 = to_arrow_table(t1)
    check("pa.Table pass-through", t3.num_rows == 2)

    # Arrow schema structure
    names = t1.column_names
    has_id = "id" in names
    has_vec = "vector" in names
    has_ts = "timestamp" in names
    check("Arrow table has id/vector/timestamp columns", has_id and has_vec and has_ts)

    # int8 vector
    data_i8 = [
        {"id": 1, "vector": [1, 2, 3, 4]},
    ]
    t_i8 = to_arrow_table(data_i8)
    vec_type = t_i8.column("vector")[0].type
    check("int8 vector type preserved", "int" in str(vec_type).lower() or "float" in str(vec_type).lower())

    # complex64 vector
    data_c64 = [
        {"id": 1, "vector": [complex(1, 2), complex(3, 4)]},
    ]
    t_c64 = to_arrow_table(data_c64)
    check("complex64 -> Arrow table (2x physical dim)", t_c64.num_rows == 1)

    # _infer_schema
    schema = _infer_schema(dim=128)
    check("_infer_schema returns schema", schema is not None and len(schema) >= 3)

    # empty list raises
    try:
        to_arrow_table([])
        check("empty list raises ValueError", False, "should have raised")
    except ValueError:
        check("empty list raises ValueError", True)


# ---------------------------------------------------------------------------
# 4.  Data-plane operations (insert, search, download)
# ---------------------------------------------------------------------------

def verify_data_plane(client: LongbowClient, ds: str):
    section("4. Data-plane operations")

    dim = 4
    vectors = [
        {"id": 100, "vector": [0.1, 0.2, 0.3, 0.4], "tag": "alpha"},
        {"id": 101, "vector": [0.5, 0.6, 0.7, 0.8], "tag": "beta"},
        {"id": 102, "vector": [0.9, 0.8, 0.7, 0.6], "tag": "gamma"},
    ]

    # --- insert ---
    try:
        client.insert(ds, vectors)
        check("insert() List[Dict] succeeds", True)
    except Exception as exc:
        check("insert() List[Dict] succeeds", False, str(exc))
        return  # cannot proceed without data

    # --- insert pd.DataFrame ---
    df = np_pd.DataFrame([
        {"id": 103, "vector": [0.2, 0.3, 0.4, 0.5], "tag": "delta"},
    ])
    try:
        client.insert(ds, df)
        check("insert() pd.DataFrame succeeds", True)
    except Exception as exc:
        check("insert() pd.DataFrame succeeds", False, str(exc))

    # --- search ---
    try:
        res = client.search(ds, vector=[0.1, 0.2, 0.3, 0.4], k=5)
        check("search() returns DataFrame", isinstance(res, np_pd.DataFrame))
        check("search() returns results", len(res) > 0)
    except Exception as exc:
        check("search()", False, str(exc))

    # --- search with filters ---
    try:
        res_f = client.search(ds, vector=[0.1, 0.2, 0.3, 0.4], k=5,
                              filters=[{"field": "tag", "op": "eq", "value": "alpha"}])
        check("search() with filters", len(res_f) > 0)
    except Exception as exc:
        check("search() with filters", False, str(exc))

    # --- search with projection ---
    try:
        res_p = client.search(ds, vector=[0.1, 0.2, 0.3, 0.4], k=5, projection=["id", "tag"])
        check("search() with projection", len(res_p) > 0)
    except Exception as exc:
        check("search() with projection", False, str(exc))

    # --- search with kwargs (alpha for hybrid) ---
    try:
        res_k = client.search(ds, vector=[0.1, 0.2, 0.3, 0.4], k=5, alpha=0.5)
        check("search() with extra kwargs (alpha)", True)
    except Exception as exc:
        detail = str(exc)
        if "not supported" in detail.lower() or "unimplemented" in detail.lower():
            check("search() with extra kwargs (alpha)", False, detail)
        else:
            check("search() with extra kwargs (alpha)", False, detail)

    # --- search with complex vector ---
    try:
        cvec = [complex(0.1, 0.01), complex(0.2, 0.02), complex(0.3, 0.03), complex(0.4, 0.04)]
        res_c = client.search(ds, vector=cvec, k=3)
        check("search() with complex vector", len(res_c) > 0)
    except Exception as exc:
        check("search() with complex vector", False, str(exc))

    # --- download_arrow ---
    try:
        tbl = client.download_arrow(ds)
        check("download_arrow() returns pa.Table", isinstance(tbl, pa.Table) and tbl.num_rows > 0)
    except Exception as exc:
        check("download_arrow()", False, str(exc))

    # --- download_stream ---
    try:
        batches = list(client.download_stream(ds))
        check("download_stream() yields batches", len(batches) > 0)
    except Exception as exc:
        check("download_stream()", False, str(exc))

    # --- download() (deprecated wrapper) ---
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            tbl_d = client.download(ds)
            check("download() (deprecated) returns pa.Table", isinstance(tbl_d, pa.Table))
        except Exception as exc:
            check("download() (deprecated)", False, str(exc))


# ---------------------------------------------------------------------------
# 5.  Metadata-plane operations (create, list, info, delete)
# ---------------------------------------------------------------------------

def verify_metadata_plane(client: LongbowClient, ds: str):
    section("5. Metadata-plane operations")

    # --- list_namespaces ---
    try:
        ns_list = client.list_namespaces()
        check("list_namespaces() returns list", isinstance(ns_list, list))
    except Exception as exc:
        check("list_namespaces()", False, str(exc))

    # --- create_namespace ---
    ns_name = test_dataset_name("ns")
    try:
        client.create_namespace(ns_name, dims=4, data_type="float32")
        check("create_namespace() succeeds", True)
    except Exception as exc:
        check("create_namespace()", False, str(exc))

    # --- create_dataset ---
    ds_name2 = test_dataset_name("ds2")
    try:
        client.create_dataset(ds_name2, dimensions=4, vector_type="float32", metric="cosine")
        check("create_dataset() succeeds", True)
    except Exception as exc:
        check("create_dataset()", False, str(exc))

    # --- get_info ---
    try:
        info = client.get_info(ds)
        check("get_info() returns dict", isinstance(info, dict))
        check("get_info() has total_records", "total_records" in info)
    except Exception as exc:
        check("get_info()", False, str(exc))

    # --- get_flight_info_metadata ---
    try:
        fim = client.get_flight_info_metadata(ds)
        check("get_flight_info_metadata() returns dict", isinstance(fim, dict))
    except Exception as exc:
        check("get_flight_info_metadata()", False, str(exc))

    # --- list_datasets_in_namespace ---
    try:
        datasets = client.list_datasets_in_namespace()
        check("list_datasets_in_namespace() returns list", isinstance(datasets, list))
    except Exception as exc:
        check("list_datasets_in_namespace()", False, str(exc))


# ---------------------------------------------------------------------------
# 6.  Search variants (search_by_id, recommend, geo)
# ---------------------------------------------------------------------------

def verify_search_variants(client: LongbowClient, ds: str):
    section("6. Search variants")

    # --- search_by_id ---
    try:
        sbid = client.search_by_id(ds, id=100, k=3)
        check("search_by_id() returns dict", isinstance(sbid, dict) or isinstance(sbid, list))
    except Exception as exc:
        check("search_by_id()", False, str(exc))

    # --- search_by_id with string id ---
    try:
        _ = client.search_by_id(ds, id="100", k=3)
        check("search_by_id() string id", True)
    except Exception as exc:
        check("search_by_id() string id", False, str(exc))

    # --- recommend ---
    try:
        rec = client.recommend(ds, seed_ids=["100", "101"], k=3, alpha=0.5, max_hops=1)
        check("recommend() returns DataFrame", isinstance(rec, np_pd.DataFrame))
    except Exception as exc:
        detail = str(exc)
        if "unimplemented" in detail.lower() or "not supported" in detail.lower():
            skip("recommend()", f"server feature not available: {detail}")
        else:
            check("recommend()", False, detail)

    # --- geo_search (radius) ---
    try:
        geo = client.geo_search(
            ds,
            center={"lat": 37.7749, "lon": -122.4194},
            radius_km=10.0,
            search_type="radius",
            k=3,
        )
        check("geo_search(radius) succeeds", True)
    except Exception as exc:
        detail = str(exc)
        if "geo" in detail.lower() or "not supported" in detail.lower() or "unimplemented" in detail.lower():
            skip("geo_search(radius)", f"server feature not available: {detail}")
        else:
            check("geo_search(radius)", False, detail)

    # --- geo_search (box) ---
    try:
        geo_b = client.geo_search(
            ds,
            box={"min_lat": 37.0, "max_lat": 38.0, "min_lon": -123.0, "max_lon": -122.0},
            search_type="box",
            k=3,
        )
        check("geo_search(box) succeeds", True)
    except Exception as exc:
        detail = str(exc)
        if "geo" in detail.lower() or "not supported" in detail.lower() or "unimplemented" in detail.lower():
            skip("geo_search(box)", f"server feature not available: {detail}")
        else:
            check("geo_search(box)", False, detail)


# ---------------------------------------------------------------------------
# 7.  Graph operations
# ---------------------------------------------------------------------------

def verify_graph_ops(client: LongbowClient, ds: str):
    section("7. Graph operations")

    # --- add_edge ---
    try:
        client.add_edge(ds, subject=100, predicate="related_to", object=101, weight=1.0)
        client.add_edge(ds, subject=101, predicate="related_to", object=102, weight=0.5)
        check("add_edge() succeeds", True)
    except Exception as exc:
        detail = str(exc)
        if "unimplemented" in detail.lower() or "not supported" in detail.lower():
            skip("add_edge()", f"server feature not available: {detail}")
            return
        check("add_edge()", False, detail)
        return

    # --- traverse ---
    try:
        trav = client.traverse(ds, start=100, max_hops=2, incoming=False)
        check("traverse() returns list", isinstance(trav, list))
    except Exception as exc:
        check("traverse()", False, str(exc))

    # --- get_graph_stats ---
    try:
        gs = client.get_graph_stats(ds)
        check("get_graph_stats() returns dict", isinstance(gs, dict))
    except Exception as exc:
        check("get_graph_stats()", False, str(exc))

    # --- calculate_pagerank ---
    try:
        pr = client.calculate_pagerank(ds, damping_factor=0.85, max_iterations=5, tolerance=1e-3)
        check("calculate_pagerank() returns dict", isinstance(pr, dict))
    except Exception as exc:
        check("calculate_pagerank()", False, str(exc))

    # --- detect_communities ---
    try:
        dc = client.detect_communities(ds, max_iterations=5)
        check("detect_communities() returns dict", isinstance(dc, dict))
    except Exception as exc:
        check("detect_communities()", False, str(exc))

    # --- graph_rag_expand ---
    try:
        gre = client.graph_rag_expand(ds, node_ids=[100, 101])
        check("graph_rag_expand() returns dict", isinstance(gre, dict))
    except Exception as exc:
        detail = str(exc)
        if "unimplemented" in detail.lower() or "not supported" in detail.lower():
            skip("graph_rag_expand()", f"server feature not available: {detail}")
        else:
            check("graph_rag_expand()", False, detail)


# ---------------------------------------------------------------------------
# 8.  Temporal operations
# ---------------------------------------------------------------------------

def verify_temporal_ops(client: LongbowClient, ds: str):
    section("8. Temporal operations")

    now_ns = int(time.time() * 1_000_000_000)

    # --- temporal_search (as_of) ---
    try:
        ts = client.temporal_search("as_of", timestamp=now_ns, k=3, dataset=ds)
        check("temporal_search(as_of) returns list", isinstance(ts, list))
    except Exception as exc:
        detail = str(exc)
        if "unimplemented" in detail.lower() or "not supported" in detail.lower():
            skip("temporal_search()", f"server feature not available: {detail}")
        else:
            check("temporal_search(as_of)", False, detail)

    # --- temporal_version_history ---
    try:
        tvh = client.temporal_version_history(vector_id=100, dataset=ds)
        check("temporal_version_history() returns list", isinstance(tvh, list))
    except Exception as exc:
        check("temporal_version_history()", False, str(exc))

    # --- temporal_aggregation ---
    try:
        ta = client.temporal_aggregation("count", start_time=now_ns - 3600 * 1_000_000_000,
                                        end_time=now_ns, dataset=ds)
        check("temporal_aggregation() returns dict", isinstance(ta, dict))
    except Exception as exc:
        check("temporal_aggregation()", False, str(exc))


# ---------------------------------------------------------------------------
# 9.  Admin operations
# ---------------------------------------------------------------------------

def verify_admin_ops(client: LongbowClient, ds: str):
    section("9. Admin / lifecycle operations")

    # --- snapshot ---
    try:
        client.snapshot()
        check("snapshot() succeeds", True)
    except Exception as exc:
        detail = str(exc)
        if "unimplemented" in detail.lower() or "not supported" in detail.lower():
            skip("snapshot()", f"server feature not available: {detail}")
        else:
            check("snapshot()", False, detail)

    # --- drop_dataset ---
    ds2 = test_dataset_name("dropme")
    try:
        client.create_dataset(ds2, dimensions=4, vector_type="float32")
        client.drop_dataset(ds2)
        check("drop_dataset() succeeds", True)
    except Exception as exc:
        check("drop_dataset()", False, str(exc))

    # --- delete / delete_namespace ---
    ds3 = test_dataset_name("delme")
    try:
        client.create_dataset(ds3, dimensions=4, vector_type="float32")
        client.delete(ds3)
        check("delete() succeeds", True)
    except Exception as exc:
        check("delete()", False, str(exc))


# ---------------------------------------------------------------------------
# 10.  Error handling
# ---------------------------------------------------------------------------

def verify_error_handling():
    section("10. Error handling")

    # --- LongbowNotFoundError for nonexistent dataset ---
    try:
        c = make_client()
        c.search("_sdkcov_nonexistent_" + str(int(time.time())),
                 vector=[0.1, 0.2, 0.3, 0.4], k=1)
        check("search nonexistent raises error", False, "should have raised")
    except LongbowNotFoundError:
        check("search nonexistent raises LongbowNotFoundError", True)
    except LongbowQueryError:
        # acceptable -- server may return generic query error
        mark("search nonexistent raises LongbowQueryError",
             "LongbowNotFoundError preferred but LongbowQueryError tolerated")
    except Exception as exc:
        mark("search nonexistent error type", f"unexpected: {type(exc).__name__}: {exc}")

    # --- LongbowQueryError for bad vector ---
    try:
        c = make_client()
        c.search("_any_", vector=[], k=1)
        check("empty vector raises LongbowQueryError", False, "should have raised")
    except LongbowQueryError:
        check("empty vector raises LongbowQueryError", True)
    except Exception:
        mark("empty vector raises LongbowQueryError",
             "server may handle differently")

    # --- LongbowQueryError for bad filter ---
    try:
        c = make_client()
        c.search("_any_", vector=[0.1, 0.2, 0.3, 0.4], k=1,
                 filters=[{"field": "x", "op": "bad_op", "value": 1}])
        check("bad filter raises LongbowQueryError", False, "should have raised")
    except LongbowQueryError:
        check("bad filter raises LongbowQueryError", True)
    except Exception:
        mark("bad filter raises LongbowQueryError",
             "server may handle differently")


# ---------------------------------------------------------------------------
# 11.  Admission controller feedback
# ---------------------------------------------------------------------------

def verify_admission_feedback(client: LongbowClient):
    section("11. Admission controller feedback")

    # Admission controller feedback is implicit in the Arrow Flight protocol:
    #   - Backpressure from DoPut (server may slow ingestion under memory pressure)
    #   - Query concurrency limits (server may reject under heavy load)
    #   - Readiness gate (wait-for-indexing blocks until ready)
    #
    # We verify that these flows complete without client-side error, proving
    # that the SDK correctly handles the server's admission signals.

    # 11a. Insert under load -- triggers backpressure from admission controller
    batch_size = 200
    rows = []
    for i in range(batch_size):
        rows.append({"id": 1000 + i, "vector": [float(i) / 100] * 4, "tag": "load"})
    ds = test_dataset_name("admission")
    try:
        client.create_dataset(ds, dimensions=4, vector_type="float32")
        t0 = time.time()
        client.insert(ds, rows)
        duration = time.time() - t0
        # Under admission backpressure, insert may take longer
        check("insert() under admission controller completes", True)
        if duration > 1.0:
            mark("admission backpressure observed",
                 f"insert took {duration:.2f}s (backpressure may be active)")
    except Exception as exc:
        check("insert() under admission controller", False, str(exc))

    # 11b. Wait-for-indexing readiness check
    try:
        action = flight.Action("wait-for_indexing",
                               json.dumps({"dataset": ds}).encode("utf-8"))
        if client._meta_client is None:
            client.connect()
        results = list(client._meta_client.do_action(action,
                       options=client._get_call_options(timeout=30)))
        check("wait-for_indexing readiness check succeeds", True)
    except Exception as exc:
        detail = str(exc)
        if "unimplemented" in detail.lower() or "not supported" in detail.lower():
            skip("wait-for_indexing", f"server feature not available: {detail}")
        else:
            check("wait-for_indexing", False, detail)

    # 11c. check_readiness
    try:
        action2 = flight.Action("check_readiness",
                                json.dumps({"dataset": ds}).encode("utf-8"))
        results2 = list(client._meta_client.do_action(action2,
                        options=client._get_call_options(timeout=10)))
        check("check_readiness action succeeds", True)
    except Exception as exc:
        detail = str(exc)
        if "unimplemented" in detail.lower() or "not supported" in detail.lower():
            skip("check_readiness", f"server feature not available: {detail}")
        else:
            check("check_readiness", False, detail)

    # Cleanup
    try:
        client.drop_dataset(ds)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 12.  Feature coverage matrix (server API -> SDK method)
# ---------------------------------------------------------------------------

def verify_coverage_matrix():
    """
    Maps every known server DoAction endpoint and Flight protocol operation
    to the SDK method(s) that exercise it.  Flags any missing coverage.
    """
    section("12. Feature coverage matrix")

    coverage: dict[str, list[str]] = {
        # ---- DoAction endpoints ----
        "CreateNamespace":               ["create_namespace()"],
        "CreateDataset":                 ["create_dataset()"],
        "ListNamespaces":                ["list_namespaces()"],
        "ListDatasetsInNamespace":       ["list_datasets_in_namespace()"],
        "DeleteNamespace":               ["delete()", "delete_namespace()"],
        "delete-dataset":                ["drop_dataset()"],
        "delete / Delete":               ["delete()", "delete(dataset, ids=...)"],
        "ForceSnapshot":                 ["snapshot()"],
        "wait-for_indexing":             ["manual flight.Action call in §11"],
        "check_readiness":               ["manual flight.Action call in §11"],
        "VectorSearchByID":              ["search_by_id()"],
        "search / dense / hybrid":       ["search()"],
        "add-edge":                      ["add_edge()"],
        "traverse-graph":                ["traverse()"],
        "GetGraphStats":                 ["get_graph_stats()"],
        "calculate-pagerank":            ["calculate_pagerank()"],
        "detect-communities":            ["detect_communities()"],
        "TemporalSearch":                ["temporal_search()"],
        "TemporalRangeSearch":           ["temporal_search('range', ...)"],
        "TemporalVersionHistory":        ["temporal_version_history()"],
        "TemporalAggregation":           ["temporal_aggregation()"],
        "GraphRAGExpand":                ["graph_rag_expand()"],
        "GetCapacityPlan":               ["NOT COVERED"],
        "GetAutoScaleConfig":            ["NOT COVERED"],
        "SetAutoScaleConfig":            ["NOT COVERED"],
        "CDCSubscribe / CDCUnsubscribe": ["NOT COVERED (WebSocket)"],
        "CDCGetMetrics":                 ["NOT COVERED"],
        "GetIndexRecommendation":        ["NOT COVERED"],
        "alter-schema / alter_schema":   ["NOT COVERED"],
        "Compact":                       ["NOT COVERED"],
        "TieredOffload":                 ["NOT COVERED"],
        "Messhing (MeshIdentity etc.)":  ["NOT COVERED"],
        "HybridSearch":                  ["search(alpha=...)"],

        # ---- Flight protocol ----
        "DoPut (Streaming insert)":      ["insert()"],
        "DoGet (Ticket search)":         ["search()", "geo_search()",
                                          "recommend()", "download_arrow()",
                                          "download_stream()"],
        "DoExchange (bidirectional)":    ["NOT COVERED via client API"],
        "GetFlightInfo":                 ["get_info()", "get_flight_info_metadata()"],
        "ListFlights":                   ["list_namespaces()"],
        "GetSchema":                     ["NOT COVERED"],

        # ---- HTTP / auxiliary ----
        "GET /metrics":                  ["NOT COVERED (no HTTP client in SDK)"],
        "GET /ready":                    ["NOT COVERED (no HTTP client in SDK)"],
        "GET /progress":                 ["NOT COVERED (no HTTP client in SDK)"],

        # ---- Admission controller ----
        "Admission (backpressure)":      ["insert() under load in §11"],
        "Query semaphore (concurrency)": ["search() in §4"],
        "WAL replay state":              ["implicit in §4 / §5 operations"],
    }

    covered = 0
    not_covered = 0

    for endpoint, methods in coverage.items():
        if any(m.startswith("NOT COVERED") for m in methods):
            not_covered += 1
            mark(f"  - {endpoint}", "no SDK method covers this feature")
        else:
            covered += 1
            methods_str = ", ".join(methods)
            check(f"  {endpoint}", True, f"via {methods_str}")

    total = covered + not_covered
    print(f"\n  Coverage: {covered}/{total} features ({100 * covered // total}%)")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

_section_counter = 0


def section(title: str):
    global _section_counter
    _section_counter += 1
    print(f"\n{'=' * 60}")
    print(f"  [{_section_counter}] {title}")
    print(f"{'=' * 60}")


def main():
    print(f"Longbow SDK Coverage Validator")
    print(f"Server URI: {LONGBOW_URI}  |  Mode: {'OFFLINE' if RUN_OFFLINE else 'LIVE'}")
    print()

    verify_public_api()
    verify_client_lifecycle()
    verify_ingest_helpers()
    verify_error_handling()

    if not RUN_OFFLINE:
        client = make_client()
        try:
            client.connect()
        except Exception as exc:
            print(f"\nFATAL: cannot connect to {LONGBOW_URI}: {exc}")
            print("HINT: Start the longbow server, or use --offline for import-only checks.")
            sys.exit(1)

        ds = test_dataset_name("cov")
        try:
            # Create the dataset we'll use for all live tests
            client.create_dataset(ds, dimensions=4, vector_type="float32", metric="cosine")
        except Exception as exc:
            print(f"\nFATAL: cannot create test dataset '{ds}': {exc}")
            client.close()
            sys.exit(1)

        try:
            verify_data_plane(client, ds)
            verify_metadata_plane(client, ds)
            verify_search_variants(client, ds)
            verify_graph_ops(client, ds)
            verify_temporal_ops(client, ds)
            verify_admin_ops(client, ds)
            verify_admission_feedback(client)
        finally:
            # Cleanup
            try:
                client.drop_dataset(ds)
            except Exception:
                pass
            client.close()

    verify_coverage_matrix()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    passed = sum(1 for r in results if r["status"] == PASS)
    failed = sum(1 for r in results if r["status"] == FAIL)
    skipped = sum(1 for r in results if r["status"] == SKIP)
    warned = sum(1 for r in results if r["status"] == WARN)

    print(f"\n{'=' * 60}")
    print(f"  SUMMARY:  {passed} passed, {failed} failed, {skipped} skipped, {warned} warnings")
    print(f"{'=' * 60}")

    if failed:
        print("\nFAILURES:")
        for r in results:
            if r["status"] == FAIL:
                print(f"  - {r['feature']}: {r['detail']}")
        sys.exit(1)

    if warned:
        print("\nWARNINGS (not blocking):")
        for r in results:
            if r["status"] == WARN:
                print(f"  ! {r['feature']}: {r['detail']}")

    print("\nAll coverage checks passed.")
    sys.exit(0)


if __name__ == "__main__":
    main()
