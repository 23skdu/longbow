#!/usr/bin/env python3
"""Multi-namespace multiagent soak scenario with server-enforced RBAC.

Default (scripts/configs/multiagent_soak.json): 3 agents, 7 namespaces.
  - private_<i>: agent i reads/writes/deletes; invisible to other agents
  - shared_<i>: agent i reads/writes/deletes; other agents read only
  - master: all agents read/write/delete

Permissions are enforced by Longbow's RBAC manager. The admin client creates
the namespaces and issues scoped per-agent API keys before agents start.

The operation mix comes from the shared ``workload`` section of the JSON config,
so this simulator and cmd/agentsoak drive the same full soak workload. Ops the
SDK cannot issue are counted as ``unsupported`` no-ops (never as client errors).

Usage:
    python3 scripts/agent_soak_scenario.py --config scripts/configs/multiagent_soak.json
"""
import argparse
import json
import math
import os
import random
import re
import sys
import threading
import time
from collections import deque
from datetime import datetime

from multiagent_config import (
    DEFAULT_CONFIG,
    Topology,
    load_config,
    security_config,
    workload_choices,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "longbowclientsdk", "src"))
try:
    from longbow import LongbowClient
    from longbow.exceptions import LongbowAuthorizationError
    HAS_SDK = True
except Exception:  # pragma: no cover - exercised only without SDK installed
    LongbowClient = None

    class LongbowAuthorizationError(Exception):
        """Placeholder so RBAC checks still parse without the SDK."""

    HAS_SDK = False


CSV_BASE_COLUMNS = [
    "timestamp", "elapsed_s", "ops", "errors", "error_rate", "qps",
    "p50_ms", "p95_ms", "p99_ms",
]
CSV_STALL_COLUMNS = ["stalled", "stalls", "stall_s", "baseline_qps", "window_qps", "unsupported"]
CSV_OP_COLUMNS = ["ops", "errors", "error_rate", "qps", "p50_ms", "p95_ms", "p99_ms", "unsupported"]

WRITE_OPS = frozenset(("insert", "upsert", "delete", "delete_ns"))

# SDK method used by each op; a missing method is a counted no-op rather than
# a client error. Ops absent from this map are handled explicitly below.
OP_METHODS = {
    "dense": "search",
    "cross_read": "search",
    "hybrid": "search",
    "sparse": "search",
    "filtered": "search",
    "range": "range_search",
    "byid": "search_by_id",
    "temporal_asof": "temporal_search",
    "temporal_range": "temporal_search",
    "temporal_window": "temporal_search",
    "geo_radius": "geo_search",
    "geo_box": "geo_search",
    "geo_hybrid": "geo_search",
    "graphrag": "graph_rag_expand",
    "traverse": "traverse",
    "recommend": "recommend",
    "multimodal": "multimodal_search",
    "federated": "federated_search",
    "text": "advanced_text_search",
    "insert": "insert",
    "upsert": "insert",
    "delete": "delete",
    "pagerank": "calculate_pagerank",
    "communities": "detect_communities",
    "get_vector": "get_vector",
    "graph_stats": "get_graph_stats",
}


def _vec(dim, rng):
    v = [rng.gauss(0, 1) for _ in range(dim)]
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


def _percentile(values, pct):
    if not values:
        return 0.0
    idx = min(len(values) - 1, int(pct / 100.0 * (len(values) - 1)))
    return values[idx] * 1000.0


def client_security_kwargs(cfg):
    """Translate the shared security section into SDK client kwargs.

    RBAC is always enforced by the server. This helper only supplies tenant and
    transport credentials; it never returns or logs an API key or token.
    """
    security = security_config(cfg)
    kwargs = {}
    if security["tenant_id"]:
        kwargs["tenant_id"] = security["tenant_id"]
    tls = security["tls"]
    if tls["enabled"]:
        # Fail closed on unreadable material; never fall back to an insecure
        # connection or disable verification.
        for opt, path in (
            ("tls_root_certs", tls["ca_file"]),
            ("tls_cert_chain", tls["cert_file"]),
            ("tls_private_key", tls["key_file"]),
        ):
            if path:
                with open(path, "rb") as f:
                    kwargs[opt] = f.read()
    oidc = security["oidc"]
    if oidc["mode"] != "none":
        token_path = os.environ.get(oidc["token_file_env"], "")
        if not token_path:
            raise RuntimeError(f"{oidc['token_file_env']} must name an OIDC token file")
        with open(token_path, encoding="utf-8") as f:
            token = f.read().strip()
        if not token:
            raise RuntimeError("OIDC token file is empty")
        kwargs["oidc_token"] = token
    return kwargs


def connect_client(cfg, api_key=None, **overrides):
    if not HAS_SDK:
        raise RuntimeError("Longbow Python SDK is required for the RBAC-provisioned soak")
    kwargs = client_security_kwargs(cfg)
    kwargs.update(overrides)
    client = LongbowClient(uri=cfg["uri"], meta_uri=cfg["meta_uri"], api_key=api_key, **kwargs)
    client.connect()
    return client


def provision(cfg, topology, admin_key):
    """Create namespaces and per-agent writer/reader keys via the admin client."""
    admin = connect_client(cfg, admin_key)
    rng = random.Random(cfg["seed"])
    tenant_id = security_config(cfg)["tenant_id"]
    keys = {}
    try:
        for ns in topology.namespaces:
            try:
                admin.create_namespace(name=ns, dims=cfg["dim"], data_type="float32")
            except Exception as e:
                if cfg["verbose"]:
                    print(f"[provision] create {ns}: {e}")
            admin.insert(ns, [{"id": f"seed_{ns}", "vector": _vec(cfg["dim"], rng), "importance": 1}])
        for i in range(topology.agents):
            writer = admin.create_api_key(
                name=f"agent_{i}_writer", role="writer", namespace="",
                datasets=topology.writable(i), tenant_id=tenant_id,
            )
            reader = admin.create_api_key(
                name=f"agent_{i}_reader", role="reader", namespace="",
                datasets=topology.read_only(i), tenant_id=tenant_id,
            )
            if not writer.get("api_key") or not reader.get("api_key"):
                raise RuntimeError(f"failed to provision RBAC keys for agent {i}")
            # Keep the raw keys in memory only; never include them in logs.
            keys[i] = {"writer": writer["api_key"], "reader": reader["api_key"]}
    finally:
        admin.close()
    return keys


def verify_rbac(cfg, topology, keys):
    """Abort startup if the server does not actually enforce the boundaries."""
    rng = random.Random(cfg["seed"] + 1)
    for i in range(topology.agents):
        other = (i + 1) % topology.agents
        client = connect_client(cfg, keys[i]["writer"])
        try:
            try:
                client.search(dataset=topology.private[other], vector=_vec(cfg["dim"], rng), k=1)
            except LongbowAuthorizationError:
                pass
            else:
                raise RuntimeError(f"agent {i} can read agent {other}'s private namespace")
            client.api_key = keys[i]["reader"]
            try:
                client.insert(topology.shared[other], [{"id": f"rbac_probe_{i}", "vector": _vec(cfg["dim"], rng)}])
            except LongbowAuthorizationError:
                pass
            else:
                raise RuntimeError(f"agent {i} can write agent {other}'s shared namespace")
        finally:
            client.close()
    print(f"RBAC boundaries verified: {topology.agents} agents, {len(topology.namespaces)} namespaces")


class StallDetector:
    """Flag a >50% QPS drop in the rolling window sustained for two minutes."""

    def __init__(self, drop_ratio=0.5, sustained_s=120.0):
        self.drop_ratio = float(drop_ratio)
        self.sustained_s = float(sustained_s)
        self.baseline = 0.0
        self.low_since = None
        self.stalled = False
        self.stalls = 0
        self.stall_started = None
        self.drop_pct = 0.0
        self.window_qps = 0.0

    def observe(self, window_qps, now=None):
        now = time.time() if now is None else float(now)
        self.window_qps = float(window_qps)
        if self.window_qps > self.baseline:
            self.baseline = self.window_qps
        if self.baseline > 0 and self.window_qps <= self.baseline * self.drop_ratio:
            self.drop_pct = (1.0 - self.window_qps / self.baseline) * 100.0
            if self.low_since is None:
                self.low_since = now
            if not self.stalled and now - self.low_since >= self.sustained_s:
                self.stalled = True
                self.stalls += 1
                self.stall_started = self.low_since
        else:
            self.low_since = None
            self.stalled = False
            self.stall_started = None
            self.drop_pct = 0.0
        return self.status(now)

    def status(self, now=None):
        now = time.time() if now is None else float(now)
        return {
            "stalled": self.stalled,
            "stalls": self.stalls,
            "stall_s": round(now - self.stall_started, 1) if self.stall_started is not None else 0.0,
            "drop_pct": round(self.drop_pct, 1),
            "baseline_qps": round(self.baseline, 3),
            "window_qps": round(self.window_qps, 3),
        }


class Metrics:
    """Cumulative, per-op, and rolling-window metrics for the soak reports."""

    def __init__(self, window_s=60.0, drop_ratio=0.5, sustained_s=120.0):
        self.lock = threading.Lock()
        self.window_s = float(window_s)
        self.detector = StallDetector(drop_ratio=drop_ratio, sustained_s=sustained_s)
        self.ops = 0
        self.errors = 0
        self.unsupported = 0
        self.latencies = deque(maxlen=100000)
        self.events = deque()
        self.by_op = {}
        self.by_op_unsupported = {}
        self.by_op_latencies = {}
        self.start = time.time()

    def record(self, op, latency, ok, unsupported=False, now=None):
        now = time.time() if now is None else float(now)
        with self.lock:
            self.ops += 1
            stats = self.by_op.setdefault(op, {"ops": 0, "errors": 0})
            stats["ops"] += 1
            if not ok:
                self.errors += 1
                stats["errors"] += 1
            if unsupported:
                self.unsupported += 1
                self.by_op_unsupported[op] = self.by_op_unsupported.get(op, 0) + 1
            self.latencies.append(latency)
            self.by_op_latencies.setdefault(op, deque(maxlen=100000)).append(latency)
            self.events.append((now, op))
            cutoff = now - self.window_s
            while self.events and self.events[0][0] < cutoff:
                self.events.popleft()

    def record_unsupported(self, op, now=None):
        """Count a deliberately skipped op without inflating the error rate."""
        self.record(op, 0.0, True, unsupported=True, now=now)

    def snapshot(self, now=None):
        now = time.time() if now is None else float(now)
        with self.lock:
            lats = sorted(self.latencies)
            elapsed = max(now - self.start, 1e-9)
            by_op_stats = {}
            for op, counts in self.by_op.items():
                op_lats = sorted(self.by_op_latencies.get(op, ()))
                op_qps = counts["ops"] / elapsed
                by_op_stats[op] = {
                    "ops": counts["ops"],
                    "errors": counts["errors"],
                    "error_rate": counts["errors"] / counts["ops"] if counts["ops"] else 0.0,
                    "qps": round(op_qps, 3),
                    "p50_ms": round(_percentile(op_lats, 50), 3),
                    "p95_ms": round(_percentile(op_lats, 95), 3),
                    "p99_ms": round(_percentile(op_lats, 99), 3),
                    "unsupported": self.by_op_unsupported.get(op, 0),
                }
            recent = sum(1 for ts, _ in self.events if ts >= now - self.window_s)
            window_qps = recent / self.window_s
            snapshot = {
                "elapsed_s": round(now - self.start, 1),
                "ops": self.ops,
                "errors": self.errors,
                "error_rate": self.errors / self.ops if self.ops else 0.0,
                "qps": self.ops / elapsed,
                "p50_ms": round(_percentile(lats, 50), 3),
                "p95_ms": round(_percentile(lats, 95), 3),
                "p99_ms": round(_percentile(lats, 99), 3),
                "by_op": {op: counts["ops"] for op, counts in self.by_op.items()},
                "by_op_errors": {op: counts["errors"] for op, counts in self.by_op.items()},
                "by_op_unsupported": dict(self.by_op_unsupported),
                "by_op_stats": by_op_stats,
                "unsupported": self.unsupported,
            }
            snapshot.update(self.detector.observe(window_qps, now))
            return snapshot


def csv_columns(op_names, include_rss=False):
    """Build CSV headers, keeping the original soak columns as a prefix."""
    columns = list(CSV_BASE_COLUMNS)
    if include_rss:
        columns.append("rss_mb")
    columns.extend(CSV_STALL_COLUMNS)
    for op in op_names:
        tag = re.sub(r"[^A-Za-z0-9]+", "_", op)
        columns.extend(f"{name}_{tag}" for name in CSV_OP_COLUMNS)
    return columns


def csv_row(snapshot, op_names, include_rss=False, timestamp=None, rss=None):
    """Build one CSV row aligned with csv_columns()."""
    values = [
        timestamp or datetime.now().isoformat(),
        snapshot["elapsed_s"], snapshot["ops"], snapshot["errors"],
        f"{snapshot['error_rate']:.4f}", f"{snapshot['qps']:.1f}",
        snapshot["p50_ms"], snapshot["p95_ms"], snapshot["p99_ms"],
    ]
    if include_rss:
        values.append(round(rss or 0.0, 1))
    values.extend([
        int(bool(snapshot.get("stalled"))), snapshot.get("stalls", 0),
        snapshot.get("stall_s", 0.0), snapshot.get("baseline_qps", 0.0),
        snapshot.get("window_qps", 0.0), snapshot.get("unsupported", 0),
    ])
    for op in op_names:
        stats = snapshot.get("by_op_stats", {}).get(op, {})
        values.extend([
            stats.get("ops", 0), stats.get("errors", 0),
            f"{stats.get('error_rate', 0.0):.4f}", f"{stats.get('qps', 0.0):.3f}",
            stats.get("p50_ms", 0.0), stats.get("p95_ms", 0.0),
            stats.get("p99_ms", 0.0), stats.get("unsupported", 0),
        ])
    return values


def run_op(client, op, ctx):
    """Issue one real request for ``op``; return 'ok' or 'unsupported'.

    ``ctx`` supplies dataset, vector, options, ids, rng and an id generator.
    Missing SDK methods and intentionally skipped destructive ops are reported
    as 'unsupported' so the caller can count a no-op without an error.
    """
    if op == "delete_ns":
        # Deleting a soak namespace would cascade errors into every other op.
        # Count this as an explicit no-op instead of mislabeling it.
        return "unsupported"
    method = OP_METHODS.get(op)
    if method is not None and not hasattr(client, method):
        return "unsupported"
    options = ctx["options"]
    dataset = ctx["dataset"]
    vector = ctx.get("vector")
    ids = ctx.get("ids", [])
    rng = ctx.get("rng") or random
    k = int(options["k"])
    node = int(options["graph"]["start_node"])

    if op in ("dense", "cross_read"):
        client.search(dataset=dataset, vector=vector, k=k)
    elif op == "hybrid":
        client.search(
            dataset=dataset, vector=vector, text_query="memory agent",
            alpha=float(options["alpha"]), k=k,
        )
    elif op == "sparse":
        # Pure BM25: text-only and alpha=0 (no vector in the payload).
        client.search(dataset=dataset, text_query="agent memory", alpha=0.0, k=k)
    elif op == "filtered":
        client.search(
            dataset=dataset, vector=vector, k=k,
            filters=[{"field": "importance", "operator": ">=", "value": 1}],
        )
    elif op == "range":
        client.range_search(dataset=dataset, vector=vector, threshold=0.5, max_results=k)
    elif op == "byid":
        if not ids:
            return "unsupported"
        client.search_by_id(dataset=dataset, id=ctx.get("id", ids[-1]), k=k)
    elif op in ("temporal_asof", "temporal_range", "temporal_window"):
        search_type = {
            "temporal_asof": "as_of",
            "temporal_range": "range",
            "temporal_window": "sliding_window",
        }[op]
        now_ns = time.time_ns()
        params = {"dataset": dataset, "search_type": search_type, "k": k}
        if op == "temporal_asof":
            params["timestamp"] = now_ns
        elif op == "temporal_range":
            params["start_time"] = now_ns - 24 * 3600 * 10 ** 9
            params["end_time"] = now_ns
        else:
            params["window_size"] = int(options["temporal_window_size"])
        client.temporal_search(**params)
    elif op in ("geo_radius", "geo_box", "geo_hybrid"):
        geo = options["geo"]
        params = {"dataset": dataset, "k": k}
        if op == "geo_box":
            params["search_type"] = "box"
            params["box"] = {
                "min_lat": geo["lat"] - 0.1, "max_lat": geo["lat"] + 0.1,
                "min_lon": geo["lon"] - 0.1, "max_lon": geo["lon"] + 0.1,
            }
        else:
            params["search_type"] = "radius" if op == "geo_radius" else "hybrid"
            params["center"] = {"lat": geo["lat"], "lon": geo["lon"]}
            params["radius_km"] = geo["radius_km"]
            if op == "geo_hybrid":
                params["query_vector"] = vector
        client.geo_search(**params)
    elif op == "graphrag":
        client.graph_rag_expand(dataset=dataset, node_ids=[node])
    elif op == "traverse":
        client.traverse(dataset=dataset, start=node, max_hops=int(options["graph"]["max_hops"]))
    elif op == "recommend":
        seeds = [str(seed) for seed in ids[:2]] or [str(node)]
        client.recommend(dataset=dataset, seed_ids=seeds, k=k, alpha=float(options["alpha"]),
                         max_hops=int(options["graph"]["max_hops"]))
    elif op == "multimodal":
        client.multimodal_search(dataset=dataset, modality="text", text_query="memory", k=min(k, 3))
    elif op == "federated":
        client.federated_search(
            collections=[dataset], tags=list(options["federated_tags"]),
            vector=vector, k=k, rrf_k=int(options["rrf_k"]),
        )
    elif op == "text":
        client.advanced_text_search(dataset=dataset, query="memory AND agent", k=k)
    elif op == "insert":
        client.insert(dataset, [{
            "id": ctx["new_id"], "vector": vector,
            "importance": ctx.get("importance", 1),
        }])
        ids.append(ctx["new_id"])
        keep = ctx.get("keep_ids", 0)
        if keep and len(ids) > keep:
            del ids[:-keep]
    elif op == "upsert":
        if not ids:
            return "unsupported"
        uid = rng.choice(ids)
        client.insert(dataset, [{"id": uid, "vector": vector}])
    elif op == "delete":
        if not ids:
            return "unsupported"
        uid = ids.pop()
        client.delete(dataset, ids=[uid])
    elif op == "pagerank":
        client.calculate_pagerank(dataset=dataset)
    elif op == "communities":
        client.detect_communities(dataset=dataset)
    elif op == "get_vector":
        if not ids:
            return "unsupported"
        client.get_vector(dataset=dataset, id=ids[-1])
    elif op == "graph_stats":
        client.get_graph_stats(dataset=dataset)
    else:
        return "unsupported"
    return "ok"


class Agent:
    def __init__(self, idx, cfg, topology, keys, metrics, workload):
        self.idx = idx
        self.args = cfg
        self.topology = topology
        self.keys = keys
        self.metrics = metrics
        self.op_names = [name for name, _ in workload]
        self.op_weights = [weight for _, weight in workload]
        self.options = cfg["workload_options"]
        self.rng = random.Random(cfg["seed"] + idx * 9973)
        self.nprng = random.Random(cfg["seed"] + idx * 7919)
        self.dim = cfg["dim"]
        self.writable = topology.writable(idx)
        self.readable = topology.readable(idx)
        self.read_only = topology.read_only(idx)
        # Admin provisioning seeds one record in every namespace.
        self.ids = {ns: [f"seed_{ns}"] for ns in topology.namespaces}
        self.client = None
        self.running = True

    def connect(self):
        self.client = connect_client(self.args, self.keys["writer"])

    def _use(self, key_name):
        self.client.api_key = self.keys[key_name]

    def _vec(self):
        return _vec(self.dim, self.nprng)

    def _time(self, op, fn):
        t0 = time.time()
        ok = True
        unsupported = False
        try:
            status = fn()
            unsupported = status == "unsupported"
        except Exception as e:
            ok = False
            if self.args["verbose"]:
                print(f"[agent {self.idx}] {op} err: {e}")
        self.metrics.record(op, time.time() - t0, ok, unsupported)

    def step(self):
        if self.client is None:
            return
        op = self.rng.choices(self.op_names, weights=self.op_weights, k=1)[0]
        if op == "cross_read":
            others = [j for j in range(self.topology.agents) if j != self.idx]
            if not others:
                return
            ns = self.topology.shared[self.rng.choice(others)]
            key_name = "reader"
        elif op in WRITE_OPS:
            ns = self.rng.choice(self.writable)
            key_name = "writer"
        else:
            ns = self.rng.choice(self.readable)
            key_name = "reader" if ns in self.read_only else "writer"
        self._use(key_name)
        ids = self.ids.setdefault(ns, [])
        ctx = {
            "dataset": ns,
            "vector": self._vec(),
            "options": self.options,
            "ids": ids,
            "id": self.rng.choice(ids) if ids else None,
            "new_id": f"m_{self.idx}_{self.rng.randrange(1, 10 ** 9)}",
            "importance": self.rng.randint(1, 10),
            "keep_ids": 500,
            "rng": self.rng,
        }
        self._time(op, lambda: run_op(self.client, op, ctx))
        time.sleep(self.args["think_time"])

    def run(self, stop):
        try:
            self.connect()
        except Exception as e:
            if self.args["verbose"]:
                print(f"[agent {self.idx}] setup err: {e}")
            return
        while not stop.is_set() and self.running:
            try:
                self.step()
            except Exception as e:
                if self.args["verbose"]:
                    print(f"[agent {self.idx}] step err: {e}")
            time.sleep(self.args["think_time"])


def main():
    ap = argparse.ArgumentParser(description="Multiagent soak scenario with server-enforced RBAC")
    ap.add_argument("--config", default=DEFAULT_CONFIG, help="Shared JSON config (default: scripts/configs/multiagent_soak.json)")
    ap.add_argument("--agents", type=int)
    ap.add_argument("--uri")
    ap.add_argument("--meta-uri", dest="meta_uri")
    ap.add_argument("--duration-hours", type=float, dest="duration_hours")
    ap.add_argument("--dim", type=int)
    ap.add_argument("--seed", type=int)
    ap.add_argument("--think-time", type=float, dest="think_time")
    ap.add_argument("--sample-interval", type=int, dest="sample_interval")
    ap.add_argument("--private-prefix", dest="private_prefix")
    ap.add_argument("--shared-prefix", dest="shared_prefix")
    ap.add_argument("--common")
    ap.add_argument("--admin-api-key", dest="admin_api_key")
    ap.add_argument("--verbose", action="store_true", default=None)
    flags = ap.parse_args()

    try:
        cfg = load_config(flags.config)
    except Exception as e:
        ap.error(f"invalid shared config: {e}")
    for key in ("agents", "uri", "meta_uri", "duration_hours", "dim", "seed", "think_time",
                "sample_interval", "private_prefix", "shared_prefix", "common", "verbose"):
        value = getattr(flags, key)
        if value is not None:
            cfg[key] = value
    try:
        workload = workload_choices(cfg)
    except ValueError as e:
        ap.error(str(e))
    topology = Topology(cfg)

    admin_key = flags.admin_api_key or os.environ.get(cfg["admin_api_key_env"], "")
    if not admin_key:
        ap.error(f"RBAC is always enforced: set {cfg['admin_api_key_env']} or pass --admin-api-key")
    keys = provision(cfg, topology, admin_key)
    verify_rbac(cfg, topology, keys)

    os.makedirs("data/soak_logs", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = f"data/soak_logs/scenario_{ts}.jsonl"
    csv_path = f"data/soak_logs/scenario_{ts}.csv"
    metrics = Metrics()
    stop = threading.Event()
    agents = [Agent(i, cfg, topology, keys, metrics, workload) for i in range(topology.agents)]
    threads = []
    for a in agents:
        t = threading.Thread(target=a.run, args=(stop,), daemon=True)
        threads.append(t)
        t.start()
    op_names = [name for name, _ in workload]
    print(f"Scenario started: {topology.agents} agents, {len(topology.namespaces)} namespaces, "
          f"{cfg['duration_hours']}h, {len(op_names)} workload ops (RBAC enforced)")
    print(f"Logging to {log_path}")
    with open(csv_path, "w") as f:
        f.write(",".join(csv_columns(op_names)) + "\n")
    end = time.time() + cfg["duration_hours"] * 3600
    last = time.time()
    try:
        while time.time() < end:
            time.sleep(1)
            if time.time() - last >= cfg["sample_interval"]:
                last = time.time()
                snap = metrics.snapshot()
                with open(log_path, "a") as f:
                    f.write(json.dumps(snap) + "\n")
                with open(csv_path, "a") as f:
                    f.write(",".join(str(v) for v in csv_row(snap, op_names)) + "\n")
                print(f"[{datetime.now().isoformat()}] ops={snap['ops']} qps={snap['qps']:.1f} "
                      f"err_rate={snap['error_rate']:.4f} p99={snap['p99_ms']}ms "
                      f"stalled={snap['stalled']} stalls={snap['stalls']}")
    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        stop.set()
        for a in agents:
            a.running = False
        for t in threads:
            t.join(timeout=5)
        final = metrics.snapshot()
        with open(log_path, "a") as f:
            f.write(json.dumps({"final": True, **final}) + "\n")
        print("Scenario finished")


if __name__ == "__main__":
    main()
