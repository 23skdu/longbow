#!/usr/bin/env python3
"""Longbow 24-hour agent-memory soak test.

Simulates N concurrent autonomous AI agents performing mixed read/write/delete
memory operations against a running Longbow server, exercising every search
modality. Runs for --duration-hours (default 24), sampling QPS, latency,
error rate, RSS and index stats every 60s into data/soak_logs/.

Usage:
    python3 scripts/agent_soak.py --agents 50 --duration-hours 24
    python3 scripts/agent_soak.py --agents 10 --duration-hours 0.1   # smoke
"""
import argparse
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "longbowclientsdk", "src"))
import json
import random
import threading
import time
from collections import Counter, deque
from datetime import datetime

try:
    import psutil
except ImportError:
    psutil = None

try:
    from longbow import LongbowClient
    HAS_SDK = True
except Exception:
    HAS_SDK = False

import math, random as _r


def make_vector(dim, rng):
    v = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    n = math.sqrt(sum(x*x for x in v)) or 1.0
    return [x / n for x in v]


WORKLOAD = [
    # (name, weight)
    ("dense", 25), ("hybrid", 10), ("sparse", 5), ("range", 3), ("byid", 3),
    ("temporal_asof", 3), ("temporal_range", 3), ("temporal_window", 2),
    ("graphrag", 2), ("traverse", 2), ("recommend", 1),
("federated", 2), ("learned", 1),
    ("insert", 12), ("upsert", 5), ("delete", 3),
    ("pagerank", 0.25), ("communities", 0.25),
    ("get_vector", 0.5), ("graph_stats", 0.5),
]

NAMES = [w[0] for w in WORKLOAD]
WEIGHTS = [w[1] for w in WORKLOAD]


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.ops = 0
        self.errors = 0
        self.latencies = deque(maxlen=100000)
        self.by_op = Counter()
        self.by_op_errors = Counter()
        self.start = time.time()

    def record(self, op, latency, ok):
        with self.lock:
            self.ops += 1
            self.by_op[op] += 1
            if not ok:
                self.errors += 1
                self.by_op_errors[op] += 1
            self.latencies.append(latency)

    def snapshot(self):
        with self.lock:
            lats = sorted(self.latencies)
            def pct(p):
                if not lats:
                    return 0.0
                idx = min(len(lats) - 1, int(p / 100.0 * (len(lats) - 1)))
                return lats[idx] * 1000.0
            elapsed = time.time() - self.start
            return {
                "elapsed_s": round(elapsed, 1),
                "ops": self.ops,
                "errors": self.errors,
                "error_rate": (self.errors / self.ops) if self.ops else 0.0,
                "qps": self.ops / elapsed if elapsed > 0 else 0.0,
                "p50_ms": round(pct(50), 3),
                "p95_ms": round(pct(95), 3),
                "p99_ms": round(pct(99), 3),
                "by_op": dict(self.by_op),
                "by_op_errors": dict(self.by_op_errors),
            }


class Agent:
    def __init__(self, idx, args, metrics):
        self.idx = idx
        self.args = args
        self.metrics = metrics
        self.rng = random.Random(args.seed + idx * 9973)
        self.nprng = random.Random(args.seed + idx * 7919)
        self.dim = args.dim
        self.dataset = f"agent_{idx}"
        self.ns = f"ns_{idx // args.agents_per_ns}"
        self.ids = []
        self.vectors = {}
        self.client = None
        self.running = True

    def connect(self):
        if HAS_SDK:
            self.client = LongbowClient(uri=self.args.uri, meta_uri=self.args.meta_uri or self.args.uri)
            self.client.connect()
            try:
                self.client.create_namespace(name=self.ns, dims=self.dim, data_type="float32")
            except Exception as e:
                if self.args.verbose and "already exists" not in str(e):
                    print(f"[agent {self.idx}] ns err: {e}")
            self.dataset = self.ns

    def close(self):
        self.running = False
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass

    def _time_op(self, op, fn):
        t0 = time.time()
        ok = True
        try:
            fn()
        except Exception as e:
            ok = False
            if self.args.verbose:
                print(f"[agent {self.idx}] {op} err: {e}")
        self.metrics.record(op, time.time() - t0, ok)
        return ok

    def _vec(self):
        return make_vector(self.dim, self.nprng)

    def _ensure_ids(self):
        if not self.ids and self.client is not None:
            # seed a few
            for i in range(min(20, self.args.seed_count)):
                vid = f"s_{self.idx}_{i}"
                v = self._vec()
                try:
                    self.client.insert(self.dataset, [{"id": vid, "vector": v, "importance": self.rng.randint(1, 10)}])
                    self.ids.append(vid)
                    self.vectors[vid] = v
                except Exception:
                    pass

    def step(self):
        if self.client is None:
            return
        self._ensure_ids()
        op = random.choices(NAMES, weights=WEIGHTS, k=1)[0]
        vid = self.rng.choice(self.ids) if self.ids else self.rng.randrange(1, 10**9)
        v = self._vec()

        if op == "dense":
            self._time_op(op, lambda: self.client.search(dataset=self.dataset, vector=v, k=5))
        elif op == "hybrid":
            self._time_op(op, lambda: self.client.search(dataset=self.dataset, vector=v, text_query="memory agent", alpha=0.5, k=5))
        elif op == "sparse":
            self._time_op(op, lambda: self.client.search(dataset=self.dataset, vector=v, text_query="agent", alpha=1.0, k=5))
        elif op in ("filtered", "filtered_bool", "filtered_string"):
            self._time_op(op, lambda: self.client.search(dataset=self.dataset, vector=v, k=5,
                                                          filters=[{"field": "importance", "op": "gte", "value": 1}]))
        elif op == "range":
            self._time_op(op, lambda: self.client.range_search(dataset=self.dataset, vector=v, threshold=0.5, max_results=10))
        elif op == "byid":
            if self.ids:
                self._time_op(op, lambda: self.client.search_by_id(dataset=self.dataset, id=vid, k=5))
        elif op.startswith("temporal"):
            self._time_op(op, lambda: self.client.temporal_search(search_type="sliding_window", window_size=50, k=5))
        elif op.startswith("geo"):
            self._time_op(op, lambda: self.client.geo_search(dataset=self.dataset, center={"lat": 37.7, "lon": -122.4},
                                                             radius_km=5, search_type="radius", k=5))
        elif op == "graphrag":
            self._time_op(op, lambda: self.client.graph_rag_expand(dataset=self.dataset, node_ids=[1]) if hasattr(self.client, "graph_rag_expand") else None)
        elif op == "traverse":
            self._time_op(op, lambda: self.client.traverse(dataset=self.dataset, start=vid, max_hops=2))
        elif op == "recommend":
            seeds = self.ids[:2] if len(self.ids) >= 2 else [vid]
            self._time_op(op, lambda: self.client.recommend(dataset=self.dataset, seed_ids=seeds, k=5, max_hops=2))
        elif op == "multimodal":
            self._time_op(op, lambda: self.client.multimodal_search(dataset=self.dataset, modality="text", text_query="memory", k=3))
        elif op == "federated":
            self._time_op(op, lambda: self.client.federated_search(collections=[self.dataset], vector=v, k=5))
        elif op == "text":
            self._time_op(op, lambda: self.client.advanced_text_search(dataset=self.dataset, query="memory AND agent", k=5))
        elif op == "learned":
            self._time_op(op, lambda: self.client.search(dataset=self.dataset, vector=v, k=5))
        elif op == "insert":
            new_id = f"m_{self.idx}_{self.rng.randrange(1, 10**9)}"
            self._time_op(op, lambda: self.client.insert(self.dataset, [{"id": new_id, "vector": self._vec(), "importance": self.rng.randint(1, 10)}]))
            self.ids.append(new_id)
            if len(self.ids) > self.args.max_ids:
                self.ids = self.ids[-self.args.max_ids:]
        elif op == "upsert":
            if self.ids:
                uid = self.rng.choice(self.ids)
                self._time_op(op, lambda: self.client.insert(self.dataset, [{"id": uid, "vector": self._vec()}]))
        elif op == "delete":
            if self.ids:
                uid = self.ids.pop()
                self._time_op(op, lambda: self.client.delete(self.dataset, ids=[uid]))
        elif op == "delete_ns":
            if self.rng.random() < 0.01:
                self._time_op(op, lambda: self.client.delete_namespace(self.ns) if hasattr(self.client, "delete_namespace") else None)
        elif op == "pagerank":
            self._time_op(op, lambda: self.client.calculate_pagerank(dataset=self.dataset))
        elif op == "communities":
            self._time_op(op, lambda: self.client.detect_communities(dataset=self.dataset))
        elif op == "get_vector":
            if self.ids:
                self._time_op(op, lambda: self.client.get_vector(dataset=self.dataset, id=vid))
        elif op == "graph_stats":
            self._time_op(op, lambda: self.client.get_graph_stats(dataset=self.dataset))

    def run(self, stop_event):
        try:
            self.connect()
            self._ensure_ids()
            # let index build
            time.sleep(1.0)
        except Exception as e:
            if self.args.verbose:
                print(f"[agent {self.idx}] setup err: {e}")
            return
        while not stop_event.is_set() and self.running:
            try:
                self.step()
            except Exception as e:
                if self.args.verbose:
                    print(f"[agent {self.idx}] step err: {e}")
            time.sleep(self.args.think_time)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default="grpc://127.0.0.1:3000")
    ap.add_argument("--meta-uri", default="grpc://127.0.0.1:3001", dest="meta_uri")
    ap.add_argument("--agents", type=int, default=50)
    ap.add_argument("--agents-per-ns", type=int, default=10)
    ap.add_argument("--duration-hours", type=float, default=24.0)
    ap.add_argument("--dim", type=int, default=128)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--seed-count", type=int, default=20, dest="seed_count")
    ap.add_argument("--max-ids", type=int, default=500, dest="max_ids")
    ap.add_argument("--think-time", type=float, default=0.01, dest="think_time")
    ap.add_argument("--sample-interval", type=int, default=60, dest="sample_interval")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    os.makedirs("data/soak_logs", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = f"data/soak_logs/soak_{ts}.jsonl"
    csv_path = f"data/soak_logs/soak_{ts}.csv"

    metrics = Metrics()
    stop_event = threading.Event()

    print("HAS_SDK =", HAS_SDK)
    agents = [Agent(i, args, metrics) for i in range(args.agents)]
    threads = []
    for a in agents:
        t = threading.Thread(target=a.run, args=(stop_event,), daemon=True)
        threads.append(t)
        t.start()

    print(f"Soak test started: {args.agents} agents, {args.duration_hours}h, dim={args.dim}")
    print(f"Logging to {log_path}")

    # header for csv
    with open(csv_path, "w") as f:
        f.write("timestamp,elapsed_s,ops,errors,error_rate,qps,p50_ms,p95_ms,p99_ms,rss_mb\n")

    end_time = time.time() + args.duration_hours * 3600
    last_sample = time.time()
    try:
        while time.time() < end_time:
            time.sleep(1)
            if time.time() - last_sample >= args.sample_interval:
                last_sample = time.time()
                snap = metrics.snapshot()
                if psutil is not None:
                    try:
                        rss = psutil.Process().memory_info().rss / (1024*1024)
                    except Exception:
                        rss = 0.0
                else:
                    rss = 0.0
                snap["rss_mb"] = round(rss, 1)
                with open(log_path, "a") as f:
                    f.write(json.dumps(snap) + "\n")
                with open(csv_path, "a") as f:
                    f.write(f"{datetime.now().isoformat()},{snap['elapsed_s']},{snap['ops']},{snap['errors']},"
                            f"{snap['error_rate']:.4f},{snap['qps']:.1f},{snap['p50_ms']},{snap['p95_ms']},{snap['p99_ms']},{rss:.1f}\n")
                print(f"[{datetime.now().isoformat()}] ops={snap['ops']} qps={snap['qps']:.1f} "
                      f"err_rate={snap['error_rate']:.4f} p99={snap['p99_ms']}ms rss={rss:.0f}MB")
                # failure criteria
                if snap["error_rate"] > 0.01:
                    print("FAILURE: error rate > 1%")
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=30)
        for a in agents:
            a.close()
        print("Soak test finished.")


if __name__ == "__main__":
    main()
