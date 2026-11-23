#!/usr/bin/env python3
"""Longbow agent-memory soak test driven by the shared workload config.

Simulates concurrent agents performing the full weighted operation mix against
a running Longbow server. RBAC is always enforced: an admin key provisions the
namespaces and scoped per-agent keys, and the permission boundaries are probed
before the soak starts.

With --agents-per-ns 2, eight agents share four group namespaces (plus the
common namespace) while retaining one scoped key set per group.

Usage:
    python3 scripts/agent_soak.py --config scripts/configs/multiagent_soak.json
    python3 scripts/agent_soak.py --agents 8 --agents-per-ns 2 --duration-hours 0.1
"""
import argparse
import json
import math
import os
import random
import sys
import threading
import time
from datetime import datetime

_SCRIPTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _SCRIPTS)
sys.path.insert(0, os.path.join(_SCRIPTS, "..", "longbowclientsdk", "src"))

try:
    import psutil
except ImportError:
    psutil = None

try:
    from longbow import LongbowClient
    HAS_SDK = True
except Exception:
    LongbowClient = None
    HAS_SDK = False

from multiagent_config import DEFAULT_CONFIG, Topology, load_config, workload_choices
from agent_soak_scenario import (
    Metrics,
    connect_client,
    csv_columns,
    csv_row,
    provision,
    run_op,
    verify_rbac,
    WRITE_OPS,
)


def make_vector(dim, rng):
    v = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


class GroupedTopology:
    """Map agents onto namespaces using the standard Topology layout.

    With ``agents_per_ns`` of 0 or 1, this is the default private-per-agent
    layout (2N+1 namespaces). Above 1, each group of agents shares one
    namespace and one scoped key set. Names and permission tiers come from
    multiagent_config.Topology, so the RBAC bootstrap is identical to the
    scenario simulator's.
    """

    def __init__(self, agents, agents_per_ns, cfg):
        if agents < 2:
            raise ValueError("agents must be >= 2")
        if agents_per_ns < 0:
            raise ValueError("agents-per-ns must not be negative")
        if agents_per_ns > 1 and agents_per_ns >= agents:
            raise ValueError("agents-per-ns must be smaller than agents so RBAC has at least two groups")

        self.grouped = agents_per_ns > 1
        if not self.grouped:
            self.base = Topology(cfg)
            self.groups = agents
            self.agents = agents
            self.private = list(self.base.private)
            self.shared = list(self.base.shared)
            self.common = self.base.common
            self.namespaces = self.private + self.shared + [self.common]
            return

        self.groups = int(math.ceil(agents / float(agents_per_ns)))
        # Provision and verify one scoped key set per group.
        self.agents = self.groups
        base_cfg = dict(cfg)
        base_cfg["agents"] = self.groups
        self.base = Topology(base_cfg)
        # Group namespaces are the shared tier from the standard layout.
        self.private = list(self.base.shared[:self.groups])
        self.shared = list(self.private)  # alias used by verify_rbac
        self.common = self.base.common
        self.namespaces = self.private + [self.common]

    def writable(self, group):
        if not self.grouped:
            return self.base.writable(group)
        return [self.private[group], self.common]

    def read_only(self, group):
        if not self.grouped:
            return self.base.read_only(group)
        return [ns for i, ns in enumerate(self.private) if i != group]

    def readable(self, group):
        if not self.grouped:
            return self.base.readable(group)
        return self.writable(group) + self.read_only(group)


class Agent:
    def __init__(self, idx, cfg, topology, keys, metrics, workload, agents_per_ns):
        self.idx = idx
        self.args = cfg
        self.topology = topology
        self.group = min(idx // agents_per_ns, topology.groups - 1)
        self.keys = keys[self.group]
        self.metrics = metrics
        self.op_names = [name for name, _ in workload]
        self.op_weights = [weight for _, weight in workload]
        self.options = cfg["workload_options"]
        self.rng = random.Random(cfg["seed"] + idx * 9973)
        self.nprng = random.Random(cfg["seed"] + idx * 7919)
        self.dim = cfg["dim"]
        self.dataset = topology.private[self.group]
        self.ids = []
        self.client = None
        self.running = True

    def connect(self):
        self.client = connect_client(self.args, self.keys["writer"])

    def close(self):
        self.running = False
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass

    def _use(self, key_name):
        self.client.api_key = self.keys[key_name]

    def _vec(self):
        return make_vector(self.dim, self.nprng)

    def _ensure_ids(self):
        if self.ids or self.client is None:
            return
        for i in range(min(20, self.args["seed_count"])):
            vid = f"s_{self.idx}_{i}"
            vec = self._vec()
            try:
                self.client.insert(self.dataset, [{"id": vid, "vector": vec, "importance": 1}])
                self.ids.append(vid)
            except Exception:
                pass

    def _time_op(self, op, fn):
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
        return ok

    def step(self):
        if self.client is None:
            return
        self._ensure_ids()
        op = self.rng.choices(self.op_names, weights=self.op_weights, k=1)[0]
        read_only = self.topology.read_only(self.group)
        if op == "cross_read":
            ns = self.rng.choice(read_only) if read_only else self.dataset
        elif op in WRITE_OPS or op in ("byid", "get_vector", "recommend"):
            ns = self.dataset
        elif read_only and self.rng.random() < 0.2:
            ns = self.rng.choice(read_only)
        else:
            ns = self.dataset
        self._use("reader" if ns in read_only else "writer")
        ids = self.ids if ns == self.dataset else []
        ctx = {
            "dataset": ns,
            "vector": self._vec(),
            "options": self.options,
            "ids": ids,
            "id": self.rng.choice(ids) if ids else None,
            "new_id": f"m_{self.idx}_{self.rng.randrange(1, 10 ** 9)}",
            "importance": self.rng.randint(1, 10),
            "keep_ids": self.args["max_ids"],
            "rng": self.rng,
        }
        self._time_op(op, lambda: run_op(self.client, op, ctx))
        time.sleep(self.args["think_time"])

    def run(self, stop_event):
        try:
            self.connect()
            self._ensure_ids()
            time.sleep(1.0)  # let the index build before the mix starts
        except Exception as e:
            if self.args["verbose"]:
                print(f"[agent {self.idx}] setup err: {e}")
            return
        while not stop_event.is_set() and self.running:
            try:
                self.step()
            except Exception as e:
                if self.args["verbose"]:
                    print(f"[agent {self.idx}] step err: {e}")
            time.sleep(self.args["think_time"])


def main():
    ap = argparse.ArgumentParser(description="Full-workload agent soak with server-enforced RBAC")
    ap.add_argument("--config", default=DEFAULT_CONFIG, help="Shared JSON config")
    ap.add_argument("--uri")
    ap.add_argument("--meta-uri", dest="meta_uri")
    ap.add_argument("--agents", type=int)
    ap.add_argument("--agents-per-ns", type=int, default=None, dest="agents_per_ns",
                    help="Agents sharing one namespace (overrides config; 0 keeps the private-per-agent layout)")
    ap.add_argument("--admin-api-key", dest="admin_api_key", help="RBAC bootstrap key (or set the configured env var)")
    ap.add_argument("--duration-hours", type=float, dest="duration_hours")
    ap.add_argument("--dim", type=int)
    ap.add_argument("--seed", type=int)
    ap.add_argument("--seed-count", type=int, default=20, dest="seed_count")
    ap.add_argument("--max-ids", type=int, default=500, dest="max_ids", help=argparse.SUPPRESS)
    ap.add_argument("--think-time", type=float, dest="think_time")
    ap.add_argument("--sample-interval", type=int, dest="sample_interval")
    ap.add_argument("--verbose", action="store_true", default=None)
    args = ap.parse_args()

    try:
        cfg = load_config(args.config)
    except Exception as e:
        if args.config != DEFAULT_CONFIG:
            ap.error(f"invalid shared config: {e}")
        print(f"shared config unavailable ({e}); using built-in defaults")
        cfg = load_config(None)
    for key in ("agents", "uri", "meta_uri", "duration_hours", "dim", "seed",
                "think_time", "sample_interval", "verbose"):
        value = getattr(args, key)
        if value is not None:
            cfg[key] = value
    if args.agents_per_ns is not None:
        cfg["agents_per_namespace"] = args.agents_per_ns
    try:
        workload = workload_choices(cfg)
    except ValueError as e:
        ap.error(str(e))
    if not HAS_SDK:
        ap.error("Longbow Python SDK is required for the RBAC-provisioned soak")
    agents_per_ns = int(cfg["agents_per_namespace"])
    topology = GroupedTopology(int(cfg["agents"]), agents_per_ns, cfg)

    admin_key = args.admin_api_key or os.environ.get(cfg["admin_api_key_env"], "")
    if not admin_key:
        ap.error(f"RBAC is always enforced: set {cfg['admin_api_key_env']} or pass --admin-api-key")
    # Raw keys stay in memory only; they are never printed or written to logs.
    keys = provision(cfg, topology, admin_key)
    verify_rbac(cfg, topology, keys)

    os.makedirs("data/soak_logs", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = f"data/soak_logs/soak_{ts}.jsonl"
    csv_path = f"data/soak_logs/soak_{ts}.csv"

    metrics = Metrics()
    stop_event = threading.Event()
    agents = [
        Agent(i, cfg, topology, keys, metrics, workload, agents_per_ns)
        for i in range(int(cfg["agents"]))
    ]
    threads = []
    for a in agents:
        t = threading.Thread(target=a.run, args=(stop_event,), daemon=True)
        threads.append(t)
        t.start()

    op_names = [name for name, _ in workload]
    print(f"Soak test started: {len(agents)} agents in {topology.groups} namespaces, "
          f"{cfg['duration_hours']}h, dim={cfg['dim']}, {len(op_names)} workload ops")
    print(f"Logging to {log_path}")

    with open(csv_path, "w") as f:
        f.write(",".join(csv_columns(op_names, include_rss=True)) + "\n")

    end_time = time.time() + cfg["duration_hours"] * 3600
    last_sample = time.time()
    try:
        while time.time() < end_time:
            time.sleep(1)
            if time.time() - last_sample >= cfg["sample_interval"]:
                last_sample = time.time()
                snap = metrics.snapshot()
                if psutil is not None:
                    try:
                        rss = psutil.Process().memory_info().rss / (1024 * 1024)
                    except Exception:
                        rss = 0.0
                else:
                    rss = 0.0
                snap["rss_mb"] = round(rss, 1)
                with open(log_path, "a") as f:
                    f.write(json.dumps(snap) + "\n")
                with open(csv_path, "a") as f:
                    f.write(",".join(str(v) for v in csv_row(
                        snap, op_names, include_rss=True, rss=rss
                    )) + "\n")
                print(f"[{datetime.now().isoformat()}] ops={snap['ops']} qps={snap['qps']:.1f} "
                      f"err_rate={snap['error_rate']:.4f} p99={snap['p99_ms']}ms "
                      f"rss={rss:.0f}MB stalled={snap['stalled']} stalls={snap['stalls']}")
                if snap["error_rate"] > 0.01:
                    print("FAILURE: error rate > 1%")
                if snap["stalled"]:
                    print(f"FAILURE: QPS stall ({snap['drop_pct']:.0f}% drop for "
                          f"{snap['stall_s']:.0f}s)")
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=30)
        for a in agents:
            a.close()
        final = metrics.snapshot()
        with open(log_path, "a") as f:
            f.write(json.dumps({"final": True, **final}) + "\n")
        print("Soak test finished.")


if __name__ == "__main__":
    main()
