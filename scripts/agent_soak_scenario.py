#!/usr/bin/env python3
"""Multi-namespace multiagent soak scenario with server-enforced RBAC.

Default (configs/multiagent_soak.json): 3 agents, 7 namespaces.
  - private_<i>: agent i reads/writes/deletes; invisible to other agents
  - shared_<i>: agent i reads/writes/deletes; other agents read only
  - master: all agents read/write/delete

Permissions are enforced by Longbow's RBAC manager. The admin client creates
the namespaces and issues scoped per-agent API keys before agents start.

Usage:
    python3 scripts/agent_soak_scenario.py --config configs/multiagent_soak.json
"""
import argparse, json, math, os, random, sys, threading, time
from datetime import datetime
from multiagent_config import DEFAULT_CONFIG, Topology, load_config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "longbowclientsdk", "src"))
from longbow import LongbowClient
from longbow.exceptions import LongbowAuthorizationError


def _vec(dim, rng):
    v = [rng.gauss(0, 1) for _ in range(dim)]
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / n for x in v]


def connect_client(cfg, api_key=None):
    client = LongbowClient(uri=cfg["uri"], meta_uri=cfg["meta_uri"], api_key=api_key)
    client.connect()
    return client


def provision(cfg, topology, admin_key):
    """Create namespaces and per-agent writer/reader keys via the admin client."""
    admin = connect_client(cfg, admin_key)
    rng = random.Random(cfg["seed"])
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
                datasets=topology.writable(i),
            )
            reader = admin.create_api_key(
                name=f"agent_{i}_reader", role="reader", namespace="",
                datasets=topology.read_only(i),
            )
            if not writer.get("api_key") or not reader.get("api_key"):
                raise RuntimeError(f"failed to provision RBAC keys for agent {i}")
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


class Metrics:
    def __init__(self):
        self.lock=threading.Lock()
        self.ops=0; self.errors=0; self.latencies=[]
        self.by_op={}; self.by_op_errors={}
        self.start=time.time()
    def record(self, op, lat, ok):
        with self.lock:
            self.ops+=1
            self.by_op[op]=self.by_op.get(op,0)+1
            if not ok:
                self.errors+=1
                self.by_op_errors[op]=self.by_op_errors.get(op,0)+1
            self.latencies.append(lat)
    def snapshot(self):
        with self.lock:
            lats=sorted(self.latencies)
            def pct(p):
                if not lats: return 0.0
                idx=min(len(lats)-1,int(p/100.0*(len(lats)-1)))
                return lats[idx]*1000.0
            el=time.time()-self.start
            return {"elapsed_s":round(el,1),"ops":self.ops,"errors":self.errors,
                    "error_rate":(self.errors/self.ops) if self.ops else 0.0,
                    "qps":self.ops/el if el>0 else 0.0,
                    "p50_ms":round(pct(50),3),"p95_ms":round(pct(95),3),
                    "p99_ms":round(pct(99),3),"by_op":dict(self.by_op),
                    "by_op_errors":dict(self.by_op_errors)}


class Agent:
    def __init__(self, idx, cfg, topology, keys, metrics):
        self.idx=idx; self.args=cfg; self.topology=topology; self.keys=keys; self.metrics=metrics
        self.rng=random.Random(cfg["seed"]+idx*9973)
        self.nprng=random.Random(cfg["seed"]+idx*7919)
        self.dim=cfg["dim"]
        self.writable=topology.writable(idx)
        self.readable=topology.readable(idx)
        self.read_only=topology.read_only(idx)
        self.ids={ns: [] for ns in topology.namespaces}
        self.client=None; self.running=True
    def connect(self):
        self.client=connect_client(self.args, self.keys["writer"])
    def _use(self, key_name):
        self.client.api_key=self.keys[key_name]
    def _vec(self):
        v=[self.nprng.gauss(0,1) for _ in range(self.dim)]
        n=math.sqrt(sum(x*x for x in v)) or 1
        return [x/n for x in v]
    def _time(self, op, fn):
        t0=time.time(); ok=True
        try: fn()
        except Exception as e:
            ok=False
            if self.args["verbose"]: print(f"[agent {self.idx}] {op} err: {e}")
        self.metrics.record(op, time.time()-t0, ok)
    def step(self):
        if self.client is None: return
        op=self.rng.choice(["search","insert","upsert","delete","cross_read"])
        if op=="search":
            ns=self.rng.choice(self.readable)
            self._use("reader" if ns in self.read_only else "writer")
            def _search():
                try:
                    self.client.search(dataset=ns, vector=self._vec(), k=5)
                except Exception as e:
                    if "not found" in str(e).lower():
                        return
                    raise
            self._time("search", _search)
        elif op=="insert":
            ns=self.rng.choice(self.writable)
            self._use("writer")
            new_id=f"m_{self.idx}_{self.rng.randrange(1,10**9)}"
            self._time("insert", lambda: self.client.insert(ns, [{"id":new_id,"vector":self._vec(),"importance":self.rng.randint(1,10)}]))
            self.ids.setdefault(ns,[]).append(new_id)
        elif op=="upsert":
            ns=self.rng.choice(self.writable)
            self._use("writer")
            if self.ids.get(ns):
                uid=self.rng.choice(self.ids[ns])
                self._time("upsert", lambda: self.client.insert(ns, [{"id":uid,"vector":self._vec()}]))
        elif op=="delete":
            ns=self.rng.choice(self.writable)
            self._use("writer")
            if self.ids.get(ns):
                uid=self.ids[ns].pop()
                self._time("delete", lambda: self.client.delete(ns, ids=[uid]))
        elif op=="cross_read":
            other=self.rng.choice([j for j in range(self.topology.agents) if j!=self.idx])
            ns=self.topology.shared[other]
            self._use("reader")
            def _cross():
                try:
                    self.client.search(dataset=ns, vector=self._vec(), k=5)
                except Exception as e:
                    if "not found" in str(e).lower():
                        return
                    raise
            self._time("cross_read", _cross)
        time.sleep(self.args["think_time"])
    def run(self, stop):
        try:
            self.connect()
        except Exception as e:
            if self.args["verbose"]: print(f"[agent {self.idx}] setup err: {e}")
            return
        while not stop.is_set() and self.running:
            try: self.step()
            except Exception as e:
                if self.args["verbose"]: print(f"[agent {self.idx}] step err: {e}")
            time.sleep(self.args["think_time"])


def main():
    ap=argparse.ArgumentParser(description="Multiagent soak scenario with server-enforced RBAC")
    ap.add_argument("--config", default=DEFAULT_CONFIG, help="Shared JSON config (default: configs/multiagent_soak.json)")
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
    flags=ap.parse_args()

    cfg=load_config(flags.config)
    for key in ("agents","uri","meta_uri","duration_hours","dim","seed","think_time",
                "sample_interval","private_prefix","shared_prefix","common","verbose"):
        value=getattr(flags, key)
        if value is not None:
            cfg[key]=value
    topology=Topology(cfg)

    admin_key=flags.admin_api_key or os.environ.get(cfg["admin_api_key_env"], "")
    if not admin_key:
        ap.error(f"RBAC is always enforced: set {cfg['admin_api_key_env']} or pass --admin-api-key")
    keys=provision(cfg, topology, admin_key)
    verify_rbac(cfg, topology, keys)

    os.makedirs("data/soak_logs", exist_ok=True)
    ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path=f"data/soak_logs/scenario_{ts}.jsonl"
    csv_path=f"data/soak_logs/scenario_{ts}.csv"
    metrics=Metrics()
    stop=threading.Event()
    agents=[Agent(i,cfg,topology,keys,metrics) for i in range(topology.agents)]
    threads=[]
    for a in agents:
        t=threading.Thread(target=a.run,args=(stop,),daemon=True)
        threads.append(t); t.start()
    print(f"Scenario started: {topology.agents} agents, {len(topology.namespaces)} namespaces, {cfg['duration_hours']}h"
          " (RBAC enforced)")
    print(f"Logging to {log_path}")
    with open(csv_path,"w") as f:
        f.write("timestamp,elapsed_s,ops,errors,error_rate,qps,p50_ms,p95_ms,p99_ms\n")
    end=time.time()+cfg["duration_hours"]*3600
    last=time.time()
    try:
        while time.time()<end:
            time.sleep(1)
            if time.time()-last>=cfg["sample_interval"]:
                last=time.time()
                snap=metrics.snapshot()
                with open(log_path,"a") as f: f.write(json.dumps(snap)+"\n")
                with open(csv_path,"a") as f:
                    f.write(f"{datetime.now().isoformat()},{snap['elapsed_s']},{snap['ops']},{snap['errors']},{snap['error_rate']:.4f},{snap['qps']:.1f},{snap['p50_ms']},{snap['p95_ms']},{snap['p99_ms']}\n")
                print(f"[{datetime.now().isoformat()}] ops={snap['ops']} qps={snap['qps']:.1f} err_rate={snap['error_rate']:.4f} p99={snap['p99_ms']}ms")
    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        stop.set()
        for a in agents: a.running=False
        for t in threads: t.join(timeout=5)
        print("Scenario finished")


if __name__=="__main__":
    main()
