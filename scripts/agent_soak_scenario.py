#!/usr/bin/env python3
"""Multi-namespace agent soak scenario.

10 agents, 21 namespaces:
  - private_<i>: owned by agent i (read/write/delete)
  - shared_<i>: agent i can write; all agents can read/search
  - master: all agents can read/write; none may delete

Usage:
    python3 scripts/agent_soak_scenario.py --agents 10 --duration-hours 1 --dim 128
"""
import argparse, os, sys, random, math, threading, time, json
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "longbowclientsdk", "src"))
from longbow import LongbowClient

def mv(dim, rng):
    v=[rng.gauss(0,1) for _ in range(dim)]
    n=math.sqrt(sum(x*x for x in v)) or 1.0
    return [x/n for x in v]

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
    def __init__(self, idx, args, metrics):
        self.idx=idx; self.args=args; self.metrics=metrics
        self.rng=random.Random(args.seed+idx*9973)
        self.nprng=random.Random(args.seed+idx*7919)
        self.dim=args.dim
        self.ids={}  # namespace -> list of ids
        self.client=None; self.running=True
    def namespaces(self):
        mine=f"private_{self.idx}"
        shared_write=f"shared_{self.idx}"
        master="master"
        shared_read=[f"shared_{j}" for j in range(self.args.agents)]
        return mine, shared_write, shared_read, master
    def connect(self):
        self.client=LongbowClient(uri=self.args.uri, meta_uri=self.args.meta_uri)
        self.client.connect()
        mine, sw, sr, master = self.namespaces()
        for ns in [mine, sw, master]+sr:
            try:
                self.client.create_namespace(name=ns, dims=self.dim, data_type="float32")
            except Exception:
                pass
            self.ids[ns]=[]
        # Seed each owned/shared namespace so searches don't hit empty datasets.
        for ns in [mine, sw, master]:
            try:
                self.client.insert(ns, [{"id": f"seed_{self.idx}", "vector": self._vec(), "importance": 1}])
                self.ids[ns].append(f"seed_{self.idx}")
            except Exception:
                pass
    def _vec(self): 
        v=[self.nprng.gauss(0,1) for _ in range(self.dim)]
        n=math.sqrt(sum(x*x for x in v)) or 1
        return [x/n for x in v]
    def _time(self, op, fn):
        t0=time.time(); ok=True
        try: fn()
        except Exception as e:
            ok=False
            if self.args.verbose: print(f"[agent {self.idx}] {op} err: {e}")
        self.metrics.record(op, time.time()-t0, ok)
    def step(self):
        if self.client is None: return
        mine, sw, sr, master = self.namespaces()
        # writable namespaces for this agent
        writable=[mine, sw, master]
        readable=[mine]+sr+[master]
        op=self.rng.choice(["search","insert","upsert","delete","cross_read"])
        if op=="search":
            ns=self.rng.choice(readable)
            def _search():
                try:
                    self.client.search(dataset=ns, vector=self._vec(), k=5)
                except Exception as e:
                    if "not found" in str(e).lower():
                        return
                    raise
            self._time("search", _search)
        elif op=="insert":
            ns=self.rng.choice(writable)
            new_id=f"m_{self.idx}_{self.rng.randrange(1,10**9)}"
            self._time("insert", lambda: self.client.insert(ns, [{"id":new_id,"vector":self._vec(),"importance":self.rng.randint(1,10)}]))
            self.ids.setdefault(ns,[]).append(new_id)
        elif op=="upsert":
            ns=self.rng.choice(writable)
            if self.ids.get(ns):
                uid=self.rng.choice(self.ids[ns])
                self._time("upsert", lambda: self.client.insert(ns, [{"id":uid,"vector":self._vec()}]))
        elif op=="delete":
            # delete allowed in private and master? master: none may delete -> skip master
            ns=self.rng.choice([mine, sw])  # only own namespaces
            if self.ids.get(ns):
                uid=self.ids[ns].pop()
                self._time("delete", lambda: self.client.delete(ns, ids=[uid]))
        elif op=="cross_read":
            # read from another agent's shared namespace
            other=self.rng.randrange(self.args.agents)
            if other==self.idx: return
            ns=f"shared_{other}"
            def _cross():
                try:
                    self.client.search(dataset=ns, vector=self._vec(), k=5)
                except Exception as e:
                    if "not found" in str(e).lower():
                        return
                    raise
            self._time("cross_read", _cross)
        time.sleep(self.args.think_time)
    def run(self, stop):
        try:
            self.connect()
            time.sleep(1.0)
        except Exception as e:
            if self.args.verbose: print(f"[agent {self.idx}] setup err: {e}")
            return
        while not stop.is_set() and self.running:
            try: self.step()
            except Exception as e:
                if self.args.verbose: print(f"[agent {self.idx}] step err: {e}")
            time.sleep(self.args.think_time)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--uri", default="grpc://127.0.0.1:3000")
    ap.add_argument("--meta-uri", default="grpc://127.0.0.1:3001", dest="meta_uri")
    ap.add_argument("--agents", type=int, default=10)
    ap.add_argument("--duration-hours", type=float, default=1.0, dest="duration_hours")
    ap.add_argument("--dim", type=int, default=128)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--think-time", type=float, default=0.01, dest="think_time")
    ap.add_argument("--sample-interval", type=int, default=60, dest="sample_interval")
    ap.add_argument("--verbose", action="store_true")
    args=ap.parse_args()

    os.makedirs("data/soak_logs", exist_ok=True)
    ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path=f"data/soak_logs/scenario_{ts}.jsonl"
    csv_path=f"data/soak_logs/scenario_{ts}.csv"
    metrics=Metrics()
    stop=threading.Event()
    agents=[Agent(i,args,metrics) for i in range(args.agents)]
    threads=[]
    for a in agents:
        t=threading.Thread(target=a.run,args=(stop,),daemon=True)
        threads.append(t); t.start()
    print(f"Scenario started: {args.agents} agents, 21 namespaces, {args.duration_hours}h")
    print(f"Logging to {log_path}")
    with open(csv_path,"w") as f:
        f.write("timestamp,elapsed_s,ops,errors,error_rate,qps,p50_ms,p95_ms,p99_ms\n")
    end=time.time()+args.duration_hours*3600
    last=time.time()
    try:
        while time.time()<end:
            time.sleep(1)
            if time.time()-last>=args.sample_interval:
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
