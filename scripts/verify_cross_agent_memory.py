#!/usr/bin/env python3
"""Cross-agent memory verification: agent A writes, agent B reads/searches.

Runs against a live longbow server. Exits 0 on success.
"""
import argparse, os, sys, random, math, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "longbowclientsdk", "src"))
from longbow import LongbowClient

def mv(dim, rng):
    v=[rng.gauss(0,1) for _ in range(dim)]
    n=math.sqrt(sum(x*x for x in v)) or 1
    return [x/n for x in v]

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--uri", default="grpc://127.0.0.1:3000")
    ap.add_argument("--meta-uri", default="grpc://127.0.0.1:3001", dest="meta_uri")
    ap.add_argument("--dim", type=int, default=128)
    ap.add_argument("--ns", default="shared_cross")
    args=ap.parse_args()

    ca=LongbowClient(uri=args.uri, meta_uri=args.meta_uri); ca.connect()
    cb=LongbowClient(uri=args.uri, meta_uri=args.meta_uri); cb.connect()
    try:
        ca.create_namespace(name=args.ns, dims=args.dim, data_type="float32")
    except Exception:
        pass
    rng=random.Random(7)
    vecs=[]
    for i in range(20):
        v=mv(args.dim, rng); vecs.append(v)
        ca.insert(args.ns, [{"id": f"a_{i}", "vector": v, "importance": i}])
    time.sleep(2)
    # Agent B searches
    q=vecs[0]
    res=cb.search(dataset=args.ns, vector=q, k=5)
    print("B search rows:", len(res))
    assert len(res) > 0, "cross-agent search returned no results"
    # Agent B get_vector for a memory written by A
    gv=cb.get_vector(args.ns, "a_3")
    print("B get_vector:", gv.get("id"))
    assert gv.get("id") == "a_3", "cross-agent get_vector failed"
    print("CROSS-AGENT MEMORY VERIFICATION: PASS")

if __name__=="__main__":
    main()