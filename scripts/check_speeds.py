#!/usr/bin/env python3
"""Extract ingestion/search speeds from benchmark result JSONs."""
import json, glob, sys

logdir = sys.argv[1] if len(sys.argv) > 1 else "data/perf_logs"
prefix = sys.argv[2] if len(sys.argv) > 2 else "cpu"
counts = sys.argv[3:] if len(sys.argv) > 3 else []

for f in sorted(glob.glob(f"{logdir}/result_{prefix}_*.json")):
    parts = f.replace(f"{logdir}/result_{prefix}_", "").replace(".json", "").split("_")
    fname = f.split("/")[-1]
    with open(f) as fh:
        rows = json.load(fh)
    rows_count = rows[0]["rows"]
    if counts and str(rows_count) not in counts:
        continue
    dtype = parts[0]
    dim = parts[1]
    print(f"{dtype} dim={dim} count={rows_count}:")
    for r in rows:
        if r["name"] in ("DoPut", "DoGet", "Search_Dense", "Search_Sparse",
                         "Search_Hybrid", "Search_Filtered", "Search_ByID",
                         "Search_GraphRAG", "Search_Geo", "Search_Temporal",
                         "Search_LearnedIndex", "Search_Recommend",
                         "Search_FilteredBool", "Search_FilteredString",
                         "Search_GlobalGraphRAG"):
            print(f"  {r['name']}: {r['throughput']:.0f} {r['throughput_unit']} ({r['duration_seconds']:.3f}s)")
    print()
