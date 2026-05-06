#!/usr/bin/env python3
import subprocess
import sys
import os
import re
import argparse

def analyze_profile(profile_path):
    print(f"Analyzing profile: {profile_path}")
    
    # Check if go tool pprof is available
    try:
        subprocess.run(["go", "version"], capture_output=True, check=True)
    except:
        print("Error: go tool not found. Please install Go.")
        return

    # 1. Look for scheduling jitter (voluntary context switches/parks)
    print("\n[1] Detecting Scheduling Jitter (runtime.gopark, runtime.Gosched)...")
    cmd = ["go", "tool", "pprof", "-text", "-nodecount=20", profile_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error running pprof: {result.stderr}")
        return

    lines = result.stdout.splitlines()
    jitter_funcs = ["runtime.gopark", "runtime.Gosched", "runtime.mcall", "runtime.park_m"]
    
    found_jitter = False
    for line in lines:
        for func in jitter_funcs:
            if func in line:
                print(f"  FOUND: {line.strip()}")
                found_jitter = True
    
    if not found_jitter:
        print("  No significant scheduling jitter detected in top 20 functions.")

    # 2. Look for Lock Contention (if it's a mutex/block profile)
    if "mutex" in profile_path or "block" in profile_path:
        print("\n[2] Detecting Lock Contention...")
        for line in lines[:10]:
            if "sync." in line or "internal/sync" in line:
                print(f"  CONTENTION: {line.strip()}")

    # 3. Analyze Worker Pool Health
    print("\n[3] Worker Pool Stack Analysis...")
    # Look for the worker loop to see where it spends time
    worker_pattern = "numaWorker"
    found_worker = False
    for line in lines:
        if worker_pattern in line:
            print(f"  WORKER STACK: {line.strip()}")
            found_worker = True
    
    if not found_worker:
        print("  Warning: numaWorker not found in top stacks. Workers might be idle or blocked elsewhere.")

def main():
    parser = argparse.ArgumentParser(description="Analyze Longbow pprof profiles for jitters and NUMA imbalances.")
    parser.add_argument("profile", help="Path to pprof file")
    args = parser.parse_args()

    if not os.path.exists(args.profile):
        print(f"Error: File {args.profile} not found.")
        sys.exit(1)

    analyze_profile(args.profile)

if __name__ == "__main__":
    main()
