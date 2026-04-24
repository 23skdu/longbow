import subprocess
import os
import sys

# Configuration
DTYPES = ["float32", "turboquant"]
DIMS = ["128", "384", "768"]
COUNTS = ["1000", "5000", "15000"]
MODES = ["cpu", "metal"]
# Specialized modes are usually handled by the script's --mode flag
SPECIAL_MODES = ["geo", "temporal", "graphrag", "recommend", "learned_index"]

def run_bench(mode, dtype, dim, count):
    print(f"--- Running Bench: mode={mode}, dtype={dtype}, dim={dim}, count={count} ---")
    cmd = [
        "python3", "scripts/unified_benchmark.py",
        "--mode", mode,
        "--dtypes", dtype,
        "--dims", dim,
        "--counts", count,
        "--duration", "30",
        "--memory", str(16 * 1024 * 1024 * 1024)
    ]
    subprocess.run(cmd)

def main():
    # 1. Standard search modes (cpu, metal)
    for mode in MODES:
        for count in COUNTS:
            for dim in DIMS:
                for dtype in DTYPES:
                    run_bench(mode, dtype, dim, count)
    
    # 2. Specialized search modes
    # Most specialized modes in unified_benchmark.py run alongside the standard ones or have their own flags.
    # Based on previous turns, the script supports --mode [geo, temporal, etc.]
    for mode in SPECIAL_MODES:
        for count in COUNTS:
            # Usually dim 128 is enough for specialized tests if not specified otherwise
            for dim in DIMS:
                for dtype in DTYPES:
                    run_bench(mode, dtype, dim, count)

if __name__ == "__main__":
    main()
