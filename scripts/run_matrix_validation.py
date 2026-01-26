
import subprocess
import time
import os
import glob

DIMS = [128, 384]
TYPES = ["float32", "float64", "int8", "complex64", "complex128"]
RESULTS_FILE = "docs/performance.md"

def run_command(cmd, check=True):
    print(f"Running: {cmd}")
    return subprocess.run(cmd, shell=True, check=check, text=True, capture_output=True)

def start_cluster():
    print("Starting cluster...")
    # This script waits for health check
    run_command("bash scripts/start_3node_6gb.sh")

def stop_cluster():
    print("Stopping cluster...")
    # Use pid files if available
    pids = glob.glob("node*.pid")
    if pids:
        for pid_file in pids:
            try:
                with open(pid_file, 'r') as f:
                    pid = f.read().strip()
                    subprocess.run(f"kill {pid}", shell=True)
            except:
                pass
        time.sleep(2)
        
    # Failsafe cleanup
    subprocess.run("pkill -f './longbow'", shell=True)
    # Cleanup data dirs to ensure fresh state
    subprocess.run("rm -rf ./data1 ./data2 ./data3", shell=True)
    subprocess.run("rm -f node*.pid", shell=True)

def run_benchmark(dim, dtype):
    print(f"Running benchmark for Dim={dim}, Type={dtype}")
    # Allow some retries if flight unavailable?
    cmd = f"python3 scripts/benchmark_comprehensive.py --dim {dim} --type {dtype}"
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if result.returncode != 0:
        print(f"Benchmark failed: {result.stderr}")
        return None
    return result.stdout

def main():
    # Ensure docs dir exists
    os.makedirs("docs", exist_ok=True)
    
    with open(RESULTS_FILE, "w") as f:
        f.write("# Performance Validation Metrics\n\n")
        f.write(f"Generated on {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("Configuration:\n")
        f.write("- Cluster: 3 Nodes (6GB Limit)\n")
        f.write("- Dataset: perf_test_v4\n")
        f.write("- Phases: 3k, 7k, 15k, 25k, 50k vectors (Cumulative)\n\n")

    for dim in DIMS:
        for dtype in TYPES:
            print(f"\n{'#'*60}")
            print(f"Testing Dim: {dim}, Type: {dtype}")
            print(f"{'#'*60}")
            
            try:
                stop_cluster() # Cleanup before start
                start_cluster()
                output = run_benchmark(dim, dtype)
                
                if output:
                    with open(RESULTS_FILE, "a") as f:
                        f.write(f"\n## {dtype} (Dim: {dim})\n\n")
                        # Extract the markdown summary part from output
                        if "Results Summary:" in output:
                            summary = output.split("Results Summary:")[1].strip()
                            # Remove "Results saved..." lines at the end
                            lines = summary.split('\n')
                            clean_summary = []
                            for line in lines:
                                if "Results saved" in line or "Profiles saved" in line:
                                    break
                                clean_summary.append(line)
                            f.write('\n'.join(clean_summary) + "\n")
                        else:
                            f.write("No summary generated.\n")
                
                stop_cluster() # Cleanup after finish
                
            except Exception as e:
                print(f"Error during {dim}/{dtype}: {e}")
                stop_cluster()

if __name__ == "__main__":
    main()
