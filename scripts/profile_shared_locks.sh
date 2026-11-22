#!/usr/bin/env bash
# Capture pprof mutex/block profiles during a shared-namespace soak.
# Usage: ./scripts/profile_shared_locks.sh <metrics_addr> <out_dir> <duration_sec>
set -euo pipefail
ADDR="${1:-127.0.0.1:9090}"
OUT="${2:-/tmp/opencode/profiles}"
DUR="${3:-60}"
mkdir -p "$OUT"
echo "Capturing mutex profile..."
curl -s "http://${ADDR}/debug/pprof/mutex?debug=1" -o "$OUT/mutex.prof" &
curl -s "http://${ADDR}/debug/pprof/block?debug=1" -o "$OUT/block.prof" &
wait
echo "Capturing CPU profile for ${DUR}s..."
curl -s "http://${ADDR}/debug/pprof/profile?seconds=${DUR}" -o "$OUT/cpu.prof"
echo "Profiles saved to $OUT"
echo "Analyze with: go tool pprof $OUT/mutex.prof"