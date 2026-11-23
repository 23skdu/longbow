#!/usr/bin/env bash
# Bounded-memory release gate for off-heap memory lifecycles and full engine suite.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

export PATH="/home/linuxbrew/.linuxbrew/bin:$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin:$PATH"

MAX_RSS_KB="${LONGBOW_ENGINE_MAX_RSS_KB:-4194304}"
TIMEOUT="${LONGBOW_ENGINE_TEST_TIMEOUT:-10m}"

echo "=== 1. Small scratch arena & SlabPool ceiling regression checks ==="
go test -count=1 -v ./internal/memory \
  -run='^(TestGetSlab_SmallCapacityUsesHeap|TestSlabArena_SmallSlabDoesNotGrowPool|TestSlabPool_MaxPooledDoesNotExpandUnderLoad|TestGCTuner_Logic)$'

echo "=== 2. Graph analytics snapshot isolation check ==="
go test -count=1 -v ./internal/store/types \
  -run='^TestCloneForAnalyticsDoesNotAllocateArenas$'

echo "=== 3. Bounded HNSW & VectorStore off-heap lifecycle checks ==="
go test -count=1 -v ./internal/store/engine \
  -run='^(TestP0_ArrowHNSW_OffHeapLifecycleBounded|TestP0_VectorStore_SnapshotLifecycleBounded)$'

echo "=== 4. Full engine package bounded-RSS verification (limit=${MAX_RSS_KB} KB) ==="
TMP_BIN="$(mktemp /tmp/longbow-engine-memgate.XXXXXX)"
cleanup() {
  rm -f "$TMP_BIN"
}
trap cleanup EXIT

go test -c -o "$TMP_BIN" ./internal/store/engine

python3 - "$TMP_BIN" "$TIMEOUT" "$MAX_RSS_KB" "$REPO_ROOT" <<'PY'
import os
import subprocess
import sys

bin_path, timeout, max_rss_kb, repo_root = (
    sys.argv[1],
    sys.argv[2],
    int(sys.argv[3]),
    sys.argv[4],
)
engine_dir = os.path.join(repo_root, "internal", "store", "engine")

proc = subprocess.Popen(
    [bin_path, f"-test.timeout={timeout}"],
    cwd=engine_dir,
)
_, status, rusage = os.wait4(proc.pid, 0)
exit_code = os.waitstatus_to_exitcode(status)
rss_kb = rusage.ru_maxrss // 1024 if sys.platform == "darwin" else rusage.ru_maxrss

print(f"Full engine suite exit={exit_code} max_rss_kb={rss_kb} limit_kb={max_rss_kb}")
if exit_code != 0:
    sys.exit(exit_code)
if rss_kb > max_rss_kb:
    print(
        f"FAIL: engine test suite peak RSS {rss_kb} KB exceeded budget {max_rss_kb} KB",
        file=sys.stderr,
    )
    sys.exit(1)
PY

STRAY_BINS=$(find "$REPO_ROOT" -maxdepth 4 -name '*_vectors.bin' -not -path '*/vendor/*' || true)
if [ -n "$STRAY_BINS" ]; then
  echo "FAIL: stray *_vectors.bin artifacts created during test run:" >&2
  echo "$STRAY_BINS" >&2
  exit 1
fi

echo "PASS: engine bounded-memory gate passed."
