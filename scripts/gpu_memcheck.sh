#!/usr/bin/env bash
# scripts/gpu_memcheck.sh
# GPU memory leak detection for CI.
# Runs compute-sanitizer --tool memcheck on the longbow binary and checks
# for CUDA memory leaks and invalid memory accesses.
#
# Usage:
#   ./scripts/gpu_memcheck.sh [binary_path] [test_args...]
#
# Requirements:
#   - NVIDIA GPU with CUDA toolkit installed
#   - compute-sanitizer (part of CUDA toolkit)
#
# Exit codes:
#   0 = no leaks or errors
#   1 = leaks or errors detected
#   2 = compute-sanitizer not available

set -euo pipefail

BINARY="${1:-./bin/longbow}"
shift 2>/dev/null || true
TEST_ARGS=("$@")

if ! command -v compute-sanitizer &>/dev/null; then
    echo "SKIP: compute-sanitizer not found (install CUDA toolkit)"
    exit 2
fi

if [ ! -f "$BINARY" ]; then
    echo "ERROR: binary not found at $BINARY"
    exit 1
fi

echo "=== GPU Memory Check ==="
echo "Binary: $BINARY"
echo "Args: ${TEST_ARGS[*]:-none}"
echo "Time: $(date -Iseconds)"
echo ""

# Run compute-sanitizer memcheck tool
OUTPUT=$(compute-sanitizer --tool memcheck \
    --leak-check full \
    --report-api-trace yes \
    --print-summary per-invocation \
    "$BINARY" "${TEST_ARGS[@]}" 2>&1) || true

echo "$OUTPUT"
echo ""

# Check for issues
LEAK_COUNT=$(echo "$OUTPUT" | grep -c "^=.*ERROR.*leaked" 2>/dev/null || echo "0")
ERROR_COUNT=$(echo "$OUTPUT" | grep -c "^=.*ERROR" 2>/dev/null || echo "0")
INVALID_ACCESS=$(echo "$OUTPUT" | grep -c "Invalid __global" 2>/dev/null || echo "0")

echo "=== Summary ==="
echo "Memory leaks: $LEAK_COUNT"
echo "CUDA errors: $ERROR_COUNT"
echo "Invalid accesses: $INVALID_ACCESS"

if [ "$LEAK_COUNT" -gt 0 ] || [ "$ERROR_COUNT" -gt 0 ] || [ "$INVALID_ACCESS" -gt 0 ]; then
    echo ""
    echo "FAIL: GPU memory issues detected"
    exit 1
fi

echo ""
echo "PASS: No GPU memory issues detected"
exit 0
