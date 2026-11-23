#!/usr/bin/env bash
# scripts/check_module_sizes.sh
# Module size guardrail (M6): warns when hand-maintained files exceed 800 LOC,
# fails when newly created hand-maintained files exceed 1,200 LOC (or when
# un-allowlisted files exceed 1,200 LOC), and tracks a shrinkable legacy allow-list.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

SOFT_LIMIT=800
HARD_LIMIT=1200
GEN_ASM_HARD_LIMIT=4000

MODE="all"
if [ "${1:-}" = "--staged" ]; then
    MODE="staged"
fi

# Grandfathered legacy files > 800 LOC with their recorded LOC baselines.
# Entries must only shrink or be removed as files are modularized.
# Implemented as a case lookup for macOS bash 3.2 compatibility (no declare -A).
ALLOWLIST_KEYS=(
    "internal/simd/simd_baseline.go"
    "internal/simd/turboquant_amd64.s"
    "internal/simd/simd_amd64.go"
    "internal/metrics/storage_metrics.go"
    "cmd/tensor-verify/main.go"
    "internal/simd/int16_kernels_amd64.s"
    "internal/store/types/graph_data.go"
    "internal/store/index/hnsw/neighbor_ops.go"
    "internal/tensor/ops.go"
    "internal/store/types/serialization.go"
    "longbowclientsdk/validate_sdk_coverage.py"
    "internal/store/engine/reranker_advanced.go"
    "internal/simd/simd_blocked.go"
    "internal/store/types/graph_data_chunks.go"
    "internal/store/index/hnsw/diskann.go"
    "internal/mesh/gossip.go"
)

allowlist_base() {
    case "$1" in
        "internal/simd/simd_baseline.go") echo 1330 ;;
        "internal/simd/turboquant_amd64.s") echo 1325 ;;
        "internal/simd/simd_amd64.go") echo 1127 ;;
        "internal/metrics/storage_metrics.go") echo 1076 ;;
        "cmd/tensor-verify/main.go") echo 1050 ;;
        "internal/simd/int16_kernels_amd64.s") echo 1035 ;;
        "internal/store/types/graph_data.go") echo 1008 ;;
        "internal/store/index/hnsw/neighbor_ops.go") echo 964 ;;
        "internal/tensor/ops.go") echo 940 ;;
        "internal/store/types/serialization.go") echo 928 ;;
        "longbowclientsdk/validate_sdk_coverage.py") echo 908 ;;
        "internal/store/engine/reranker_advanced.go") echo 827 ;;
        "internal/simd/simd_blocked.go") echo 891 ;;
        "internal/store/types/graph_data_chunks.go") echo 881 ;;
        "internal/store/index/hnsw/diskann.go") echo 873 ;;
        "internal/mesh/gossip.go") echo 869 ;;
        *) echo 0 ;;
    esac
}

is_hand_maintained_source() {
    local p="$1"
    case "$p" in
        vendor/*|.git/*|*_test.go) return 1 ;;
        internal/simd/kernels_*_amd64.s) return 1 ;;
        *.go|*.py|*.cu|*.cuh|*.m|*.h|*.s) return 0 ;;
        *) return 1 ;;
    esac
}

if [ "$MODE" = "staged" ]; then
    FAIL=0
    WARNS=0
    while IFS=$'\t' read -r status path; do
        [ -z "$path" ] && continue
        [ -f "$path" ] || continue
        is_hand_maintained_source "$path" || continue
        lines=$(wc -l < "$path")
        if [ "$status" = "A" ] && [ "$lines" -gt "$HARD_LIMIT" ]; then
            echo "::error::New file $path is ${lines} LOC (exceeds ${HARD_LIMIT} LOC hard ceiling)."
            FAIL=1
        elif [ "$lines" -gt "$SOFT_LIMIT" ]; then
            base="$(allowlist_base "$path")"
            if [ "$base" -eq 0 ] || [ "$lines" -gt "$base" ]; then
                echo "::warning::Hand-maintained file $path is ${lines} LOC (> ${SOFT_LIMIT} LOC target)."
                WARNS=$((WARNS + 1))
            fi
        fi
    done < <(git diff --cached --name-status --diff-filter=AM)

    if [ "$FAIL" -ne 0 ]; then
        exit 1
    fi
    echo "Module size advisory (--staged): ${WARNS} warning(s), 0 hard failures."
    exit 0
fi

# Full repository audit mode
FAIL=0
WARNS=0
ALLOW_USED=0

while IFS= read -r path; do
    [ -f "$path" ] || continue
    if [[ "$path" == internal/simd/kernels_*_amd64.s ]]; then
        lines=$(wc -l < "$path")
        if [ "$lines" -gt "$GEN_ASM_HARD_LIMIT" ]; then
            echo "    [INFO] Generated ASM $path: ${lines} LOC (above ${GEN_ASM_HARD_LIMIT} LOC split target)"
        fi
        continue
    fi
    is_hand_maintained_source "$path" || continue
    lines=$(wc -l < "$path")
    if [ "$lines" -gt "$SOFT_LIMIT" ]; then
        base="$(allowlist_base "$path")"
        if [ "$base" -eq 0 ]; then
            if [ "$lines" -gt "$HARD_LIMIT" ]; then
                echo "FAIL: Un-allowlisted file $path is ${lines} LOC (> ${HARD_LIMIT} LOC hard ceiling)."
                FAIL=1
            else
                echo "WARN: Un-allowlisted file $path is ${lines} LOC (> ${SOFT_LIMIT} LOC target)."
                WARNS=$((WARNS + 1))
            fi
        else
            ALLOW_USED=$((ALLOW_USED + 1))
            if [ "$lines" -gt "$base" ]; then
                echo "FAIL: Allowlisted file $path grew from ${base} to ${lines} LOC."
                FAIL=1
            fi
        fi
    fi
done < <(git ls-files)

STALE=0
for path in "${ALLOWLIST_KEYS[@]}"; do
    if [ ! -f "$path" ]; then
        echo "WARN: Allowlist entry $path no longer exists (remove from ALLOWLIST)."
        STALE=$((STALE + 1))
        continue
    fi
    lines=$(wc -l < "$path")
    if [ "$lines" -le "$SOFT_LIMIT" ]; then
        echo "WARN: Allowlist entry $path is now ${lines} LOC (<= ${SOFT_LIMIT}); remove from ALLOWLIST."
        STALE=$((STALE + 1))
    fi
done

echo "Module size check summary: ${ALLOW_USED} allowlisted (>800 LOC), ${WARNS} un-allowlisted warning(s), ${STALE} stale allowlist entry(ies)."
if [ "$FAIL" -ne 0 ]; then
    exit 1
fi
exit 0
