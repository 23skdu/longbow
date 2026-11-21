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
declare -A ALLOWLIST=(
    ["internal/simd/simd_baseline.go"]=1330
    ["internal/simd/turboquant_amd64.s"]=1325
    ["internal/simd/simd_amd64.go"]=1127
    ["internal/metrics/storage_metrics.go"]=1076
    ["cmd/tensor-verify/main.go"]=1050
    ["internal/simd/int16_kernels_amd64.s"]=1035
    ["internal/store/types/graph_data.go"]=1008
    ["internal/store/store.go"]=967
    ["internal/store/index/neighbor_ops.go"]=964
    ["internal/store/cloud_control_plane.go"]=957
    ["internal/tensor/ops.go"]=940
    ["internal/store/index/ivf_flat.go"]=939
    ["internal/store/types/serialization.go"]=928
    ["longbowclientsdk/validate_sdk_coverage.py"]=908
    ["internal/store/store_actions.go"]=905
    ["internal/store/graph_store.go"]=895
    ["internal/simd/simd_blocked.go"]=891
    ["internal/store/types/graph_data_chunks.go"]=881
    ["internal/store/index/diskann.go"]=873
    ["internal/mesh/gossip.go"]=869
    ["internal/store/index/sharded_hnsw.go"]=867
    ["internal/store/index/ivf_pq_index.go"]=857
    ["internal/store/reranker_advanced.go"]=827
    ["internal/store/multimodal.go"]=812
)

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
            base="${ALLOWLIST[$path]:-0}"
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
        base="${ALLOWLIST[$path]:-0}"
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
for path in "${!ALLOWLIST[@]}"; do
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
