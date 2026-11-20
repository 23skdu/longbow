#!/usr/bin/env bash
# scripts/check_version_consistency.sh
# Release guard: verify that every release-version artifact agrees on one version.
#
# Sources of truth for the *server/binary* release version:
#   - pkg/version/version.go        -> Version = "X.Y.Z"      (binary, ADBC driver, SDK reporting)
#   - helm/longbow/Chart.yaml       -> version: X.Y.Z          (chart packaging version)
#                                      appVersion: "X.Y.Z"    (shipped server version)
#
# The Python SDK (longbowclientsdk/pyproject.toml) and edge-js SDK
# (sdk/edge-js/package.json) are independently versioned client packages and
# are intentionally not part of this server-release check.
#
# Usage:
#   ./scripts/check_version_consistency.sh [REPO_ROOT]
#
# Exit codes:
#   0 = all release versions agree
#   1 = version drift detected (or malformed version string)
#   2 = a required file could not be read

set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
GO_FILE="$ROOT/pkg/version/version.go"
CHART_FILE="$ROOT/helm/longbow/Chart.yaml"

fail() { echo "FAIL: $*" >&2; exit 1; }
setup_fail() { echo "FAIL: $*" >&2; exit 2; }

[[ -f "$GO_FILE" ]] || setup_fail "missing $GO_FILE"
[[ -f "$CHART_FILE" ]] || setup_fail "missing $CHART_FILE"

go_version="$(grep -E '^[[:space:]]*Version[[:space:]]*=[[:space:]]*"' "$GO_FILE" | head -n1 | sed -E 's/.*"([^"]+)".*/\1/')"
chart_version="$(grep -E '^version:[[:space:]]*' "$CHART_FILE" | head -n1 | sed -E 's/^version:[[:space:]]*//' | tr -d '"'"'"'')"
chart_app_version="$(grep -E '^appVersion:[[:space:]]*' "$CHART_FILE" | head -n1 | sed -E 's/^appVersion:[[:space:]]*//' | tr -d '"'"'"'')"

[[ -n "$go_version" ]] || fail "could not parse Version from $GO_FILE"
[[ -n "$chart_version" ]] || fail "could not parse version from $CHART_FILE"
[[ -n "$chart_app_version" ]] || fail "could not parse appVersion from $CHART_FILE"

semver_re='^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'
for v in "$go_version" "$chart_version" "$chart_app_version"; do
  [[ "$v" =~ $semver_re ]] || fail "malformed version '$v' (expected X.Y.Z or X.Y.Z-prerelease)"
done

echo "=== Longbow Release Version Consistency ==="
printf '  %-28s %s\n' "pkg/version/version.go:" "$go_version"
printf '  %-28s %s\n' "helm Chart.yaml version:" "$chart_version"
printf '  %-28s %s\n' "helm Chart.yaml appVersion:" "$chart_app_version"

if [[ "$go_version" != "$chart_version" || "$go_version" != "$chart_app_version" ]]; then
  fail "version drift detected. Run: ./scripts/bump_version.sh <version> to update all release artifacts together."
fi

echo "PASS: all release artifacts agree on v$go_version"
