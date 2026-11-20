#!/usr/bin/env bash
# scripts/bump_version.sh
# Atomically update every server-release version artifact to a single version.
#
# Usage:
#   ./scripts/bump_version.sh 0.2.7
#   make bump-version V=0.2.7
#
# Files updated:
#   - pkg/version/version.go      (Version = "X.Y.Z")
#   - helm/longbow/Chart.yaml     (version: X.Y.Z, appVersion: "X.Y.Z")
#
# The Python and edge-js SDKs are independently versioned and are not touched.

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <version> [REPO_ROOT]  (e.g. $0 0.2.7)" >&2
  exit 2
fi

new_version="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${2:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
cd "$REPO_ROOT"
semver_re='^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'
if [[ ! "$new_version" =~ $semver_re ]]; then
  echo "Invalid version '$new_version' (expected X.Y.Z or X.Y.Z-prerelease)" >&2
  exit 2
fi

go_file="pkg/version/version.go"
chart_file="helm/longbow/Chart.yaml"
[[ -f "$go_file" ]] || { echo "missing $go_file" >&2; exit 2; }
[[ -f "$chart_file" ]] || { echo "missing $chart_file" >&2; exit 2; }

sed -i -E "s/^([[:space:]]*Version[[:space:]]*=[[:space:]]*\")[^\"]+(\")/\1${new_version}\2/" "$go_file"
sed -i -E "s/^version:[[:space:]]*.*/version: ${new_version}/" "$chart_file"
sed -i -E "s/^appVersion:[[:space:]]*.*/appVersion: \"${new_version}\"/" "$chart_file"

echo "Bumped server release version to v${new_version}:"
echo "  - $go_file"
echo "  - $chart_file"
"$SCRIPT_DIR/check_version_consistency.sh" "$REPO_ROOT"
