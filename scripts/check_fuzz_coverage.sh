#!/usr/bin/env bash
# scripts/check_fuzz_coverage.sh
# Verify 100% package fuzz coverage: every Go package in the module must
# define at least one property-based fuzz target (func Fuzz* in a *_test.go).
#
# Exit codes:
#   0 = every package defines at least one Fuzz* target
#   1 = one or more packages are missing a fuzz target
#   2 = package enumeration failed

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

# Ensure Homebrew Go is on PATH in non-interactive shells and Git hooks
export PATH="/home/linuxbrew/.linuxbrew/bin:$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin:$PATH"

if ! PKGS=$(go list -f '{{.ImportPath}} {{.Dir}}' ./... 2>/dev/null); then
  echo "ERROR: go list ./... failed (build broken?)"
  exit 2
fi

UNFUZZED=""
TOTAL=0
while read -r pkg dir; do
  [ -z "$pkg" ] && continue
  TOTAL=$((TOTAL + 1))
  # Match a top-level fuzz target declaration; -q exits 0 if any test file
  # in the package dir matches, non-zero (incl. glob-miss, exit 2) otherwise.
  if ! grep -qs '^func Fuzz' "$dir"/*_test.go; then
    UNFUZZED="$UNFUZZED $pkg"
  fi
done <<< "$PKGS"

if [ -n "$UNFUZZED" ]; then
  echo "FAIL: found packages without fuzz targets:"
  echo "$UNFUZZED" | tr ' ' '\n' | sed '/^$/d'
  echo "Every package must define at least one 'func Fuzz*(f *testing.F)' target."
  exit 1
fi

echo "PASS: 100% package fuzz coverage ($TOTAL packages, each defines a Fuzz* target)"
exit 0
