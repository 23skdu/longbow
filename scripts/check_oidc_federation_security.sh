#!/usr/bin/env bash
# scripts/check_oidc_federation_security.sh
# Security audit checks for the planned OIDC federation feature
# (AWS + Google IAM based authentication to Longbow and datasets/namespaces).
#
# Exit codes:
#   0 = all checks passed
#   1 = a check failed
#   2 = tooling unavailable

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

export PATH="/home/linuxbrew/.linuxbrew/bin:$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin:$PATH"

echo "=== OIDC Federation Security Audit ==="

# 1. No hardcoded cloud credentials / long-lived secrets in source.
echo "--- Checking for hardcoded cloud credentials ---"
if grep -RniE 'AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{35}|ya29\.[0-9A-Za-z_-]+' \
    --include="*.go" --include="*.yaml" --include="*.yml" --include="*.json" \
    . 2>/dev/null | grep -v '/vendor/' ; then
  echo "FAIL: potential hardcoded cloud credential found"
  exit 1
fi
echo "PASS: no hardcoded cloud credentials detected"

# 2. Go vet on security-sensitive packages.
echo "--- Running go vet on security packages ---"
go vet ./internal/security/... ./internal/store/... ./internal/middleware/...
echo "PASS: go vet clean"

# 3. Targeted tests for OIDC federation (no-op pass until implemented).
echo "--- Running targeted OIDC federation tests ---"
go test -short -run 'OIDC|Federation|WebIdentity|WorkloadIdentity|JWKS' \
  ./internal/security/... ./internal/store/... ./internal/middleware/... 2>&1 | tail -20
echo "PASS: targeted OIDC federation test gate"

# 4. Ensure security docs reference OIDC federation plan.
echo "--- Checking roadmap references ---"
if ! grep -qi "OIDC Federation" docs/development/roadmap.md; then
  echo "FAIL: docs/development/roadmap.md missing OIDC Federation plan"
  exit 1
fi
echo "PASS: roadmap contains OIDC Federation plan"

echo "=== OIDC Federation Security Audit PASSED ==="
exit 0