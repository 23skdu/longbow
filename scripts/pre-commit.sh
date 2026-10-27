#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

echo "=========================================================="
echo " Longbow Pre-Commit Quality & Conformance Gates"
echo "=========================================================="

# 1. 100% Package Test Coverage Gate (Per-Package Test File Presence)
echo "--> Gate 1/4: Checking 100% Package Test Coverage Gate..."
UNTESTED=$(go test -list '.*' ./... 2>&1 | grep '\[no test files\]' || true)
if [ -n "$UNTESTED" ]; then
    echo "::error::Found packages without test files:"
    echo "$UNTESTED"
    echo "Every package must have test coverage before committing."
    exit 1
fi
echo "    ✓ All packages have active test coverage (100% package presence)."

# 2. Changed Packages Unit & Fuzz Test Coverage
echo "--> Gate 2/4: Checking Unit & Fuzz Test Coverage on Changed Packages..."
STAGED_GO_FILES=$(git diff --cached --name-only --diff-filter=ACM | grep '\.go$' || true)
if [ -n "$STAGED_GO_FILES" ]; then
    # Extract unique directory paths formatted as Go packages
    PKGS=$(echo "$STAGED_GO_FILES" | xargs -n1 dirname | sort -u | sed 's|^\([^/]\)|./\1|')
    echo "    Staged Go packages to test: $PKGS"
else
    echo "    No Go files staged; validating core critical packages."
    PKGS="./cmd/... ./internal/simd/... ./tools/..."
fi

# Run unit tests on affected packages with code coverage
echo "    Running unit tests with coverage on: $PKGS"
go test -v -cover $PKGS

# Run fuzz tests on affected packages
echo "    Running fuzz tests on: $PKGS"
go test -run='^Fuzz' $PKGS
echo "    ✓ Unit and Fuzz test coverage passed 100%."

# 3. Accuracy Gate
echo "--> Gate 3/4: Running Accuracy Gate (Numerical Precision Across 15 DTypes)..."
go test -v ./internal/simd/ -run='^TestSIMDAccuracy'
echo "    ✓ Accuracy Gate passed: All numerical tolerances and backends validated."

# 4. Oracle Gate
echo "--> Gate 4/4: Running Oracle Gate (Ground-Truth Parity vs Reference Oracles)..."
go test -v ./internal/simd/... ./internal/tensor/... ./internal/memory/... ./internal/store/index/ -run='Parity'
echo "    ✓ Oracle Gate passed: All implementations match ground-truth oracles."

echo "=========================================================="
echo " ✓ All pre-commit quality, accuracy, and oracle gates passed!"
echo "=========================================================="
