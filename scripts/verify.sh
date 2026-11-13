#!/usr/bin/env bash
# Longbow Pre-Commit Verification & Quality Gate Hook (scripts/verify.sh)
# Installed into .git/hooks/pre-commit via `make makehooks` (or `make install-hooks`).
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

# Ensure Homebrew Go 1.27.4+ and Go tools are on PATH in non-interactive Git hooks
export PATH="/home/linuxbrew/.linuxbrew/bin:$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin:$PATH"

echo "=========================================================="
echo " Longbow Pre-Commit Verification & Conformance Gates"
echo "=========================================================="

# Determine staged Go packages (or fallback to core critical packages)
STAGED_GO_FILES=$(git diff --cached --name-only --diff-filter=ACM | grep '\.go$' || true)
if [ -n "$STAGED_GO_FILES" ]; then
    PKGS=$(echo "$STAGED_GO_FILES" | xargs -n1 dirname | sort -u | sed 's|^\([^/]\)|./\1|')
    echo "    Staged Go packages: $PKGS"
else
    PKGS="./cmd/... ./internal/simd/... ./internal/pq/... ./internal/tensor/... ./tools/..."
    echo "    No Go files staged; validating core critical packages: $PKGS"
fi

# 1. Code Formatting & Linting Gate
echo "--> Gate 1/6: Running Formatting & Static Analysis Checks..."
if [ -n "$STAGED_GO_FILES" ]; then
    UNFORMATTED=$(gofmt -l $STAGED_GO_FILES || true)
    if [ -n "$UNFORMATTED" ]; then
        echo "::error::Unformatted staged Go files detected (run 'make fmt'):"
        echo "$UNFORMATTED"
        exit 1
    fi
fi
go vet $PKGS
if command -v golangci-lint >/dev/null 2>&1; then
    golangci-lint run $PKGS
fi
echo "    ✓ Gate 1 passed: Formatting, go vet, and fast linters clean."

# 2. Go Security Check Gate
echo "--> Gate 2/6: Running Go Security Checks (gosec AST analysis)..."
if command -v gosec >/dev/null 2>&1; then
    gosec -quiet -exclude=G115 $PKGS
fi
echo "    ✓ Gate 2 passed: Security AST checks clean."

# 3. 100% Package Test & Fuzz Presence Gate
echo "--> Gate 3/6: Checking 100% Package Test & Fuzz Presence Gate..."
UNTESTED=$(go test -list '.*' ./... 2>&1 | grep '\[no test files\]' || true)
if [ -n "$UNTESTED" ]; then
    echo "::error::Found packages without test files:"
    echo "$UNTESTED"
    echo "Every package must have test coverage before committing."
    exit 1
fi
./scripts/check_fuzz_coverage.sh
echo "    ✓ Gate 3 passed: All packages have test files and fuzz targets (100% presence)."

# 4. Unit & Fuzz Test Gate (Bounded Short Mode)
echo "--> Gate 4/6: Running Unit & Fuzz Tests on Target Packages..."
FAST_UNIT_PKGS=$(echo "$PKGS" | tr ' ' '\n' | grep -vE '^\./internal/store(/index)?$' | tr '\n' ' ' || true)
if [ -n "$FAST_UNIT_PKGS" ]; then
    go test -short -cover $FAST_UNIT_PKGS
fi
go test -short -run='^Fuzz' $PKGS ./internal/pq/ ./internal/tensor/
echo "    ✓ Gate 4 passed: Unit and Fuzz test suites passed."

# 5. Numerical Precision & Accuracy Gate
echo "--> Gate 5/6: Running Accuracy Gate (15 Vector DTypes + Batch & Dispatch Tiers)..."
go test -v ./internal/simd/ -run='^TestSIMDAccuracy'
echo "    ✓ Gate 5 passed: All numerical precision tolerances and backends validated."

# 6. Ground-Truth Oracle Parity Gate
echo "--> Gate 6/6: Running Oracle Gate (Ground-Truth Parity vs Reference Oracles)..."
go test -v ./internal/simd/... ./internal/pq/... ./internal/tensor/... ./internal/memory/... ./internal/store/index/ -run='Parity'
echo "    ✓ Gate 6 passed: All implementations match ground-truth reference oracles."

echo "=========================================================="
echo " ✓ All 6 verification, security, accuracy & oracle gates passed!"
echo "=========================================================="

