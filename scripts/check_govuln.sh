#!/usr/bin/env bash
# scripts/check_govuln.sh
# Run govulncheck and fail only on vulnerabilities outside the allowlist.
#
# Exit codes:
#   0 = clean, or only allowlisted vulnerabilities found
#   1 = unexpected vulnerability found
#   2 = govulncheck not available or scan failed to run

set -euo pipefail

# Allowlisted GO IDs (see .trivyignore and docs/nextsteps.md Part 9).
# These have Fixed in: N/A upstream; tracked as accepted risk.
ALLOWLIST=(
  "GO-2026-5046"  # hamba/avro CPU exhaustion
  "GO-2026-5047"  # hamba/avro integer overflow
  "GO-2026-5048"  # hamba/avro unbounded map DoS
  "GO-2026-5932"  # x/crypto openpgp unmaintained (required, not called)
)

if ! command -v govulncheck &>/dev/null; then
  echo "SKIP: govulncheck not found (install: go install golang.org/x/vuln/cmd/govulncheck@latest)"
  exit 2
fi

echo "=== govulncheck ==="
set +e
OUTPUT=$(govulncheck ./... 2>&1)
EXIT=$?
set -e

echo "$OUTPUT"
echo ""

# govulncheck exit codes: 0 = clean, 1+ = error, 3 = vulnerabilities found.
# Extract vulnerability IDs from text output (lines like "Vulnerability #N: GO-...")
mapfile -t FOUND < <(echo "$OUTPUT" | grep -oE 'GO-[0-9]{4}-[0-9]+' | sort -u)

if [[ ${#FOUND[@]} -eq 0 ]]; then
  if [[ $EXIT -eq 0 ]]; then
    echo "PASS: no known vulnerabilities"
    exit 0
  fi
  echo "ERROR: govulncheck exited with $EXIT but no vulnerability IDs parsed"
  exit 2
fi

UNEXPECTED=()
for id in "${FOUND[@]}"; do
  allowed=false
  for a in "${ALLOWLIST[@]}"; do
    if [[ "$id" == "$a" ]]; then
      allowed=true
      break
    fi
  done
  if ! $allowed; then
    UNEXPECTED+=("$id")
  fi
done

if [[ ${#UNEXPECTED[@]} -gt 0 ]]; then
  echo "FAIL: unexpected vulnerability(ies): ${UNEXPECTED[*]}"
  exit 1
fi

echo "PASS: only allowlisted vulnerabilities found (${FOUND[*]})"
exit 0
