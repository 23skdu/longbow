#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
exec "$REPO_ROOT/scripts/verify.sh" "$@"
