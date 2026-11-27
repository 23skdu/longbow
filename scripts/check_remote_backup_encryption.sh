#!/usr/bin/env bash
# Release gate: remote backup configurations must use an approved encryption mode.
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

echo "=== Remote backup encryption release gate ==="
go test -count=1 ./internal/storage/ -run 'RemoteBackupEncryptionRequired|EncryptedRemoteStorage'
go test -count=1 ./internal/store/engine/ -run 'RemoteBackupEncryptionRequiredBackupConfig'

echo "PASS: remote backup encryption at rest is enforced."
