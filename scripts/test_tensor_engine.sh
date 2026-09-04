#!/usr/bin/env bash
# scripts/test_tensor_engine.sh
# Convenience bash entrypoint for scripts/test_tensor_engine.py

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Prefer python3 in path
PYTHON_BIN="python3"
if ! command -v "${PYTHON_BIN}" &> /dev/null; then
    if command -v python &> /dev/null; then
        PYTHON_BIN="python"
    else
        echo "Error: python3 is required to run the tensor engine test suite." >&2
        exit 1
    fi
fi

exec "${PYTHON_BIN}" "${SCRIPT_DIR}/test_tensor_engine.py" "$@"
