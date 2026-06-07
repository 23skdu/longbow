#!/usr/bin/env python3
"""Smoke-test the c-shared ADBC driver build.

This test verifies the ``liblongbow_adbc.so`` produced by
``make build-adbc`` exports the full ADBC 1.0.0 C-API surface
(not just the ``AdbcDriverInit`` entry point). Each ADBC entry
point must be exported with a valid function pointer so the
driver manager can wire it into the AdbcDriver function pointer
table.

Run::

    make build-adbc
    /home/rsd/longbow-venv/bin/python3 tests/adbc/test_driver_so.py

The test exits 0 if all 1.0.0-required ADBC entry points are
exported; non-zero otherwise. This test is intentionally
independent of the Apache Arrow ADBC Python driver so it can run
in CI without ``adbc-driver-manager`` installed.
"""

import ctypes
import os
import sys


# ADBC 1.0.0 entry points that the driver manager's CHECK_REQUIRED
# macro refuses to omit. Each must be exported in the .so with a
# non-NULL function pointer.
ADBC_1_0_0_REQUIRED = (
    "AdbcLongbowAdbcInit",  # Adbc<DriverName>Init
    "AdbcDatabaseNew",
    "AdbcDatabaseInit",
    "AdbcDatabaseRelease",
    "AdbcConnectionNew",
    "AdbcConnectionInit",
    "AdbcConnectionRelease",
    "AdbcStatementNew",
    "AdbcStatementRelease",
    "AdbcStatementSetSqlQuery",
    "AdbcStatementExecuteQuery",
)


def find_driver_so() -> str | None:
    candidates = [
        os.path.abspath("liblongbow_adbc.so"),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "liblongbow_adbc.so")),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return None


def main() -> int:
    so_path = find_driver_so()
    if so_path is None:
        print("liblongbow_adbc.so not found. Run 'make build-adbc' first.", file=sys.stderr)
        return 2

    print(f"Loading {so_path} ({os.path.getsize(so_path)} bytes)")
    try:
        lib = ctypes.CDLL(so_path)
    except OSError as e:
        print(f"FAIL: ctypes.CDLL({so_path}): {e}", file=sys.stderr)
        return 1

    failed = []
    for name in ADBC_1_0_0_REQUIRED:
        try:
            sym = getattr(lib, name)
        except AttributeError:
            print(f"FAIL: {name} not exported")
            failed.append(name)
            continue
        addr = ctypes.cast(sym, ctypes.c_void_p).value
        if addr == 0:
            print(f"FAIL: {name} exported but address is NULL")
            failed.append(name)
        else:
            print(f"OK:   {name} @ 0x{addr:x}")

    if failed:
        print(f"\n{len(failed)} symbol(s) missing or null: {failed}", file=sys.stderr)
        return 1

    print(f"\nAll {len(ADBC_1_0_0_REQUIRED)} ADBC 1.0.0-required entry points exported.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
