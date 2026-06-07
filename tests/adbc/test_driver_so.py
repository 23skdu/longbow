#!/usr/bin/env python3
"""Smoke-test the c-shared ADBC driver build.

This test verifies the ``liblongbow_adbc.so`` produced by
``make build-adbc`` is loadable as a C shared library and that the
``AdbcDriverInit`` entry point exported by ``cmd/adbc/main.go`` is
present.  It does NOT exercise a full ADBC round-trip — the current
``cmd/adbc/main.go`` is a stub that only exports ``AdbcDriverInit``
(the c-shared binding is not yet wired up to the full C-API; see
``cmd/adbc/main.go:73-87`` for the TODO).

Run::

    make build-adbc
    /home/rsd/longbow-venv/bin/python3 tests/adbc/test_driver_so.py

The test exits 0 if the library is loadable and contains the expected
symbols; non-zero otherwise.  The test is intentionally independent
of the Apache Arrow ADBC Python driver so it can run in CI without
``adbc-driver-manager`` installed.
"""

import ctypes
import os
import sys


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

    # The Go c-shared build exports a single Go-style symbol;
    # cgo wraps it as `_cgoexp_<hash>_AdbcDriverInit`.  We check both.
    init_sym = "AdbcDriverInit"
    found = False
    for name in (init_sym,):
        try:
            sym = getattr(lib, name)
            found = True
            print(f"OK: {name} exported (address: 0x{ctypes.cast(sym, ctypes.c_void_p).value:x})")
            break
        except AttributeError:
            pass

    if not found:
        # cgo-renamed symbol format: _cgoexp_<hash>_<name>
        try:
            for attr in dir(lib):
                if attr.endswith("_" + init_sym) or attr == init_sym:
                    found = True
                    print(f"OK: cgo-renamed symbol found: {attr}")
                    break
        except Exception:
            pass

    if not found:
        print(f"FAIL: {init_sym} not found in {so_path}", file=sys.stderr)
        return 1

    # Document the missing C-API entry point.  When the full ADBC
    # C-API binding is wired up in cmd/adbc/main.go, this test should
    # additionally assert that AdbcDatabaseNew is exported.
    if not hasattr(lib, "AdbcDatabaseNew"):
        print("INFO: AdbcDatabaseNew not exported (c-API is stub-only). "
              "See cmd/adbc/main.go:73-87 for the wiring TODO.")

    print("Smoke test passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
