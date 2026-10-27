#!/usr/bin/env python3
"""Smoke-test the c-shared ADBC driver build.

This test verifies the ``liblongbow_adbc.so`` produced by
``make build-adbc`` exports the full ADBC 1.0.0 and 1.1.0 C-API
surface (including both ``AdbcDriverInit`` and ``AdbcLongbowAdbcInit``).
It also tests driver initialization and version negotiation.

Run::

    make build-adbc
    python3 tests/adbc/test_driver_so.py

The test exits 0 if all checks pass; non-zero otherwise.
"""

import ctypes
import os
import sys

ADBC_VERSION_1_0_0 = 1000000
ADBC_VERSION_1_1_0 = 1001000

ADBC_STATUS_OK = 0
ADBC_STATUS_INVALID_ARGUMENT = 5

# Required symbols exported by liblongbow_adbc.so
ADBC_EXPORTED_SYMBOLS = (
    "AdbcDriverInit",
    "AdbcLongbowAdbcInit",
    "AdbcDatabaseNew",
    "AdbcDatabaseInit",
    "AdbcDatabaseRelease",
    "AdbcDatabaseSetOption",
    "AdbcConnectionNew",
    "AdbcConnectionInit",
    "AdbcConnectionRelease",
    "AdbcConnectionCommit",
    "AdbcConnectionRollback",
    "AdbcConnectionSetOption",
    "AdbcConnectionGetInfo",
    "AdbcConnectionGetObjects",
    "AdbcConnectionGetTableSchema",
    "AdbcConnectionGetTableTypes",
    "AdbcConnectionCancel",
    "AdbcStatementNew",
    "AdbcStatementRelease",
    "AdbcStatementSetOption",
    "AdbcStatementSetSqlQuery",
    "AdbcStatementExecuteQuery",
    "AdbcStatementCancel",
    "AdbcStatementExecuteSchema",
)


class AdbcError(ctypes.Structure):
    _fields_ = [
        ("message", ctypes.c_char_p),
        ("vendor_code", ctypes.c_int32),
        ("sqlstate", ctypes.c_char * 5),
        ("release", ctypes.c_void_p),
        ("private_data", ctypes.c_void_p),
        ("private_driver", ctypes.c_void_p),
    ]


class AdbcDriver(ctypes.Structure):
    _fields_ = [
        ("private_data", ctypes.c_void_p),
        ("private_manager", ctypes.c_void_p),
        ("release", ctypes.c_void_p),
        ("DatabaseInit", ctypes.c_void_p),
        ("DatabaseNew", ctypes.c_void_p),
        ("DatabaseSetOption", ctypes.c_void_p),
        ("DatabaseRelease", ctypes.c_void_p),
        ("ConnectionCommit", ctypes.c_void_p),
        ("ConnectionGetInfo", ctypes.c_void_p),
        ("ConnectionGetObjects", ctypes.c_void_p),
        ("ConnectionGetTableSchema", ctypes.c_void_p),
        ("ConnectionGetTableTypes", ctypes.c_void_p),
        ("ConnectionInit", ctypes.c_void_p),
        ("ConnectionNew", ctypes.c_void_p),
        ("ConnectionSetOption", ctypes.c_void_p),
        ("ConnectionReadPartition", ctypes.c_void_p),
        ("ConnectionRelease", ctypes.c_void_p),
        ("ConnectionRollback", ctypes.c_void_p),
        ("StatementBind", ctypes.c_void_p),
        ("StatementBindStream", ctypes.c_void_p),
        ("StatementExecuteQuery", ctypes.c_void_p),
        ("StatementExecutePartitions", ctypes.c_void_p),
        ("StatementGetParameterSchema", ctypes.c_void_p),
        ("StatementNew", ctypes.c_void_p),
        ("StatementPrepare", ctypes.c_void_p),
        ("StatementRelease", ctypes.c_void_p),
        ("StatementSetOption", ctypes.c_void_p),
        ("StatementSetSqlQuery", ctypes.c_void_p),
        ("StatementSetSubstraitPlan", ctypes.c_void_p),
        # 1.1.0 additions
        ("ErrorGetDetailCount", ctypes.c_void_p),
        ("ErrorGetDetail", ctypes.c_void_p),
        ("ErrorFromArrayStream", ctypes.c_void_p),
        ("DatabaseGetOption", ctypes.c_void_p),
        ("DatabaseGetOptionBytes", ctypes.c_void_p),
        ("DatabaseGetOptionDouble", ctypes.c_void_p),
        ("DatabaseGetOptionInt", ctypes.c_void_p),
        ("DatabaseSetOptionBytes", ctypes.c_void_p),
        ("DatabaseSetOptionDouble", ctypes.c_void_p),
        ("DatabaseSetOptionInt", ctypes.c_void_p),
        ("ConnectionCancel", ctypes.c_void_p),
        ("ConnectionGetOption", ctypes.c_void_p),
        ("ConnectionGetOptionBytes", ctypes.c_void_p),
        ("ConnectionGetOptionDouble", ctypes.c_void_p),
        ("ConnectionGetOptionInt", ctypes.c_void_p),
        ("ConnectionGetStatistics", ctypes.c_void_p),
        ("ConnectionGetStatisticNames", ctypes.c_void_p),
        ("ConnectionSetOptionBytes", ctypes.c_void_p),
        ("ConnectionSetOptionDouble", ctypes.c_void_p),
        ("ConnectionSetOptionInt", ctypes.c_void_p),
        ("StatementCancel", ctypes.c_void_p),
        ("StatementExecuteSchema", ctypes.c_void_p),
        ("StatementGetOption", ctypes.c_void_p),
        ("StatementGetOptionBytes", ctypes.c_void_p),
        ("StatementGetOptionDouble", ctypes.c_void_p),
        ("StatementGetOptionInt", ctypes.c_void_p),
        ("StatementSetOptionBytes", ctypes.c_void_p),
        ("StatementSetOptionDouble", ctypes.c_void_p),
        ("StatementSetOptionInt", ctypes.c_void_p),
    ]


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
    for name in ADBC_EXPORTED_SYMBOLS:
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

    print(f"\nAll {len(ADBC_EXPORTED_SYMBOLS)} ADBC entry points exported successfully.")

    # Test AdbcDriverInit version negotiation
    init_fn = lib.AdbcDriverInit
    init_fn.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.POINTER(AdbcError)]
    init_fn.restype = ctypes.c_int

    # 1. Test ADBC 1.1.0 initialization
    driver_1_1 = AdbcDriver()
    err = AdbcError()
    status = init_fn(ADBC_VERSION_1_1_0, ctypes.byref(driver_1_1), ctypes.byref(err))
    if status != ADBC_STATUS_OK:
        msg = err.message.decode("utf-8") if err.message else "unknown error"
        print(f"FAIL: AdbcDriverInit(1.1.0) failed with status {status}: {msg}", file=sys.stderr)
        return 1
    if not driver_1_1.StatementExecuteQuery:
        print("FAIL: StatementExecuteQuery not populated in 1.1.0 driver table", file=sys.stderr)
        return 1
    if not driver_1_1.StatementExecuteSchema:
        print("FAIL: StatementExecuteSchema not populated in 1.1.0 driver table", file=sys.stderr)
        return 1
    if not driver_1_1.ConnectionGetInfo:
        print("FAIL: ConnectionGetInfo not populated in 1.1.0 driver table", file=sys.stderr)
        return 1
    print("OK:   AdbcDriverInit(ADBC_VERSION_1_1_0) succeeded and populated 1.1.0 vtable.")

    # 2. Test ADBC 1.0.0 initialization
    driver_1_0 = AdbcDriver()
    err = AdbcError()
    status = init_fn(ADBC_VERSION_1_0_0, ctypes.byref(driver_1_0), ctypes.byref(err))
    if status != ADBC_STATUS_OK:
        msg = err.message.decode("utf-8") if err.message else "unknown error"
        print(f"FAIL: AdbcDriverInit(1.0.0) failed with status {status}: {msg}", file=sys.stderr)
        return 1
    if not driver_1_0.StatementExecuteQuery:
        print("FAIL: StatementExecuteQuery not populated in 1.0.0 driver table", file=sys.stderr)
        return 1
    if driver_1_0.StatementExecuteSchema:
        print("FAIL: StatementExecuteSchema must NOT be populated when caller requested 1.0.0", file=sys.stderr)
        return 1
    print("OK:   AdbcDriverInit(ADBC_VERSION_1_0_0) succeeded and respected 1.0.0 struct boundary.")

    # 3. Test unsupported version rejection
    err = AdbcError()
    status = init_fn(2000000, None, ctypes.byref(err))
    if status != ADBC_STATUS_INVALID_ARGUMENT:
        print(f"FAIL: expected ADBC_STATUS_INVALID_ARGUMENT for version 2000000, got {status}", file=sys.stderr)
        return 1
    msg = err.message.decode("utf-8") if err.message else ""
    if "unsupported ADBC version" not in msg:
        print(f"FAIL: expected unsupported version error message, got {msg!r}", file=sys.stderr)
        return 1
    if err.release:
        release_fn = ctypes.CFUNCTYPE(None, ctypes.POINTER(AdbcError))(err.release)
        release_fn(ctypes.byref(err))
    print("OK:   AdbcDriverInit correctly rejected unsupported version with valid AdbcError.")

    print("\nAll ADBC C-API and ABI negotiation tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
