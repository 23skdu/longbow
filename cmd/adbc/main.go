package main

/*
#include <stdint.h>
#include <stdlib.h>

// Arrow C Data Interface structs
struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema*);
    int (*get_next)(struct ArrowArrayStream*, struct ArrowArray*);
    const char* (*get_last_error)(struct ArrowArrayStream*);
    void (*release)(struct ArrowArrayStream*);
    void* private_data;
};

// ADBC C-API structs (1.0.0 subset)
struct AdbcDatabase {
    void* private_data;
    void* private_driver;
};

struct AdbcConnection {
    void* private_data;
    void* private_driver;
};

struct AdbcStatement {
    void* private_data;
    void* private_driver;
};

// Forward declarations for AdbcDriver and AdbcError so the
// function pointer typedefs below can reference them.
struct AdbcDriver;
struct AdbcError;

// Typedef function pointer types so cgo exposes them as Go func
// types (not *[0]byte) in the AdbcDriver struct.
typedef int (*AdbcReleaseFn)(struct AdbcDriver*, struct AdbcError*);
typedef int (*AdbcDatabaseInitFn)(struct AdbcDatabase*, struct AdbcError*);
typedef int (*AdbcDatabaseNewFn)(struct AdbcDatabase*, struct AdbcError*);
typedef int (*AdbcDatabaseSetOptionFn)(struct AdbcDatabase*, const char*, const char*, struct AdbcError*);
typedef int (*AdbcDatabaseReleaseFn)(struct AdbcDatabase*, struct AdbcError*);
typedef int (*AdbcConnectionCommitFn)(struct AdbcConnection*, struct AdbcError*);
typedef int (*AdbcConnectionGetInfoFn)(struct AdbcConnection*, const uint32_t*, size_t, struct ArrowArrayStream*, struct AdbcError*);
typedef int (*AdbcConnectionGetObjectsFn)(struct AdbcConnection*, int, const char*, const char*, const char*, const char**, const char*, struct ArrowArrayStream*, struct AdbcError*);
typedef int (*AdbcConnectionGetTableSchemaFn)(struct AdbcConnection*, const char*, const char*, const char*, struct ArrowSchema*, struct AdbcError*);
typedef int (*AdbcConnectionGetTableTypesFn)(struct AdbcConnection*, struct ArrowArrayStream*, struct AdbcError*);
typedef int (*AdbcConnectionInitFn)(struct AdbcConnection*, struct AdbcDatabase*, struct AdbcError*);
typedef int (*AdbcConnectionNewFn)(struct AdbcConnection*, struct AdbcError*);
typedef int (*AdbcConnectionSetOptionFn)(struct AdbcConnection*, const char*, const char*, struct AdbcError*);
typedef int (*AdbcConnectionReadPartitionFn)(struct AdbcConnection*, const uint8_t*, size_t, struct ArrowArrayStream*, struct AdbcError*);
typedef int (*AdbcConnectionReleaseFn)(struct AdbcConnection*, struct AdbcError*);
typedef int (*AdbcConnectionRollbackFn)(struct AdbcConnection*, struct AdbcError*);
typedef int (*AdbcStatementBindFn)(struct AdbcStatement*, struct ArrowArray*, struct ArrowSchema*, struct AdbcError*);
typedef int (*AdbcStatementBindStreamFn)(struct AdbcStatement*, struct ArrowArrayStream*, struct AdbcError*);
typedef int (*AdbcStatementExecuteQueryFn)(struct AdbcStatement*, struct ArrowArrayStream*, int64_t*, struct AdbcError*);
typedef int (*AdbcStatementExecutePartitionsFn)(struct AdbcStatement*, struct ArrowSchema*, void*, int64_t*, struct AdbcError*);
typedef int (*AdbcStatementGetParameterSchemaFn)(struct AdbcStatement*, struct ArrowSchema*, struct AdbcError*);
typedef int (*AdbcStatementNewFn)(struct AdbcConnection*, struct AdbcStatement*, struct AdbcError*);
typedef int (*AdbcStatementPrepareFn)(struct AdbcStatement*, struct AdbcError*);
typedef int (*AdbcStatementReleaseFn)(struct AdbcStatement*, struct AdbcError*);
typedef int (*AdbcStatementSetOptionFn)(struct AdbcStatement*, const char*, const char*, struct AdbcError*);
typedef int (*AdbcStatementSetSqlQueryFn)(struct AdbcStatement*, const char*, struct AdbcError*);
typedef int (*AdbcStatementSetSubstraitPlanFn)(struct AdbcStatement*, const uint8_t*, size_t, struct AdbcError*);
typedef void (*AdbcErrorReleaseFn)(struct AdbcError*);

// AdbcDriver struct layout MUST match the ADBC spec exactly.
// Field order from adbc.h (1.0.0 + 1.1.0 surface, in order).
// Fields the longbow stub does not implement are left as `void*`
// in the struct so the driver manager's FILL_DEFAULT can populate
// them with the appropriate stub if needed.
struct AdbcDriver {
    void* private_data;
    void* private_manager;
    AdbcReleaseFn release;
    AdbcDatabaseInitFn DatabaseInit;
    AdbcDatabaseNewFn DatabaseNew;
    AdbcDatabaseSetOptionFn DatabaseSetOption;
    AdbcDatabaseReleaseFn DatabaseRelease;
    AdbcConnectionCommitFn ConnectionCommit;
    AdbcConnectionGetInfoFn ConnectionGetInfo;
    AdbcConnectionGetObjectsFn ConnectionGetObjects;
    AdbcConnectionGetTableSchemaFn ConnectionGetTableSchema;
    AdbcConnectionGetTableTypesFn ConnectionGetTableTypes;
    AdbcConnectionInitFn ConnectionInit;
    AdbcConnectionNewFn ConnectionNew;
    AdbcConnectionSetOptionFn ConnectionSetOption;
    AdbcConnectionReadPartitionFn ConnectionReadPartition;
    AdbcConnectionReleaseFn ConnectionRelease;
    AdbcConnectionRollbackFn ConnectionRollback;
    AdbcStatementBindFn StatementBind;
    AdbcStatementBindStreamFn StatementBindStream;
    AdbcStatementExecuteQueryFn StatementExecuteQuery;
    AdbcStatementExecutePartitionsFn StatementExecutePartitions;
    AdbcStatementGetParameterSchemaFn StatementGetParameterSchema;
    AdbcStatementNewFn StatementNew;
    AdbcStatementPrepareFn StatementPrepare;
    AdbcStatementReleaseFn StatementRelease;
    AdbcStatementSetOptionFn StatementSetOption;
    AdbcStatementSetSqlQueryFn StatementSetSqlQuery;
    AdbcStatementSetSubstraitPlanFn StatementSetSubstraitPlan;
};

struct AdbcError {
    char* message;
    int32_t vendor_code;
    char sqlstate[5];
    AdbcErrorReleaseFn release;
    void* private_data;
    void* private_driver;
};

// ADBC status codes (subset)
#define ADBC_STATUS_OK 0
#define ADBC_STATUS_UNKNOWN 1
#define ADBC_STATUS_NOT_IMPLEMENTED 2
#define ADBC_STATUS_NOT_FOUND 3
#define ADBC_STATUS_ALREADY_EXISTS 4
#define ADBC_STATUS_INVALID_ARGUMENT 5
#define ADBC_STATUS_INVALID_STATE 6
#define ADBC_STATUS_INTERNAL 7
#define ADBC_STATUS_IO 8
#define ADBC_STATUS_CANCELLED 9
#define ADBC_STATUS_AUTHENTICATION 10
#define ADBC_STATUS_AUTHORIZATION 11

// ADBC API version. The driver manager calls AdbcDriverInit with
// the ADBC version it expects; we accept 1.0.0 (== 1).
#define ADBC_VERSION_1_0_0 1

// C-to-Go bridge wrappers for the AdbcDriver function pointer
// table. Storing the address of these wrappers in the AdbcDriver
// struct is the only way to expose Go functions as assignable C
// function pointers (Go function values are not directly
// assignable to C function pointer fields).
//
// Each bridge forwards to the cgo-generated C wrapper around
// the corresponding Go `//export`ed function. The `//export`
// directive creates C-callable functions with the same name as
// the Go function (e.g., `AdbcDatabaseNew`). cgo also generates
// an internal symbol (`GoExportAdbcXxx`) used by the cgo runtime
// to marshal args between C and Go. We forward to the public
// name (without prefix) so cgo can wire up the indirection.
//
// `__attribute__((weak))` allows the bridge definitions to be
// linked even when cgo compiles the preamble into both the
// regular cgo bridge object and the _cgo_export.c object
// (both of which would otherwise have conflicting definitions).
__attribute__((weak)) int bridge_DatabaseNew(struct AdbcDatabase* db, struct AdbcError* err) {
    extern int AdbcDatabaseNew(struct AdbcDatabase*, struct AdbcError*);
    return AdbcDatabaseNew(db, err);
}
__attribute__((weak)) int bridge_DatabaseInit(struct AdbcDatabase* db, struct AdbcError* err) {
    extern int AdbcDatabaseInit(struct AdbcDatabase*, struct AdbcError*);
    return AdbcDatabaseInit(db, err);
}
__attribute__((weak)) int bridge_DatabaseRelease(struct AdbcDatabase* db, struct AdbcError* err) {
    extern int AdbcDatabaseRelease(struct AdbcDatabase*, struct AdbcError*);
    return AdbcDatabaseRelease(db, err);
}
__attribute__((weak)) int bridge_DatabaseSetOption(struct AdbcDatabase* db, char* key, char* value, struct AdbcError* err) {
    extern int AdbcDatabaseSetOption(struct AdbcDatabase*, char*, char*, struct AdbcError*);
    return AdbcDatabaseSetOption(db, key, value, err);
}
__attribute__((weak)) int bridge_ConnectionNew(struct AdbcConnection* conn, struct AdbcError* err) {
    extern int AdbcConnectionNew(struct AdbcConnection*, struct AdbcError*);
    return AdbcConnectionNew(conn, err);
}
__attribute__((weak)) int bridge_ConnectionInit(struct AdbcConnection* conn, struct AdbcDatabase* db, struct AdbcError* err) {
    extern int AdbcConnectionInit(struct AdbcConnection*, struct AdbcDatabase*, struct AdbcError*);
    return AdbcConnectionInit(conn, db, err);
}
__attribute__((weak)) int bridge_ConnectionRelease(struct AdbcConnection* conn, struct AdbcError* err) {
    extern int AdbcConnectionRelease(struct AdbcConnection*, struct AdbcError*);
    return AdbcConnectionRelease(conn, err);
}
__attribute__((weak)) int bridge_ConnectionCommit(struct AdbcConnection* conn, struct AdbcError* err) {
    extern int AdbcConnectionCommit(struct AdbcConnection*, struct AdbcError*);
    return AdbcConnectionCommit(conn, err);
}
__attribute__((weak)) int bridge_ConnectionRollback(struct AdbcConnection* conn, struct AdbcError* err) {
    extern int AdbcConnectionRollback(struct AdbcConnection*, struct AdbcError*);
    return AdbcConnectionRollback(conn, err);
}
__attribute__((weak)) int bridge_ConnectionSetOption(struct AdbcConnection* conn, char* key, char* value, struct AdbcError* err) {
    extern int AdbcConnectionSetOption(struct AdbcConnection*, char*, char*, struct AdbcError*);
    return AdbcConnectionSetOption(conn, key, value, err);
}
__attribute__((weak)) int bridge_StatementNew(struct AdbcConnection* conn, struct AdbcStatement* stmt, struct AdbcError* err) {
    extern int AdbcStatementNew(struct AdbcConnection*, struct AdbcStatement*, struct AdbcError*);
    return AdbcStatementNew(conn, stmt, err);
}
__attribute__((weak)) int bridge_StatementRelease(struct AdbcStatement* stmt, struct AdbcError* err) {
    extern int AdbcStatementRelease(struct AdbcStatement*, struct AdbcError*);
    return AdbcStatementRelease(stmt, err);
}
__attribute__((weak)) int bridge_StatementSetOption(struct AdbcStatement* stmt, char* key, char* value, struct AdbcError* err) {
    extern int AdbcStatementSetOption(struct AdbcStatement*, char*, char*, struct AdbcError*);
    return AdbcStatementSetOption(stmt, key, value, err);
}
__attribute__((weak)) int bridge_StatementSetSqlQuery(struct AdbcStatement* stmt, char* query, struct AdbcError* err) {
    extern int AdbcStatementSetSqlQuery(struct AdbcStatement*, char*, struct AdbcError*);
    return AdbcStatementSetSqlQuery(stmt, query, err);
}
__attribute__((weak)) int bridge_StatementExecuteQuery(struct AdbcStatement* stmt, struct ArrowArrayStream* out, int64_t* rows, struct AdbcError* err) {
    extern int AdbcStatementExecuteQuery(struct AdbcStatement*, struct ArrowArrayStream*, int64_t*, struct AdbcError*);
    return AdbcStatementExecuteQuery(stmt, out, rows, err);
}

// C-to-Go bridge for the AdbcError.release callback.
__attribute__((weak)) void bridge_setCErrorRelease(struct AdbcError* err) {
    extern void setCErrorRelease(struct AdbcError*);
    setCErrorRelease(err);
}
*/
import "C"

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/23skdu/longbow/internal/adbc"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

const (
	adbcStatusOK              = 0
	adbcStatusUnknown         = 1
	adbcStatusNotImplemented  = 2
	adbcStatusNotFound        = 3
	adbcStatusInvalidArgument = 5
	adbcStatusInvalidState    = 6
	adbcStatusIO              = 8
)

//export AdbcLongbowAdbcInit
func AdbcLongbowAdbcInit(version C.int, driver unsafe.Pointer, err *C.struct_AdbcError) C.int {
	// ADBC API version negotiation.
	// Version encoding: MAJOR*1000*1000 + MINOR*1000 + PATCH
	// ADBC_VERSION_1_0_0 = 1000000
	// ADBC_VERSION_1_1_0 = 1001000
	// We support the 1.x family (1000000..1001999).
	if version != 0 && (version < 1000000 || version >= 1002000) {
		setCError(err, fmt.Sprintf("AdbcLongbowAdbcInit: unsupported ADBC version %d (supported: 1.x)", version))
		return C.int(adbcStatusInvalidArgument)
	}

	cDriver := (*C.struct_AdbcDriver)(driver)
	// Field order MUST match the ADBC spec struct layout.
	cDriver.DatabaseInit = (C.AdbcDatabaseInitFn)(C.bridge_DatabaseInit)
	cDriver.DatabaseNew = (C.AdbcDatabaseNewFn)(C.bridge_DatabaseNew)
	cDriver.DatabaseSetOption = (C.AdbcDatabaseSetOptionFn)(C.bridge_DatabaseSetOption)
	cDriver.DatabaseRelease = (C.AdbcDatabaseReleaseFn)(C.bridge_DatabaseRelease)
	cDriver.ConnectionCommit = (C.AdbcConnectionCommitFn)(C.bridge_ConnectionCommit)
	cDriver.ConnectionInit = (C.AdbcConnectionInitFn)(C.bridge_ConnectionInit)
	cDriver.ConnectionNew = (C.AdbcConnectionNewFn)(C.bridge_ConnectionNew)
	cDriver.ConnectionSetOption = (C.AdbcConnectionSetOptionFn)(C.bridge_ConnectionSetOption)
	cDriver.ConnectionRelease = (C.AdbcConnectionReleaseFn)(C.bridge_ConnectionRelease)
	cDriver.ConnectionRollback = (C.AdbcConnectionRollbackFn)(C.bridge_ConnectionRollback)
	cDriver.StatementNew = (C.AdbcStatementNewFn)(C.bridge_StatementNew)
	cDriver.StatementRelease = (C.AdbcStatementReleaseFn)(C.bridge_StatementRelease)
	cDriver.StatementSetOption = (C.AdbcStatementSetOptionFn)(C.bridge_StatementSetOption)
	cDriver.StatementSetSqlQuery = (C.AdbcStatementSetSqlQueryFn)(C.bridge_StatementSetSqlQuery)
	cDriver.StatementExecuteQuery = (C.AdbcStatementExecuteQueryFn)(C.bridge_StatementExecuteQuery)
	return C.int(adbcStatusOK)
}

// ---------------------------------------------------------------------------
// ADBC C-API implementations.
//
// Each function is `//export`ed so the C bridge wrappers in the preamble
// can forward calls. The C bridge wrappers then store their addresses in
// the AdbcDriver function pointer table (see AdbcDriverInit).
// ---------------------------------------------------------------------------

// setCError populates the C.AdbcError struct with a Go error message.
// The error's `release` callback frees the message when the C caller
// is done with it.
func setCError(err *C.struct_AdbcError, msg string) {
	if err == nil {
		return
	}
	// Allocate a C string. The release callback in setCErrorRelease
	// frees it via C.free.
	cstr := C.CString(msg)
	err.message = cstr
	err.vendor_code = 0
	err.sqlstate[0] = 0
	err.release = (C.AdbcErrorReleaseFn)(C.bridge_setCErrorRelease)
	err.private_data = unsafe.Pointer(cstr)
	err.private_driver = nil
}

//export setCErrorRelease
func setCErrorRelease(err *C.struct_AdbcError) {
	if err == nil {
		return
	}
	if err.private_data != nil {
		C.free(err.private_data)
		err.private_data = nil
	}
	err.message = nil
}

//export AdbcDatabaseNew
func AdbcDatabaseNew(db *C.struct_AdbcDatabase, err *C.struct_AdbcError) C.int {
	if db == nil {
		return C.int(adbcStatusInvalidArgument)
	}
	driver := adbc.NewDriver()
	goDB, goErr := driver.NewDatabase(nil)
	if goErr != nil {
		setCError(err, fmt.Sprintf("AdbcDatabaseNew: %v", goErr))
		return C.int(adbcStatusUnknown)
	}
	db.private_data = unsafe.Pointer(&goDB)
	return C.int(adbcStatusOK)
}

//export AdbcDatabaseInit
func AdbcDatabaseInit(db *C.struct_AdbcDatabase, err *C.struct_AdbcError) C.int {
	if db == nil || db.private_data == nil {
		setCError(err, "AdbcDatabaseInit: database not initialized (call AdbcDatabaseNew first)")
		return C.int(adbcStatusInvalidState)
	}
	// The longbow ADBC stub does not require any async init.
	return C.int(adbcStatusOK)
}

//export AdbcDatabaseRelease
func AdbcDatabaseRelease(db *C.struct_AdbcDatabase, err *C.struct_AdbcError) C.int {
	if db == nil || db.private_data == nil {
		return C.int(adbcStatusInvalidState)
	}
	db.private_data = nil
	return C.int(adbcStatusOK)
}

//export AdbcConnectionNew
func AdbcConnectionNew(conn *C.struct_AdbcConnection, err *C.struct_AdbcError) C.int {
	if conn == nil {
		return C.int(adbcStatusInvalidArgument)
	}
	driver := adbc.NewDriver()
	db, goErr := driver.NewDatabase(nil)
	if goErr != nil {
		setCError(err, fmt.Sprintf("AdbcConnectionNew: %v", goErr))
		return C.int(adbcStatusUnknown)
	}
	goConn, goErr := db.Open(context.Background())
	if goErr != nil {
		setCError(err, fmt.Sprintf("AdbcConnectionNew: %v", goErr))
		return C.int(adbcStatusUnknown)
	}
	conn.private_data = unsafe.Pointer(&goConn)
	return C.int(adbcStatusOK)
}

//export AdbcConnectionInit
func AdbcConnectionInit(conn *C.struct_AdbcConnection, db *C.struct_AdbcDatabase, err *C.struct_AdbcError) C.int {
	if conn == nil || conn.private_data == nil {
		setCError(err, "AdbcConnectionInit: connection not initialized")
		return C.int(adbcStatusInvalidState)
	}
	return C.int(adbcStatusOK)
}

//export AdbcConnectionRelease
func AdbcConnectionRelease(conn *C.struct_AdbcConnection, err *C.struct_AdbcError) C.int {
	if conn == nil || conn.private_data == nil {
		return C.int(adbcStatusInvalidState)
	}
	conn.private_data = nil
	return C.int(adbcStatusOK)
}

//export AdbcStatementNew
func AdbcStatementNew(conn *C.struct_AdbcConnection, stmt *C.struct_AdbcStatement, err *C.struct_AdbcError) C.int {
	if conn == nil || conn.private_data == nil || stmt == nil {
		setCError(err, "AdbcStatementNew: invalid arguments")
		return C.int(adbcStatusInvalidArgument)
	}
	goConnPtr := (*adbc.Connection)(unsafe.Pointer(conn.private_data))
	goStmt := adbc.NewStatement(goConnPtr)
	stmt.private_data = unsafe.Pointer(&goStmt)
	return C.int(adbcStatusOK)
}

//export AdbcStatementRelease
func AdbcStatementRelease(stmt *C.struct_AdbcStatement, err *C.struct_AdbcError) C.int {
	if stmt == nil || stmt.private_data == nil {
		return C.int(adbcStatusInvalidState)
	}
	stmt.private_data = nil
	return C.int(adbcStatusOK)
}

//export AdbcStatementSetSqlQuery
func AdbcStatementSetSqlQuery(stmt *C.struct_AdbcStatement, query *C.char, err *C.struct_AdbcError) C.int {
	if stmt == nil || stmt.private_data == nil || query == nil {
		setCError(err, "AdbcStatementSetSqlQuery: invalid arguments")
		return C.int(adbcStatusInvalidArgument)
	}
	goStmtPtr := (*adbc.Statement)(unsafe.Pointer(stmt.private_data))
	if setErr := goStmtPtr.SetSqlQuery(C.GoString(query)); setErr != nil {
		setCError(err, fmt.Sprintf("AdbcStatementSetSqlQuery: %v", setErr))
		return C.int(adbcStatusUnknown)
	}
	return C.int(adbcStatusOK)
}

//export AdbcStatementExecuteQuery
func AdbcStatementExecuteQuery(stmt *C.struct_AdbcStatement, out *C.struct_ArrowArrayStream, rows *C.int64_t, err *C.struct_AdbcError) C.int {
	if stmt == nil || stmt.private_data == nil || out == nil {
		setCError(err, "AdbcStatementExecuteQuery: invalid arguments")
		return C.int(adbcStatusInvalidArgument)
	}
	goStmtPtr := (*adbc.Statement)(unsafe.Pointer(stmt.private_data))
	reader, rowsAffected, goErr := goStmtPtr.ExecuteQuery(context.Background())
	if goErr != nil {
		setCError(err, fmt.Sprintf("AdbcStatementExecuteQuery: %v", goErr))
		return C.int(adbcStatusUnknown)
	}
	if rows != nil {
		*rows = C.int64_t(rowsAffected)
	}
	cdata.ExportRecordReader(reader, (*cdata.CArrowArrayStream)(unsafe.Pointer(out)))
	return C.int(adbcStatusOK)
}

// Additional 1.0.0 entry points wired up to keep the AdbcDriver
// function pointer table complete (manager's CHECK_REQUIRED will
// refuse drivers that don't populate DatabaseSetOption,
// ConnectionCommit/Rollback/SetOption, StatementSetOption).

//export AdbcDatabaseSetOption
func AdbcDatabaseSetOption(db *C.struct_AdbcDatabase, key, value *C.char, err *C.struct_AdbcError) C.int {
	// longbow's ADBC stub does not support options; signal
	// NOT_IMPLEMENTED so the driver manager can fall back.
	setCError(err, "AdbcDatabaseSetOption: not implemented in longbow ADBC stub")
	return C.int(adbcStatusNotImplemented)
}

//export AdbcConnectionCommit
func AdbcConnectionCommit(conn *C.struct_AdbcConnection, err *C.struct_AdbcError) C.int {
	if conn == nil || conn.private_data == nil {
		return C.int(adbcStatusInvalidState)
	}
	goConnPtr := (*adbc.Connection)(unsafe.Pointer(conn.private_data))
	if commitErr := goConnPtr.Commit(context.Background()); commitErr != nil {
		setCError(err, fmt.Sprintf("AdbcConnectionCommit: %v", commitErr))
		return C.int(adbcStatusUnknown)
	}
	return C.int(adbcStatusOK)
}

//export AdbcConnectionRollback
func AdbcConnectionRollback(conn *C.struct_AdbcConnection, err *C.struct_AdbcError) C.int {
	if conn == nil || conn.private_data == nil {
		return C.int(adbcStatusInvalidState)
	}
	goConnPtr := (*adbc.Connection)(unsafe.Pointer(conn.private_data))
	if rbErr := goConnPtr.Rollback(context.Background()); rbErr != nil {
		setCError(err, fmt.Sprintf("AdbcConnectionRollback: %v", rbErr))
		return C.int(adbcStatusUnknown)
	}
	return C.int(adbcStatusOK)
}

//export AdbcConnectionSetOption
func AdbcConnectionSetOption(conn *C.struct_AdbcConnection, key, value *C.char, err *C.struct_AdbcError) C.int {
	setCError(err, "AdbcConnectionSetOption: not implemented in longbow ADBC stub")
	return C.int(adbcStatusNotImplemented)
}

//export AdbcStatementSetOption
func AdbcStatementSetOption(stmt *C.struct_AdbcStatement, key, value *C.char, err *C.struct_AdbcError) C.int {
	setCError(err, "AdbcStatementSetOption: not implemented in longbow ADBC stub")
	return C.int(adbcStatusNotImplemented)
}

func main() {
	// Empty main required for c-shared build.
}
