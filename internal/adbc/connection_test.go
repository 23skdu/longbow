package adbc_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"

	longbowadbc "github.com/23skdu/longbow/internal/adbc"
)

func newTestConnection(t *testing.T) adbc.Connection {
	t.Helper()
	driver := longbowadbc.NewDriver()
	db, err := driver.NewDatabase(nil)
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func assertNotImplemented(t *testing.T, err error, ctx string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error, got nil", ctx)
	}
	adbcErr, ok := err.(adbc.Error)
	if !ok {
		t.Fatalf("%s: expected adbc.Error, got %T (%v)", ctx, err, err)
	}
	if adbcErr.Code != adbc.StatusNotImplemented {
		t.Fatalf("%s: expected StatusNotImplemented (%d), got %d (%v)",
			ctx, adbc.StatusNotImplemented, adbcErr.Code, adbcErr)
	}
}

func TestConnection_Close(t *testing.T) {
	conn := newTestConnection(t)
	if err := conn.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestConnection_SetOption_NotImplemented(t *testing.T) {
	conn := newTestConnection(t)
	// SetOption is defined on the concrete *Connection type but is not
	// part of the adbc.Connection interface in Apache Arrow ADBC Go 18
	// (the interface only has SetOptions(ctx, opts)). We still cover it
	// to lock in the StatusNotImplemented contract.
	concrete, ok := conn.(*longbowadbc.Connection)
	if !ok {
		t.Fatalf("expected *longbowadbc.Connection, got %T", conn)
	}
	assertNotImplemented(t, concrete.SetOption("k", "v"), "SetOption")
}

func TestConnection_NewStatement(t *testing.T) {
	conn := newTestConnection(t)
	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	if stmt == nil {
		t.Fatal("NewStatement: got nil statement")
	}
	if err := stmt.Close(); err != nil {
		t.Errorf("stmt.Close: %v", err)
	}
}

func TestConnection_GetInfo_NotImplemented(t *testing.T) {
	conn := newTestConnection(t)
	reader, err := conn.GetInfo(context.Background(), nil)
	assertNotImplemented(t, err, "GetInfo(nil)")
	if reader != nil {
		t.Errorf("GetInfo: expected nil reader on error, got %T", reader)
	}

	codes := []adbc.InfoCode{adbc.InfoCode(0), adbc.InfoCode(42)}
	reader, err = conn.GetInfo(context.Background(), codes)
	assertNotImplemented(t, err, "GetInfo(codes)")
	if reader != nil {
		t.Errorf("GetInfo: expected nil reader on error, got %T", reader)
	}
}

func TestConnection_GetObjects_NotImplemented(t *testing.T) {
	conn := newTestConnection(t)
	catalog := "cat"
	dbSchema := "schema"
	tableName := "t"
	columnName := "c"
	tableType := []string{"table"}
	reader, err := conn.GetObjects(context.Background(),
		adbc.ObjectDepthAll, &catalog, &dbSchema, &tableName, &columnName, tableType)
	assertNotImplemented(t, err, "GetObjects")
	if reader != nil {
		t.Errorf("GetObjects: expected nil reader on error, got %T", reader)
	}
}

func TestConnection_GetTableSchema_NotImplemented(t *testing.T) {
	conn := newTestConnection(t)
	catalog := "cat"
	dbSchema := "schema"
	schema, err := conn.GetTableSchema(context.Background(), &catalog, &dbSchema, "t")
	assertNotImplemented(t, err, "GetTableSchema")
	if schema != nil {
		t.Errorf("GetTableSchema: expected nil schema on error, got %v", schema)
	}
}

func TestConnection_GetTableTypes_NotImplemented(t *testing.T) {
	conn := newTestConnection(t)
	reader, err := conn.GetTableTypes(context.Background())
	assertNotImplemented(t, err, "GetTableTypes")
	if reader != nil {
		t.Errorf("GetTableTypes: expected nil reader on error, got %T", reader)
	}
}

func TestConnection_ReadPartition_NotImplemented(t *testing.T) {
	conn := newTestConnection(t)
	reader, err := conn.ReadPartition(context.Background(), []byte("partition"))
	assertNotImplemented(t, err, "ReadPartition")
	if reader != nil {
		t.Errorf("ReadPartition: expected nil reader on error, got %T", reader)
	}
}

func TestConnection_Commit_NotImplemented(t *testing.T) {
	conn := newTestConnection(t)
	assertNotImplemented(t, conn.Commit(context.Background()), "Commit")
}

func TestConnection_Rollback_NotImplemented(t *testing.T) {
	conn := newTestConnection(t)
	assertNotImplemented(t, conn.Rollback(context.Background()), "Rollback")
}
