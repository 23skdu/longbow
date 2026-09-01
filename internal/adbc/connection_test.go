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

func TestConnection_GetInfo(t *testing.T) {
	conn := newTestConnection(t)
	reader, err := conn.GetInfo(context.Background(), nil)
	if err != nil {
		t.Fatalf("GetInfo(nil): %v", err)
	}
	if reader == nil {
		t.Fatal("GetInfo(nil): got nil reader")
	}
	reader.Release()
}

func TestConnection_GetObjects(t *testing.T) {
	conn := newTestConnection(t)
	reader, err := conn.GetObjects(context.Background(),
		adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("GetObjects: %v", err)
	}
	if reader == nil {
		t.Fatal("GetObjects: got nil reader")
	}
	reader.Release()
}

func TestConnection_GetTableSchema_NotFound(t *testing.T) {
	conn := newTestConnection(t)
	_, err := conn.GetTableSchema(context.Background(), nil, nil, "nonexistent_dataset")
	if err == nil {
		t.Fatal("GetTableSchema: expected error for nonexistent dataset")
	}
}

func TestConnection_GetTableTypes(t *testing.T) {
	conn := newTestConnection(t)
	reader, err := conn.GetTableTypes(context.Background())
	if err != nil {
		t.Fatalf("GetTableTypes: %v", err)
	}
	if reader == nil {
		t.Fatal("GetTableTypes: got nil reader")
	}
	reader.Release()
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
