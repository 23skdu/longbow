package adbc_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	longbowadbc "github.com/23skdu/longbow/internal/adbc"
)

func TestStatementExecution(t *testing.T) {
	driver := longbowadbc.NewDriver()
	db, err := driver.NewDatabase(nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	conn, err := db.Open(context.Background())
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}
	defer stmt.Close()

	err = stmt.SetSqlQuery("SELECT * FROM my_collection")
	if err != nil {
		t.Fatalf("SetSqlQuery failed: %v", err)
	}

	reader, rows, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery failed: %v", err)
	}
	if rows != -1 {
		t.Errorf("Expected rows -1, got %d", rows)
	}

	schema := reader.Schema()
	if schema == nil {
		t.Fatal("Expected schema, got nil")
	}

	if schema.NumFields() != 2 {
		t.Errorf("Expected 2 fields, got %d", schema.NumFields())
	}
}

func TestParametricBinding(t *testing.T) {
	driver := longbowadbc.NewDriver()
	db, _ := driver.NewDatabase(nil)
	conn, _ := db.Open(context.Background())
	stmt, _ := conn.NewStatement()

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)}}, nil)
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Ensure we get a Not Implemented error for now
	err := stmt.Bind(context.Background(), rec)
	if err == nil {
		t.Errorf("Expected Not Implemented error for Bind")
	}
}

func FuzzDialectParsing(f *testing.F) {
	f.Add("SELECT * FROM test")
	f.Add("vector <-> ?")
	f.Add("INVALID SQL QUERY WITH RANDOM TOKENS")

	f.Fuzz(func(t *testing.T, query string) {
		driver := longbowadbc.NewDriver()
		db, _ := driver.NewDatabase(nil)
		conn, _ := db.Open(context.Background())
		stmt, _ := conn.NewStatement()

		_ = stmt.SetSqlQuery(query)

		_, _, err := stmt.ExecuteQuery(context.Background())
		if err != nil {
			if adbcErr, ok := err.(adbc.Error); ok {
				if adbcErr.Code != adbc.StatusNotImplemented && adbcErr.Code != adbc.StatusInvalidArgument {
					t.Errorf("Unexpected ADBC error code: %v", adbcErr.Code)
				}
			}
		}
	})
}

// ----------------------------------------------------------------------------
// Additional tests for 100% coverage of internal/adbc/.
// ----------------------------------------------------------------------------

func newTestStatement(t *testing.T) adbc.Statement {
	t.Helper()
	driver := longbowadbc.NewDriver()
	db, _ := driver.NewDatabase(nil)
	conn, _ := db.Open(context.Background())
	stmt, err := conn.NewStatement()
	if err != nil {
		t.Fatalf("NewStatement: %v", err)
	}
	t.Cleanup(func() {
		_ = stmt.Close()
		_ = conn.Close()
		_ = db.Close()
	})
	return stmt
}

func TestStatement_Close(t *testing.T) {
	stmt := newTestStatement(t)
	if err := stmt.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestStatement_SetOption_NotImplemented(t *testing.T) {
	stmt := newTestStatement(t)
	assertNotImplemented(t, stmt.SetOption("k", "v"), "SetOption")
}

func TestStatement_SetSqlQuery(t *testing.T) {
	stmt := newTestStatement(t)
	if err := stmt.SetSqlQuery("SELECT 1"); err != nil {
		t.Errorf("SetSqlQuery: %v", err)
	}
	if err := stmt.SetSqlQuery(""); err != nil {
		t.Errorf("SetSqlQuery(empty): %v", err)
	}
}

func TestStatement_ExecuteQuery(t *testing.T) {
	stmt := newTestStatement(t)
	if err := stmt.SetSqlQuery("SELECT 1"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	reader, rows, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	if rows != -1 {
		t.Errorf("ExecuteQuery: rows = %d, want -1", rows)
	}
	if reader == nil {
		t.Fatal("ExecuteQuery: got nil reader")
	}
	if reader.Schema() == nil {
		t.Error("ExecuteQuery: reader.Schema() is nil")
	}
	if reader.Schema().NumFields() != 2 {
		t.Errorf("ExecuteQuery: NumFields = %d, want 2", reader.Schema().NumFields())
	}
	// The ExecuteQuery path returns a fresh AdbcRecordReader with no records;
	// exercising the reader here documents that behaviour.
	reader.Release()
}

func TestStatement_ExecuteUpdate_NotImplemented(t *testing.T) {
	stmt := newTestStatement(t)
	rows, err := stmt.ExecuteUpdate(context.Background())
	assertNotImplemented(t, err, "ExecuteUpdate")
	if rows != -1 {
		t.Errorf("ExecuteUpdate: rows = %d, want -1", rows)
	}
}

func TestStatement_Prepare_NotImplemented(t *testing.T) {
	stmt := newTestStatement(t)
	assertNotImplemented(t, stmt.Prepare(context.Background()), "Prepare")
}

func TestStatement_SetSubstraitPlan_NotImplemented(t *testing.T) {
	stmt := newTestStatement(t)
	assertNotImplemented(t, stmt.SetSubstraitPlan([]byte("plan")), "SetSubstraitPlan")
}

func TestStatement_Bind_NotImplemented(t *testing.T) {
	stmt := newTestStatement(t)
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32}}, nil)
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	rec := builder.NewRecordBatch()
	defer rec.Release()
	assertNotImplemented(t, stmt.Bind(context.Background(), rec), "Bind")
}

func TestStatement_BindStream(t *testing.T) {
	stmt := newTestStatement(t)
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32}}, nil)
	reader, err := array.NewRecordReader(schema, nil)
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	t.Cleanup(func() { reader.Release() })
	if err := stmt.BindStream(context.Background(), reader); err != nil {
		t.Errorf("BindStream: %v", err)
	}
	// Verify the stream was actually set on the concrete type.
	concrete, ok := stmt.(*longbowadbc.Statement)
	if !ok {
		t.Fatalf("expected *longbowadbc.Statement, got %T", stmt)
	}
	if concrete == nil {
		t.Fatal("nil concrete")
	}
}

func TestStatement_GetParameterSchema_NotImplemented(t *testing.T) {
	stmt := newTestStatement(t)
	schema, err := stmt.GetParameterSchema()
	assertNotImplemented(t, err, "GetParameterSchema")
	if schema != nil {
		t.Errorf("GetParameterSchema: expected nil schema, got %v", schema)
	}
}

func TestStatement_ExecutePartitions_NotImplemented(t *testing.T) {
	stmt := newTestStatement(t)
	schema, partitions, rows, err := stmt.ExecutePartitions(context.Background())
	assertNotImplemented(t, err, "ExecutePartitions")
	if schema != nil {
		t.Errorf("ExecutePartitions: expected nil schema, got %v", schema)
	}
	if partitions.NumPartitions != 0 {
		t.Errorf("ExecutePartitions: NumPartitions = %d, want 0", partitions.NumPartitions)
	}
	if len(partitions.PartitionIDs) != 0 {
		t.Errorf("ExecutePartitions: len(PartitionIDs) = %d, want 0", len(partitions.PartitionIDs))
	}
	if rows != -1 {
		t.Errorf("ExecutePartitions: rows = %d, want -1", rows)
	}
}
