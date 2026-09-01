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

func TestStatement_SetSqlQuery(t *testing.T) {
	stmt := newTestStatement(t)
	if err := stmt.SetSqlQuery("SELECT 1"); err != nil {
		t.Errorf("SetSqlQuery: %v", err)
	}
	if err := stmt.SetSqlQuery(""); err != nil {
		t.Errorf("SetSqlQuery(empty): %v", err)
	}
}

func TestStatement_ExecuteQuery_Stub(t *testing.T) {
	stmt := newTestStatement(t)
	if err := stmt.SetSqlQuery("SELECT * FROM nonexistent"); err != nil {
		t.Fatalf("SetSqlQuery: %v", err)
	}
	_, _, err := stmt.ExecuteQuery(context.Background())
	if err == nil {
		t.Fatal("ExecuteQuery: expected error for nonexistent dataset")
	}
}

func TestStatement_ExecuteQuery_EmptySQL(t *testing.T) {
	stmt := newTestStatement(t)
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
	if reader.Schema().NumFields() != 2 {
		t.Errorf("ExecuteQuery: NumFields = %d, want 2", reader.Schema().NumFields())
	}
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
	schema := newTestArrowSchema()
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	rec := builder.NewRecordBatch()
	defer rec.Release()
	assertNotImplemented(t, stmt.Bind(context.Background(), rec), "Bind")
}

func TestStatement_BindStream(t *testing.T) {
	stmt := newTestStatement(t)
	schema := newTestArrowSchema()
	reader, err := array.NewRecordReader(schema, nil)
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	t.Cleanup(func() { reader.Release() })
	if err := stmt.BindStream(context.Background(), reader); err != nil {
		t.Errorf("BindStream: %v", err)
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
		t.Errorf("ExecutePartitions: len(Partitions) = %d, want 0", len(partitions.PartitionIDs))
	}
	if rows != -1 {
		t.Errorf("ExecutePartitions: rows = %d, want -1", rows)
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
				switch adbcErr.Code {
				case adbc.StatusNotImplemented, adbc.StatusInvalidArgument, adbc.StatusNotFound:
				default:
					t.Errorf("Unexpected ADBC error code: %v", adbcErr.Code)
				}
			}
		}
	})
}

func newTestArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
}

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
