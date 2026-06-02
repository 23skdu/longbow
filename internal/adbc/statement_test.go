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
