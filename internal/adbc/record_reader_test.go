package adbc

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// makeTestRecord builds an Arrow RecordBatch with `n` int32 values using
// the given allocator and schema. The returned batch has refcount 1.
func makeTestRecord(t *testing.T, pool memory.Allocator, schema *arrow.Schema, n int64) arrow.RecordBatch {
	t.Helper()
	builder := array.NewRecordBuilder(pool, schema)
	t.Cleanup(builder.Release)
	b, ok := builder.Field(0).(*array.Int32Builder)
	if !ok {
		t.Fatalf("expected *array.Int32Builder, got %T", builder.Field(0))
	}
	for i := int64(0); i < n; i++ {
		b.Append(int32(i))
	}
	return builder.NewRecordBatch()
}

func TestAdbcRecordReader_Empty(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32}}, nil)
	r := &AdbcRecordReader{schema: schema}
	if r.Schema() == nil {
		t.Fatal("Schema() = nil")
	}
	// No records: Next must return false and RecordBatch must return nil.
	if r.Next() {
		t.Error("empty reader: Next() returned true")
	}
	if r.RecordBatch() != nil {
		t.Errorf("empty reader: RecordBatch() = %v, want nil", r.RecordBatch())
	}
	if r.Record() != nil {
		t.Errorf("empty reader: Record() = %v, want nil", r.Record())
	}
	if r.Err() != nil {
		t.Errorf("empty reader: Err() = %v, want nil", r.Err())
	}
}

func TestAdbcRecordReader_Iteration(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32}}, nil)

	rec1 := makeTestRecord(t, pool, schema, 3)
	rec2 := makeTestRecord(t, pool, schema, 5)

	r := &AdbcRecordReader{schema: schema}
	if r.Schema() == nil {
		t.Fatal("Schema() = nil")
	}
	if r.Schema().NumFields() != 1 {
		t.Errorf("Schema().NumFields() = %d, want 1", r.Schema().NumFields())
	}

	// Before any Next: RecordBatch must return nil.
	if r.RecordBatch() != nil {
		t.Errorf("before Next: RecordBatch() = %v, want nil", r.RecordBatch())
	}

	// Populate and iterate. We set refCount=1 to mirror the convention
	// used by arrow/array.simpleRecords: the creator's initial ref is
	// balanced by a single consumer Release. See TestAdbcRecordReader_ExecuteQueryRefCount
	// for the production ExecuteQuery path (which currently starts at 0
	// because the public API never populates r.records).
	rec1.Retain()
	rec2.Retain()
	r.records = []arrow.RecordBatch{rec1, rec2}
	r.refCount = 1

	if !r.Next() {
		t.Fatal("first Next returned false")
	}
	if got := r.RecordBatch(); got == nil {
		t.Error("after first Next, RecordBatch() = nil")
	} else if got.NumRows() != 3 {
		t.Errorf("after first Next, RecordBatch().NumRows() = %d, want 3", got.NumRows())
	}
	if got := r.Record(); got == nil {
		t.Error("after first Next, Record() = nil")
	} else if got.NumRows() != 3 {
		t.Errorf("after first Next, Record().NumRows() = %d, want 3", got.NumRows())
	}

	if !r.Next() {
		t.Fatal("second Next returned false")
	}
	if got := r.RecordBatch(); got == nil {
		t.Error("after second Next, RecordBatch() = nil")
	} else if got.NumRows() != 5 {
		t.Errorf("after second Next, RecordBatch().NumRows() = %d, want 5", got.NumRows())
	}

	// Per the arrow/array convention, RecordBatch() is only valid after
	// a successful Next(). After Next() returns false, callers should
	// not call RecordBatch() — its return value is undefined. We do not
	// assert on it here to keep the test aligned with the production
	// behaviour and the upstream Apache convention.
	if r.Next() {
		t.Error("third Next returned true")
	}
}

func TestAdbcRecordReader_RefCount(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32}}, nil)
	rec1 := makeTestRecord(t, pool, schema, 2)
	rec1.Retain()
	rec2 := makeTestRecord(t, pool, schema, 4)
	rec2.Retain()

	r := &AdbcRecordReader{schema: schema, records: []arrow.RecordBatch{rec1, rec2}, refCount: 1}

	// Multiple retains must not release the records prematurely.
	r.Retain()
	r.Retain()
	r.Release()
	r.Release()
	if r.records == nil {
		t.Error("premature release: records slice set to nil after refcount > 0")
	}
	// Final release drops the records.
	r.Release()
	if r.records != nil {
		t.Errorf("final release: records slice not set to nil: %v", r.records)
	}
}

func TestAdbcRecordReader_ConcurrentRetainRelease(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32}}, nil)
	rec := makeTestRecord(t, pool, schema, 1)
	rec.Retain()
	r := &AdbcRecordReader{schema: schema, records: []arrow.RecordBatch{rec}, refCount: 1}

	const workers = 16
	const iters = 1000
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				r.Retain()
				r.Release()
			}
		}()
	}
	wg.Wait()
	// After all workers: refCount is back to 1 (the creator's initial).
	// One more Release drops it to 0 and triggers cleanup.
	r.Release()
	if r.records != nil {
		t.Errorf("after concurrent release, records slice not nil: %v", r.records)
	}
}

func TestNewAdbcRecordReader_Valid(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32}}, nil)
	rec1 := makeTestRecord(t, pool, schema, 3)
	rec2 := makeTestRecord(t, pool, schema, 5)

	r, err := NewAdbcRecordReader(schema, []arrow.RecordBatch{rec1, rec2})
	if err != nil {
		t.Fatalf("NewAdbcRecordReader: %v", err)
	}
	if r == nil {
		t.Fatal("NewAdbcRecordReader: got nil reader")
	}
	// refCount=1 by convention; records slice is populated; schema matches.
	if r.refCount != 1 {
		t.Errorf("refCount = %d, want 1", r.refCount)
	}
	if len(r.records) != 2 {
		t.Errorf("len(records) = %d, want 2", len(r.records))
	}
	if r.Schema() == nil || !r.Schema().Equal(schema) {
		t.Errorf("Schema() mismatch")
	}
	// Iteration works as expected.
	if !r.Next() || r.RecordBatch().NumRows() != 3 {
		t.Errorf("first Next/RecordBatch failed: NumRows = %d", r.RecordBatch().NumRows())
	}
	if !r.Next() || r.RecordBatch().NumRows() != 5 {
		t.Errorf("second Next/RecordBatch failed: NumRows = %d", r.RecordBatch().NumRows())
	}
	if r.Next() {
		t.Error("third Next returned true")
	}
	// Single Release drops refCount to 0 and triggers cleanup.
	r.Release()
	if r.records != nil {
		t.Errorf("after final release, records not nil: %v", r.records)
	}
}

func TestNewAdbcRecordReader_SchemaMismatch(t *testing.T) {
	pool := memory.NewGoAllocator()
	wanted := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32}}, nil)
	other := arrow.NewSchema([]arrow.Field{{Name: "y", Type: arrow.PrimitiveTypes.Int64}}, nil)
	// Build a record that uses `other`'s schema (Int64) but pass `wanted`
	// (Int32) to NewAdbcRecordReader to trigger the mismatch path.
	builder := array.NewRecordBuilder(pool, other)
	t.Cleanup(builder.Release)
	b, ok := builder.Field(0).(*array.Int64Builder)
	if !ok {
		t.Fatalf("expected *array.Int64Builder, got %T", builder.Field(0))
	}
	b.Append(int64(1))
	rec := builder.NewRecordBatch()

	r, err := NewAdbcRecordReader(wanted, []arrow.RecordBatch{rec})
	if err == nil {
		t.Fatal("NewAdbcRecordReader: expected schema-mismatch error, got nil")
	}
	if r != nil {
		t.Errorf("NewAdbcRecordReader: expected nil reader on error, got %v", r)
	}
}

func TestNewAdbcRecordReader_Empty(t *testing.T) {
	// Constructor with no records: refCount=1, records=nil, Next() always false.
	schema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int32}}, nil)
	r, err := NewAdbcRecordReader(schema, nil)
	if err != nil {
		t.Fatalf("NewAdbcRecordReader(nil records): %v", err)
	}
	if r == nil {
		t.Fatal("NewAdbcRecordReader: got nil")
	}
	if r.refCount != 1 {
		t.Errorf("refCount = %d, want 1", r.refCount)
	}
	if r.Next() {
		t.Error("Next() returned true on empty reader")
	}
	r.Release() // drops refCount to 0; cleanup is a no-op
}

// TestAdbcRecordReader_ExecuteQueryRefCount locks in the refcount
// contract of the production ExecuteQuery path. The returned reader
// must have refCount=1 (the creator's initial ref, balanced by a
// single consumer Release) — this matches the arrow/array
// .simpleRecords convention.
func TestAdbcRecordReader_ExecuteQueryRefCount(t *testing.T) {
	driver := NewDriver()
	db, _ := driver.NewDatabase(nil)
	conn, _ := db.Open(context.Background())
	stmt, _ := conn.NewStatement()
	defer stmt.Close()
	defer conn.Close()
	defer db.Close()

	reader, _, err := stmt.ExecuteQuery(context.Background())
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	concrete, ok := reader.(*AdbcRecordReader)
	if !ok {
		t.Fatalf("expected *AdbcRecordReader, got %T", reader)
	}
	if concrete.refCount != 1 {
		t.Errorf("ExecuteQuery: refCount = %d, want 1 (must match simpleRecords convention)", concrete.refCount)
	}
	if len(concrete.records) != 0 {
		t.Errorf("ExecuteQuery: records len = %d, want 0", len(concrete.records))
	}
	// The single consumer Release must drop refCount to 0 and trigger
	// the cleanup path (which is a no-op when records is nil, but the
	// branch must be reachable).
	reader.Release()
}
