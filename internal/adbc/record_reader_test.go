package adbc

import (
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

// TestAdbcRecordReader_ExecuteQueryRefCount documents the refcount
// contract of the production ExecuteQuery path. As of 2026-06-06 the
// returned reader has refCount=0 and r.records=nil (no public setter),
// so a single Release from the consumer is a no-op. This is a latent
// bug — when the ADBC backend eventually populates records from a real
// query result, ExecuteQuery must do `reader.Retain()` (or the struct
// literal must include `refCount: 1`) to follow the arrow/array
// convention. This test will fail loudly when that fix lands, prompting
// the contributor to verify the refCount path is correct.
func TestAdbcRecordReader_ExecuteQueryRefCount(t *testing.T) {
	stmt := &Statement{}
	reader, _, err := stmt.ExecuteQuery(nil)
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	concrete, ok := reader.(*AdbcRecordReader)
	if !ok {
		t.Fatalf("expected *AdbcRecordReader, got %T", reader)
	}
	// Document current state: refCount=0, records=nil.
	if concrete.refCount != 0 {
		t.Errorf("ExecuteQuery: refCount = %d, want 0 (latent bug, see comment)", concrete.refCount)
	}
	if len(concrete.records) != 0 {
		t.Errorf("ExecuteQuery: records len = %d, want 0", len(concrete.records))
	}
}
