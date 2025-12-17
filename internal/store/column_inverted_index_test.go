package store

import (
"sync"
"testing"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestNewColumnInvertedIndex verifies basic creation
func TestNewColumnInvertedIndex(t *testing.T) {
idx := NewColumnInvertedIndex()
if idx == nil {
t.Fatal("NewColumnInvertedIndex returned nil")
}
}

// TestColumnInvertedIndex_IndexColumn verifies indexing a string column
func TestColumnInvertedIndex_IndexColumn(t *testing.T) {
idx := NewColumnInvertedIndex()
mem := memory.NewGoAllocator()

// Create a simple record with string column
schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.BinaryTypes.String},
{Name: "category", Type: arrow.BinaryTypes.String},
}, nil)

bldr := array.NewRecordBuilder(mem, schema)
defer bldr.Release()

bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"cat1", "cat2", "cat1"}, nil)

rec := bldr.NewRecordBatch()
defer rec.Release()

// Index the category column
idx.IndexRecord("test_dataset", 0, rec, []string{"category"})

// Verify index was built
rows := idx.Lookup("test_dataset", "category", "cat1")
if len(rows) != 2 {
t.Errorf("Expected 2 rows for cat1, got %d", len(rows))
}

rows = idx.Lookup("test_dataset", "category", "cat2")
if len(rows) != 1 {
t.Errorf("Expected 1 row for cat2, got %d", len(rows))
}
}

// TestColumnInvertedIndex_LookupNotFound verifies empty result for missing values
func TestColumnInvertedIndex_LookupNotFound(t *testing.T) {
idx := NewColumnInvertedIndex()
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "category", Type: arrow.BinaryTypes.String},
}, nil)

bldr := array.NewRecordBuilder(mem, schema)
defer bldr.Release()

bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"cat1", "cat2"}, nil)

rec := bldr.NewRecordBatch()
defer rec.Release()

idx.IndexRecord("test_dataset", 0, rec, []string{"category"})

// Lookup non-existent value
rows := idx.Lookup("test_dataset", "category", "nonexistent")
if len(rows) != 0 {
t.Errorf("Expected 0 rows for nonexistent, got %d", len(rows))
}

// Lookup non-existent column
rows = idx.Lookup("test_dataset", "nonexistent_col", "cat1")
if len(rows) != 0 {
t.Errorf("Expected 0 rows for nonexistent column, got %d", len(rows))
}

// Lookup non-existent dataset
rows = idx.Lookup("nonexistent_dataset", "category", "cat1")
if len(rows) != 0 {
t.Errorf("Expected 0 rows for nonexistent dataset, got %d", len(rows))
}
}

// TestColumnInvertedIndex_MultipleRecords verifies indexing multiple records
func TestColumnInvertedIndex_MultipleRecords(t *testing.T) {
idx := NewColumnInvertedIndex()
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "status", Type: arrow.BinaryTypes.String},
}, nil)

// Record 0
bldr := array.NewRecordBuilder(mem, schema)
bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"active", "inactive"}, nil)
rec0 := bldr.NewRecordBatch()
idx.IndexRecord("ds", 0, rec0, []string{"status"})
rec0.Release()
bldr.Release()

// Record 1
bldr = array.NewRecordBuilder(mem, schema)
bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"active", "active", "pending"}, nil)
rec1 := bldr.NewRecordBatch()
idx.IndexRecord("ds", 1, rec1, []string{"status"})
rec1.Release()
bldr.Release()

// Lookup should return positions from both records
rows := idx.Lookup("ds", "status", "active")
if len(rows) != 3 {
t.Errorf("Expected 3 rows for active, got %d", len(rows))
}

// Verify record indices are correct
hasRec0 := false
hasRec1 := false
for _, r := range rows {
if r.RecordIdx == 0 {
hasRec0 = true
}
if r.RecordIdx == 1 {
hasRec1 = true
}
}
if !hasRec0 || !hasRec1 {
t.Error("Expected rows from both records")
}
}

// TestColumnInvertedIndex_Int64Column verifies indexing int64 columns
func TestColumnInvertedIndex_Int64Column(t *testing.T) {
idx := NewColumnInvertedIndex()
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

bldr := array.NewRecordBuilder(mem, schema)
defer bldr.Release()

bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{100, 200, 100, 300}, nil)

rec := bldr.NewRecordBatch()
defer rec.Release()

idx.IndexRecord("ds", 0, rec, []string{"user_id"})

rows := idx.Lookup("ds", "user_id", "100")
if len(rows) != 2 {
t.Errorf("Expected 2 rows for user_id=100, got %d", len(rows))
}
}

// TestColumnInvertedIndex_RemoveRecord verifies removing a record from index
func TestColumnInvertedIndex_RemoveRecord(t *testing.T) {
idx := NewColumnInvertedIndex()
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "category", Type: arrow.BinaryTypes.String},
}, nil)

bldr := array.NewRecordBuilder(mem, schema)
bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"cat1", "cat2"}, nil)
rec := bldr.NewRecordBatch()
idx.IndexRecord("ds", 0, rec, []string{"category"})
rec.Release()
bldr.Release()

// Verify indexed
rows := idx.Lookup("ds", "category", "cat1")
if len(rows) != 1 {
t.Fatalf("Expected 1 row before removal, got %d", len(rows))
}

// Remove record
idx.RemoveRecord("ds", 0)

// Verify removed
rows = idx.Lookup("ds", "category", "cat1")
if len(rows) != 0 {
t.Errorf("Expected 0 rows after removal, got %d", len(rows))
}
}

// TestColumnInvertedIndex_RemoveDataset verifies removing entire dataset
func TestColumnInvertedIndex_RemoveDataset(t *testing.T) {
idx := NewColumnInvertedIndex()
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "category", Type: arrow.BinaryTypes.String},
}, nil)

bldr := array.NewRecordBuilder(mem, schema)
bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"cat1"}, nil)
rec := bldr.NewRecordBatch()
idx.IndexRecord("ds", 0, rec, []string{"category"})
rec.Release()
bldr.Release()

idx.RemoveDataset("ds")

rows := idx.Lookup("ds", "category", "cat1")
if len(rows) != 0 {
t.Errorf("Expected 0 rows after dataset removal, got %d", len(rows))
}
}

// TestColumnInvertedIndex_ConcurrentAccess verifies thread safety
func TestColumnInvertedIndex_ConcurrentAccess(t *testing.T) {
idx := NewColumnInvertedIndex()
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.BinaryTypes.String},
}, nil)

var wg sync.WaitGroup

// Concurrent writers
for i := 0; i < 10; i++ {
wg.Add(1)
go func(idx_num int) {
defer wg.Done()
bldr := array.NewRecordBuilder(mem, schema)
bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"concurrent"}, nil)
rec := bldr.NewRecordBatch()
idx.IndexRecord("ds", idx_num, rec, []string{"id"})
rec.Release()
bldr.Release()
}(i)
}

// Concurrent readers
for i := 0; i < 10; i++ {
wg.Add(1)
go func() {
defer wg.Done()
_ = idx.Lookup("ds", "id", "concurrent")
}()
}

wg.Wait()

// Final check
rows := idx.Lookup("ds", "id", "concurrent")
if len(rows) != 10 {
t.Errorf("Expected 10 rows after concurrent writes, got %d", len(rows))
}
}

// TestColumnInvertedIndex_GetMatchingRowIndices verifies batch lookup
func TestColumnInvertedIndex_GetMatchingRowIndices(t *testing.T) {
idx := NewColumnInvertedIndex()
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "category", Type: arrow.BinaryTypes.String},
}, nil)

bldr := array.NewRecordBuilder(mem, schema)
bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"a", "b", "c", "a", "b"}, nil)
rec := bldr.NewRecordBatch()
idx.IndexRecord("ds", 0, rec, []string{"category"})
rec.Release()
bldr.Release()

// Get row indices for specific record
indices := idx.GetMatchingRowIndices("ds", 0, "category", "a")
if len(indices) != 2 {
t.Errorf("Expected 2 indices, got %d", len(indices))
}

// Verify correct indices (0 and 3)
expected := map[int]bool{0: true, 3: true}
for _, i := range indices {
if !expected[i] {
t.Errorf("Unexpected index %d", i)
}
}
}

// TestColumnInvertedIndex_HasIndex verifies index existence check
func TestColumnInvertedIndex_HasIndex(t *testing.T) {
idx := NewColumnInvertedIndex()
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "indexed", Type: arrow.BinaryTypes.String},
{Name: "not_indexed", Type: arrow.BinaryTypes.String},
}, nil)

bldr := array.NewRecordBuilder(mem, schema)
bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"a"}, nil)
bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"b"}, nil)
rec := bldr.NewRecordBatch()
idx.IndexRecord("ds", 0, rec, []string{"indexed"}) // Only index "indexed" column
rec.Release()
bldr.Release()

if !idx.HasIndex("ds", "indexed") {
t.Error("Expected HasIndex to return true for indexed column")
}

if idx.HasIndex("ds", "not_indexed") {
t.Error("Expected HasIndex to return false for non-indexed column")
}

if idx.HasIndex("nonexistent", "indexed") {
t.Error("Expected HasIndex to return false for nonexistent dataset")
}
}

// BenchmarkColumnInvertedIndex_Lookup benchmarks lookup performance
func BenchmarkColumnInvertedIndex_Lookup(b *testing.B) {
idx := NewColumnInvertedIndex()
mem := memory.NewGoAllocator()

schema := arrow.NewSchema([]arrow.Field{
{Name: "category", Type: arrow.BinaryTypes.String},
}, nil)

// Create 100 records with 1000 rows each
for recIdx := 0; recIdx < 100; recIdx++ {
bldr := array.NewRecordBuilder(mem, schema)
values := make([]string, 1000)
for i := 0; i < 1000; i++ {
values[i] = "cat" + string(rune('0'+i%10))
}
bldr.Field(0).(*array.StringBuilder).AppendValues(values, nil)
rec := bldr.NewRecordBatch()
idx.IndexRecord("ds", recIdx, rec, []string{"category"})
rec.Release()
bldr.Release()
}

b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = idx.Lookup("ds", "category", "cat5")
}
}
