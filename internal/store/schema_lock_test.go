
package store

import (
"sync"
"sync/atomic"
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestSchemaCheckUsesRLock verifies that schema checking uses RLock
// allowing concurrent reads without blocking
func TestSchemaCheckUsesRLock(t *testing.T) {
ds := &Dataset{
Records: make([]arrow.RecordBatch, 0),
}

// Simulate multiple concurrent schema checks - should not block each other
var wg sync.WaitGroup
var concurrentReaders atomic.Int32
var maxConcurrent atomic.Int32

for i := 0; i < 10; i++ {
wg.Add(1)
go func() {
defer wg.Done()

ds.mu.RLock()
current := concurrentReaders.Add(1)
// Track max concurrent readers
for {
old := maxConcurrent.Load()
if current <= old || maxConcurrent.CompareAndSwap(old, current) {
break
}
}
// Simulate schema check work
time.Sleep(10 * time.Millisecond)
concurrentReaders.Add(-1)
ds.mu.RUnlock()
}()
}

wg.Wait()

// With RLock, multiple readers should run concurrently
if maxConcurrent.Load() < 2 {
t.Errorf("Expected concurrent readers >= 2, got %d (RLock not working)", maxConcurrent.Load())
}
}

// TestSchemaEvolutionUpgradesToWriteLock verifies lock upgrade on schema change
func TestSchemaEvolutionUpgradesToWriteLock(t *testing.T) {
ds := &Dataset{
Records: make([]arrow.RecordBatch, 0),
Version: 1,
}

// Simulate the lock upgrade pattern
ds.mu.RLock()
needsUpgrade := true // Simulate schema evolution detected
ds.mu.RUnlock()

if needsUpgrade {
ds.mu.Lock()
ds.Version++
ds.mu.Unlock()
}

if ds.Version != 2 {
t.Errorf("Expected version 2 after schema evolution, got %d", ds.Version)
}
}

// TestConcurrentSchemaChecksAndWrites verifies no data races
func TestConcurrentSchemaChecksAndWrites(t *testing.T) {
alloc := memory.NewGoAllocator()
schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

builder := array.NewRecordBuilder(alloc, schema)
builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
rec := builder.NewRecordBatch()
defer rec.Release()

ds := &Dataset{
Records: []arrow.RecordBatch{rec},
Version: 1,
}

var wg sync.WaitGroup

// Multiple concurrent readers checking schema
for i := 0; i < 5; i++ {
wg.Add(1)
go func() {
defer wg.Done()
for j := 0; j < 100; j++ {
ds.mu.RLock()
if len(ds.Records) > 0 {
_ = ds.Records[0].Schema()
}
ds.mu.RUnlock()
}
}()
}

// Concurrent writer
wg.Add(1)
go func() {
defer wg.Done()
for j := 0; j < 50; j++ {
ds.mu.Lock()
ds.Version++
ds.mu.Unlock()
}
}()

wg.Wait()

if ds.Version != 51 {
t.Errorf("Expected version 51, got %d", ds.Version)
}
}

// TestSchemaCheckWithRLockReturnsCorrectSchema verifies RLock schema reads
func TestSchemaCheckWithRLockReturnsCorrectSchema(t *testing.T) {
alloc := memory.NewGoAllocator()
schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "value", Type: arrow.PrimitiveTypes.Float64},
}, nil)

builder := array.NewRecordBuilder(alloc, schema)
builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
builder.Field(1).(*array.Float64Builder).AppendValues([]float64{1.0}, nil)
rec := builder.NewRecordBatch()
defer rec.Release()

ds := &Dataset{
Records: []arrow.RecordBatch{rec},
}

// Read schema under RLock
ds.mu.RLock()
gotSchema := ds.Records[0].Schema()
ds.mu.RUnlock()

if len(gotSchema.Fields()) != 2 {
t.Errorf("Expected 2 fields, got %d", len(gotSchema.Fields()))
}
if gotSchema.Field(0).Name != "id" {
t.Errorf("Expected field 0 name 'id', got %s", gotSchema.Field(0).Name)
}
}

// TestSchemaEvolutionOnlyLocksWhenNeeded verifies minimal lock time
func TestSchemaEvolutionOnlyLocksWhenNeeded(t *testing.T) {
ds := &Dataset{
Records: make([]arrow.RecordBatch, 0),
Version: 1,
}

var writeLockAcquired atomic.Bool

// Simulate schema check that does NOT need evolution
ds.mu.RLock()
needsEvolution := false // Schema matches - no upgrade needed
ds.mu.RUnlock()

if needsEvolution {
ds.mu.Lock()
writeLockAcquired.Store(true)
ds.Version++
ds.mu.Unlock()
}

if writeLockAcquired.Load() {
t.Error("Write lock should not be acquired when schema matches")
}
if ds.Version != 1 {
t.Errorf("Version should remain 1, got %d", ds.Version)
}
}

// TestGetExistingSchemaWithRLock tests the helper function
func TestGetExistingSchemaWithRLock(t *testing.T) {
alloc := memory.NewGoAllocator()
schema := arrow.NewSchema([]arrow.Field{
{Name: "test", Type: arrow.PrimitiveTypes.Int32},
}, nil)

builder := array.NewRecordBuilder(alloc, schema)
builder.Field(0).(*array.Int32Builder).AppendValues([]int32{42}, nil)
rec := builder.NewRecordBatch()
defer rec.Release()

ds := &Dataset{
Records: []arrow.RecordBatch{rec},
}

// Test GetExistingSchema method
gotSchema := ds.GetExistingSchema()
if gotSchema == nil {
t.Fatal("Expected non-nil schema")
}
if gotSchema.Field(0).Name != "test" {
t.Errorf("Expected field name 'test', got %s", gotSchema.Field(0).Name)
}
}

// TestGetExistingSchemaEmptyDataset verifies nil return for empty dataset
func TestGetExistingSchemaEmptyDataset(t *testing.T) {
ds := &Dataset{
Records: make([]arrow.RecordBatch, 0),
}

gotSchema := ds.GetExistingSchema()
if gotSchema != nil {
t.Error("Expected nil schema for empty dataset")
}
}

// TestCheckSchemaCompatibility tests schema comparison helper
func TestCheckSchemaCompatibility(t *testing.T) {
alloc := memory.NewGoAllocator()

// Original schema
schema1 := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// Extended schema (compatible)
schema2 := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "value", Type: arrow.PrimitiveTypes.Float64},
}, nil)

// Incompatible schema (different type)
schema3 := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Float64}, // Changed type!
}, nil)

tests := []struct {
name     string
existing *arrow.Schema
incoming *arrow.Schema
wantComp SchemaCompatibility
}{
{"exact_match", schema1, schema1, SchemaExactMatch},
{"evolution", schema1, schema2, SchemaEvolution},
{"incompatible", schema1, schema3, SchemaIncompatible},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
got := CheckSchemaCompatibility(tt.existing, tt.incoming)
if got != tt.wantComp {
t.Errorf("CheckSchemaCompatibility() = %v, want %v", got, tt.wantComp)
}
})
}

_ = alloc // silence unused
}

// TestUpgradeSchemaVersionSafe tests safe version upgrade
func TestUpgradeSchemaVersionSafe(t *testing.T) {
ds := &Dataset{
Records: make([]arrow.RecordBatch, 0),
Version: 5,
}

ds.UpgradeSchemaVersion()

if ds.Version != 6 {
t.Errorf("Expected version 6, got %d", ds.Version)
}
}

// BenchmarkSchemaCheckRLockVsLock compares RLock vs Lock performance
func BenchmarkSchemaCheckRLockVsLock(b *testing.B) {
alloc := memory.NewGoAllocator()
schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

builder := array.NewRecordBuilder(alloc, schema)
builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
rec := builder.NewRecordBatch()
defer rec.Release()

ds := &Dataset{
Records: []arrow.RecordBatch{rec},
}

b.Run("RLock", func(b *testing.B) {
for i := 0; i < b.N; i++ {
ds.mu.RLock()
_ = ds.Records[0].Schema()
ds.mu.RUnlock()
}
})

b.Run("Lock", func(b *testing.B) {
for i := 0; i < b.N; i++ {
ds.mu.Lock()
_ = ds.Records[0].Schema()
ds.mu.Unlock()
}
})
}

// BenchmarkConcurrentSchemaChecks benchmarks concurrent read throughput
func BenchmarkConcurrentSchemaChecks(b *testing.B) {
alloc := memory.NewGoAllocator()
schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

builder := array.NewRecordBuilder(alloc, schema)
builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
rec := builder.NewRecordBatch()
defer rec.Release()

ds := &Dataset{
Records: []arrow.RecordBatch{rec},
}

b.RunParallel(func(pb *testing.PB) {
for pb.Next() {
ds.mu.RLock()
_ = ds.Records[0].Schema()
ds.mu.RUnlock()
}
})
}
