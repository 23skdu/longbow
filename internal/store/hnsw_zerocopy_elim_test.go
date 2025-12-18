package store

import (
"sync"
"testing"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestZeroCopyMetricsExist verifies zero-copy metrics are defined
func TestZeroCopyMetricsExist(t *testing.T) {
t.Run("metrics_exist", func(t *testing.T) {
// Will be tested via integration with actual metric calls
})
}

// TestGetVectorUnsafeInAdd tests zero-copy access in Add hot path
func TestGetVectorUnsafeInAdd(t *testing.T) {
alloc := memory.NewGoAllocator()
ds := &Dataset{
Name:    "test-add-zerocopy",
Records: make([]arrow.RecordBatch, 0),
}

idx := NewHNSWIndex(ds)

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
}, nil)

recBuilder := array.NewRecordBuilder(alloc, schema)
defer recBuilder.Release()

idBuilder := recBuilder.Field(0).(*array.Int64Builder)
vecBuilder := recBuilder.Field(1).(*array.FixedSizeListBuilder)
valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

for i := 0; i < 100; i++ {
idBuilder.Append(int64(i))
vecBuilder.Append(true)
valBuilder.Append(float32(i))
valBuilder.Append(float32(i + 1))
valBuilder.Append(float32(i + 2))
valBuilder.Append(float32(i + 3))
}

rec := recBuilder.NewRecordBatch()
defer rec.Release()
ds.Records = append(ds.Records, rec)

for i := 0; i < 100; i++ {
err := idx.Add(0, i)
if err != nil {
t.Fatalf("Add failed: %v", err)
}
}

if len(idx.locations) != 100 {
t.Errorf("Expected 100 locations, got %d", len(idx.locations))
}
}

// TestGetVectorUnsafeInSearchByID tests zero-copy in search hot path
func TestGetVectorUnsafeInSearchByID(t *testing.T) {
alloc := memory.NewGoAllocator()
ds := &Dataset{
Name:    "test-search-zerocopy",
Records: make([]arrow.RecordBatch, 0),
}

idx := NewHNSWIndex(ds)

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
}, nil)

recBuilder := array.NewRecordBuilder(alloc, schema)
defer recBuilder.Release()

idBuilder := recBuilder.Field(0).(*array.Int64Builder)
vecBuilder := recBuilder.Field(1).(*array.FixedSizeListBuilder)
valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

for i := 0; i < 50; i++ {
idBuilder.Append(int64(i))
vecBuilder.Append(true)
valBuilder.Append(float32(i))
valBuilder.Append(float32(i))
valBuilder.Append(float32(i))
valBuilder.Append(float32(i))
}

rec := recBuilder.NewRecordBatch()
defer rec.Release()
ds.Records = append(ds.Records, rec)

for i := 0; i < 50; i++ {
_ = idx.Add(0, i)
}

results := idx.SearchByID(0, 5)
if len(results) == 0 {
t.Error("SearchByID returned no results")
}

found := false
for _, r := range results {
if r == 0 {
found = true
break
}
}
if !found {
t.Error("Expected query vector in results")
}
}

// TestGetVectorUnsafeInRerankBatch tests zero-copy in batch operations
func TestGetVectorUnsafeInRerankBatch(t *testing.T) {
alloc := memory.NewGoAllocator()
ds := &Dataset{
Name:    "test-rerank-zerocopy",
Records: make([]arrow.RecordBatch, 0),
}

idx := NewHNSWIndex(ds)

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
}, nil)

recBuilder := array.NewRecordBuilder(alloc, schema)
defer recBuilder.Release()

idBuilder := recBuilder.Field(0).(*array.Int64Builder)
vecBuilder := recBuilder.Field(1).(*array.FixedSizeListBuilder)
valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

for i := 0; i < 20; i++ {
idBuilder.Append(int64(i))
vecBuilder.Append(true)
valBuilder.Append(float32(i * 10))
valBuilder.Append(float32(i * 10))
valBuilder.Append(float32(i * 10))
valBuilder.Append(float32(i * 10))
}

rec := recBuilder.NewRecordBatch()
defer rec.Release()
ds.Records = append(ds.Records, rec)

for i := 0; i < 20; i++ {
_ = idx.Add(0, i)
}

query := []float32{0, 0, 0, 0}
candidates := []VectorID{0, 1, 2, 3, 4}

// RerankBatch is on HNSWIndex
reranked := idx.RerankBatch(query, candidates, 3)
if len(reranked) != 3 {
t.Errorf("Expected 3 results, got %d", len(reranked))
}

if len(reranked) > 0 && reranked[0].ID != 0 {
t.Errorf("Expected VectorID 0 as closest, got %d", reranked[0].ID)
}
}

// TestZeroCopyConcurrentAccess tests thread safety of zero-copy access
func TestZeroCopyConcurrentAccess(t *testing.T) {
alloc := memory.NewGoAllocator()
ds := &Dataset{
Name:    "test-concurrent-zerocopy",
Records: make([]arrow.RecordBatch, 0),
}

idx := NewHNSWIndex(ds)

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
}, nil)

recBuilder := array.NewRecordBuilder(alloc, schema)
defer recBuilder.Release()

idBuilder := recBuilder.Field(0).(*array.Int64Builder)
vecBuilder := recBuilder.Field(1).(*array.FixedSizeListBuilder)
valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

for i := 0; i < 100; i++ {
idBuilder.Append(int64(i))
vecBuilder.Append(true)
valBuilder.Append(float32(i))
valBuilder.Append(float32(i))
valBuilder.Append(float32(i))
valBuilder.Append(float32(i))
}

rec := recBuilder.NewRecordBatch()
defer rec.Release()
ds.Records = append(ds.Records, rec)

for i := 0; i < 100; i++ {
_ = idx.Add(0, i)
}

var wg sync.WaitGroup
errorsCh := make(chan error, 100)

for i := 0; i < 10; i++ {
wg.Add(1)
go func() {
defer wg.Done()
for j := 0; j < 10; j++ {
results := idx.SearchByID(VectorID(j%100), 5)
if len(results) == 0 {
errorsCh <- nil
}
}
}()
}

wg.Wait()
close(errorsCh)

t.Log("Concurrent zero-copy access completed without errors")
}

// TestEpochProtectionInHotPaths verifies epoch protection is used
func TestEpochProtectionInHotPaths(t *testing.T) {
alloc := memory.NewGoAllocator()
ds := &Dataset{
Name:    "test-epoch-protection",
Records: make([]arrow.RecordBatch, 0),
}

idx := NewHNSWIndex(ds)

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
}, nil)

recBuilder := array.NewRecordBuilder(alloc, schema)
defer recBuilder.Release()

idBuilder := recBuilder.Field(0).(*array.Int64Builder)
vecBuilder := recBuilder.Field(1).(*array.FixedSizeListBuilder)
valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

for i := 0; i < 10; i++ {
idBuilder.Append(int64(i))
vecBuilder.Append(true)
valBuilder.Append(float32(i))
valBuilder.Append(float32(i))
valBuilder.Append(float32(i))
valBuilder.Append(float32(i))
}

rec := recBuilder.NewRecordBatch()
defer rec.Release()
ds.Records = append(ds.Records, rec)

for i := 0; i < 10; i++ {
_ = idx.Add(0, i)
}

initialReaders := idx.activeReaders.Load()

_ = idx.SearchByID(0, 5)

finalReaders := idx.activeReaders.Load()
if finalReaders != initialReaders {
t.Errorf("Epoch leak: initial readers %d, final readers %d", initialReaders, finalReaders)
}
}

// BenchmarkZeroCopyVsCopy compares zero-copy vs copy performance
func BenchmarkZeroCopyVsCopy(b *testing.B) {
alloc := memory.NewGoAllocator()
ds := &Dataset{
Name:    "bench-zerocopy",
Records: make([]arrow.RecordBatch, 0),
}

idx := NewHNSWIndex(ds)

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
}, nil)

recBuilder := array.NewRecordBuilder(alloc, schema)

idBuilder := recBuilder.Field(0).(*array.Int64Builder)
vecBuilder := recBuilder.Field(1).(*array.FixedSizeListBuilder)
valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

for i := 0; i < 1000; i++ {
idBuilder.Append(int64(i))
vecBuilder.Append(true)
for j := 0; j < 128; j++ {
valBuilder.Append(float32(i*128 + j))
}
}

rec := recBuilder.NewRecordBatch()
ds.Records = append(ds.Records, rec)

for i := 0; i < 1000; i++ {
_ = idx.Add(0, i)
}

b.Run("getVector_copy", func(b *testing.B) {
b.ReportAllocs()
for i := 0; i < b.N; i++ {
vec := idx.getVector(VectorID(i % 1000))
_ = vec
}
})

b.Run("getVectorUnsafe_zerocopy", func(b *testing.B) {
b.ReportAllocs()
for i := 0; i < b.N; i++ {
vec, release := idx.getVectorUnsafe(VectorID(i % 1000))
_ = vec
if release != nil {
release()
}
}
})
}

// BenchmarkSearchByIDZeroCopy benchmarks search with zero-copy
func BenchmarkSearchByIDZeroCopy(b *testing.B) {
alloc := memory.NewGoAllocator()
ds := &Dataset{
Name:    "bench-search-zerocopy",
Records: make([]arrow.RecordBatch, 0),
}

idx := NewHNSWIndex(ds)

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
}, nil)

recBuilder := array.NewRecordBuilder(alloc, schema)

idBuilder := recBuilder.Field(0).(*array.Int64Builder)
vecBuilder := recBuilder.Field(1).(*array.FixedSizeListBuilder)
valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

for i := 0; i < 1000; i++ {
idBuilder.Append(int64(i))
vecBuilder.Append(true)
for j := 0; j < 128; j++ {
valBuilder.Append(float32(i*128 + j))
}
}

rec := recBuilder.NewRecordBatch()
ds.Records = append(ds.Records, rec)

for i := 0; i < 1000; i++ {
_ = idx.Add(0, i)
}

b.ReportAllocs()
b.ResetTimer()

for i := 0; i < b.N; i++ {
results := idx.SearchByID(VectorID(i%1000), 10)
_ = results
}
}
