package store

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestDatasetMetadata tests the metadata struct
func TestDatasetMetadata_Fields(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
	}, nil)

	meta := DatasetMetadata{
		Name:       "test_dataset",
		Schema:     schema,
		TotalRows:  1000,
		BatchCount: 5,
	}

	if meta.Name != "test_dataset" {
		t.Errorf("expected name test_dataset, got %s", meta.Name)
	}
	if meta.TotalRows != 1000 {
		t.Errorf("expected 1000 rows, got %d", meta.TotalRows)
	}
	if meta.BatchCount != 5 {
		t.Errorf("expected 5 batches, got %d", meta.BatchCount)
	}
	if meta.Schema.NumFields() != 2 {
		t.Errorf("expected 2 fields, got %d", meta.Schema.NumFields())
	}
}

// TestCOWMetadataMap_NewDefault tests default configuration
func TestCOWMetadataMap_NewDefault(t *testing.T) {
	cow := NewCOWMetadataMap()
	if cow == nil {
		t.Fatal("NewCOWMetadataMap returned nil")
	}

	// Should start empty
	snap := cow.Snapshot()
	if len(snap) != 0 {
		t.Errorf("expected empty snapshot, got %d entries", len(snap))
	}
}

// TestCOWMetadataMap_SetAndGet tests basic operations
func TestCOWMetadataMap_SetAndGet(t *testing.T) {
	cow := NewCOWMetadataMap()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	meta := DatasetMetadata{
		Name:       "dataset1",
		Schema:     schema,
		TotalRows:  500,
		BatchCount: 2,
	}

	cow.Set("dataset1", meta)

	// Get should return the metadata
	got, ok := cow.Get("dataset1")
	if !ok {
		t.Fatal("expected to find dataset1")
	}
	if got.Name != "dataset1" {
		t.Errorf("expected name dataset1, got %s", got.Name)
	}
	if got.TotalRows != 500 {
		t.Errorf("expected 500 rows, got %d", got.TotalRows)
	}

	// Non-existent key
	_, ok = cow.Get("nonexistent")
	if ok {
		t.Error("expected nonexistent to not be found")
	}
}

// TestCOWMetadataMap_Update tests updating existing entry
func TestCOWMetadataMap_Update(t *testing.T) {
	cow := NewCOWMetadataMap()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Initial set
	cow.Set("dataset1", DatasetMetadata{
		Name:      "dataset1",
		Schema:    schema,
		TotalRows: 100,
	})

	// Update with more rows
	cow.Set("dataset1", DatasetMetadata{
		Name:      "dataset1",
		Schema:    schema,
		TotalRows: 200,
	})

	got, _ := cow.Get("dataset1")
	if got.TotalRows != 200 {
		t.Errorf("expected 200 rows after update, got %d", got.TotalRows)
	}
}

// TestCOWMetadataMap_Delete tests deletion
func TestCOWMetadataMap_Delete(t *testing.T) {
	cow := NewCOWMetadataMap()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	cow.Set("dataset1", DatasetMetadata{Name: "dataset1", Schema: schema})
	cow.Set("dataset2", DatasetMetadata{Name: "dataset2", Schema: schema})

	cow.Delete("dataset1")

	_, ok := cow.Get("dataset1")
	if ok {
		t.Error("expected dataset1 to be deleted")
	}

	_, ok = cow.Get("dataset2")
	if !ok {
		t.Error("expected dataset2 to still exist")
	}
}

// TestCOWMetadataMap_Snapshot tests snapshot is isolated
func TestCOWMetadataMap_Snapshot(t *testing.T) {
	cow := NewCOWMetadataMap()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	cow.Set("dataset1", DatasetMetadata{Name: "dataset1", Schema: schema, TotalRows: 100})
	cow.Set("dataset2", DatasetMetadata{Name: "dataset2", Schema: schema, TotalRows: 200})

	// Take snapshot
	snap := cow.Snapshot()

	if len(snap) != 2 {
		t.Errorf("expected 2 entries in snapshot, got %d", len(snap))
	}

	// Modify after snapshot
	cow.Set("dataset3", DatasetMetadata{Name: "dataset3", Schema: schema})
	cow.Delete("dataset1")

	// Original snapshot should be unchanged
	if len(snap) != 2 {
		t.Errorf("snapshot should be isolated, expected 2, got %d", len(snap))
	}

	// New snapshot should reflect changes
	snap2 := cow.Snapshot()
	if len(snap2) != 2 {
		t.Errorf("new snapshot expected 2 (dataset2, dataset3), got %d", len(snap2))
	}
}

// TestCOWMetadataMap_SnapshotNoLockContention tests concurrent read access
func TestCOWMetadataMap_SnapshotNoLockContention(t *testing.T) {
	cow := NewCOWMetadataMap()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Add some datasets
	for i := 0; i < 100; i++ {
		name := "dataset" + string(rune('a'+i%26))
		cow.Set(name, DatasetMetadata{Name: name, Schema: schema, TotalRows: int64(i * 100)})
	}

	// Concurrent readers and writers
	var wg sync.WaitGroup
	var readCount atomic.Int64
	var writeCount atomic.Int64

	// Start readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				snap := cow.Snapshot()
				_ = len(snap) // Use snapshot
				readCount.Add(1)
			}
		}()
	}

	// Start writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				name := "concurrent" + string(rune('a'+(id+j)%26))
				cow.Set(name, DatasetMetadata{Name: name, Schema: schema, TotalRows: int64(j)})
				writeCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	// Verify high read throughput
	if readCount.Load() != 10000 {
		t.Errorf("expected 10000 reads, got %d", readCount.Load())
	}
	if writeCount.Load() != 500 {
		t.Errorf("expected 500 writes, got %d", writeCount.Load())
	}
}

// TestCOWMetadataMap_UpdateMetadataFromRecords tests helper function
func TestCOWMetadataMap_UpdateMetadataFromRecords(t *testing.T) {
	cow := NewCOWMetadataMap()
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Create mock records with 100 rows each
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()

	for i := 0; i < 100; i++ {
		rb.Field(0).(*array.Int64Builder).Append(int64(i))
	}
	rec := rb.NewRecordBatch()
	defer rec.Release()

	batches := []arrow.RecordBatch{rec}

	// Update metadata from records
	cow.UpdateFromRecords("test", batches)

	meta, ok := cow.Get("test")
	if !ok {
		t.Fatal("expected metadata to be set")
	}
	if meta.TotalRows != 100 {
		t.Errorf("expected 100 rows, got %d", meta.TotalRows)
	}
	if meta.BatchCount != 1 {
		t.Errorf("expected 1 batch, got %d", meta.BatchCount)
	}
	if meta.Schema == nil {
		t.Error("expected schema to be set")
	}
}

// TestCOWMetadataMap_IncrementRows tests atomic row increment
func TestCOWMetadataMap_IncrementRows(t *testing.T) {
	cow := NewCOWMetadataMap()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	cow.Set("dataset1", DatasetMetadata{Name: "dataset1", Schema: schema, TotalRows: 0, BatchCount: 0})

	// Increment rows
	cow.IncrementStats("dataset1", 50, 1)

	meta, _ := cow.Get("dataset1")
	if meta.TotalRows != 50 {
		t.Errorf("expected 50 rows, got %d", meta.TotalRows)
	}
	if meta.BatchCount != 1 {
		t.Errorf("expected 1 batch, got %d", meta.BatchCount)
	}

	// Increment again
	cow.IncrementStats("dataset1", 30, 1)

	meta, _ = cow.Get("dataset1")
	if meta.TotalRows != 80 {
		t.Errorf("expected 80 rows, got %d", meta.TotalRows)
	}
}

// BenchmarkCOWMetadataMap_Snapshot benchmarks snapshot performance
func BenchmarkCOWMetadataMap_Snapshot(b *testing.B) {
	cow := NewCOWMetadataMap()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Add 1000 datasets
	for i := 0; i < 1000; i++ {
		name := "dataset" + string(rune(i))
		cow.Set(name, DatasetMetadata{Name: name, Schema: schema, TotalRows: int64(i * 100)})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		snap := cow.Snapshot()
		_ = len(snap)
	}
}

// BenchmarkCOWMetadataMap_ConcurrentAccess benchmarks concurrent read/write
func BenchmarkCOWMetadataMap_ConcurrentAccess(b *testing.B) {
	cow := NewCOWMetadataMap()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	for i := 0; i < 100; i++ {
		name := "dataset" + string(rune(i))
		cow.Set(name, DatasetMetadata{Name: name, Schema: schema})
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				// Write
				cow.Set("bench", DatasetMetadata{Name: "bench", Schema: schema, TotalRows: int64(i)})
			} else {
				// Read
				_ = cow.Snapshot()
			}
			i++
		}
	})
}
