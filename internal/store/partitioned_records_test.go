package store


import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// Subtask 2: PartitionedRecords Tests (TDD Red Phase)
// =============================================================================

// --- Config Tests ---

func TestPartitionedRecordsConfigDefaults(t *testing.T) {
	cfg := DefaultPartitionedRecordsConfig()

	if cfg.NumPartitions <= 0 {
		t.Error("NumPartitions should be positive")
	}
	if cfg.NumPartitions != runtime.NumCPU() {
		t.Errorf("NumPartitions should default to NumCPU(), got %d, want %d", cfg.NumPartitions, runtime.NumCPU())
	}
}

func TestPartitionedRecordsConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     PartitionedRecordsConfig
		wantErr bool
	}{
		{"valid default", DefaultPartitionedRecordsConfig(), false},
		{"valid custom partitions", PartitionedRecordsConfig{NumPartitions: 8}, false},
		{"valid single partition", PartitionedRecordsConfig{NumPartitions: 1}, false},
		{"invalid zero partitions", PartitionedRecordsConfig{NumPartitions: 0}, true},
		{"invalid negative partitions", PartitionedRecordsConfig{NumPartitions: -1}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// --- Creation Tests ---

func TestNewPartitionedRecords(t *testing.T) {
	cfg := PartitionedRecordsConfig{NumPartitions: 4}
	pr := NewPartitionedRecords(cfg)

	if pr == nil {
		t.Fatal("NewPartitionedRecords returned nil")
	}
	if pr.NumPartitions() != 4 {
		t.Errorf("NumPartitions() = %d, want 4", pr.NumPartitions())
	}
	if pr.TotalBatches() != 0 {
		t.Errorf("TotalBatches() = %d, want 0", pr.TotalBatches())
	}
}

func TestNewPartitionedRecordsDefault(t *testing.T) {
	pr := NewPartitionedRecordsDefault()

	if pr == nil {
		t.Fatal("NewPartitionedRecordsDefault returned nil")
	}
	if pr.NumPartitions() != runtime.NumCPU() {
		t.Errorf("NumPartitions() = %d, want %d", pr.NumPartitions(), runtime.NumCPU())
	}
}

// --- Helper to create test RecordBatch ---

func createTestRecordBatch(t *testing.T, id int64) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).Append(id)

	rec := bldr.NewRecordBatch()
	return rec
}

// --- Append Tests ---

func TestPartitionedRecordsAppend(t *testing.T) {
	pr := NewPartitionedRecords(PartitionedRecordsConfig{NumPartitions: 4})

	batch := createTestRecordBatch(t, 1)
	defer batch.Release()

	// Append to partition 0
	pr.Append(batch, 0)

	if pr.TotalBatches() != 1 {
		t.Errorf("TotalBatches() = %d, want 1", pr.TotalBatches())
	}
	if pr.PartitionBatches(0) != 1 {
		t.Errorf("PartitionBatches(0) = %d, want 1", pr.PartitionBatches(0))
	}
}

func TestPartitionedRecordsAppendMultiplePartitions(t *testing.T) {
	pr := NewPartitionedRecords(PartitionedRecordsConfig{NumPartitions: 4})

	// Append to different partitions
	for i := 0; i < 10; i++ {
		batch := createTestRecordBatch(t, int64(i))
		pr.Append(batch, uint64(i)) // Will be routed to partition i % 4
		batch.Release()
	}

	if pr.TotalBatches() != 10 {
		t.Errorf("TotalBatches() = %d, want 10", pr.TotalBatches())
	}
}

func TestPartitionedRecordsAppendWithKey(t *testing.T) {
	pr := NewPartitionedRecords(PartitionedRecordsConfig{NumPartitions: 4})

	batch := createTestRecordBatch(t, 100)
	defer batch.Release()

	// AppendWithKey uses hash-based routing
	partition := pr.AppendWithKey(batch, 12345)

	if partition < 0 || partition >= 4 {
		t.Errorf("AppendWithKey returned invalid partition: %d", partition)
	}
	if pr.TotalBatches() != 1 {
		t.Errorf("TotalBatches() = %d, want 1", pr.TotalBatches())
	}
}

// --- GetAll Tests ---

func TestPartitionedRecordsGetAll(t *testing.T) {
	pr := NewPartitionedRecords(PartitionedRecordsConfig{NumPartitions: 4})

	// Append batches to various partitions
	for i := 0; i < 8; i++ {
		batch := createTestRecordBatch(t, int64(i))
		pr.Append(batch, uint64(i))
		batch.Release()
	}

	// GetAll should return all batches
	all := pr.GetAll()

	if len(all) != 8 {
		t.Errorf("GetAll() returned %d batches, want 8", len(all))
	}
}

func TestPartitionedRecordsGetPartition(t *testing.T) {
	pr := NewPartitionedRecords(PartitionedRecordsConfig{NumPartitions: 4})

	// Append 3 batches to partition 1
	for i := 0; i < 3; i++ {
		batch := createTestRecordBatch(t, int64(i))
		pr.AppendToPartition(batch, 1)
		batch.Release()
	}

	// Get only partition 1
	batches := pr.GetPartition(1)

	if len(batches) != 3 {
		t.Errorf("GetPartition(1) returned %d batches, want 3", len(batches))
	}
}

// --- Concurrent Access Tests ---

func TestPartitionedRecordsConcurrentAppends(t *testing.T) {
	pr := NewPartitionedRecords(PartitionedRecordsConfig{NumPartitions: 8})

	var wg sync.WaitGroup
	appendCount := int64(0)

	// Concurrent appends from multiple goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				batch := createTestRecordBatch(t, int64(goroutineID*100+j))
				pr.Append(batch, uint64(goroutineID))
				batch.Release()
				atomic.AddInt64(&appendCount, 1)
			}
		}(i)
	}

	wg.Wait()

	if pr.TotalBatches() != int(appendCount) {
		t.Errorf("TotalBatches() = %d, want %d", pr.TotalBatches(), appendCount)
	}
}

func TestPartitionedRecordsConcurrentReadsWrites(t *testing.T) {
	pr := NewPartitionedRecords(PartitionedRecordsConfig{NumPartitions: 4})

	// Pre-populate with some data
	for i := 0; i < 20; i++ {
		batch := createTestRecordBatch(t, int64(i))
		pr.Append(batch, uint64(i))
		batch.Release()
	}

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				batch := createTestRecordBatch(t, int64(id*100+j))
				pr.Append(batch, uint64(id))
				batch.Release()
			}
		}(i)
	}

	// Readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = pr.GetAll()
			}
		}()
	}

	wg.Wait()
	// If we get here without panic/deadlock, test passes
}

// --- Iteration Tests ---

func TestPartitionedRecordsForEach(t *testing.T) {
	pr := NewPartitionedRecords(PartitionedRecordsConfig{NumPartitions: 4})

	// Add some batches
	for i := 0; i < 6; i++ {
		batch := createTestRecordBatch(t, int64(i))
		pr.Append(batch, uint64(i))
		batch.Release()
	}

	count := 0
	pr.ForEach(func(batch arrow.RecordBatch, partition int) bool {
		count++
		return true // continue
	})

	if count != 6 {
		t.Errorf("ForEach visited %d batches, want 6", count)
	}
}

func TestPartitionedRecordsForEachEarlyExit(t *testing.T) {
	pr := NewPartitionedRecords(PartitionedRecordsConfig{NumPartitions: 4})

	// Add batches
	for i := 0; i < 10; i++ {
		batch := createTestRecordBatch(t, int64(i))
		pr.Append(batch, uint64(i))
		batch.Release()
	}

	count := 0
	pr.ForEach(func(batch arrow.RecordBatch, partition int) bool {
		count++
		return count < 3 // stop after 3
	})

	if count != 3 {
		t.Errorf("ForEach with early exit visited %d batches, want 3", count)
	}
}

// --- Statistics Tests ---

func TestPartitionedRecordsStats(t *testing.T) {
	pr := NewPartitionedRecords(PartitionedRecordsConfig{NumPartitions: 4})

	// Operations
	for i := 0; i < 8; i++ {
		batch := createTestRecordBatch(t, int64(i))
		pr.Append(batch, uint64(i))
		batch.Release()
	}
	_ = pr.GetAll()

	stats := pr.Stats()

	if stats.TotalBatches != 8 {
		t.Errorf("stats.TotalBatches = %d, want 8", stats.TotalBatches)
	}
	if stats.NumPartitions != 4 {
		t.Errorf("stats.NumPartitions = %d, want 4", stats.NumPartitions)
	}
}

// --- Benchmark Tests ---

func BenchmarkPartitionedRecordsAppend(b *testing.B) {
	pr := NewPartitionedRecords(PartitionedRecordsConfig{NumPartitions: runtime.NumCPU()})
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := uint64(0)
		for pb.Next() {
			bldr := array.NewRecordBuilder(mem, schema)
			bldr.Field(0).(*array.Int64Builder).Append(int64(key))
			rec := bldr.NewRecordBatch()
			pr.Append(rec, key)
			rec.Release()
			bldr.Release()
			key++
		}
	})
}

func BenchmarkPartitionedVsSlice(b *testing.B) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b.Run("SingleSlice", func(b *testing.B) {
		var records []arrow.RecordBatch
		var mu sync.Mutex

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				bldr := array.NewRecordBuilder(mem, schema)
				bldr.Field(0).(*array.Int64Builder).Append(1)
				rec := bldr.NewRecordBatch()
				mu.Lock()
				records = append(records, rec)
				mu.Unlock()
				bldr.Release()
			}
		})
	})

	b.Run("PartitionedRecords", func(b *testing.B) {
		pr := NewPartitionedRecordsDefault()

		b.RunParallel(func(pb *testing.PB) {
			key := uint64(0)
			for pb.Next() {
				bldr := array.NewRecordBuilder(mem, schema)
				bldr.Field(0).(*array.Int64Builder).Append(1)
				rec := bldr.NewRecordBatch()
				pr.Append(rec, key)
				bldr.Release()
				key++
			}
		})
	})
}
