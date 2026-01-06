package store


import (
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// Subtask 3: ShardedDataset Tests (TDD Red Phase)
// =============================================================================

// --- Config Tests ---

func TestShardedDatasetConfigDefaults(t *testing.T) {
	cfg := DefaultShardedDatasetConfig()

	if cfg.NumShards <= 0 {
		t.Error("NumShards should be positive")
	}
	if cfg.NumShards != runtime.NumCPU() {
		t.Errorf("NumShards should default to NumCPU()")
	}
}

func TestShardedDatasetConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ShardedDatasetConfig
		wantErr bool
	}{
		{"valid default", DefaultShardedDatasetConfig(), false},
		{"valid 8 shards", ShardedDatasetConfig{NumShards: 8}, false},
		{"invalid zero", ShardedDatasetConfig{NumShards: 0}, true},
		{"invalid negative", ShardedDatasetConfig{NumShards: -1}, true},
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

func TestNewShardedDataset(t *testing.T) {
	cfg := ShardedDatasetConfig{NumShards: 4}
	sd := NewShardedDataset("test_dataset", cfg)

	if sd == nil {
		t.Fatal("NewShardedDataset returned nil")
	}
	if sd.Name() != "test_dataset" {
		t.Errorf("Name() = %s, want test_dataset", sd.Name())
	}
	if sd.NumShards() != 4 {
		t.Errorf("NumShards() = %d, want 4", sd.NumShards())
	}
	if sd.TotalRecords() != 0 {
		t.Errorf("TotalRecords() = %d, want 0", sd.TotalRecords())
	}
}

func TestNewShardedDatasetDefault(t *testing.T) {
	sd := NewShardedDatasetDefault("default_test")

	if sd == nil {
		t.Fatal("NewShardedDatasetDefault returned nil")
	}
	if sd.NumShards() != runtime.NumCPU() {
		t.Errorf("NumShards() = %d, want %d", sd.NumShards(), runtime.NumCPU())
	}
}

// --- Helper ---

func createTestBatch(t *testing.T, id int64) arrow.RecordBatch {
	t.Helper()
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).Append(id)
	return bldr.NewRecordBatch()
}

// --- Append Tests ---

func TestShardedDatasetAppend(t *testing.T) {
	sd := NewShardedDataset("append_test", ShardedDatasetConfig{NumShards: 4})

	batch := createTestBatch(t, 1)
	defer batch.Release()

	// Append with routing key
	sd.Append(batch, 12345)

	if sd.TotalRecords() != 1 {
		t.Errorf("TotalRecords() = %d, want 1", sd.TotalRecords())
	}
}

func TestShardedDatasetAppendToShard(t *testing.T) {
	sd := NewShardedDataset("shard_test", ShardedDatasetConfig{NumShards: 4})

	// Append 3 batches to shard 1
	for i := 0; i < 3; i++ {
		batch := createTestBatch(t, int64(i))
		sd.AppendToShard(batch, 1)
		batch.Release()
	}

	if sd.ShardRecordCount(1) != 3 {
		t.Errorf("ShardRecordCount(1) = %d, want 3", sd.ShardRecordCount(1))
	}
}

// --- GetAllRecords Tests ---

func TestShardedDatasetGetAllRecords(t *testing.T) {
	sd := NewShardedDataset("getall_test", ShardedDatasetConfig{NumShards: 4})

	// Add batches to multiple shards
	for i := 0; i < 8; i++ {
		batch := createTestBatch(t, int64(i))
		sd.Append(batch, uint64(i))
		batch.Release()
	}

	all := sd.GetAllRecords()
	if len(all) != 8 {
		t.Errorf("GetAllRecords() returned %d, want 8", len(all))
	}
}

func TestShardedDatasetGetShardRecords(t *testing.T) {
	sd := NewShardedDataset("getshard_test", ShardedDatasetConfig{NumShards: 4})

	// Append to specific shard
	for i := 0; i < 5; i++ {
		batch := createTestBatch(t, int64(i))
		sd.AppendToShard(batch, 2)
		batch.Release()
	}

	records := sd.GetShardRecords(2)
	if len(records) != 5 {
		t.Errorf("GetShardRecords(2) = %d, want 5", len(records))
	}
}

// --- Concurrent Access Tests ---

func TestShardedDatasetConcurrentAppends(t *testing.T) {
	sd := NewShardedDataset("concurrent_test", ShardedDatasetConfig{NumShards: 8})

	var wg sync.WaitGroup
	appendersCount := 10
	batchesPerAppender := 50

	for i := 0; i < appendersCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < batchesPerAppender; j++ {
				batch := createTestBatch(t, int64(id*100+j))
				sd.Append(batch, uint64(id))
				batch.Release()
			}
		}(i)
	}

	wg.Wait()

	expected := appendersCount * batchesPerAppender
	if sd.TotalRecords() != expected {
		t.Errorf("TotalRecords() = %d, want %d", sd.TotalRecords(), expected)
	}
}

func TestShardedDatasetConcurrentReadsWrites(t *testing.T) {
	sd := NewShardedDataset("rw_test", ShardedDatasetConfig{NumShards: 4})

	// Pre-populate
	for i := 0; i < 20; i++ {
		batch := createTestBatch(t, int64(i))
		sd.Append(batch, uint64(i))
		batch.Release()
	}

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				batch := createTestBatch(t, int64(id*100+j))
				sd.Append(batch, uint64(id))
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
				_ = sd.GetAllRecords()
			}
		}()
	}

	wg.Wait()
	// No panic/deadlock = pass
}

// --- ForEach Iteration Tests ---

func TestShardedDatasetForEach(t *testing.T) {
	sd := NewShardedDataset("foreach_test", ShardedDatasetConfig{NumShards: 4})

	for i := 0; i < 6; i++ {
		batch := createTestBatch(t, int64(i))
		sd.Append(batch, uint64(i))
		batch.Release()
	}

	count := 0
	sd.ForEach(func(batch arrow.RecordBatch, shard int) bool {
		count++
		return true
	})

	if count != 6 {
		t.Errorf("ForEach visited %d, want 6", count)
	}
}

// --- Compatibility with Dataset Tests ---

func TestShardedDatasetToLegacyRecords(t *testing.T) {
	sd := NewShardedDataset("legacy_test", ShardedDatasetConfig{NumShards: 4})

	for i := 0; i < 5; i++ {
		batch := createTestBatch(t, int64(i))
		sd.Append(batch, uint64(i))
		batch.Release()
	}

	// ToLegacyRecords returns []arrow.RecordBatch for backward compat
	legacy := sd.ToLegacyRecords()
	if len(legacy) != 5 {
		t.Errorf("ToLegacyRecords() = %d, want 5", len(legacy))
	}
}

// --- Stats Tests ---

func TestShardedDatasetStats(t *testing.T) {
	sd := NewShardedDataset("stats_test", ShardedDatasetConfig{NumShards: 4})

	for i := 0; i < 10; i++ {
		batch := createTestBatch(t, int64(i))
		sd.Append(batch, uint64(i))
		batch.Release()
	}

	stats := sd.Stats()

	if stats.TotalRecords != 10 {
		t.Errorf("stats.TotalRecords = %d, want 10", stats.TotalRecords)
	}
	if stats.NumShards != 4 {
		t.Errorf("stats.NumShards = %d, want 4", stats.NumShards)
	}
}

// --- Benchmark ---

func BenchmarkShardedDatasetAppend(b *testing.B) {
	sd := NewShardedDatasetDefault("bench_test")
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
			sd.Append(rec, key)
			rec.Release()
			bldr.Release()
			key++
		}
	})
}

func BenchmarkShardedVsLegacyDataset(b *testing.B) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b.Run("LegacyDataset", func(b *testing.B) {
		ds := &Dataset{Name: "legacy", Records: nil}

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				bldr := array.NewRecordBuilder(mem, schema)
				bldr.Field(0).(*array.Int64Builder).Append(1)
				rec := bldr.NewRecordBatch()
				ds.dataMu.Lock()
				ds.Records = append(ds.Records, rec)
				ds.dataMu.Unlock()
				bldr.Release()
			}
		})
	})

	b.Run("ShardedDataset", func(b *testing.B) {
		sd := NewShardedDatasetDefault("sharded")

		b.RunParallel(func(pb *testing.PB) {
			key := uint64(0)
			for pb.Next() {
				bldr := array.NewRecordBuilder(mem, schema)
				bldr.Field(0).(*array.Int64Builder).Append(1)
				rec := bldr.NewRecordBatch()
				sd.Append(rec, key)
				bldr.Release()
				key++
			}
		})
	})
}
