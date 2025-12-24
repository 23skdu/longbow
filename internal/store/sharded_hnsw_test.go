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

func makeShardedTestRecord(mem memory.Allocator, dims int, numVectors int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	idBuilder := array.NewInt64Builder(mem)
	listBuilder := array.NewFixedSizeListBuilder(mem, int32(dims), arrow.PrimitiveTypes.Float32)
	vecBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(int64(i))
		listBuilder.Append(true)
		for j := 0; j < dims; j++ {
			vecBuilder.Append(float32(i + j))
		}
	}

	return array.NewRecordBatch(schema, []arrow.Array{idBuilder.NewArray(), listBuilder.NewArray()}, int64(numVectors))
}

// TestShardedHNSWConfig_Defaults verifies default configuration
func TestShardedHNSWConfig_Defaults(t *testing.T) {
	cfg := DefaultShardedHNSWConfig()

	if cfg.NumShards <= 0 {
		t.Errorf("expected positive NumShards, got %d", cfg.NumShards)
	}
	if cfg.NumShards != runtime.NumCPU() {
		t.Errorf("expected NumShards=%d (NumCPU), got %d", runtime.NumCPU(), cfg.NumShards)
	}
	if cfg.M <= 0 {
		t.Errorf("expected positive M, got %d", cfg.M)
	}
	if cfg.EfConstruction <= 0 {
		t.Errorf("expected positive EfConstruction, got %d", cfg.EfConstruction)
	}
}

// TestShardedHNSWConfig_Validation verifies configuration validation
func TestShardedHNSWConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ShardedHNSWConfig
		wantErr bool
	}{
		{"valid default", DefaultShardedHNSWConfig(), false},
		{"zero shards", ShardedHNSWConfig{NumShards: 0, M: 16, EfConstruction: 200}, true},
		{"negative shards", ShardedHNSWConfig{NumShards: -1, M: 16, EfConstruction: 200}, true},
		{"zero M", ShardedHNSWConfig{NumShards: 4, M: 0, EfConstruction: 200}, true},
		{"zero EfConstruction", ShardedHNSWConfig{NumShards: 4, M: 16, EfConstruction: 0}, true},
		{"single shard", ShardedHNSWConfig{NumShards: 1, M: 16, EfConstruction: 200}, false},
		{"many shards", ShardedHNSWConfig{NumShards: 64, M: 16, EfConstruction: 200}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// TestShardedHNSW_ShardRouting verifies consistent hash-based routing
func TestShardedHNSW_ShardRouting(t *testing.T) {
	cfg := DefaultShardedHNSWConfig()
	cfg.ShardSplitThreshold = 100 // Set threshold to force splits
	cfg.NumShards = 1             // Initial shards

	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)

	// Range-based routing: shard = id / threshold
	for id := VectorID(0); id < 400; id++ {
		expectedShard := int(id) / cfg.ShardSplitThreshold
		shard := sharded.GetShardForID(id)
		if shard != expectedShard {
			t.Errorf("ID %d: expected shard %d, got %d", id, expectedShard, shard)
		}
	}

	// Verify distribution
	counts := make(map[int]int)
	for id := VectorID(0); id < 400; id++ {
		counts[sharded.GetShardForID(id)]++
	}

	if len(counts) != 4 {
		t.Errorf("expected 4 shards populated, got %d", len(counts))
	}
	for i := 0; i < 4; i++ {
		if counts[i] != 100 {
			t.Errorf("shard %d: expected 100 vectors, got %d", i, counts[i])
		}
	}
}

// TestShardedHNSW_AddToShard verifies adding vectors routes to correct shard
func TestShardedHNSW_AddToShard(t *testing.T) {
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = 4

	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)

	mem := memory.NewGoAllocator()
	rec := makeShardedTestRecord(mem, 3, 100)
	defer rec.Release()
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	ds.dataMu.Unlock()

	// Add vectors
	for i := 0; i < 100; i++ {
		id, err := sharded.AddSafe(rec, i, 0)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
		if id != VectorID(i) {
			t.Errorf("expected ID %d, got %d", i, id)
		}
	}

	// Verify total count
	if sharded.Len() != 100 {
		t.Errorf("expected 100 vectors, got %d", sharded.Len())
	}

	// Verify vectors distributed across shards
	stats := sharded.ShardStats()
	totalInShards := 0
	for _, s := range stats {
		totalInShards += s.Count
		if s.Count == 0 {
			t.Logf("Warning: shard %d is empty", s.ShardID)
		}
	}
	if totalInShards != 100 {
		t.Errorf("expected 100 vectors in shards, got %d", totalInShards)
	}
}

// TestShardedHNSW_ParallelAdds verifies concurrent additions don't corrupt state
func TestShardedHNSW_ParallelAdds(t *testing.T) {
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = 8

	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)

	numGoroutines := 16
	vectorsPerGoroutine := 100

	var wg sync.WaitGroup
	var errCount atomic.Int32

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			mem := memory.NewGoAllocator()
			rec := makeShardedTestRecord(mem, 3, vectorsPerGoroutine)
			defer rec.Release()

			for i := 0; i < vectorsPerGoroutine; i++ {
				_, err := sharded.AddSafe(rec, i, 0)
				if err != nil {
					errCount.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()

	if errCount.Load() > 0 {
		t.Errorf("%d errors during parallel adds", errCount.Load())
	}

	expected := numGoroutines * vectorsPerGoroutine
	if sharded.Len() != expected {
		t.Errorf("expected %d vectors, got %d", expected, sharded.Len())
	}
}

// TestShardedHNSW_Search verifies search across all shards
func TestShardedHNSW_Search(t *testing.T) {
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = 4
	cfg.M = 8
	cfg.EfConstruction = 50

	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)

	mem := memory.NewGoAllocator()
	rec := makeShardedTestRecord(mem, 3, 100)
	defer rec.Release()
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	ds.dataMu.Unlock()

	// Add 100 vectors
	for i := 0; i < 100; i++ {
		_, err := sharded.AddSafe(rec, i, 0)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	// Search
	query := []float32{50.0, 50.0, 50.0}
	results := sharded.SearchVectors(query, 10, nil)

	if len(results) == 0 {
		t.Fatal("expected search results, got none")
	}
}

// TestShardedHNSW_SearchEmpty verifies search on empty index
func TestShardedHNSW_SearchEmpty(t *testing.T) {
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = 4

	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)

	query := []float32{1.0, 2.0, 3.0}
	results := sharded.SearchVectors(query, 10, nil)

	if len(results) != 0 {
		t.Errorf("expected 0 results on empty index, got %d", len(results))
	}
}

// TestShardedHNSW_GetLocation verifies location retrieval
func TestShardedHNSW_GetLocation(t *testing.T) {
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = 4

	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)

	mem := memory.NewGoAllocator()
	rec := makeShardedTestRecord(mem, 3, 20)
	defer rec.Release()
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	ds.dataMu.Unlock()

	// Add vectors
	for i := 0; i < 20; i++ {
		_, err := sharded.AddSafe(rec, i, 0)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	// Verify retrieval
	loc, ok := sharded.GetLocation(VectorID(5))
	if !ok {
		t.Error("expected location for ID 5")
	}
	if loc.RowIdx != 5 {
		t.Errorf("expected row idx 5, got %d", loc.RowIdx)
	}

	// Non-existent ID
	_, ok = sharded.GetLocation(VectorID(999))
	if ok {
		t.Error("expected not found for ID 999")
	}
}

// TestShardedHNSW_ShardStats verifies per-shard statistics
func TestShardedHNSW_ShardStats(t *testing.T) {
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = 4

	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)

	mem := memory.NewGoAllocator()
	rec := makeShardedTestRecord(mem, 3, 100)
	defer rec.Release()
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	ds.dataMu.Unlock()

	// Add 100 vectors
	for i := 0; i < 100; i++ {
		_, _ = sharded.AddSafe(rec, i, 0)
	}

	stats := sharded.ShardStats()

	if len(stats) != 4 {
		t.Errorf("expected 4 shard stats, got %d", len(stats))
	}

	total := 0
	for _, s := range stats {
		total += s.Count
	}

	if total != 100 {
		t.Errorf("expected total 100 across shards, got %d", total)
	}
}

// TestShardedHNSW_ConcurrentAddAndSearch verifies thread safety
func TestShardedHNSW_ConcurrentAddAndSearch(t *testing.T) {
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = 4
	cfg.M = 8
	cfg.EfConstruction = 50

	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)

	mem := memory.NewGoAllocator()
	rec := makeShardedTestRecord(mem, 3, 200)
	defer rec.Release()

	// Pre-populate
	for i := 0; i < 50; i++ {
		_, _ = sharded.AddSafe(rec, i, 0)
	}

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Concurrent adders
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 50; i < 100; i++ {
				_, _ = sharded.AddSafe(rec, i, 0)
			}
		}(g)
	}

	// Concurrent searchers
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				query := []float32{float32(i), float32(i), float32(i)}
				_ = sharded.SearchVectors(query, 5, nil)
			}
		}()
	}

	wg.Wait()
	close(done)
}

func BenchmarkHNSW_SingleAdd(b *testing.B) {
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = 1
	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)
	mem := memory.NewGoAllocator()
	rec := makeShardedTestRecord(mem, 128, b.N)
	defer rec.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sharded.AddSafe(rec, i, 0)
	}
}

func BenchmarkHNSW_ShardedParallelAdd(b *testing.B) {
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = runtime.NumCPU()
	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)
	mem := memory.NewGoAllocator()
	rec := makeShardedTestRecord(mem, 128, b.N)
	defer rec.Release()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// In realistic scenario we'd use atomics for rowIdx
			_, _ = sharded.AddSafe(rec, 0, 0)
		}
	})
}

func BenchmarkHNSW_ShardedSearch(b *testing.B) {
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = runtime.NumCPU()
	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)
	mem := memory.NewGoAllocator()
	rec := makeShardedTestRecord(mem, 128, 1000)
	defer rec.Release()

	for i := 0; i < 1000; i++ {
		_, _ = sharded.AddSafe(rec, i, 0)
	}

	query := make([]float32, 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sharded.SearchVectors(query, 10, nil)
	}
}

func TestShardedHNSW_SearchByID(t *testing.T) {
	cfg := DefaultShardedHNSWConfig()
	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)
	mem := memory.NewGoAllocator()
	rec := makeShardedTestRecord(mem, 3, 10)
	defer rec.Release()
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	ds.dataMu.Unlock()

	id, _ := sharded.AddSafe(rec, 0, 0)
	results := sharded.SearchByID(id, 5)
	if len(results) == 0 {
		t.Fatal("expected results")
	}
}

func TestShardedHNSW_GetDimension(t *testing.T) {
	cfg := DefaultShardedHNSWConfig()
	ds := &Dataset{dataMu: sync.RWMutex{}}
	sharded := NewShardedHNSW(cfg, ds)
	if sharded.GetDimension() != 0 {
		t.Error("expected 0")
	}
}
