package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/coder/hnsw"
)

// TestHNSWIndex_Warmup tests the warmup method on HNSWIndex
func TestHNSWIndex_Warmup(t *testing.T) {
	t.Run("warmup on empty index does not panic", func(t *testing.T) {
		ds := &Dataset{Name: "test_empty"}
		idx := NewHNSWIndex(ds)

		// Should not panic
		touched := idx.Warmup()

		if touched != 0 {
			t.Errorf("expected 0 nodes touched, got %d", touched)
		}
	})

	t.Run("warmup touches all nodes", func(t *testing.T) {
		ds := &Dataset{Name: "test_warmup"}
		idx := NewHNSWIndex(ds)

		// Add test vectors directly to graph for testing
		vectors := [][]float32{
			{1.0, 2.0, 3.0, 4.0},
			{5.0, 6.0, 7.0, 8.0},
			{9.0, 10.0, 11.0, 12.0},
		}

		for i, vec := range vectors {
			idx.Graph.Add(hnsw.MakeNode(VectorID(i), vec))
		}

		// Warmup should touch all nodes
		touched := idx.Warmup()

		if touched != len(vectors) {
			t.Errorf("expected %d nodes touched, got %d", len(vectors), touched)
		}
	})

	t.Run("warmup is safe for concurrent access", func(t *testing.T) {
		ds := &Dataset{Name: "test_concurrent"}
		idx := NewHNSWIndex(ds)

		// Add vectors
		for i := 0; i < 100; i++ {
			vec := []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)}
			idx.Graph.Add(hnsw.MakeNode(VectorID(i), vec))
		}

		// Run warmup concurrently
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				idx.Warmup()
			}()
		}
		wg.Wait()
		// No panic = success
	})
}

// TestVectorStore_Warmup tests warmup on VectorStore level
func TestVectorStore_Warmup(t *testing.T) {
	t.Run("warmup on empty store", func(t *testing.T) {
		logger := zap.NewNop()
		store := NewVectorStore(memory.DefaultAllocator, logger, 1<<30, 0, time.Hour)
		defer func() { _ = store.Close() }()

		// Should not panic
		warmupStats := store.Warmup()

		if warmupStats.DatasetsWarmed != 0 {
			t.Errorf("expected 0 datasets, got %d", warmupStats.DatasetsWarmed)
		}
	})

	t.Run("warmup warms all datasets with indexes", func(t *testing.T) {
		logger := zap.NewNop()
		store := NewVectorStore(memory.DefaultAllocator, logger, 1<<30, 0, time.Hour)
		defer func() { _ = store.Close() }()

		// Create test datasets with indexes
		for i := 0; i < 3; i++ {
			ds := &Dataset{Name: fmt.Sprintf("test_ds_%d", i)}
			ds.Index = NewHNSWIndex(ds)
			for j := 0; j < 10; j++ {
				vec := []float32{float32(j), float32(j + 1), float32(j + 2), float32(j + 3)}
				ds.Index.(*HNSWIndex).Graph.Add(hnsw.MakeNode(VectorID(j), vec))
			}
			store.mu.Lock()
			store.datasets[ds.Name] = ds
			store.mu.Unlock()
		}

		warmupStats := store.Warmup()

		if warmupStats.DatasetsWarmed != 3 {
			t.Errorf("expected 3 datasets warmed, got %d", warmupStats.DatasetsWarmed)
		}
		if warmupStats.TotalNodesWarmed != 30 {
			t.Errorf("expected 30 total nodes warmed, got %d", warmupStats.TotalNodesWarmed)
		}
	})

	t.Run("warmup skips datasets without indexes", func(t *testing.T) {
		logger := zap.NewNop()
		store := NewVectorStore(memory.DefaultAllocator, logger, 1<<30, 0, time.Hour)
		defer func() { _ = store.Close() }()

		// Dataset with index
		ds1 := &Dataset{Name: "with_index"}
		ds1.Index = NewHNSWIndex(ds1)
		ds1.Index.(*HNSWIndex).Graph.Add(hnsw.MakeNode(VectorID(0), []float32{1, 2, 3, 4}))
		store.mu.Lock()
		store.datasets[ds1.Name] = ds1
		store.mu.Unlock()

		// Dataset without index
		ds2 := &Dataset{Name: "without_index"}
		store.mu.Lock()
		store.datasets[ds2.Name] = ds2
		store.mu.Unlock()

		warmupStats := store.Warmup()

		if warmupStats.DatasetsWarmed != 1 {
			t.Errorf("expected 1 dataset warmed, got %d", warmupStats.DatasetsWarmed)
		}
		if warmupStats.DatasetsSkipped != 1 {
			t.Errorf("expected 1 dataset skipped, got %d", warmupStats.DatasetsSkipped)
		}
	})

	t.Run("warmup duration is tracked", func(t *testing.T) {
		logger := zap.NewNop()
		store := NewVectorStore(memory.DefaultAllocator, logger, 1<<30, 0, time.Hour)
		defer func() { _ = store.Close() }()

		ds := &Dataset{Name: "test_duration"}
		ds.Index = NewHNSWIndex(ds)
		for i := 0; i < 100; i++ {
			vec := []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)}
			ds.Index.(*HNSWIndex).Graph.Add(hnsw.MakeNode(VectorID(i), vec))
		}
		store.mu.Lock()
		store.datasets[ds.Name] = ds
		store.mu.Unlock()

		warmupStats := store.Warmup()

		if warmupStats.Duration <= 0 {
			t.Error("expected positive duration")
		}
	})
}

// TestWarmupStats tests the WarmupStats struct
func TestWarmupStats(t *testing.T) {
	t.Run("stats string representation", func(t *testing.T) {
		stats := WarmupStats{
			DatasetsWarmed:   5,
			DatasetsSkipped:  2,
			TotalNodesWarmed: 1000,
			Duration:         50 * time.Millisecond,
		}

		str := stats.String()
		if str == "" {
			t.Error("expected non-empty string representation")
		}
	})
}

// TestShardedHNSW_Warmup tests warmup on ShardedHNSW
func TestShardedHNSW_Warmup(t *testing.T) {
	t.Run("warmup sharded index", func(t *testing.T) {
		cfg := DefaultShardedHNSWConfig()
		cfg.NumShards = 4
		sharded := NewShardedHNSW(cfg, nil)

		// Add vectors to different shards
		for i := 0; i < 100; i++ {
			vec := []float32{float32(i), float32(i), float32(i)}
			// Manually add to shard to bypass dataset dependency
			shardIdx := sharded.GetShardForID(VectorID(i))
			sharded.shards[shardIdx].mu.Lock()
			sharded.shards[shardIdx].graph.Add(hnsw.MakeNode(VectorID(i), vec))
			sharded.shards[shardIdx].mu.Unlock()
		}

		touched := sharded.Warmup()
		if touched != 100 {
			t.Errorf("expected 100 touched, got %d", touched)
		}
	})
}
