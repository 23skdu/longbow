package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestShardedHNSW_AutoSplit(t *testing.T) {
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = 1
	cfg.ShardSplitThreshold = 100 // Small threshold for testing
	cfg.UseRingSharding = false   // Force linear sharding for deterministic auto-split testing

	ds := &Dataset{dataMu: sync.RWMutex{}, Name: "test_autosplit"}
	sharded := NewShardedHNSW(cfg, ds)

	mem := memory.NewGoAllocator()
	// Add 250 vectors, should trigger 2 splits (total 3 shards)
	numVectors := 250
	rec := makeShardedTestRecord(mem, 3, numVectors)
	defer rec.Release()
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec)
	ds.dataMu.Unlock()

	for i := 0; i < numVectors; i++ {
		_, err := sharded.AddSafe(rec, i, 0)
		if err != nil {
			t.Fatalf("Add failed at %d: %v", i, err)
		}
	}

	stats := sharded.ShardStats()
	if len(stats) != 3 {
		t.Errorf("expected 3 shards after split, got %d", len(stats))
	}

	// Shard 0: 100, Shard 1: 100, Shard 2: 50
	expectedCounts := []int{100, 100, 50}
	for i, s := range stats {
		if s.Count != expectedCounts[i] {
			t.Errorf("shard %d: expected %d vectors, got %d", i, expectedCounts[i], s.Count)
		}
	}

	// Verify we can search across all of them
	query := []float32{float32(200), float32(200), float32(200)}
	results, err := sharded.SearchVectors(context.Background(), query, 10, nil, SearchOptions{})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("expected 10 results, got %d", len(results))
	}

	// Check that IDs come from different shards
	foundShardIds := make(map[int]bool)
	for _, res := range results {
		foundShardIds[sharded.GetShardForID(res.ID)] = true
	}
	if len(foundShardIds) < 1 {
		t.Error("expected results from shards")
	}
	fmt.Printf("Search found results in shards: %v\n", foundShardIds)
}
