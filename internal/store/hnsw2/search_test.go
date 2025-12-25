package hnsw2

import (
	"testing"
	
	"github.com/23skdu/longbow/internal/store"
)

func TestSearch_EmptyIndex(t *testing.T) {
	dataset := &store.Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultConfig())
	
	query := []float32{1.0, 2.0, 3.0}
	results, err := index.Search(query, 10, 20, nil)
	
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	
	if len(results) != 0 {
		t.Errorf("expected 0 results from empty index, got %d", len(results))
	}
}

func TestSearch_InvalidK(t *testing.T) {
	dataset := &store.Dataset{Name: "test"}
	index := NewArrowHNSW(dataset, DefaultConfig())
	
	query := []float32{1.0, 2.0, 3.0}
	
	// k = 0 should return empty results
	results, err := index.Search(query, 0, 20, nil)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for k=0, got %d", len(results))
	}
	
	// k < 0 should return empty results
	results, err = index.Search(query, -1, 20, nil)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for k<0, got %d", len(results))
	}
}

func TestSearchContext_Pooling(t *testing.T) {
	pool := NewSearchContextPool()
	
	// Get context
	ctx1 := pool.Get()
	if ctx1 == nil {
		t.Fatal("pool.Get() returned nil")
	}
	
	// Use context
	ctx1.candidates.Push(Candidate{ID: 1, Dist: 1.0})
	ctx1.visited.Set(5)
	
	// Return to pool
	pool.Put(ctx1)
	
	// Get again - should be reused
	ctx2 := pool.Get()
	if ctx2 == nil {
		t.Fatal("pool.Get() returned nil on second call")
	}
	
	// Should be cleared
	if ctx2.candidates.Len() != 0 {
		t.Error("candidates not cleared after Put")
	}
	if ctx2.visited.IsSet(5) {
		t.Error("visited not cleared after Put")
	}
}

func BenchmarkSearch_SmallIndex(b *testing.B) {
	// TODO: Implement when Insert is ready
	b.Skip("Skipping until Insert is implemented")
}

func BenchmarkSearch_LargeIndex(b *testing.B) {
	// TODO: Implement when Insert is ready
	b.Skip("Skipping until Insert is implemented")
}
