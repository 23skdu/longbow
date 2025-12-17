package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestShardedInvertedIndex_BasicAddSearch verifies basic functionality
func TestShardedInvertedIndex_BasicAddSearch(t *testing.T) {
	idx := NewShardedInvertedIndex()

	idx.Add(VectorID(0), "error code 500 internal server error")
	idx.Add(VectorID(1), "warning timeout exceeded")
	idx.Add(VectorID(2), "error code 404 not found")
	idx.Add(VectorID(3), "success operation completed")
	idx.Add(VectorID(4), "error fatal crash dump")

	results := idx.Search("error", 10)
	if len(results) != 3 {
		t.Errorf("expected 3 results for 'error', got %d", len(results))
	}

	found := make(map[VectorID]bool)
	for _, r := range results {
		found[r.ID] = true
	}
	for _, id := range []VectorID{0, 2, 4} {
		if !found[id] {
			t.Errorf("expected ID %d in results", id)
		}
	}
}

// TestShardedInvertedIndex_Delete verifies delete functionality
func TestShardedInvertedIndex_Delete(t *testing.T) {
	idx := NewShardedInvertedIndex()

	idx.Add(VectorID(0), "error code 500")
	idx.Add(VectorID(1), "error code 404")

	results := idx.Search("error", 10)
	if len(results) != 2 {
		t.Errorf("expected 2 results before delete, got %d", len(results))
	}

	idx.Delete(VectorID(0))

	results = idx.Search("error", 10)
	if len(results) != 1 {
		t.Errorf("expected 1 result after delete, got %d", len(results))
	}
	if results[0].ID != VectorID(1) {
		t.Errorf("expected ID 1 to remain, got %d", results[0].ID)
	}
}

// TestShardedInvertedIndex_ConcurrentSearch verifies concurrent searches
func TestShardedInvertedIndex_ConcurrentSearch(t *testing.T) {
	idx := NewShardedInvertedIndex()

	terms := []string{"error", "warning", "info", "debug", "trace"}
	for i := 0; i < 1000; i++ {
		text := fmt.Sprintf("%s message number %d", terms[i%len(terms)], i)
		idx.Add(VectorID(i), text)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(term string) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				results := idx.Search(term, 10)
				if len(results) == 0 {
					errors <- fmt.Errorf("no results for %s", term)
					return
				}
			}
		}(terms[i%len(terms)])
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestShardedInvertedIndex_ConcurrentAdd verifies concurrent adds
func TestShardedInvertedIndex_ConcurrentAdd(t *testing.T) {
	idx := NewShardedInvertedIndex()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				id := VectorID(base*50 + j)
				text := fmt.Sprintf("document %d with unique_%d", id, id)
				idx.Add(id, text)
			}
		}(i)
	}

	wg.Wait()

	results := idx.Search("document", 1000)
	if len(results) != 1000 {
		t.Errorf("expected 1000 documents, got %d", len(results))
	}
}

// TestShardedInvertedIndex_ConcurrentMixed tests mixed operations
func TestShardedInvertedIndex_ConcurrentMixed(t *testing.T) {
	idx := NewShardedInvertedIndex()

	for i := 0; i < 500; i++ {
		idx.Add(VectorID(i), fmt.Sprintf("initial doc %d", i))
	}

	var wg sync.WaitGroup
	var addCount, deleteCount, searchCount atomic.Int64

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				idx.Add(VectorID(500+id*50+j), fmt.Sprintf("new doc %d", id*50+j))
				addCount.Add(1)
			}
		}(i)
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				idx.Delete(VectorID(id*50 + j))
				deleteCount.Add(1)
			}
		}(i)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				idx.Search("doc", 10)
				searchCount.Add(1)
			}
		}()
	}

	wg.Wait()

	if addCount.Load() != 500 {
		t.Errorf("expected 500 adds, got %d", addCount.Load())
	}
	if deleteCount.Load() != 250 {
		t.Errorf("expected 250 deletes, got %d", deleteCount.Load())
	}
	if searchCount.Load() != 500 {
		t.Errorf("expected 500 searches, got %d", searchCount.Load())
	}
}

// BenchmarkShardedInvertedIndex_Search benchmarks sharded search performance
func BenchmarkShardedInvertedIndex_Search(b *testing.B) {
	idx := NewShardedInvertedIndex()

	terms := []string{"error", "warning", "info", "debug", "trace"}
	for i := 0; i < 10000; i++ {
		text := fmt.Sprintf("%s message number %d with some extra text", terms[i%len(terms)], i)
		idx.Add(VectorID(i), text)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			idx.Search(terms[i%len(terms)], 10)
			i++
		}
	})
}

// BenchmarkShardedInvertedIndex_Add benchmarks sharded add performance
func BenchmarkShardedInvertedIndex_Add(b *testing.B) {
	idx := NewShardedInvertedIndex()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			text := fmt.Sprintf("document %d with term_%d", i, i)
			idx.Add(VectorID(i), text)
			i++
		}
	})
}

// BenchmarkOriginalInvertedIndex_Search benchmarks original for comparison
func BenchmarkOriginalInvertedIndex_Search(b *testing.B) {
	idx := NewInvertedIndex()

	terms := []string{"error", "warning", "info", "debug", "trace"}
	for i := 0; i < 10000; i++ {
		text := fmt.Sprintf("%s message number %d with some extra text", terms[i%len(terms)], i)
		idx.Add(VectorID(i), text)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			idx.Search(terms[i%len(terms)], 10)
			i++
		}
	})
}

// BenchmarkOriginalInvertedIndex_Add benchmarks original add for comparison
func BenchmarkOriginalInvertedIndex_Add(b *testing.B) {
	idx := NewInvertedIndex()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			text := fmt.Sprintf("document %d with term_%d", i, i)
			idx.Add(VectorID(i), text)
			i++
		}
	})
}
