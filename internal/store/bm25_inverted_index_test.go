package store

import (
	"sync"
	"testing"
)

// =============================================================================
// BM25InvertedIndex Tests
// =============================================================================

func TestNewBM25InvertedIndex(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())
	if idx == nil {
		t.Fatal("NewBM25InvertedIndex returned nil")
	}
	if idx.DocCount() != 0 {
		t.Errorf("expected 0 docs, got %d", idx.DocCount())
	}
}

func TestBM25InvertedIndexAdd(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	// Add first document
	idx.Add(VectorID(1), "the quick brown fox")
	if idx.DocCount() != 1 {
		t.Errorf("expected 1 doc, got %d", idx.DocCount())
	}

	// Check document length was tracked
	docLen := idx.GetDocLength(VectorID(1))
	if docLen != 4 { // 4 terms
		t.Errorf("expected doc length 4, got %d", docLen)
	}

	// Add second document
	idx.Add(VectorID(2), "the lazy dog")
	if idx.DocCount() != 2 {
		t.Errorf("expected 2 docs, got %d", idx.DocCount())
	}
}

func TestBM25InvertedIndexDelete(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	idx.Add(VectorID(1), "hello world")
	idx.Add(VectorID(2), "goodbye world")

	// Delete first document
	idx.Delete(VectorID(1))
	if idx.DocCount() != 1 {
		t.Errorf("expected 1 doc after delete, got %d", idx.DocCount())
	}

	// Verify document length removed
	docLen := idx.GetDocLength(VectorID(1))
	if docLen != 0 {
		t.Errorf("expected doc length 0 after delete, got %d", docLen)
	}

	// Search should not find deleted doc
	results := idx.SearchBM25("hello", 10)
	for _, r := range results {
		if r.ID == VectorID(1) {
			t.Error("deleted document should not appear in results")
		}
	}
}

func TestBM25InvertedIndexSearchBM25(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	// Add documents with varying term frequencies
	idx.Add(VectorID(1), "apple apple apple banana")    // high TF for apple
	idx.Add(VectorID(2), "apple banana banana banana")  // high TF for banana
	idx.Add(VectorID(3), "cherry cherry cherry cherry") // no apple/banana

	// Search for "apple" - doc 1 should rank higher
	results := idx.SearchBM25("apple", 10)
	if len(results) == 0 {
		t.Fatal("expected results for apple query")
	}
	if results[0].ID != VectorID(1) {
		t.Errorf("expected doc 1 to rank first for apple, got %d", results[0].ID)
	}

	// Search for "banana" - doc 2 should rank higher
	results = idx.SearchBM25("banana", 10)
	if len(results) == 0 {
		t.Fatal("expected results for banana query")
	}
	if results[0].ID != VectorID(2) {
		t.Errorf("expected doc 2 to rank first for banana, got %d", results[0].ID)
	}

	// Search for "cherry" - only doc 3
	results = idx.SearchBM25("cherry", 10)
	if len(results) != 1 {
		t.Errorf("expected 1 result for cherry, got %d", len(results))
	}
}

func TestBM25InvertedIndexMultiTermQuery(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	idx.Add(VectorID(1), "quick brown fox")
	idx.Add(VectorID(2), "lazy brown dog")
	idx.Add(VectorID(3), "quick lazy cat")

	// Multi-term query "quick brown" should prefer doc 1 (has both terms)
	results := idx.SearchBM25("quick brown", 10)
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	// Doc 1 has both "quick" and "brown", should score highest
	if results[0].ID != VectorID(1) {
		t.Errorf("expected doc 1 (has both terms) to rank first, got %d", results[0].ID)
	}
}

func TestBM25InvertedIndexIDFWeighting(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	// Add 10 documents, all containing "common"
	// Only doc 1 contains "rare"
	for i := 1; i <= 10; i++ {
		if i == 1 {
			idx.Add(VectorID(i), "common rare")
		} else {
			idx.Add(VectorID(i), "common word")
		}
	}

	// Search for "rare" should return doc 1 with high score
	results := idx.SearchBM25("rare", 10)
	if len(results) != 1 {
		t.Errorf("expected 1 result for rare term, got %d", len(results))
	}
	if len(results) > 0 && results[0].ID != VectorID(1) {
		t.Errorf("expected doc 1 for rare term")
	}

	// Rare term should have higher weight than common term
	rareScore := results[0].Score
	commonResults := idx.SearchBM25("common", 10)
	// Average common score should be lower due to low IDF
	var commonScoreAvg float32
	for _, r := range commonResults {
		commonScoreAvg += r.Score
	}
	commonScoreAvg /= float32(len(commonResults))

	if rareScore <= commonScoreAvg {
		t.Errorf("rare term score (%f) should be higher than common term avg (%f)",
			rareScore, commonScoreAvg)
	}
}

func TestBM25InvertedIndexLengthNormalization(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	// Short document with term
	idx.Add(VectorID(1), "target")
	// Long document with same term frequency
	idx.Add(VectorID(2), "target padding padding padding padding padding padding padding")

	results := idx.SearchBM25("target", 10)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Shorter document should score higher (B=0.75 penalizes long docs)
	if results[0].ID != VectorID(1) {
		t.Errorf("expected short doc (1) to rank higher than long doc (2)")
	}
}

func TestBM25InvertedIndexGetTermDocFreq(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	idx.Add(VectorID(1), "apple banana")
	idx.Add(VectorID(2), "apple cherry")
	idx.Add(VectorID(3), "banana cherry")

	// "apple" appears in 2 documents
	df := idx.GetTermDocFreq("apple")
	if df != 2 {
		t.Errorf("expected doc freq 2 for apple, got %d", df)
	}

	// "banana" appears in 2 documents
	df = idx.GetTermDocFreq("banana")
	if df != 2 {
		t.Errorf("expected doc freq 2 for banana, got %d", df)
	}

	// "unknown" appears in 0 documents
	df = idx.GetTermDocFreq("unknown")
	if df != 0 {
		t.Errorf("expected doc freq 0 for unknown, got %d", df)
	}
}

func TestBM25InvertedIndexEmptyQuery(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())
	idx.Add(VectorID(1), "hello world")

	// Empty query should return nil
	results := idx.SearchBM25("", 10)
	if results != nil {
		t.Errorf("expected nil for empty query, got %v", results)
	}

	// Whitespace-only query should return nil
	results = idx.SearchBM25("   ", 10)
	if results != nil {
		t.Errorf("expected nil for whitespace query")
	}
}

func TestBM25InvertedIndexConcurrency(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	// Concurrent adds
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			idx.Add(VectorID(id), "concurrent test document")
		}(i)
	}
	wg.Wait()

	if idx.DocCount() != 100 {
		t.Errorf("expected 100 docs after concurrent adds, got %d", idx.DocCount())
	}

	// Concurrent searches during adds
	for i := 100; i < 150; i++ {
		wg.Add(2)
		go func(id int) {
			defer wg.Done()
			idx.Add(VectorID(id), "more documents")
		}(i)
		go func() {
			defer wg.Done()
			idx.SearchBM25("test", 10)
		}()
	}
	wg.Wait()
}

func TestBM25InvertedIndexLimit(t *testing.T) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	// Add 20 documents all matching "common"
	for i := 1; i <= 20; i++ {
		idx.Add(VectorID(i), "common term here")
	}

	// Limit to 5 results
	results := idx.SearchBM25("common", 5)
	if len(results) != 5 {
		t.Errorf("expected 5 results with limit, got %d", len(results))
	}

	// No limit (0) should return all
	results = idx.SearchBM25("common", 0)
	if len(results) != 20 {
		t.Errorf("expected 20 results without limit, got %d", len(results))
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkBM25InvertedIndexAdd(b *testing.B) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Add(VectorID(i), "benchmark test document with several words")
	}
}

func BenchmarkBM25InvertedIndexSearchBM25(b *testing.B) {
	idx := NewBM25InvertedIndex(DefaultBM25Config())

	// Pre-populate with 10000 documents
	for i := 0; i < 10000; i++ {
		idx.Add(VectorID(i), "document with various terms for searching")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.SearchBM25("various terms", 10)
	}
}
