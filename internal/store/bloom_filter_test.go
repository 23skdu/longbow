package store

import (
	"fmt"
	"sync"
	"testing"
)

// TestBloomFilter_BasicAddContains tests basic bloom filter operations
func TestBloomFilter_BasicAddContains(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01) // 1000 items, 1% false positive rate

	// Add some terms
	terms := []string{"hello", "world", "golang", "bloom", "filter"}
	for _, term := range terms {
		bf.Add(term)
	}

	// Verify all added terms are found (no false negatives)
	for _, term := range terms {
		if !bf.Contains(term) {
			t.Errorf("bloom filter should contain %q but doesn't", term)
		}
	}
}

// TestBloomFilter_NoFalseNegatives verifies bloom filters never have false negatives
func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	bf := NewBloomFilter(10000, 0.01)

	// Add 5000 terms
	for i := 0; i < 5000; i++ {
		bf.Add(fmt.Sprintf("term_%d", i))
	}

	// Verify ALL added terms are found
	for i := 0; i < 5000; i++ {
		term := fmt.Sprintf("term_%d", i)
		if !bf.Contains(term) {
			t.Fatalf("false negative detected for %q - bloom filters must never have false negatives", term)
		}
	}
}

// TestBloomFilter_FalsePositiveRate verifies false positive rate is within bounds
func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	numItems := 10000
	targetFPR := 0.01 // 1%
	bf := NewBloomFilter(numItems, targetFPR)

	// Add items
	for i := 0; i < numItems; i++ {
		bf.Add(fmt.Sprintf("added_%d", i))
	}

	// Test non-existent items for false positives
	numTests := 100000
	falsePositives := 0
	for i := 0; i < numTests; i++ {
		// These were never added
		if bf.Contains(fmt.Sprintf("notadded_%d", i)) {
			falsePositives++
		}
	}

	actualFPR := float64(falsePositives) / float64(numTests)
	// Allow 2x target rate as acceptable margin
	if actualFPR > targetFPR*2 {
		t.Errorf("false positive rate too high: got %.4f, want < %.4f", actualFPR, targetFPR*2)
	}
	t.Logf("False positive rate: %.4f (target: %.4f)", actualFPR, targetFPR)
}

// TestBloomFilter_ConcurrentSafety tests thread-safe operations
func TestBloomFilter_ConcurrentSafety(t *testing.T) {
	bf := NewBloomFilter(100000, 0.01)
	var wg sync.WaitGroup
	numGoroutines := 20
	itemsPerGoroutine := 1000

	// Concurrent adds
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				bf.Add(fmt.Sprintf("g%d_term_%d", gid, i))
			}
		}(g)
	}
	wg.Wait()

	// Verify all items are present
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < itemsPerGoroutine; i++ {
			term := fmt.Sprintf("g%d_term_%d", g, i)
			if !bf.Contains(term) {
				t.Errorf("missing term %q after concurrent add", term)
			}
		}
	}
}

// TestBloomFilter_ConcurrentAddAndContains tests mixed operations
func TestBloomFilter_ConcurrentAddAndContains(t *testing.T) {
	bf := NewBloomFilter(50000, 0.01)
	var wg sync.WaitGroup
	numWriters := 10
	numReaders := 10
	itemsPerWriter := 1000

	// Start writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < itemsPerWriter; i++ {
				bf.Add(fmt.Sprintf("writer%d_item%d", wid, i))
			}
		}(w)
	}

	// Start readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Just check random terms, shouldn't panic
			for i := 0; i < 1000; i++ {
				_ = bf.Contains(fmt.Sprintf("random_%d", i))
			}
		}()
	}

	wg.Wait()
}

// TestShardedInvertedIndex_BloomFilterIntegration tests bloom filter with sharded index
func TestShardedInvertedIndex_BloomFilterIntegration(t *testing.T) {
	idx := NewShardedInvertedIndex()

	// Add documents with known terms
	idx.Add(1, "golang programming language")
	idx.Add(2, "rust programming language")
	idx.Add(3, "python scripting language")

	// Search for existing term - should find results
	results := idx.Search("programming", 10)
	if len(results) != 2 {
		t.Errorf("expected 2 results for 'programming', got %d", len(results))
	}

	// Search for non-existent term - bloom filter should speed this up
	results = idx.Search("nonexistentterm12345", 10)
	if len(results) != 0 {
		t.Errorf("expected 0 results for non-existent term, got %d", len(results))
	}
}

// TestShardedInvertedIndex_BloomFilterNegativeLookup tests bloom filter optimization
func TestShardedInvertedIndex_BloomFilterNegativeLookup(t *testing.T) {
	idx := NewShardedInvertedIndex()

	// Add many documents
	for i := VectorID(0); i < 1000; i++ {
		idx.Add(i, fmt.Sprintf("document %d contains specific content", i))
	}

	// Non-existent terms should return empty quickly via bloom filter
	nonExistent := []string{"xyzabc", "qwerty123", "nothere", "missing", "absent"}
	for _, term := range nonExistent {
		results := idx.Search(term, 10)
		if len(results) != 0 {
			t.Errorf("expected 0 results for %q, got %d", term, len(results))
		}
	}
}

// BenchmarkBloomFilter_Contains benchmarks bloom filter lookups
func BenchmarkBloomFilter_Contains(b *testing.B) {
	bf := NewBloomFilter(100000, 0.01)
	for i := 0; i < 10000; i++ {
		bf.Add(fmt.Sprintf("term_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bf.Contains(fmt.Sprintf("term_%d", i%20000))
	}
}

// BenchmarkShardedIndex_NegativeLookupWithBloom benchmarks negative lookups
func BenchmarkShardedIndex_NegativeLookupWithBloom(b *testing.B) {
	idx := NewShardedInvertedIndex()
	for i := VectorID(0); i < 10000; i++ {
		idx.Add(i, fmt.Sprintf("document %d with content", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Search for non-existent terms - bloom filter should make this fast
		_ = idx.Search(fmt.Sprintf("nonexistent_%d", i), 10)
	}
}

// BenchmarkShardedIndex_PositiveLookup benchmarks positive lookups for comparison
func BenchmarkShardedIndex_PositiveLookup(b *testing.B) {
	idx := NewShardedInvertedIndex()
	for i := VectorID(0); i < 10000; i++ {
		idx.Add(i, fmt.Sprintf("document %d with content", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Search for existing term
		_ = idx.Search("document", 10)
	}
}
