package store

import (
	"sync"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/RoaringBitmap/roaring/v2"
)

// InvertedIndex maps string terms to document IDs (row indices) using compressed bitmaps
type InvertedIndex struct {
	mu            sync.RWMutex
	index         map[string]*roaring.Bitmap
	termsCount    int
	totalPostings uint64
}

// NewInvertedIndex creates a new empty inverted index
func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		index: make(map[string]*roaring.Bitmap),
	}
}

// Add inserts a term-docID pair into the index
func (idx *InvertedIndex) Add(term string, docID uint32) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	bm, ok := idx.index[term]
	if !ok {
		bm = roaring.NewBitmap()
		idx.index[term] = bm
		idx.termsCount++
	}

	// Only increment metrics if new
	if !bm.Contains(docID) {
		bm.Add(docID)
		idx.totalPostings++
		metrics.InvertedIndexPostingsTotal.Inc()
	}
}

// Delete removes a term-docID pair from the index
func (idx *InvertedIndex) Delete(term string, docID uint32) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if bm, ok := idx.index[term]; ok {
		if bm.Contains(docID) {
			bm.Remove(docID)
			idx.totalPostings--
			metrics.InvertedIndexPostingsTotal.Dec()
			if bm.IsEmpty() {
				delete(idx.index, term)
				idx.termsCount--
			}
		}
	}
}

// AddBatch adds multiple terms for a single document
func (idx *InvertedIndex) AddBatch(terms []string, docID uint32) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, term := range terms {
		bm, ok := idx.index[term]
		if !ok {
			bm = roaring.NewBitmap()
			idx.index[term] = bm
			idx.termsCount++
		}
		if !bm.Contains(docID) {
			bm.Add(docID)
			idx.totalPostings++
			metrics.InvertedIndexPostingsTotal.Inc()
		}
	}
}

// Get returns the bitmap for a given term, or nil if not found
// Returns a CLONE/Copy to be safe for concurrent reading/iteration by caller?
// Roaring bitmaps are generally not thread-safe for mutation, but read-only is fine
// IF write lock is held during mutation.
// But we return it to caller who might use it while writer mutates.
// So we MUST return a Clone.
func (idx *InvertedIndex) Get(term string) *roaring.Bitmap {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if bm, ok := idx.index[term]; ok {
		return bm.Clone()
	}
	return nil
}

// Stats returns basic stats about the index
func (idx *InvertedIndex) Stats() (int, uint64) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.termsCount, idx.totalPostings
}

// MemoryUsage estimates memory usage (expensive)
func (idx *InvertedIndex) MemoryUsage() uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	usage := uint64(0)
	for _, bm := range idx.index {
		usage += bm.GetSizeInBytes()
	}
	return usage
}
