package store

import (
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
)

// BitmapIndex stores roaring bitmaps for metadata fields to enable fast pre-filtering.
// It is thread-safe and optimized for low-cardinality fields.
type BitmapIndex struct {
	mu sync.RWMutex
	// maps field_name -> value -> bitmap of IDs
	indexes map[string]map[string]*roaring.Bitmap
}

// NewBitmapIndex creates a new BitmapIndex.
func NewBitmapIndex() *BitmapIndex {
	return &BitmapIndex{
		indexes: make(map[string]map[string]*roaring.Bitmap),
	}
}

// Add adds a document ID to the index for a given field and value.
func (b *BitmapIndex) Add(id uint32, field, value string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	fieldIdx, ok := b.indexes[field]
	if !ok {
		fieldIdx = make(map[string]*roaring.Bitmap)
		b.indexes[field] = fieldIdx
	}

	bm, ok := fieldIdx[value]
	if !ok {
		bm = roaring.New()
		fieldIdx[value] = bm
	}

	bm.Add(id)
	return nil
}

// Remove removes a document ID from the index for a given field and value.
func (b *BitmapIndex) Remove(id uint32, field, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	fieldIdx, ok := b.indexes[field]
	if !ok {
		return
	}

	bm, ok := fieldIdx[value]
	if !ok {
		return
	}

	bm.Remove(id)
}

// Filter returns a bitmap representing document IDs that match all provided criteria (AND logic).
// If no criteria provided, it returns nil or potentially an empty bitmap.
// For HNSW pre-filtering, a nil/empty return would usually mean "no matches".
func (b *BitmapIndex) Filter(criteria map[string]string) (*roaring.Bitmap, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(criteria) == 0 {
		return nil, nil
	}

	var result *roaring.Bitmap

	for field, value := range criteria {
		fieldIdx, ok := b.indexes[field]
		if !ok {
			// Field doesn't exist, so intersection is empty
			return roaring.New(), nil
		}

		bm, ok := fieldIdx[value]
		if !ok {
			// Value doesn't exist, so intersection is empty
			return roaring.New(), nil
		}

		if result == nil {
			// First field, clone the bitmap
			result = bm.Clone()
		} else {
			// Intersect with existing results (AND)
			result.And(bm)
		}

		// Optimization: if result is empty, stop
		if result.IsEmpty() {
			break
		}
	}

	return result, nil
}

// Count returns the total number of bitmaps in the index.
func (b *BitmapIndex) Count() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	count := 0
	for _, fieldIdx := range b.indexes {
		count += len(fieldIdx)
	}
	return count
}
