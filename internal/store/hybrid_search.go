package store

import (
	"context"
	"fmt"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// HybridSearch performs a vector search filtered by metadata criteria
func (s *VectorStore) HybridSearch(ctx context.Context, datasetName string, queryVector []float32, k int, filters map[string]string) ([]SearchResult, error) {
	start := time.Now()
	metrics.FlightOperationsTotal.WithLabelValues("hybrid_search", "processing").Inc()

	s.mu.RLock()
	ds, ok := s.datasets[datasetName]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("dataset not found: %s", datasetName)
	}

	// 1. Build Filter Bitmap
	// Combine all filters using AND logic
	var filterBitmap *Bitset // Our thread-safe wrapper

	ds.dataMu.RLock()
	hasFilters := len(filters) > 0
	if hasFilters {
		for col, val := range filters {
			idx, ok := ds.InvertedIndexes[col]
			if !ok {
				// Filter column not found or no index - treat as empty match (strict AND)
				ds.dataMu.RUnlock()
				return []SearchResult{}, nil
			}

			termRaw := val // simple match
			bmRaw := idx.Get(termRaw)

			if bmRaw == nil || bmRaw.IsEmpty() {
				ds.dataMu.RUnlock()
				return []SearchResult{}, nil
			}

			// Intersect
			if filterBitmap == nil {
				// First filter
				filterBitmap = &Bitset{bitmap: bmRaw} // Take ownership of Clone from Get
			} else {
				// AND
				filterBitmap.bitmap.And(bmRaw)
			}

			// Short-circuit
			if filterBitmap.bitmap.IsEmpty() {
				ds.dataMu.RUnlock()
				return []SearchResult{}, nil
			}
		}
	}
	ds.dataMu.RUnlock()

	// 3. Perform Search with Bitmap
	var results []SearchResult
	if hasFilters {
		// Use optimized inverted index bitmap search
		if ds.Index != nil {
			results = ds.Index.SearchVectorsWithBitmap(queryVector, k, filterBitmap)
		}
	} else {
		// Standard search
		// Empty filter list for standard search
		if ds.Index != nil {
			results = ds.Index.SearchVectors(queryVector, k, nil)
		}
	}

	metrics.FlightDurationSeconds.WithLabelValues("hybrid_search").Observe(time.Since(start).Seconds())

	// 4. Map internal IDs to user IDs
	return s.MapInternalToUserIDs(ds, results), nil
}
