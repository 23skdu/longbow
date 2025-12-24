package store

import (
	"context"

	"github.com/23skdu/longbow/internal/metrics"
	"go.uber.org/zap"
)

// SearchHybrid performs a hybrid search combining dense vector search and sparse keyword search.
func SearchHybrid(ctx context.Context, s *VectorStore, name string, query []float32, textQuery string, k int, alpha float32, rrfK int) ([]SearchResult, error) {
	s.logger.Info("SearchHybrid called",
		zap.String("dataset", name),
		zap.String("text_query", textQuery),
		zap.Float32("alpha", alpha),
		zap.Int("k", k))

	ds, err := s.getDataset(name)
	if err != nil {
		return nil, err
	}

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	var denseResults []SearchResult
	var sparseResults []SearchResult

	// 1. Dense (Vector) Search
	if alpha > 0 && len(query) > 0 {
		if ds.Index != nil {
			var err error
			denseResults, err = ds.Index.SearchVectors(query, k*2, nil)
			if err != nil {
				s.logger.Error("Vector search failed in hybrid search", zap.Error(err))
				// Continue with sparse results only?
				// For now, let's treat it as a hard failure if dense was requested but failed.
				return nil, err
			}
			metrics.HybridSearchVectorTotal.Inc()
		}
	}

	// 2. Sparse (Keyword) Search
	if alpha < 1.0 && textQuery != "" {
		if ds.BM25Index != nil {
			sparseResults = ds.BM25Index.SearchBM25(textQuery, k*2)
			metrics.HybridSearchKeywordTotal.Inc()
		}
	}

	// 3. Fusion logic
	var finalResults []SearchResult
	if alpha == 1.0 {
		finalResults = denseResults
	} else if alpha == 0.0 {
		finalResults = sparseResults
	} else {
		// Fusion! Use RRF.
		if rrfK <= 0 {
			rrfK = 60 // Default
		}
		finalResults = ReciprocalRankFusion(denseResults, sparseResults, rrfK, k)
	}

	// Map internal IDs to user IDs (Phase 14 integration)
	resolved := s.MapInternalToUserIDs(ds, finalResults)
	if len(resolved) > k {
		resolved = resolved[:k]
	}

	return resolved, nil
}

// HybridSearch performs a filtered vector search using inverted indexes for pre-filtering.
func HybridSearch(ctx context.Context, s *VectorStore, name string, query []float32, k int, filters map[string]string) ([]SearchResult, error) {
	ds, err := s.getDataset(name)
	if err != nil {
		return nil, err
	}

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	var filterBitmap *Bitset
	hasFilters := len(filters) > 0

	if hasFilters {
		for col, val := range filters {
			idx, ok := ds.InvertedIndexes[col]
			if !ok {
				continue
			}

			bm := idx.Get(val)
			if bm == nil {
				// Term not found in this column, empty result
				return nil, nil
			}

			if filterBitmap == nil {
				filterBitmap = &Bitset{bitmap: bm}
			} else {
				filterBitmap.mu.Lock()
				filterBitmap.bitmap.And(bm)
				filterBitmap.mu.Unlock()
			}
		}
	}

	var results []SearchResult
	if filterBitmap != nil && filterBitmap.Count() > 0 {
		// Perform filtered search
		results = ds.Index.SearchVectorsWithBitmap(query, k, filterBitmap)
	} else if !hasFilters {
		// No filters, standard search
		var err error
		results, err = ds.Index.SearchVectors(query, k, nil)
		if err != nil {
			return nil, err
		}
	} else {
		// Filters yielded no results
		return nil, nil
	}

	// Map internal IDs to user IDs
	resolved := s.MapInternalToUserIDs(ds, results)
	return resolved, nil
}
