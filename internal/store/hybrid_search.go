package store

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HybridSearchRequest encapsulates parameters for hybrid search.
type HybridSearchRequest struct {
	Dataset     string
	QueryVector []float32
	QueryText   string
	K           int
	Alpha       float32 // Weight for vector score (0-1)
	Filters     []query.Filter
	Bitset      *query.Bitset
}

// SearchHybrid performs a hybrid search combining dense vector search and sparse keyword search.
// If alpha is < 0, it is automatically estimated using EstimateAlpha.
func SearchHybrid(ctx context.Context, s *VectorStore, name string, queryVec []float32, textQuery string, k int, alpha float32, rrfK int, graphAlpha float32, graphDepth int) ([]SearchResult, error) {
	// Adaptive Alpha
	if alpha < 0 {
		alpha = EstimateAlpha(textQuery)
	}

	defer func(start time.Time) {
		metrics.SearchLatencySeconds.WithLabelValues(name, "hybrid_rrf").Observe(time.Since(start).Seconds())
	}(time.Now())

	start := time.Now()
	s.logger.Info().
		Str("dataset", name).
		Str("text_query", textQuery).
		Float32("alpha", alpha).
		Int("k", k).
		Msg("SearchHybrid started")

	ds, ok := s.getDataset(name)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "dataset %s not found", name)
	}

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	var denseResults []SearchResult
	var sparseResults []SearchResult

	// 1. Dense (Vector) Search
	if alpha > 0 && len(queryVec) > 0 {
		if ds.Index != nil {
			var err error
			denseResults, err = ds.Index.SearchVectors(ctx, queryVec, k*2, nil, SearchOptions{})
			if err != nil {
				s.logger.Error().Err(err).Msg("Vector search failed in hybrid search")
				// Continue with sparse results only?
				// For now, let's treat it as a hard failure if dense was requested but failed.
				return nil, err
			}
			metrics.HybridSearchVectorTotal.Inc()
		}
	}

	// 2. Sparse (Keyword) Search
	if alpha < 1.0 && textQuery != "" {
		bm25Start := time.Now()

		// Prefer arena-based index for better performance
		if ds.BM25ArenaIndex != nil {
			// Use arena-based BM25 index
			sparseResults = searchBM25Arena(ds.BM25ArenaIndex, textQuery, k*2)
			metrics.HybridSearchKeywordTotal.Inc()
			metrics.HybridSearchBM25Duration.WithLabelValues(name).Observe(time.Since(bm25Start).Seconds())
		} else if ds.BM25Index != nil {
			// Fallback to legacy BM25 index
			sparseResults = ds.BM25Index.SearchBM25(textQuery, k*2)
			metrics.HybridSearchKeywordTotal.Inc()
			metrics.HybridSearchBM25Duration.WithLabelValues(name).Observe(time.Since(bm25Start).Seconds())
		}
	}

	// 3. Fusion logic
	var finalResults []SearchResult
	switch alpha {
	case 1.0:
		finalResults = denseResults
	case 0.0:
		finalResults = sparseResults
	default:
		// Fusion! Use RRF.
		if rrfK <= 0 {
			rrfK = 60 // Default
		}
		finalResults = ReciprocalRankFusion(denseResults, sparseResults, rrfK, k)
	}

	// 4. Graph Re-ranking (GraphRAG)
	if graphAlpha > 0 && ds.Graph != nil {
		if graphDepth <= 0 {
			graphDepth = 2 // Default hop depth
		}
		// Rerank using graph topology
		// We use the current finalResults as seeds
		// Note: RankWithGraph returns expanded set, we might want to trim back to k or allow expansion
		// Usually RAG wants context, so expansion is good. But we should probably limit result size eventually.
		ranked := ds.Graph.RankWithGraph(finalResults, graphAlpha, graphDepth)
		if len(ranked) > 0 {
			finalResults = ranked
		}
	}

	// Map internal IDs to user IDs (Phase 14 integration)
	resolved := s.mapInternalToUserIDsLocked(ds, finalResults)
	if len(resolved) > k {
		resolved = resolved[:k]
	}

	s.logger.Info().
		Str("dataset", name).
		Dur("duration", time.Since(start)).
		Int("count", len(resolved)).
		Msg("SearchHybrid completed")

	return resolved, nil
}

// HybridSearch performs a filtered vector search using inverted indexes for pre-filtering.
func HybridSearch(ctx context.Context, s *VectorStore, name string, queryVec []float32, k int, filters map[string]string) ([]SearchResult, error) {
	defer func(start time.Time) {
		metrics.SearchLatencySeconds.WithLabelValues(name, "hybrid_filtered").Observe(time.Since(start).Seconds())
	}(time.Now())
	ds, ok := s.getDataset(name)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "dataset %s not found", name)
	}

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	var filterBitmap *query.Bitset
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
				filterBitmap = query.NewBitsetFromRoaring(bm)
			} else {
				filterBitmap.And(bm)
			}
		}
	}

	var results []SearchResult
	switch {
	case filterBitmap != nil && filterBitmap.Count() > 0:
		// Perform filtered search
		results = ds.Index.SearchVectorsWithBitmap(ctx, queryVec, k, filterBitmap, SearchOptions{})
	case !hasFilters:
		// No filters, standard search
		var err error
		results, err = ds.Index.SearchVectors(ctx, queryVec, k, nil, SearchOptions{})
		if err != nil {
			return nil, err
		}
	default:
		// Filters yielded no results
		return nil, nil
	}
	return results, nil
}

// RankFusion performs Reciprocal Rank Fusion.
func RankFusion(list1, list2 []SearchResult, k, rrfK int) []SearchResult {
	scores := make(map[uint32]float32) // Use VectorID (uint32)

	// Helper to add scores
	add := func(list []SearchResult) {
		for rank, item := range list {
			// RRF score = 1 / (k + rank)
			score := float32(1.0) / float32(rrfK+rank+1)
			scores[uint32(item.ID)] += score
		}
	}

	add(list1)
	add(list2)

	// Sort
	final := make([]SearchResult, 0, len(scores))
	for id, score := range scores {
		final = append(final, SearchResult{ID: VectorID(id), Score: score})
	}

	sort.Slice(final, func(i, j int) bool {
		return final[i].Score > final[j].Score
	})

	if len(final) > k {
		final = final[:k]
	}
	return final
}

// HybridSearchWithBitmap performs hybrid search using a pre-computed bitmap for filtering.
func (s *VectorStore) HybridSearchWithBitmap(ctx context.Context, req *HybridSearchRequest) ([]SearchResult, error) {
	// Placeholder
	return nil, nil
}

// EstimateAlpha calculates a heuristic alpha value based on query length.
func EstimateAlpha(q string) float32 {
	tokens := strings.Fields(q)
	n := len(tokens)
	if n < 3 {
		return 0.3
	}
	if n <= 5 {
		return 0.5
	}
	return 0.8
}
