package store

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
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
	Bitset      *types.Bitset
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
	index := ds.Index
	bm25 := ds.BM25Index
	bm25Arena := ds.BM25ArenaIndex
	ds.dataMu.RUnlock()

	var denseResults []SearchResult
	var sparseResults []SearchResult
	var wg sync.WaitGroup
	var denseErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		if alpha > 0 && len(queryVec) > 0 {
			if index != nil {
				denseResults, denseErr = index.SearchVectors(ctx, queryVec, k*2, nil, SearchOptions{})
				if denseErr != nil {
					s.logger.Error().Err(denseErr).Msg("Vector search failed in hybrid search")
					return
				}
				metrics.HybridSearchVectorTotal.Inc()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if alpha < 1.0 && textQuery != "" {
			bm25Start := time.Now()

			if bm25Arena != nil {
				sparseResults = searchBM25Arena(bm25Arena, textQuery, k*2, nil)
				metrics.HybridSearchKeywordTotal.Inc()
				metrics.HybridSearchBM25Duration.WithLabelValues(name).Observe(time.Since(bm25Start).Seconds())
			} else if bm25 != nil {
				sparseResults = bm25.SearchBM25(textQuery, k*2, nil)
				metrics.HybridSearchKeywordTotal.Inc()
				metrics.HybridSearchBM25Duration.WithLabelValues(name).Observe(time.Since(bm25Start).Seconds())
			}
		}
	}()

	wg.Wait()

	if denseErr != nil {
		return nil, denseErr
	}

	// 3. Fusion logic
	var finalResults []SearchResult
	mergeStart := time.Now()
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
		finalResults = ReciprocalRankFusion(name, denseResults, sparseResults, rrfK, k)
		metrics.HybridSearchMergeDuration.WithLabelValues(name).Observe(time.Since(mergeStart).Seconds())
		metrics.HybridRRFFusionLatencySeconds.WithLabelValues(name).Observe(time.Since(mergeStart).Seconds())
	}

	// 4. Graph Re-ranking (GraphRAG) - Re-acquire RLock for post-processing
	ds.dataMu.RLock()
	if graphAlpha > 0 && ds.Graph != nil {
		if graphDepth <= 0 {
			graphDepth = 2 // Default hop depth
		}
		metrics.HybridGraphReRankEnabled.WithLabelValues(name, "true").Inc()
		rerankStart := time.Now()

		// Store results before reranking to track "graph_expanded" origin
		preRerankIds := make(map[types.VectorID]bool)
		for _, r := range finalResults {
			preRerankIds[r.ID] = true
		}

		// Rerank using graph topology
		ranked := ds.Graph.RankWithGraph(finalResults, graphAlpha, graphDepth)
		metrics.HybridGraphReRankLatencySeconds.WithLabelValues(name).Observe(time.Since(rerankStart).Seconds())

		if len(ranked) > 0 {
			finalResults = ranked
			// Track results newly introduced by graph expansion
			for _, r := range finalResults {
				if !preRerankIds[r.ID] {
					metrics.HybridResultOriginTotal.WithLabelValues(name, "graph_expanded").Inc()
				}
			}
		}
	} else {
		metrics.HybridGraphReRankEnabled.WithLabelValues(name, "false").Inc()
	}

	// Map internal IDs to user IDs (Phase 14 integration)
	resolved := s.mapInternalToUserIDsLocked(ds, finalResults)
	ds.dataMu.RUnlock()
	if len(resolved) > k {
		resolved = resolved[:k]
	}

	s.logger.Info().
		Str("dataset", name).
		Dur("duration", time.Since(start)).
		Int("count", len(resolved)).
		Msg("SearchHybrid completed")

	// Record the query for learned index training, including the active embedding context.
	embProvider, embModel := s.GetActiveEmbedding()
	s.RecordQueryPerformance(QueryFeatures{
		VectorDimension:   len(queryVec),
		SearchK:           k,
		IsHybrid:          true,
		IsFiltered:        false,
		EmbeddingProvider: embProvider,
		EmbeddingModel:    embModel,
	}, time.Since(start).Seconds()*1000, 1.0, IndexTypeHNSW, embProvider, embModel)

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

	var filterBitmap *types.Bitset
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
				filterBitmap = types.NewBitsetFromRoaring(bm)
			} else {
				filterBitmap.And(bm)
			}
		}
	}

	var results []SearchResult
	switch {
	case filterBitmap != nil && filterBitmap.Count() > 0:
		// Perform filtered search
		var err error
		results, err = ds.Index.SearchVectorsWithBitmap(ctx, queryVec, k, filterBitmap.AsRoaring(), SearchOptions{})
		if err != nil {
			return nil, err
		}
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
		final = append(final, SearchResult{ID: types.VectorID(id), Score: score})
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
