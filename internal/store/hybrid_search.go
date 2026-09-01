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
func (s *VectorStore) SearchHybrid(ctx context.Context, name string, queryVec []float32, textQuery string, k int, alpha float32, rrfK int, graphAlpha float32, graphDepth int, rawHybrid bool) ([]SearchResult, error) {
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
				sparseResults = bm25.SearchBM25(textQuery, k*2, nil, s.resultPool)
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
		if rawHybrid {
			// Return raw un-fused results (Dense marked with Source=0, Sparse marked with Source=1)
			for i := range denseResults {
				denseResults[i].Source = 0
			}
			for i := range sparseResults {
				sparseResults[i].Source = 1
			}
			finalResults = append(denseResults, sparseResults...)
		} else {
			// Fusion! Use RRF.
			if rrfK <= 0 {
				rrfK = 60 // Default
			}
			finalResults = ReciprocalRankFusion(name, denseResults, sparseResults, rrfK, k, s.resultPool)
			metrics.HybridSearchMergeDuration.WithLabelValues(name).Observe(time.Since(mergeStart).Seconds())
			metrics.HybridRRFFusionLatencySeconds.WithLabelValues(name).Observe(time.Since(mergeStart).Seconds())
		}
	}

	// 4. Graph Re-ranking (GraphRAG) - Snapshot graph pointer under lock, then release before BFS
	ds.dataMu.RLock()
	graph := ds.Graph
	ds.dataMu.RUnlock()

	if graphAlpha > 0 && graph != nil {
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

		// Rerank using graph topology (Distributed BFS expansion).
		// GraphStore has its own adjMu lock — no need to hold dataMu for this.
		ranked := graph.RankWithGraphDistributed(ctx, name, queryVec, finalResults, graphAlpha, graphDepth, s)
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
	ds.dataMu.RLock()
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
func (s *VectorStore) HybridSearch(ctx context.Context, name string, queryVec []float32, k int, filters map[string]string) ([]SearchResult, error) {
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

// ReciprocalRankFusion performs Reciprocal Rank Fusion.
func ReciprocalRankFusion(dataset string, list1, list2 []SearchResult, rrfK, k int, pool *SearchResultPool) []SearchResult {
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

	// Get result slice from pool
	var final []SearchResult
	if pool != nil {
		final = pool.Get(len(scores))
	} else {
		final = make([]SearchResult, 0, len(scores))
	}
	for id, score := range scores {
		final = append(final, SearchResult{ID: types.VectorID(id), Score: score})
	}

	sort.Slice(final, func(i, j int) bool {
		return final[i].Score > final[j].Score
	})

	if len(final) > k {
		final = final[:k]
	}

	// Note: The caller is responsible for returning the slice to the pool
	// but since SearchHybrid is the main caller and it returns the results to the user,
	// we have a lifetime issue.
	// For now, we'll return a copy and Put the pooled slice back.

	results := make([]SearchResult, len(final))
	copy(results, final)
	if pool != nil {
		pool.Put(final)
	}

	return results
}

// HybridSearchWithBitmap performs hybrid search using a pre-computed bitmap for filtering.
// It combines dense vector search (filtered by the bitmap) with optional sparse keyword
// search (BM25) when a text query is provided, fusing results via Reciprocal Rank Fusion.
func (s *VectorStore) HybridSearchWithBitmap(ctx context.Context, req *HybridSearchRequest) ([]SearchResult, error) {
	if req.Alpha < 0 {
		req.Alpha = EstimateAlpha(req.QueryText)
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
	}

	ds.dataMu.RLock()
	index := ds.Index
	bm25Arena := ds.BM25ArenaIndex
	bm25Legacy := ds.BM25Index
	ds.dataMu.RUnlock()

	var denseResults []SearchResult
	var sparseResults []SearchResult
	var wg sync.WaitGroup
	var denseErr error

	// Dense vector search filtered by bitmap
	if req.Alpha > 0 && len(req.QueryVector) > 0 && index != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var filterBitmap *types.Bitset
			if req.Bitset != nil {
				filterBitmap = req.Bitset
			}
			var err error
			if filterBitmap != nil && filterBitmap.Count() > 0 {
				denseResults, err = index.SearchVectorsWithBitmap(ctx, req.QueryVector, req.K*2, filterBitmap.AsRoaring(), SearchOptions{})
			} else {
				denseResults, err = index.SearchVectors(ctx, req.QueryVector, req.K*2, nil, SearchOptions{})
			}
			if err != nil {
				denseErr = err
			}
		}()
	}

	// Sparse keyword search (BM25)
	if req.Alpha < 1.0 && req.QueryText != "" {
		if bm25Arena != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sparseResults = searchBM25Arena(bm25Arena, req.QueryText, req.K*2, nil)
			}()
		} else if bm25Legacy != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sparseResults = bm25Legacy.SearchBM25(req.QueryText, req.K*2, nil, s.resultPool)
			}()
		}
	}

	wg.Wait()

	if denseErr != nil {
		return nil, denseErr
	}

	// Fusion via RRF
	var finalResults []SearchResult
	switch req.Alpha {
	case 1.0:
		finalResults = denseResults
	case 0.0:
		finalResults = sparseResults
	default:
		rrfK := 60
		finalResults = ReciprocalRankFusion(req.Dataset, denseResults, sparseResults, rrfK, req.K, s.resultPool)
	}

	if len(finalResults) > req.K {
		finalResults = finalResults[:req.K]
	}

	return finalResults, nil
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
