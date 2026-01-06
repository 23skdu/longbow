package store

import (
	"context"
	"sort"
	"strings"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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

// HybridSearch performs a hybrid search (placeholder implementation).
func (s *VectorStore) HybridSearch(ctx context.Context, req HybridSearchRequest) ([]SearchResult, error) {
	// 1. Vector Search
	if len(req.QueryVector) > 0 {
		// Call internal vector search
		// Assuming we can find the index by some means or iterate datasets?
		// But this method on VectorStore usually iterates datasets?
		// Or is it for a specific dataset?
		// The params doesn't distinguish dataset.
		// Usually HybridSearch is per dataset or global?
		// In `vector_search_action.go`, it calls `s.SearchHybrid(..., req.Dataset, ...)`
		// So `SearchHybrid` takes dataset name.
		return nil, nil
	}
	return nil, nil
}

// SearchHybrid is the method called by vector_search_action.go
// It performs Reciprocal Rank Fusion (RRF) or similar combination.
func (s *VectorStore) SearchHybrid(ctx context.Context, datasetName string, queryVec []float32, textQuery string, k int, alpha float32, rrfK int) ([]SearchResult, error) {
	ds, err := s.getDataset(datasetName)
	if err != nil {
		return nil, err
	}

	// 1. Vector Search
	var vecResults []SearchResult
	if len(queryVec) > 0 {
		ds.dataMu.RLock()
		if ds.Index != nil {
			vecResults, _ = ds.Index.SearchVectors(queryVec, k*2, nil) // Get more for re-ranking
			// Map IDs
			vecResults = s.MapInternalToUserIDs(ds, vecResults)
		}
		ds.dataMu.RUnlock()
	}

	// 2. Text Search (BM25) - Placeholder
	// Ideally we have an inverted index.
	var textResults []SearchResult
	if textQuery != "" {
		// Mock BM25 search or call actual if available
		// s.invertedIndex.Search(textQuery) ...
	}

	// 3. Fusion (RRF or Alpha-weighted)
	// For simplicity, if we have both, we use RRF.
	if len(vecResults) > 0 && len(textResults) > 0 {
		return RankFusion(vecResults, textResults, k, rrfK), nil
	} else if len(vecResults) > 0 {
		if len(vecResults) > k {
			vecResults = vecResults[:k]
		}
		return vecResults, nil
	} else if len(textResults) > 0 {
		return textResults, nil
	}

	return []SearchResult{}, nil
}

// RankFusion performs Reciprocal Rank Fusion.
func RankFusion(list1, list2 []SearchResult, k int, rrfK int) []SearchResult {
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
func (s *VectorStore) HybridSearchWithBitmap(ctx context.Context, req HybridSearchRequest) ([]SearchResult, error) {
	// Placeholder
	return nil, nil
}

// findStringColumn helper
func findStringColumn(rec arrow.RecordBatch, name string) *array.String {
	for i, f := range rec.Schema().Fields() {
		if strings.EqualFold(f.Name, name) {
			if col, ok := rec.Column(i).(*array.String); ok {
				return col
			}
		}
	}
	return nil
}

// EstimateAlpha calculates a heuristic alpha value based on query length.
func EstimateAlpha(query string) float32 {
	tokens := strings.Fields(query)
	n := len(tokens)
	if n < 3 {
		return 0.3
	}
	if n <= 5 {
		return 0.5
	}
	return 0.8
}
