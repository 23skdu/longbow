package store

import (
	"errors"
)

// HybridQuery represents a hybrid search query combining vector and BM25 search
type HybridQuery struct {
	Enabled   bool    // Whether hybrid search is enabled for this query
	TextQuery string  // The text query for BM25/keyword search
	Alpha     float64 // Weighting: 1.0 = pure vector, 0.0 = pure keyword
	K         int     // Number of results to return
}

// DefaultHybridQuery returns a HybridQuery with sensible defaults
func DefaultHybridQuery() HybridQuery {
	return HybridQuery{
		Enabled:   false,
		TextQuery: "",
		Alpha:     0.5, // Equal weighting by default
		K:         10,
	}
}

// Validate checks if the HybridQuery is valid
func (q *HybridQuery) Validate() error {
	if q.Alpha < 0.0 || q.Alpha > 1.0 {
		return errors.New("alpha must be between 0.0 and 1.0")
	}
	if q.K <= 0 {
		return errors.New("k must be positive")
	}
	if q.Enabled && q.TextQuery == "" {
		return errors.New("text query required when hybrid search is enabled")
	}
	return nil
}

// IsPureVector returns true if this is a pure vector search (alpha=1.0)
func (q *HybridQuery) IsPureVector() bool {
	return q.Alpha == 1.0
}

// IsPureKeyword returns true if this is a pure keyword/BM25 search (alpha=0.0)
func (q *HybridQuery) IsPureKeyword() bool {
	return q.Alpha == 0.0
}

// CombineHybridResults combines vector and BM25 results using alpha weighting
// vector scores are distances (lower is better), BM25 scores are relevance (higher is better)
func CombineHybridResults(vectorResults, bm25Results []SearchResult, alpha float32, k int) []SearchResult {
	if len(vectorResults) == 0 && len(bm25Results) == 0 {
		return nil
	}

	// Use RRF by default as it handles different score scales well
	return ReciprocalRankFusion(vectorResults, bm25Results, 60, k)
}

// CombineHybridResultsRRF combines results using Reciprocal Rank Fusion (legacy alignment)
func CombineHybridResultsRRF(vectorResults, bm25Results []SearchResult, rrfK, limit int) []SearchResult {
	return ReciprocalRankFusion(vectorResults, bm25Results, rrfK, limit)
}
