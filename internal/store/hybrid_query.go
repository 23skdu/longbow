package store

import (
"errors"
"sort"
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
func CombineHybridResults(vectorResults, bm25Results []SearchResult, alpha float64, k int) []SearchResult {
if len(vectorResults) == 0 && len(bm25Results) == 0 {
return nil
}

// Use RRF by default as it handles different score scales well
return CombineHybridResultsRRF(vectorResults, bm25Results, 60, k)
}

// CombineHybridResultsRRF combines results using Reciprocal Rank Fusion
// RRF score = sum(1 / (k + rank)) across all result sets where item appears
func CombineHybridResultsRRF(vectorResults, bm25Results []SearchResult, rrfK, limit int) []SearchResult {
scores := make(map[VectorID]float64)

// Add RRF scores from vector results (already sorted by distance, lowest first)
for rank, r := range vectorResults {
scores[r.ID] += 1.0 / float64(rrfK+rank+1)
}

// Add RRF scores from BM25 results (already sorted by score, highest first)
for rank, r := range bm25Results {
scores[r.ID] += 1.0 / float64(rrfK+rank+1)
}

// Convert to slice and sort by combined score (higher is better)
type scoredResult struct {
id    VectorID
score float64
}

results := make([]scoredResult, 0, len(scores))
for id, score := range scores {
results = append(results, scoredResult{id: id, score: score})
}

sort.Slice(results, func(i, j int) bool {
return results[i].score > results[j].score // Higher score = better
})

// Limit results
if limit > 0 && len(results) > limit {
results = results[:limit]
}

// Convert back to SearchResult (using score as Score field for output)
output := make([]SearchResult, len(results))
for i, r := range results {
output[i] = SearchResult{
ID:       r.id,
Score: float32(r.score), // RRF score (higher is better)
}
}

return output
}
