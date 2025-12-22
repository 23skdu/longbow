package store

import (
	"sort"
)

// ReciprocalRankFusion combines results from multiple search systems using their ranks.
// Formula: score = sum(1 / (k + rank))
// Reference: https://dl.acm.org/doi/10.1145/1571941.1572114
func ReciprocalRankFusion(dense, sparse []SearchResult, k, limit int) []SearchResult {
	scores := make(map[VectorID]float64)

	// k is typically 60 as per the original paper
	if k <= 0 {
		k = 60
	}

	// Add dense results (assumed already sorted by distance DESC or score ASC/DESC)
	// For HNSW results are sorted by distance ASC, so we use their original rank.
	for rank, r := range dense {
		scores[r.ID] += 1.0 / float64(k+rank+1)
	}

	// Add sparse results (assumed already sorted by score DESC)
	for rank, r := range sparse {
		scores[r.ID] += 1.0 / float64(k+rank+1)
	}

	if len(scores) == 0 {
		return nil
	}

	// Convert to slice and sort by score descending
	results := make([]SearchResult, 0, len(scores))
	for id, score := range scores {
		results = append(results, SearchResult{
			ID:    id,
			Score: float32(score),
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if limit > 0 && len(results) > limit {
		return results[:limit]
	}

	return results
}
