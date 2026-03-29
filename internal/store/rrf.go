package store

import (
	"sort"

	"github.com/23skdu/longbow/internal/metrics"
)

// ReciprocalRankFusion combines results from multiple search systems using their ranks.
// Formula: score = sum(1 / (k + rank))
// Reference: https://dl.acm.org/doi/10.1145/1571941.1572114
func ReciprocalRankFusion(dataset string, dense, sparse []SearchResult, k, limit int) []SearchResult {
	scores := make(map[VectorID]float64)
	denseSet := make(map[VectorID]bool)
	sparseSet := make(map[VectorID]bool)

	// k is typically 60 as per the original paper
	if k <= 0 {
		k = 60
	}

	// Add dense results (assumed already sorted by distance DESC or score ASC/DESC)
	// For HNSW results are sorted by distance ASC, so we use their original rank.
	for rank, r := range dense {
		scores[r.ID] += 1.0 / float64(k+rank+1)
		denseSet[r.ID] = true
	}

	// Add sparse results (assumed already sorted by score DESC)
	for rank, r := range sparse {
		scores[r.ID] += 1.0 / float64(k+rank+1)
		sparseSet[r.ID] = true
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
		results = results[:limit]
	}

	// Report metrics
	if dataset != "" && len(results) > 0 {
		var denseCount, sparseCount int
		for _, r := range results {
			fromDense := denseSet[r.ID]
			fromSparse := sparseSet[r.ID]

			if fromDense {
				denseCount++
				metrics.HybridResultOriginTotal.WithLabelValues(dataset, "dense").Inc()
			}
			if fromSparse {
				sparseCount++
				metrics.HybridResultOriginTotal.WithLabelValues(dataset, "sparse").Inc()
			}
		}

		total := float64(len(results))
		metrics.HybridDenseResultRatio.WithLabelValues(dataset).Set(float64(denseCount) / total)
		metrics.HybridSparseResultRatio.WithLabelValues(dataset).Set(float64(sparseCount) / total)
	}

	return results
}
