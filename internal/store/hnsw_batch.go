package store

import (
	"math"
	"sort"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
)

// RankedResult represents a search result with its distance score.
// Used for reranking and batch search operations that return distances.
type RankedResult struct {
	ID       VectorID
	Distance float32
}

// SearchWithBatchDistance performs k-NN search using batch distance calculations.
// This is a two-stage retrieval:
// 1. Coarse search using the HNSW graph to get initial candidates
// 2. Batch distance calculation on all candidates for precise ranking
//
// Using batch SIMD operations provides significant speedup over per-vector
// distance calculations by reducing function call overhead and maximizing
// CPU pipeline utilization.
func (h *HNSWIndex) SearchWithBatchDistance(query []float32, k int) []RankedResult {
	if len(query) == 0 || k <= 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		metrics.BatchDistanceDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	// Stage 1: Get candidates from HNSW graph
	// Request more candidates for better recall after batch reranking
	expandedK := k * 2
	if expandedK < 20 {
		expandedK = 20 // Minimum candidates for good coverage
	}

	neighbors := h.Graph.Search(query, expandedK)
	if len(neighbors) == 0 {
		return nil
	}

	// Stage 2: Collect candidate vectors for batch distance calculation
	candidateVectors := make([][]float32, 0, len(neighbors))
	candidateIDs := make([]VectorID, 0, len(neighbors))

	for _, n := range neighbors {
		vec := h.getVector(n.Key)
		if vec != nil {
			candidateVectors = append(candidateVectors, vec)
			candidateIDs = append(candidateIDs, n.Key)
		}
	}

	if len(candidateVectors) == 0 {
		return nil
	}

	// Stage 3: Batch distance calculation using SIMD
	metrics.BatchDistanceCallsTotal.Inc()
	metrics.BatchDistanceBatchSize.Observe(float64(len(candidateVectors)))

	distances := make([]float32, len(candidateVectors))
	h.computeBatchDistance(query, candidateVectors, distances)

	// Stage 4: Build and sort ranked results
	ranked := make([]RankedResult, len(candidateIDs))
	for i, id := range candidateIDs {
		ranked[i] = RankedResult{ID: id, Distance: distances[i]}
	}

	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].Distance < ranked[j].Distance
	})

	// Return top-k
	if k > len(ranked) {
		k = len(ranked)
	}

	return ranked[:k]
}

// SearchBatchOptimized performs k-NN search for multiple query vectors
// using optimized batch distance calculations.
//
// This method is more efficient than calling SearchWithBatchDistance multiple
// times because it can amortize the overhead of vector collection and leverage
// cache locality when processing multiple queries.
func (h *HNSWIndex) SearchBatchOptimized(queries [][]float32, k int) [][]RankedResult {
	if len(queries) == 0 || k <= 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		metrics.BatchDistanceDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	results := make([][]RankedResult, len(queries))

	// Expanded k for better recall
	expandedK := k * 2
	if expandedK < 20 {
		expandedK = 20
	}

	// Collect all unique candidate IDs across queries for potential reuse
	allCandidatesMap := make(map[VectorID][]float32)
	queryCandidates := make([][]VectorID, len(queries))

	// Stage 1: Gather candidates for all queries
	for qi, query := range queries {
		neighbors := h.Graph.Search(query, expandedK)
		queryCandidates[qi] = make([]VectorID, 0, len(neighbors))

		for _, n := range neighbors {
			queryCandidates[qi] = append(queryCandidates[qi], n.Key)
			if _, exists := allCandidatesMap[n.Key]; !exists {
				if vec := h.getVector(n.Key); vec != nil {
					allCandidatesMap[n.Key] = vec
				}
			}
		}
	}

	// Stage 2: Process each query with batch distance calculation
	for qi, query := range queries {
		candIDs := queryCandidates[qi]
		if len(candIDs) == 0 {
			results[qi] = nil
			continue
		}

		// Collect vectors for this query's candidates
		candVectors := make([][]float32, 0, len(candIDs))
		validIDs := make([]VectorID, 0, len(candIDs))

		for _, id := range candIDs {
			if vec, exists := allCandidatesMap[id]; exists {
				candVectors = append(candVectors, vec)
				validIDs = append(validIDs, id)
			}
		}

		if len(candVectors) == 0 {
			results[qi] = nil
			continue
		}

		// Batch distance calculation
		metrics.BatchDistanceCallsTotal.Inc()
		metrics.BatchDistanceBatchSize.Observe(float64(len(candVectors)))

		distances := make([]float32, len(candVectors))
		h.computeBatchDistance(query, candVectors, distances)

		// Build ranked results
		ranked := make([]RankedResult, len(validIDs))
		for i, id := range validIDs {
			ranked[i] = RankedResult{ID: id, Distance: distances[i]}
		}

		sort.Slice(ranked, func(i, j int) bool {
			return ranked[i].Distance < ranked[j].Distance
		})

		// Return top-k for this query
		topK := k
		if topK > len(ranked) {
			topK = len(ranked)
		}
		results[qi] = ranked[:topK]
	}

	return results
}

// computeBatchDistance computes the distance between a query and multiple vectors using the index's metric.
func (h *HNSWIndex) computeBatchDistance(query []float32, vectors [][]float32, results []float32) {
	if h.batchDistFunc != nil {
		h.batchDistFunc(query, vectors, results)
		return
	}
	simd.EuclideanDistanceBatch(query, vectors, results)
}

// SearchBatch is a convenience method that calls SearchBatchOptimized.
func (h *HNSWIndex) SearchBatch(queries [][]float32, k int) [][]RankedResult {
	return h.SearchBatchOptimized(queries, k)
}

// RerankBatch computes exact distances for a set of candidate IDs and returns top-k.
func (h *HNSWIndex) RerankBatch(query []float32, candidateIDs []VectorID, k int) []RankedResult {
	if len(query) == 0 || len(candidateIDs) == 0 || k <= 0 {
		return nil
	}

	h.mu.RLock()
	// Collect vectors
	vectors := make([][]float32, 0, len(candidateIDs))
	validIDs := make([]VectorID, 0, len(candidateIDs))
	for _, id := range candidateIDs {
		if vec := h.getVector(id); vec != nil {
			vectors = append(vectors, vec)
			validIDs = append(validIDs, id)
		}
	}
	h.mu.RUnlock()

	if len(vectors) == 0 {
		return nil
	}

	distances := make([]float32, len(vectors))
	h.computeBatchDistance(query, vectors, distances)

	ranked := make([]RankedResult, len(validIDs))
	for i, id := range validIDs {
		ranked[i] = RankedResult{ID: id, Distance: distances[i]}
	}

	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].Distance < ranked[j].Distance
	})

	if k > len(ranked) {
		k = len(ranked)
	}

	return ranked[:k]
}

// SearchBatchWithArena performs batch search using an arena allocator.
func (h *HNSWIndex) SearchBatchWithArena(queries [][]float32, k int, arena *SearchArena) [][]RankedResult {
	if len(queries) == 0 {
		return nil
	}
	results := make([][]RankedResult, len(queries))
	for i, q := range queries {
		ids := h.SearchWithArena(q, k, arena)
		if len(ids) == 0 {
			results[i] = nil
			continue
		}

		// Map IDs to RankedResults (requires distance calculation)
		// We could use computeBatchDistance or single distance calc
		ranked := make([]RankedResult, len(ids))
		for j, id := range ids {
			vec := h.getVector(id)
			dist := float32(0)
			if vec != nil {
				d, err := simd.EuclideanDistance(q, vec) // Assuming Euclidean default for test
				if err != nil {
					dist = math.MaxFloat32
				} else {
					dist = d
				}
			}
			ranked[j] = RankedResult{ID: id, Distance: dist}
		}
		results[i] = ranked
	}
	return results
}
