package store

import (
	"context"
	"errors"
	"math"
	"sort"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

// Recommend implements the hybrid vector-graph recommendation logic (v0.1.9).
// It blends vector similarity to the centroid of seeds with graph-based connectivity.
func (s *VectorStore) Recommend(ctx context.Context, req *qry.RecommendRequest) ([]lbtypes.SearchResult, error) {
	start := time.Now()
	dataset := req.Dataset

	// 1. Get Dataset
	ds, ok := s.getDataset(dataset)
	if !ok {
		metrics.RecommendationsTotal.WithLabelValues(dataset, "error").Inc()
		return nil, errors.New("dataset not found")
	}

	metrics.RecommendationsSeedCount.WithLabelValues(dataset).Observe(float64(len(req.SeedIDs)))

	// 2. Resolve Seed Vectors and Internal IDs
	var seedVectors [][]float32
	var coreSeedIDs []lbtypes.VectorID

	ds.dataMu.RLock()
	for _, uid := range req.SeedIDs {
		if loc, ok := ds.PrimaryIndex[uid]; ok {
			// Resolve internal ID (Batch*ChunkSize + Row)
			internalID := lbtypes.VectorID(loc.BatchIdx*lbtypes.ChunkSize + loc.RowIdx) // #nosec G115
			if loc.BatchIdx < len(ds.Records) {
				rec := ds.Records[loc.BatchIdx]
				vec, err := extractVectorFromCol(rec, loc.RowIdx)
				if err == nil && vec != nil {
					seedVectors = append(seedVectors, vec)
					coreSeedIDs = append(coreSeedIDs, internalID)
				}
			}
		}
	}
	ds.dataMu.RUnlock()

	// Handle empty seed resolution (graceful error)
	if len(seedVectors) == 0 {
		metrics.RecommendationsTotal.WithLabelValues(dataset, "error").Inc()
		return nil, errors.New("no valid seeds found in dataset")
	}

	// 3. Compute Centroid
	centroid, err := computeCentroid(seedVectors)
	if err != nil {
		metrics.RecommendationsTotal.WithLabelValues(dataset, "error").Inc()
		return nil, err
	}

	// 4. Initial ANN Search for Candidates
	// We oversample to allow graph connectivity to re-rank candidates outside top K dense vectors
	searchK := req.K * 3
	if searchK < 50 {
		searchK = 50
	}

	candidates, err := ds.Index.SearchVectors(ctx, centroid, searchK, nil, SearchOptions{
		IncludeVectors: false,
	})
	if err != nil {
		metrics.RecommendationsTotal.WithLabelValues(dataset, "error").Inc()
		return nil, err
	}

	// 5. Calculate Graph Connectivity (Multi-Hop Closeness with Decay)
	connectivity := s.getGraphConnectivity(ds, coreSeedIDs, req.MaxHops, req.Decay)

	// 6. Hybrid Re-scoring
	// Formula: α * similarity(centroid, v) + (1-α) * connectivity(seeds, v)
	alpha := req.Alpha
	for i := range candidates {
		sim := candidates[i].Score
		conn := connectivity[candidates[i].ID]
		candidates[i].Score = (alpha * sim) + ((1.0 - alpha) * conn)
	}

	// 7. Re-sort by Hybrid Score
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score // Max-score first
	})

	// 8. Trim to Requested K
	if len(candidates) > req.K {
		candidates = candidates[:req.K]
	}

	// 9. Map Internal IDs back to User IDs for the response
	results := s.MapInternalToUserIDs(ds, candidates)

	metrics.RecommendationsLatencySeconds.WithLabelValues(dataset).Observe(time.Since(start).Seconds())
	metrics.RecommendationsTotal.WithLabelValues(dataset, "success").Inc()

	return results, nil
}

// getGraphConnectivity performs a multi-seed BFS traversal to assign closeness scores.
// It returns a map of VectorID to a connectivity score [0.0, 1.0].
func (s *VectorStore) getGraphConnectivity(ds *Dataset, seeds []lbtypes.VectorID, maxHops int, decay float32) map[lbtypes.VectorID]float32 {
	if maxHops < 1 {
		maxHops = 1
	}
	if decay <= 0 {
		decay = 0.5
	}
	if decay > 1.0 {
		decay = 1.0
	}

	scores := make(map[lbtypes.VectorID]float32)
	queue := seeds
	visited := make(map[lbtypes.VectorID]int)

	// Seeds themselves get the maximum connectivity score
	for _, sid := range seeds {
		visited[sid] = 0
		scores[sid] = 1.0
	}

	// BFS Layers (Multi-hop closeness)
	for h := 0; h < maxHops; h++ {
		var nextQueue []lbtypes.VectorID
		reward := float32(math.Pow(float64(decay), float64(h+1)))

		for _, id := range queue {
			// Get outgoing edges for connectivity
			edges := ds.Graph.GetEdgesBySubject(uint32(id))
			for _, e := range edges {
				neighborID := lbtypes.VectorID(e.Object)
				if _, seen := visited[neighborID]; !seen {
					visited[neighborID] = h + 1
					scores[neighborID] = reward
					nextQueue = append(nextQueue, neighborID)
				}
			}
		}
		if len(nextQueue) == 0 {
			break
		}
		queue = nextQueue
	}

	return scores
}
