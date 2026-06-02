package store

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

// GraphAnalytics provides analysis algorithms for the graph structure.
type GraphAnalytics struct {
	graphProvider func() *types.GraphData
}

// GraphProperties contains basic statistics about the graph structure.
type GraphProperties struct {
	NodeCount int
	EdgeCount int
	AvgDegree float32
	Density   float32
}

// NewGraphAnalytics creates a new GraphAnalytics instance with the provided graph provider.
func NewGraphAnalytics(graphProvider func() *types.GraphData) *GraphAnalytics {
	return &GraphAnalytics{
		graphProvider: graphProvider,
	}
}

// AnalyzeProperties calculates basic graph statistics.
func (ga *GraphAnalytics) AnalyzeProperties(ctx context.Context) (*GraphProperties, error) {
	graph := ga.graphProvider()
	if graph == nil {
		return nil, fmt.Errorf("graph data is nil")
	}

	var nodeCount atomic.Int64
	var edgeCount atomic.Int64

	// Parallel Scan Layer 0
	if len(graph.Neighbors) > 0 && len(graph.Neighbors[0]) > 0 {
		chunks := graph.Neighbors[0]

		// Use a worker pool or simple goroutines per chunk (since they are isolated)
		// We'll use a semaphore to limit concurrency
		maxWorkers := 8 // Default
		sem := make(chan struct{}, maxWorkers)

		var wg sync.WaitGroup
		for cID, offset := range chunks {
			if offset == 0 {
				continue
			}

			wg.Add(1)
			sem <- struct{}{}
			go func(cID int) {
				defer wg.Done()
				defer func() { <-sem }()

				countChunk := graph.GetCountsChunk(0, cID)
				if countChunk == nil {
					return
				}

				localNodes := int64(0)
				localEdges := int64(0)

				for cOff := 0; cOff < types.ChunkSize; cOff++ {
					count := atomic.LoadInt32(&countChunk[cOff])
					if count > 0 {
						localNodes++
						localEdges += int64(count)
					}
				}
				nodeCount.Add(localNodes)
				edgeCount.Add(localEdges)
			}(cID)

			select {
			case <-ctx.Done():
				wg.Wait()
				return nil, ctx.Err()
			default:
			}
		}
		wg.Wait()
	}

	props := &GraphProperties{
		NodeCount: int(nodeCount.Load()),
		EdgeCount: int(edgeCount.Load()),
	}

	if nc := nodeCount.Load(); nc > 0 {
		ec := edgeCount.Load()
		props.AvgDegree = float32(ec) / float32(nc)
		if nc > 1 {
			// Directed density
			props.Density = float32(ec) / (float32(nc) * float32(nc-1))
		}
	}

	return props, nil
}

// CentralityResult holds centrality scores for top nodes.
type CentralityResult struct {
	Scores map[uint32]float32
}

// PageRankConfig controls the PageRank execution.
type PageRankConfig struct {
	DampingFactor float32
	MaxIterations int
	Tolerance     float32
}

// DefaultPageRankConfig returns standard defaults.
func DefaultPageRankConfig() PageRankConfig {
	return PageRankConfig{
		DampingFactor: 0.85,
		MaxIterations: 20,
		Tolerance:     1e-4,
	}
}

// CalculatePageRank computes the PageRank for all nodes in the graph (Layer 0).
// Note: This builds a reverse adjacency list in memory, which can be expensive.
func (ga *GraphAnalytics) CalculatePageRank(ctx context.Context, config PageRankConfig) (*CentralityResult, error) {
	graph := ga.graphProvider()
	if graph == nil {
		return nil, fmt.Errorf("graph data is nil")
	}

	// 1. Identification & Reverse Indexing
	// We need to map which nodes point to a node to compute PR.
	// HNSW stores u -> [v1, v2...]. PageRank needs v -> [u1, u2...]

	// Map valid nodes to dense indices (0..N-1) for array efficiency?
	// For simplicity, we'll use a map[uint32][]uint32 first, or flat arrays if IDs are compact.
	// Assuming IDs are roughly compact around 0..Capacity.

	// Let's use a map for reverse adjacency to be safe with sparse IDs,
	// but arrays for scores if we can determine max ID.

	// 1. Determine Max Node ID and Node Count
	capacity := graph.Capacity
	validNodes := make([]bool, capacity)
	outDegrees := make([]int32, capacity)

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU()
	chunkSize := (capacity + numWorkers - 1) / numWorkers

	for w := 0; w < numWorkers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > capacity {
			end = capacity
		}
		if start >= end {
			break
		}

		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			for i := s; i < e; i++ {
				neighbors := graph.GetNeighbors(0, uint32(i), nil) // #nosec G115
				if len(neighbors) > 0 {
					validNodes[i] = true
					outDegrees[i] = int32(len(neighbors)) // #nosec G115
				}
			}
		}(start, end)
	}
	wg.Wait()

	realMaxID := uint32(0)
	realNodeCount := 0
	for i, valid := range validNodes {
		if valid {
			realNodeCount++
			realMaxID = uint32(i)
		}
	}

	if realNodeCount == 0 {
		return &CentralityResult{Scores: map[uint32]float32{}}, nil
	}

	// 2. Build Reverse Adjacency
	reverseAdj := make([][]uint32, realMaxID+1)
	for i, valid := range validNodes {
		if valid {
			neighbors := graph.GetNeighbors(0, uint32(i), nil) // #nosec G115
			for _, nbr := range neighbors {
				if nbr < uint32(len(reverseAdj)) { // #nosec G115
					reverseAdj[nbr] = append(reverseAdj[nbr], uint32(i))
				}
			}
		}
	}

	// 3. Initialize Scores
	scores := make([]float32, realMaxID+1)
	initialScore := 1.0 / float32(realNodeCount)
	for i, valid := range validNodes {
		if valid {
			scores[i] = initialScore
		}
	}

	newScores := make([]float32, realMaxID+1)

	// 4. PageRank Iterations
	for iter := 0; iter < config.MaxIterations; iter++ {
		danglingSum := float32(0.0)
		for i, valid := range validNodes {
			if valid && outDegrees[i] == 0 {
				danglingSum += scores[i]
			}
		}

		danglingFactor := (config.DampingFactor * danglingSum) / float32(realNodeCount)
		baseScore := (1.0-config.DampingFactor)/float32(realNodeCount) + danglingFactor

		// Parallel Update
		var iterWG sync.WaitGroup
		for w := 0; w < numWorkers; w++ {
			start := w * chunkSize
			end := start + chunkSize
			if end > int(realMaxID)+1 {
				end = int(realMaxID) + 1
			}
			if start >= end {
				break
			}

			iterWG.Add(1)
			go func(s, e int) {
				defer iterWG.Done()
				for i := s; i < e; i++ {
					if !validNodes[i] {
						continue
					}

					incomingSum := float32(0.0)
					for j, source := range reverseAdj[i] {
						if j+4 < len(reverseAdj[i]) {
							nextSource := reverseAdj[i][j+4]
							// Prefetch the arrays for future iterations to mask memory latency
							simd.Prefetch(unsafe.Pointer(&outDegrees[nextSource])) // #nosec G103
							simd.Prefetch(unsafe.Pointer(&scores[nextSource]))     // #nosec G103
						}
						degree := outDegrees[source]
						if degree > 0 {
							incomingSum += scores[source] / float32(degree)
						}
					}
					newScores[i] = baseScore + (config.DampingFactor * incomingSum)
				}
			}(start, end)
		}
		iterWG.Wait()

		// Convergence check and update scores
		maxDiff := float32(0.0)
		for i, valid := range validNodes {
			if valid {
				d := newScores[i] - scores[i]
				if d < 0 {
					d = -d
				}
				if d > maxDiff {
					maxDiff = d
				}
				scores[i] = newScores[i]
			}
		}

		if maxDiff < config.Tolerance {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	// 5. Build Result
	resScores := make(map[uint32]float32)
	for i, valid := range validNodes {
		if valid {
			resScores[uint32(i)] = scores[i] // #nosec G115
		}
	}

	return &CentralityResult{Scores: resScores}, nil
}

// CommunityResult holds the detected communities.
type CommunityResult struct {
	CommunityCount int
	Labels         map[uint32]uint32 // NodeID -> CommunityID
}

// DetectCommunities runs Label Propagation Algorithm (LPA).
func (ga *GraphAnalytics) DetectCommunities(ctx context.Context, maxIter int) (*CommunityResult, error) {
	graph := ga.graphProvider()
	if graph == nil {
		return nil, fmt.Errorf("graph data is nil")
	}

	// 1. Initialize labels: each node has its own Label = NodeID
	labels := make(map[uint32]uint32)
	nodes := make([]uint32, 0)

	// Identifying valid nodes
	if len(graph.Neighbors) > 0 && len(graph.Neighbors[0]) > 0 {
		chunks := graph.Neighbors[0]
		for cID, offset := range chunks {
			if offset == 0 {
				continue
			}
			countChunk := graph.GetCountsChunk(0, cID)
			if countChunk == nil {
				continue
			}

			for cOff := 0; cOff < types.ChunkSize; cOff++ {
				// Only consider nodes with neighbors for propagation?
				// Isolated nodes form their own community.
				if atomic.LoadInt32(&countChunk[cOff]) > 0 {
					id := uint32(cID*types.ChunkSize + cOff)
					labels[id] = id
					nodes = append(nodes, id)
				}
			}
		}
	}

	if len(nodes) == 0 {
		return &CommunityResult{CommunityCount: 0, Labels: map[uint32]uint32{}}, nil
	}

	// 2. Iterations
	changed := true
	for i := 0; i < maxIter && changed; i++ {
		changed = false

		// Shuffle nodes? (LPA requires random order for stability/convergence usually)
		// Skipping shuffle for determinism/simplicity for now,
		// but typically we should shuffle `nodes` slice.

		for _, node := range nodes {
			neighbors := graph.GetNeighbors(0, node, nil)
			if len(neighbors) == 0 {
				continue
			}

			// Count labels
			labelCounts := make(map[uint32]int)
			for _, nbr := range neighbors {
				l, ok := labels[nbr]
				if ok {
					labelCounts[l]++
				}
			}

			// Find max
			var maxLabel uint32
			maxCount := -1
			// Tie-breaking: usually random. Here: consistently maxLabel.

			for l, c := range labelCounts {
				if c > maxCount {
					maxCount = c
					maxLabel = l
				} else if c == maxCount {
					if l > maxLabel { // Deterministic tie-break
						maxLabel = l
					}
				}
			}

			if labels[node] != maxLabel {
				labels[node] = maxLabel
				changed = true
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	// Count unique communities
	unique := make(map[uint32]bool)
	for _, l := range labels {
		unique[l] = true
	}

	return &CommunityResult{
		CommunityCount: len(unique),
		Labels:         labels,
	}, nil
}
