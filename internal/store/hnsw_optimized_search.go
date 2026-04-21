package store

import (
	"container/heap"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/simd"
)

type HNSWSearchConfig struct {
	EnablePrefetch            bool
	PrefetchDistance          int
	EnableEarlyTermination    bool
	EarlyTerminationThreshold float32
	ParallelSearch            bool
	NumSearchThreads          int
	BatchSize                 int
}

var DefaultHNSWSearchConfig = HNSWSearchConfig{
	EnablePrefetch:            true,
	PrefetchDistance:          3,
	EnableEarlyTermination:    true,
	EarlyTerminationThreshold: 0.95,
	ParallelSearch:            false,
	NumSearchThreads:          4,
	BatchSize:                 64,
}

type OptimizedSearchContext struct {
	PrefetchEnabled bool
	PrefetchDist    int
}

func (ctx *OptimizedSearchContext) PrefetchNode(nodeID uint32) {
	if !ctx.PrefetchEnabled {
		return
	}
	_ = nodeID
}

type BeamSearchCandidate struct {
	ID    uint32
	Dist  float32
	Layer int
}

type BeamSearchHeap []BeamSearchCandidate

func (h BeamSearchHeap) Len() int           { return len(h) }
func (h BeamSearchHeap) Less(i, j int) bool { return h[i].Dist < h[j].Dist }
func (h BeamSearchHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *BeamSearchHeap) Push(x any) {
	*h = append(*h, x.(BeamSearchCandidate))
}

func (h *BeamSearchHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func BeamSearchOptimized(
	query []float32,
	vectors [][]float32,
	ef int,
	config HNSWSearchConfig,
) []BeamSearchCandidate {
	if len(vectors) == 0 {
		return nil
	}

	numResults := ef
	if numResults > len(vectors) {
		numResults = len(vectors)
	}

	candidates := &BeamSearchHeap{}
	results := make([]BeamSearchCandidate, 0, numResults)
	visited := make([]bool, len(vectors))

	heap.Init(candidates)

	if len(query) == 0 || len(vectors) == 0 {
		return results
	}

	firstVec := vectors[0]
	if len(firstVec) == 0 {
		return results
	}

	firstDist, _ := simd.EuclideanDistance(query, firstVec)
	heap.Push(candidates, BeamSearchCandidate{ID: 0, Dist: firstDist, Layer: 0})
	visited[0] = true

	for candidates.Len() > 0 {
		if config.EnableEarlyTermination && len(results) >= numResults && candidates.Len() > 0 {
			bestDist := results[0].Dist
			worstCandDist := (*candidates)[0].Dist
			if worstCandDist > bestDist*config.EarlyTerminationThreshold {
				break
			}
		}

		current := heap.Pop(candidates).(BeamSearchCandidate)

		results = append(results, current)
		if len(results) >= numResults {
			continue
		}

		startIdx := int(current.ID) + 1
		if startIdx >= len(vectors) {
			continue
		}

		endIdx := startIdx + config.BatchSize
		if endIdx > len(vectors) {
			endIdx = len(vectors)
		}

		for i := startIdx; i < endIdx; i++ {
			if visited[i] {
				continue
			}
			visited[i] = true

			dist, err := simd.EuclideanDistance(query, vectors[i])
			if err != nil {
				continue
			}

			if len(results) < numResults || dist < results[len(results)-1].Dist {
				heap.Push(candidates, BeamSearchCandidate{
					ID:    uint32(i), // #nosec G115
					Dist:  dist,
					Layer: 0,
				})
			}
		}
	}

	if len(results) > numResults {
		results = results[:numResults]
	}

	return results
}

type ParallelSearchResult struct {
	mu      sync.Mutex
	results []BeamSearchCandidate
	idx     uint32
	counter uint32
	total   uint32
}

func NewParallelSearchResult(k int, total uint32) *ParallelSearchResult {
	return &ParallelSearchResult{
		results: make([]BeamSearchCandidate, 0, k),
		total:   total,
	}
}

func (r *ParallelSearchResult) Add(candidate BeamSearchCandidate) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.results = append(r.results, candidate)
	if len(r.results) > cap(r.results) {
		r.results = r.results[:len(r.results)-1]
	}
}

func (r *ParallelSearchResult) Get() []BeamSearchCandidate {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make([]BeamSearchCandidate, len(r.results))
	copy(result, r.results)
	return result
}

func ParallelBeamSearch(
	query []float32,
	vectors [][]float32,
	ef int,
	numThreads int,
	config HNSWSearchConfig,
) []BeamSearchCandidate {
	if len(vectors) == 0 {
		return nil
	}

	numResults := ef
	if numResults > len(vectors) {
		numResults = len(vectors)
	}

	vectorsPerThread := len(vectors) / numThreads
	if vectorsPerThread == 0 {
		numThreads = 1
		vectorsPerThread = len(vectors)
	}

	var wg sync.WaitGroup
	sharedResult := NewParallelSearchResult(numResults, uint32(len(vectors))) // #nosec G115

	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			start := threadID * vectorsPerThread
			end := start + vectorsPerThread
			if threadID == numThreads-1 {
				end = len(vectors)
			}

			threadVectors := vectors[start:end]
			threadResults := BeamSearchOptimized(query, threadVectors, numResults, config)

			for i := range threadResults {
				threadResults[i].ID = uint32(int(threadResults[i].ID) + start) // #nosec G115
				sharedResult.Add(threadResults[i])
			}
		}(t)
	}

	wg.Wait()

	finalResults := sharedResult.Get()

	for len(finalResults) > numResults {
		minIdx := 0
		minDist := finalResults[0].Dist
		for i := 1; i < len(finalResults); i++ {
			if finalResults[i].Dist < minDist {
				minDist = finalResults[i].Dist
				minIdx = i
			}
		}
		finalResults = append(finalResults[:minIdx], finalResults[minIdx+1:]...)
	}

	return finalResults
}

func CalculateMinPossibleDistance(query []float32, neighborCount int, avgDist float32) float32 {
	return avgDist * float32(neighborCount) * 0.5
}

type SearchMetrics struct {
	NodesVisited   int32
	Computations   int32
	EarlyTermCount int32
	PrefetchHits   int32
	PrefetchMisses int32
}

func (m *SearchMetrics) RecordVisit() {
	atomic.AddInt32(&m.NodesVisited, 1)
}

func (m *SearchMetrics) RecordComputation() {
	atomic.AddInt32(&m.Computations, 1)
}

func (m *SearchMetrics) RecordEarlyTermination() {
	atomic.AddInt32(&m.EarlyTermCount, 1)
}

func (m *SearchMetrics) Get() (visited, computations, earlyTerm int32) {
	visited = atomic.LoadInt32(&m.NodesVisited)
	computations = atomic.LoadInt32(&m.Computations)
	earlyTerm = atomic.LoadInt32(&m.EarlyTermCount)
	return
}

func (m *SearchMetrics) Reset() {
	atomic.StoreInt32(&m.NodesVisited, 0)
	atomic.StoreInt32(&m.Computations, 0)
	atomic.StoreInt32(&m.EarlyTermCount, 0)
	atomic.StoreInt32(&m.PrefetchHits, 0)
	atomic.StoreInt32(&m.PrefetchMisses, 0)
}
