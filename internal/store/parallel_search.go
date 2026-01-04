package store

import (
	"runtime"
	"sort"
	"sync"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/coder/hnsw"
)

// ParallelSearchConfig controls parallel search behavior
type ParallelSearchConfig struct {
	Enabled   bool
	Workers   int
	Threshold int
}

// DefaultParallelSearchConfig returns sensible defaults
func DefaultParallelSearchConfig() ParallelSearchConfig {
	return ParallelSearchConfig{
		Enabled:   true,
		Workers:   runtime.NumCPU(),
		Threshold: 50,
	}
}

// getParallelSearchConfig returns the current config
func (h *HNSWIndex) getParallelSearchConfig() ParallelSearchConfig {
	return h.parallelConfig
}

// SetParallelSearchConfig updates the parallel search configuration
func (h *HNSWIndex) SetParallelSearchConfig(cfg ParallelSearchConfig) {
	h.parallelConfig = cfg
}

// processResultsParallel processes HNSW neighbors using worker pool
func (h *HNSWIndex) processResultsParallel(query []float32, neighbors []hnsw.Node[VectorID], k int, filters []Filter) []SearchResult {
	cfg := h.getParallelSearchConfig()
	numWorkers := cfg.Workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	chunkSize := (len(neighbors) + numWorkers - 1) / numWorkers
	if chunkSize < 50 { // Increased threshold to avoid overhead for small chunks
		return h.processResultsSerial(query, neighbors, k, filters)
	}

	metrics.HnswParallelSearchSplits.WithLabelValues(h.dataset.Name).Inc()

	// Pre-allocate results array of arrays to avoid mutex on append
	// simpler than shared slice with atomics since result count per chunk varies due to filtering
	chunksResults := make([][]SearchResult, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		if start >= len(neighbors) {
			break
		}
		end := start + chunkSize
		if end > len(neighbors) {
			end = len(neighbors)
		}

		wg.Add(1)
		go func(workerID int, chunk []hnsw.Node[VectorID]) {
			defer wg.Done()
			chunksResults[workerID] = h.processChunk(query, chunk, filters)
		}(i, neighbors[start:end])
	}
	wg.Wait()

	// Merge results
	// First calculate total size
	totalLen := 0
	for _, res := range chunksResults {
		totalLen += len(res)
	}

	allResults := make([]SearchResult, 0, totalLen)
	for _, res := range chunksResults {
		allResults = append(allResults, res...)
	}

	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Score < allResults[j].Score
	})

	if len(allResults) > k {
		allResults = allResults[:k]
	}

	return allResults
}

// processChunk processes a chunk of HNSW neighbors
func (h *HNSWIndex) processChunk(query []float32, neighbors []hnsw.Node[VectorID], filters []Filter) []SearchResult {
	results := make([]SearchResult, 0, len(neighbors))

	// Step 1: Batch location lookup
	// Optimized: Parallel access via ChunkedLocationStore is lock-free for reads
	// and doesn't require global Mu RLock for the list itself.
	locations := make([]Location, len(neighbors))
	found := make([]bool, len(neighbors))

	for i, n := range neighbors {
		nodeID := n.Key
		loc, ok := h.locationStore.Get(nodeID)
		if ok {
			locations[i] = loc
			found[i] = true
		}
	}

	// Step 2: Batch record access and filtering
	// Minimize dataset.dataMu contention

	type vectorTask struct {
		id  VectorID
		vec []float32
	}
	// Pre-allocate to reduce allocs
	tasks := make([]vectorTask, 0, len(neighbors))

	h.dataset.dataMu.RLock()

	var evaluator *FilterEvaluator
	if len(filters) > 0 {
		if len(h.dataset.Records) > 0 {
			evaluator, _ = NewFilterEvaluator(h.dataset.Records[0], filters)
		}
	}

	for i, n := range neighbors {
		if !found[i] {
			continue
		}
		loc := locations[i]

		if loc.BatchIdx >= len(h.dataset.Records) {
			continue
		}
		rec := h.dataset.Records[loc.BatchIdx]

		if evaluator != nil && !evaluator.Matches(loc.RowIdx) {
			continue
		}

		// Extract vector (copy)
		vec, err := h.extractVector(rec, loc.RowIdx)
		if err == nil && vec != nil {
			tasks = append(tasks, vectorTask{id: n.Key, vec: vec})
		}
	}
	h.dataset.dataMu.RUnlock()

	// Step 3: Compute distances using SIMD Batch Processing
	// Flatten vectors for batch API
	vecs := make([][]float32, len(tasks))
	for i, t := range tasks {
		vecs[i] = t.vec
	}

	scores := make([]float32, len(tasks))

	// Use batch SIMD where possible
	if h.pqEnabled && h.pqEncoder != nil {
		table := h.pqEncoder.ComputeDistanceTableFlat(query)
		m := h.pqEncoder.CodeSize()
		packedLen := (m + 3) / 4

		flatCodes := make([]byte, len(vecs)*m)
		validForBatch := make([]bool, len(vecs))
		batchCount := 0

		distFunc := h.GetDistanceFunc()
		for i, v := range vecs {
			if len(v) == packedLen {
				ptr := unsafe.Pointer(&v[0])
				src := unsafe.Slice((*byte)(ptr), m)
				copy(flatCodes[batchCount*m:], src)
				validForBatch[i] = true
				batchCount++
			} else {
				// Fallback for raw / mixed
				scores[i] = distFunc(query, v)
			}
		}

		if batchCount > 0 {
			batchResults := make([]float32, batchCount)
			h.pqEncoder.ADCDistanceBatch(table, flatCodes[:batchCount*m], batchResults)

			bj := 0
			for i := range vecs {
				if validForBatch[i] {
					scores[i] = batchResults[bj]
					bj++
				}
			}
		}
	} else {
		switch h.Metric {
		case MetricEuclidean:
			simd.EuclideanDistanceBatch(query, vecs, scores)
		case MetricCosine:
			simd.CosineDistanceBatch(query, vecs, scores)
		case MetricDotProduct:
			simd.DotProductBatch(query, vecs, scores)
		default:
			simd.EuclideanDistanceBatch(query, vecs, scores)
		}
	}

	for i, t := range tasks {
		results = append(results, SearchResult{
			ID:    t.id,
			Score: scores[i],
		})
	}

	return results
}

// processResultsSerial is the fallback serial implementation
func (h *HNSWIndex) processResultsSerial(query []float32, neighbors []hnsw.Node[VectorID], k int, filters []Filter) []SearchResult {
	distFunc := h.GetDistanceFunc()

	var evaluator *FilterEvaluator
	if len(filters) > 0 {
		h.dataset.dataMu.RLock()
		if len(h.dataset.Records) > 0 {
			evaluator, _ = NewFilterEvaluator(h.dataset.Records[0], filters)
		}
		h.dataset.dataMu.RUnlock()
	}

	res := make([]SearchResult, 0, len(neighbors))
	count := 0

	for _, n := range neighbors {
		if count >= k {
			break
		}

		loc, found := h.GetLocation(n.Key)
		if !found {
			continue
		}

		h.dataset.dataMu.RLock()
		if h.dataset.Records == nil || loc.BatchIdx >= len(h.dataset.Records) {
			h.dataset.dataMu.RUnlock()
			continue
		}
		rec := h.dataset.Records[loc.BatchIdx]

		if evaluator != nil && !evaluator.Matches(loc.RowIdx) {
			h.dataset.dataMu.RUnlock()
			continue
		}

		vec, _ := h.extractVector(rec, loc.RowIdx)
		h.dataset.dataMu.RUnlock()

		if vec == nil {
			continue
		}

		dist := distFunc(query, vec)
		res = append(res, SearchResult{
			ID:    n.Key,
			Score: dist,
		})
		count++
	}

	return res
}
