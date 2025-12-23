package store

import (
	"runtime"
	"sort"
	"sync"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	if chunkSize < 10 {
		return h.processResultsSerial(query, neighbors, k, filters)
	}

	resultsChan := make(chan []SearchResult, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(neighbors) {
			break
		}
		if end > len(neighbors) {
			end = len(neighbors)
		}

		wg.Add(1)
		go func(chunk []hnsw.Node[VectorID]) {
			defer wg.Done()
			results := h.processChunk(query, chunk, filters)
			resultsChan <- results
		}(neighbors[start:end])
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	allResults := make([]SearchResult, 0, len(neighbors))
	for results := range resultsChan {
		allResults = append(allResults, results...)
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
	// Minimize h.mu contention by holding it once for the chunk
	locations := make([]Location, len(neighbors))
	found := make([]bool, len(neighbors))

	h.mu.RLock()
	for i, n := range neighbors {
		if int(n.Key) < len(h.locations) {
			locations[i] = h.locations[n.Key]
			found[i] = true
		}
	}
	h.mu.RUnlock()

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
		vec := h.extractVector(rec, loc.RowIdx)
		if vec != nil {
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
	// Note: PQ distance batching is not yet implemented in simd package, so fallback for PQ
	if h.pqEnabled && h.pqEncoder != nil {
		distFunc := h.GetDistanceFunc()
		for i, v := range vecs {
			scores[i] = distFunc(query, v)
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

		vec := h.extractVector(rec, loc.RowIdx)
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

// extractVector extracts vector from record (caller must hold dataMu RLock)
func (h *HNSWIndex) extractVector(rec arrow.RecordBatch, rowIdx int) []float32 {
	for i := 0; i < int(rec.NumCols()); i++ {
		field := rec.Schema().Field(i)
		if field.Name == "vector" {
			col := rec.Column(i)
			if listArr, ok := col.(*array.FixedSizeList); ok {
				if listArr.IsNull(rowIdx) {
					return nil
				}
				// FixedSizeList stores data in a single flat child array
				values := listArr.Data().Children()[0]
				floatArr := array.NewFloat32Data(values)
				defer floatArr.Release()

				width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
				start := rowIdx * width
				end := start + width

				// Safety check bounds
				if start < 0 || end > floatArr.Len() {
					return nil
				}

				// Copy data to avoid safety issues with underlying buffer reuse
				// (Though for search we might get away with direct slice if careful,
				// but let's be safe as per getVector impl)
				src := floatArr.Float32Values()[start:end]
				dst := make([]float32, len(src))
				copy(dst, src)
				return dst
			}
			break
		}
	}
	return nil
}
