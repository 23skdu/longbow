package store

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/coder/hnsw"
)

// VectorID represents a unique identifier for a vector in the index.
// It maps to a specific location (Batch, Row) in the Arrow buffers.
type VectorID uint32

// Location points to the physical location of a vector in the Arrow records.
type Location struct {
	BatchIdx int
	RowIdx   int
}

// HNSWIndex wraps the hnsw.Graph and manages the mapping from ID to Arrow data.
type HNSWIndex struct {
	Graph         *hnsw.Graph[VectorID]
	mu            sync.RWMutex
	locations     []Location
	dataset       *Dataset
	dims          int           // Vector dimensions
	dimsOnce      sync.Once     // Ensures thread-safe dims initialization
	currentEpoch  atomic.Uint64 // Current epoch for zero-copy reclamation
	activeReaders atomic.Int32  // Count of readers in current epoch
	resultPool    *resultPool   // Pool for search result slices
	Metric        VectorMetric  // Distance metric used by this index
}

// NewHNSWIndex creates a new index for the given dataset using Euclidean distance.
func NewHNSWIndex(ds *Dataset) *HNSWIndex {
	return NewHNSWIndexWithMetric(ds, MetricEuclidean)
}

// NewHNSWIndexWithMetric creates a new index for the given dataset with the specified metric.
func NewHNSWIndexWithMetric(ds *Dataset, metric VectorMetric) *HNSWIndex {
	h := &HNSWIndex{
		dataset:   ds,
		locations: make([]Location, 0),
		Metric:    metric,
	}
	// Initialize the graph with VectorID as the key type.
	h.Graph = hnsw.NewGraph[VectorID]()
	h.resultPool = newResultPool()
	// Set distance function based on metric
	h.Graph.Distance = h.GetDistanceFunc()
	return h
}

// NewHNSWIndexWithCapacity creates a new index with pre-allocated locations slice.
func NewHNSWIndexWithCapacity(ds *Dataset, capacity int) *HNSWIndex {
	h := &HNSWIndex{
		dataset:   ds,
		locations: make([]Location, 0, capacity),
		Metric:    MetricEuclidean,
	}
	h.Graph = hnsw.NewGraph[VectorID]()
	h.resultPool = newResultPool()
	h.Graph.Distance = simd.EuclideanDistance
	return h
}

// GetDistanceFunc returns the SIMD distance function for the index's metric.
func (h *HNSWIndex) GetDistanceFunc() func(a, b []float32) float32 {
	switch h.Metric {
	case MetricCosine:
		return simd.CosineDistance
	case MetricDotProduct:
		return func(a, b []float32) float32 {
			return -simd.DotProduct(a, b)
		}
	case MetricEuclidean:
		return simd.EuclideanDistance
	default:
		return simd.EuclideanDistance
	}
}

// getVector retrieves the float32 slice for a given ID.
// It returns a copy of the vector to ensure safety against concurrent eviction.
func (h *HNSWIndex) getVector(id VectorID) []float32 {
	indexLockStart1 := time.Now()
	h.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues("read").Observe(time.Since(indexLockStart1).Seconds())
	if int(id) >= len(h.locations) {
		h.mu.RUnlock()
		return nil
	}
	loc := h.locations[id]
	h.mu.RUnlock()

	// Lock the dataset to safely access records
	indexLockStart2 := time.Now()
	h.dataset.dataMu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues("read").Observe(time.Since(indexLockStart2).Seconds())
	defer h.dataset.dataMu.RUnlock()

	// Check if records still exist
	if h.dataset.Records == nil || loc.BatchIdx >= len(h.dataset.Records) {
		return nil
	}
	rec := h.dataset.Records[loc.BatchIdx]

	// Find the vector column (assuming it is named "vector")
	var vecCol arrow.Array
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			vecCol = rec.Column(i)
			break
		}
	}

	if vecCol == nil {
		return nil
	}

	// Cast to FixedSizeList
	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok || listArr == nil || listArr.Data() == nil || len(listArr.Data().Children()) == 0 {
		return nil
	}
	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)
	defer floatArr.Release()

	// Calculate offset and length
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * width
	end := start + width

	if start < 0 || end > floatArr.Len() {
		return nil
	}

	// Return a copy of the slice
	src := floatArr.Float32Values()[start:end]
	dst := make([]float32, len(src))
	copy(dst, src)
	return dst
}

// enterEpoch marks a reader as active in the current epoch.
func (h *HNSWIndex) enterEpoch() {
	h.activeReaders.Add(1)
}

// exitEpoch marks a reader as finished with the current epoch.
func (h *HNSWIndex) exitEpoch() {
	h.activeReaders.Add(-1)
}

// advanceEpoch increments the epoch counter after waiting for all readers to exit.
func (h *HNSWIndex) advanceEpoch() {
	for h.activeReaders.Load() > 0 {
		// spin
	}
	h.currentEpoch.Add(1)
}

// Search performs k-NN search using the provided query vector.
func (h *HNSWIndex) Search(query []float32, k int) []VectorID {
	// coder/hnsw is not thread-safe for concurrent Search and Add.
	// Hold RLock to serialize against Add (which holds write lock).
	h.mu.RLock()
	neighbors := h.Graph.Search(query, k)
	h.mu.RUnlock()

	res := make([]VectorID, len(neighbors))
	for i, n := range neighbors {
		res[i] = n.Key
	}
	return res
}

// SearchVectors performs k-NN search returning full results with scores (distances).
func (h *HNSWIndex) SearchVectors(query []float32, k int) []SearchResult {
	// coder/hnsw is not thread-safe for concurrent Search and Add.
	// Hold RLock to serialize against Add (which holds write lock).
	h.mu.RLock()
	neighbors := h.Graph.Search(query, k)
	h.mu.RUnlock()

	res := make([]SearchResult, len(neighbors))
	distFunc := h.GetDistanceFunc()
	for i, n := range neighbors {
		// Get vector for the node to compute distance
		h.mu.RLock()
		vec := h.getVectorDirectLocked(n.Key)
		h.mu.RUnlock()

		var dist float32
		if vec != nil {
			dist = distFunc(query, vec)
		}

		res[i] = SearchResult{
			ID:    n.Key,
			Score: dist,
		}
	}
	return res
}

// getVectorUnsafe returns a direct reference to the vector data without copying.
// The caller MUST call the returned release function when done accessing the data.
// The returned slice is only valid until release() is called.
// This provides zero-copy access for read-only search hot paths.
func (h *HNSWIndex) getVectorUnsafe(id VectorID) (vec []float32, release func()) {
	h.enterEpoch()

	indexLockStart5 := time.Now()
	h.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues("read").Observe(time.Since(indexLockStart5).Seconds())
	if int(id) >= len(h.locations) {
		h.mu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}
	loc := h.locations[id]
	h.mu.RUnlock()

	indexLockStart6 := time.Now()
	h.dataset.dataMu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues("read").Observe(time.Since(indexLockStart6).Seconds())

	if h.dataset.Records == nil || loc.BatchIdx >= len(h.dataset.Records) {
		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}
	rec := h.dataset.Records[loc.BatchIdx]
	if rec == nil { // Tombstone
		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	var vecCol arrow.Array
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			vecCol = rec.Column(i)
			break
		}
	}

	if vecCol == nil {
		h.dataset.mu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		h.dataset.mu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	if len(listArr.Data().Children()) == 0 {
		h.dataset.mu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}
	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * width
	end := start + width

	if start < 0 || end > floatArr.Len() {
		floatArr.Release()
		h.dataset.mu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	// Return direct slice - caller must not hold reference after release
	vec = floatArr.Float32Values()[start:end]

	// Release function cleans up in reverse order
	release = func() {
		floatArr.Release()
		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
	}

	return vec, release
}

// Add inserts a new vector location into the index and adds it to the graph.
func (h *HNSWIndex) Add(batchIdx, rowIdx int) error {
	indexLockStart7 := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	metrics.IndexLockWaitDuration.WithLabelValues("write").Observe(time.Since(indexLockStart7).Seconds())

	id := VectorID(len(h.locations))
	h.locations = append(h.locations, Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	// Use Zero-Copy Vector Access via helper
	// This avoids allocating a new slice copy on heap.
	vecRaw := h.getVectorDirectLocked(id)
	if vecRaw == nil {
		return nil
	}

	// COPY the vector data to ensure stable memory in the HNSW graph.
	// This makes the index immune to future compaction/eviction of Arrow records.
	vec := make([]float32, len(vecRaw))
	copy(vec, vecRaw)

	// Initialize dims for pool on first vector (thread-safe as we hold mu.Lock)
	h.dimsOnce.Do(func() {
		h.dims = len(vec)
	})

	// Add to HNSW graph - MUST be under lock as coder/hnsw is not thread-safe for Add
	h.Graph.Add(hnsw.MakeNode(id, vec))

	// Track HNSW metrics
	metrics.HnswNodeCount.WithLabelValues(h.dataset.Name).Set(float64(len(h.locations)))
	nodeCount := float64(len(h.locations))
	if nodeCount > 1 {
		metrics.HnswGraphHeight.WithLabelValues(h.dataset.Name).Set(math.Log(nodeCount) / math.Log(4))
	}
	return nil
}

// AddSafe adds a vector using a direct record batch reference.
// It COPIES the vector to ensure it remains stable even if the record batch is released.
func (h *HNSWIndex) AddSafe(rec arrow.RecordBatch, rowIdx, batchIdx int) error {
	if rec == nil {
		return fmt.Errorf("AddSafe: record is nil")
	}

	// Extract vector from record batch
	var vecCol arrow.Array
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			if i < int(rec.NumCols()) {
				vecCol = rec.Column(i)
				break
			}
		}
	}

	if vecCol == nil {
		return fmt.Errorf("AddSafe: vector column not found")
	}

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok || listArr == nil || listArr.Data() == nil || len(listArr.Data().Children()) == 0 {
		return fmt.Errorf("AddSafe: invalid vector column format")
	}

	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)
	defer floatArr.Release()

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := rowIdx * width
	end := start + width

	if start < 0 || end > floatArr.Len() {
		return fmt.Errorf("AddSafe: row index out of bounds")
	}

	// COPY the vector data to ensure stable memory in the HNSW graph
	vecRaw := floatArr.Float32Values()[start:end]
	vec := make([]float32, len(vecRaw))
	copy(vec, vecRaw)

	indexLockStart7 := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	metrics.IndexLockWaitDuration.WithLabelValues("write").Observe(time.Since(indexLockStart7).Seconds())

	id := VectorID(len(h.locations))
	h.locations = append(h.locations, Location{BatchIdx: batchIdx, RowIdx: rowIdx})

	// Initialize dims for pool on first vector (thread-safe as we hold mu.Lock)
	h.dimsOnce.Do(func() {
		h.dims = len(vec)
	})

	// Add to HNSW graph - MUST be under lock as coder/hnsw is not thread-safe for Add
	h.Graph.Add(hnsw.MakeNode(id, vec))

	// Track HNSW metrics
	metrics.HnswNodeCount.WithLabelValues(h.dataset.Name).Set(float64(len(h.locations)))
	nodeCount := float64(len(h.locations))
	if nodeCount > 1 {
		metrics.HnswGraphHeight.WithLabelValues(h.dataset.Name).Set(math.Log(nodeCount) / math.Log(4))
	}

	return nil
}

// getVectorDirectLocked retrieves the vector slice directly from Arrow memory without copy.
// Caller MUST hold h.mu.Lock or h.mu.RLock.
// It uses dataset.dataMu for safety but does NOT use epoch protection or return a release function,
// because the vector reference stored in the graph is tied to the Dataset lifecycle.
func (h *HNSWIndex) getVectorDirectLocked(id VectorID) []float32 {
	if int(id) >= len(h.locations) {
		return nil
	}
	loc := h.locations[id]

	h.dataset.dataMu.RLock()
	defer h.dataset.dataMu.RUnlock()

	if h.dataset.Records == nil || loc.BatchIdx >= len(h.dataset.Records) {
		return nil
	}
	rec := h.dataset.Records[loc.BatchIdx]

	var vecCol arrow.Array
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			if i >= int(rec.NumCols()) {
				// Malformed record: missing column data
				return nil
			}
			vecCol = rec.Column(i)
			break
		}
	}
	if vecCol == nil {
		return nil
	}

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok || listArr == nil || listArr.Data() == nil || len(listArr.Data().Children()) == 0 {
		return nil
	}

	values := listArr.Data().Children()[0]
	// unsafe access via view
	floatArr := array.NewFloat32Data(values)
	defer floatArr.Release()

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * width
	end := start + width

	if start < 0 || end > floatArr.Len() {
		return nil
	}

	// Return slice directly
	return floatArr.Float32Values()[start:end]
}

// vectorData holds vector ID and data for parallel processing
type vectorData struct {
	id  VectorID
	vec []float32
}

// AddBatchParallel adds multiple vectors in parallel using worker goroutines.
// It uses a three-phase approach:
// 1. Pre-allocate locations atomically under single lock
// 2. Parallel vector retrieval using worker goroutines
// 3. Sequential graph insertion (hnsw library requirement)
// This can reduce build time by up to 85% compared to sequential insertion
// by parallelizing the expensive vector fetch operations.
func (h *HNSWIndex) AddBatchParallel(locations []Location, workers int) error {
	if len(locations) == 0 {
		return nil
	}

	// Clamp workers to reasonable bounds
	if workers < 1 {
		workers = 1
	}
	if workers > len(locations) {
		workers = len(locations)
	}

	// Phase 1: Pre-allocate all locations atomically
	indexLockStart8 := time.Now()
	h.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues("write").Observe(time.Since(indexLockStart8).Seconds())
	baseID := VectorID(len(h.locations))
	h.locations = append(h.locations, locations...)
	h.mu.Unlock()

	// Phase 2: Parallel vector retrieval
	results := make([]vectorData, len(locations))
	chunkSize := (len(locations) + workers - 1) / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if end > len(locations) {
			end = len(locations)
		}
		if start >= len(locations) {
			break
		}

		wg.Add(1)
		go func(start, end int, base VectorID) {
			defer wg.Done()
			for i := start; i < end; i++ {
				id := base + VectorID(i)
				vec := h.getVector(id)
				results[i] = vectorData{id: id, vec: vec}
			}
		}(start, end, baseID)
	}
	wg.Wait()

	// Initialize dims for pool on first vector if needed (thread-safe)
	if len(results) > 0 && results[0].vec != nil {
		h.dimsOnce.Do(func() {
			h.dims = len(results[0].vec)
		})
	}

	// Phase 3: Sequential graph insertion with mutex protection
	// The hnsw library's Graph.Add is not thread-safe
	for _, vd := range results {
		if vd.vec == nil {
			continue
		}
		indexLockStart9 := time.Now()
		h.mu.Lock()
		metrics.IndexLockWaitDuration.WithLabelValues("write").Observe(time.Since(indexLockStart9).Seconds())
		h.Graph.Add(hnsw.MakeNode(vd.id, vd.vec))
		h.mu.Unlock()
	}

	return nil
}

// Len returns the number of vectors in the index
func (h *HNSWIndex) Len() int {
	indexLockStart10 := time.Now()
	h.mu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues("read").Observe(time.Since(indexLockStart10).Seconds())
	defer h.mu.RUnlock()
	return len(h.locations)
}

func (h *HNSWIndex) SearchByID(id VectorID, k int) []VectorID {
	if k <= 0 {
		return nil
	}

	// Use zero-copy access for the query vector (Unsafe path)
	// This avoids allocating a scratch buffer or copying the vector.
	// We hold the epoch lock (via getVectorUnsafe) only for the duration of the search.
	vec, release := h.getVectorUnsafe(id)
	if vec == nil || release == nil {
		return nil
	}
	defer release()

	// coder/hnsw is not thread-safe for concurrent Search and Add.
	h.mu.RLock()
	neighbors := h.Graph.Search(vec, k)
	h.mu.RUnlock()

	res := h.resultPool.get(len(neighbors))
	for i, n := range neighbors {
		res[i] = n.Key
	}
	return res
}

// PutResults returns a search result slice to the pool for reuse.
// Callers should call this when done with SearchByID results.
func (h *HNSWIndex) PutResults(results []VectorID) {
	h.resultPool.put(results)
}

// RegisterReader increments the active reader count for zero-copy safety
func (h *HNSWIndex) RegisterReader() {
	h.activeReaders.Add(1)
	metrics.HnswActiveReaders.WithLabelValues(h.dataset.Name).Inc()
}

// Close releases resources associated with the index.
func (h *HNSWIndex) Close() error {
	// Future cleanup: release pools, unregister metrics, etc.
	return nil
}

// UnregisterReader decrements the active reader count
func (h *HNSWIndex) UnregisterReader() {
	h.activeReaders.Add(-1)
	metrics.HnswActiveReaders.WithLabelValues(h.dataset.Name).Dec()
}

// SearchWithArena performs k-NN search using the provided arena for allocations.
// If arena is nil or exhausted, falls back to heap allocation.
// The returned slice is allocated from the arena and should NOT be returned to resultPool.
// Call arena.Reset() after processing results to reclaim memory for next request.
func (h *HNSWIndex) SearchWithArena(query []float32, k int, arena *SearchArena) []VectorID {
	if len(query) == 0 || k <= 0 {
		return nil
	}

	// coder/hnsw is not thread-safe for concurrent Search and Add.
	h.mu.RLock()
	neighbors := h.Graph.Search(query, k)
	h.mu.RUnlock()

	if len(neighbors) == 0 {
		return nil
	}

	// Try to allocate result slice from arena
	var res []VectorID
	if arena != nil {
		res = arena.AllocVectorIDSlice(len(neighbors))
	}

	// Fall back to heap allocation if arena unavailable or exhausted
	if res == nil {
		res = make([]VectorID, len(neighbors))
	}

	// Copy results
	for i, n := range neighbors {
		res[i] = n.Key
	}

	return res
}

// SearchByIDUnsafe performs k-NN search using zero-copy vector access.
// This avoids the allocation and copy overhead of SearchByID by using
// getVectorUnsafe with epoch protection. The vector data is only valid
// during the search operation.
// Returns nil if id is invalid or k <= 0.
func (h *HNSWIndex) SearchByIDUnsafe(id VectorID, k int) []VectorID {
	if k <= 0 {
		return nil
	}

	// Get vector with zero-copy - epoch protection is inside getVectorUnsafe
	vec, release := h.getVectorUnsafe(id)
	if vec == nil || release == nil {
		return nil
	}
	// Ensure release is called when done with vector data
	defer release()

	// coder/hnsw is not thread-safe for concurrent Search and Add.
	h.mu.RLock()
	neighbors := h.Graph.Search(vec, k)
	h.mu.RUnlock()

	if len(neighbors) == 0 {
		return nil
	}

	// Allocate result slice from pool
	res := h.resultPool.get(len(neighbors))
	for i, n := range neighbors {
		res[i] = n.Key
	}
	return res
}

// GetDimension returns the vector dimension for this index
func (h *HNSWIndex) GetDimension() uint32 {
	return uint32(h.dims)
}
