package store

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/gpu"
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

// GetLocation returns the storage location for a given VectorID
func (h *HNSWIndex) GetLocation(id VectorID) (Location, bool) {
	// Optimized: No global lock needed for reading locations
	return h.locationStore.Get(id)
}

// HNSWIndex wraps the hnsw.Graph and manages the mapping from ID to Arrow data.
type HNSWIndex struct {
	Graph *hnsw.Graph[VectorID]
	mu    sync.RWMutex
	// locations     []Location // Removed in favor of locationStore
	locationStore *ChunkedLocationStore
	dataset       *Dataset
	dims          int           // Vector dimensions
	dimsOnce      sync.Once     // Ensures thread-safe dims initialization
	currentEpoch  atomic.Uint64 // Current epoch for zero-copy reclamation
	activeReaders atomic.Int32  // Count of readers in current epoch
	resultPool    *resultPool   // Pool for search result slices
	Metric        VectorMetric  // Distance metric used by this index
	numaTopology  *NUMATopology // NUMA topology for cross-node tracking

	// GPU acceleration (optional)
	gpuIndex    gpu.Index
	gpuEnabled  bool
	gpuFallback bool // true if GPU init failed, using CPU only

	nextVecID atomic.Uint32

	// Product Quantization
	pqEnabled           bool
	pqTrainingEnabled   bool
	pqTrainingThreshold int
	pqEncoder           *PQEncoder
	pqCodes             [][]uint8 // Indexed by VectorID
	pqCodesMu           sync.RWMutex

	// Parallel Search Configuration
	parallelConfig ParallelSearchConfig

	// Zero-copy optimizations
	vectorColIdx atomic.Int32 // Cached index of the "vector" column
}

// NewHNSWIndex creates a new index for the given dataset using Euclidean distance.
func NewHNSWIndex(ds *Dataset) *HNSWIndex {
	return NewHNSWIndexWithMetric(ds, MetricEuclidean)
}

// NewHNSWIndexWithMetric creates a new index for the given dataset with the specified metric.
func NewHNSWIndexWithMetric(ds *Dataset, metric VectorMetric) *HNSWIndex {
	h := &HNSWIndex{
		dataset:        ds,
		locationStore:  NewChunkedLocationStore(),
		Metric:         metric,
		parallelConfig: DefaultParallelSearchConfig(),
	}
	if ds != nil {
		h.numaTopology = ds.Topo
	}
	// Initialize the graph with VectorID as the key type.
	h.Graph = hnsw.NewGraph[VectorID]()
	h.resultPool = newResultPool()
	// Set distance function based on metric
	h.Graph.Distance = h.GetDistanceFunc()
	h.vectorColIdx.Store(-1)

	return h
}

// NewHNSWIndexWithCapacity creates a new index with pre-allocated locations slice.
func NewHNSWIndexWithCapacity(ds *Dataset, capacity int) *HNSWIndex {
	h := &HNSWIndex{
		dataset:       ds,
		locationStore: NewChunkedLocationStore(),
		Metric:        MetricEuclidean,
		numaTopology:  ds.Topo,
	}
	if capacity > 0 {
		h.locationStore.Grow(capacity)
	}
	h.Graph = hnsw.NewGraph[VectorID]()
	h.resultPool = newResultPool()
	h.Graph.Distance = simd.EuclideanDistance
	h.vectorColIdx.Store(-1)

	return h
}

// BatchRemapInfo describes how a batch ID maps to a new one
type BatchRemapInfo struct {
	NewBatchIdx int
	NewRowIdxs  []int // Maps oldRowIdx to newRowIdx in NewBatchIdx, or -1 if dropped
}

// RemapLocations safely updates vector locations after compaction.
// It iterates over all locations and applies the remapping if the batch index matches.
// RemapLocations safely updates vector locations after compaction.
// It iterates over all locations and applies the remapping if the batch index matches.
func (h *HNSWIndex) RemapLocations(remapping map[int]BatchRemapInfo) {
	h.locationStore.IterateMutable(func(_ VectorID, val *atomic.Uint64) {
		currentVal := val.Load()
		loc := unpackLocation(currentVal)

		if info, ok := remapping[loc.BatchIdx]; ok {
			if loc.RowIdx >= 0 && loc.RowIdx < len(info.NewRowIdxs) {
				newRowIdx := info.NewRowIdxs[loc.RowIdx]
				if newRowIdx == -1 {
					val.Store(packLocation(Location{BatchIdx: -1, RowIdx: -1}))
				} else {
					val.Store(packLocation(Location{
						BatchIdx: info.NewBatchIdx,
						RowIdx:   newRowIdx,
					}))
				}
			} else {
				// Out of bounds
				val.Store(packLocation(Location{BatchIdx: -1, RowIdx: -1}))
			}
		} else if loc.BatchIdx == -1 {
			// Ensure consistency (no-op really as unpacking -1,-1 repacks to same)
			// But if we want to be explicit:
			// val.Store(packLocation(Location{BatchIdx: -1, RowIdx: -1}))
		}
	})
}

// SetPQEncoder enables product quantization with the provided encoder.
func (h *HNSWIndex) SetPQEncoder(encoder *PQEncoder) {
	h.pqCodesMu.Lock()
	h.pqEncoder = encoder
	h.pqEnabled = true
	// Initialize code storage if needed
	if h.pqCodes == nil {
		h.pqCodes = make([][]uint8, 0)
	}
	h.pqCodesMu.Unlock() // Unlock specific lock

	metrics.HNSWPQEnabled.WithLabelValues(h.dataset.Name).Set(1)

	// Update distance function in graph - Requires GLOBAL Lock
	h.mu.Lock()
	h.Graph.Distance = h.GetDistanceFunc()
	h.mu.Unlock()
}

// TrainPQ trains a PQ encoder on the current dataset elements and enables it.
// This is a blocking operation.
func (h *HNSWIndex) TrainPQ(dimensions, m, ksub, iterations int) error {
	start := time.Now()
	defer func() {
		metrics.HNSWPQTrainingDuration.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}()
	// 1. Gather all current vectors
	// This might be expensive for large datasets.
	// For now, we take a sample or all.
	// Let's assume we fit in memory for training sample.
	count := int(h.nextVecID.Load())
	if count == 0 {
		return fmt.Errorf("cannot train PQ on empty index")
	}

	sampleSize := 10000
	if count < sampleSize {
		sampleSize = count
	}

	vectors := make([][]float32, 0, sampleSize)
	// Sample uniformly
	step := count / sampleSize
	if step == 0 {
		step = 1
	}

	for i := 0; i < count; i += step {
		vec := h.getVector(VectorID(i))
		if vec != nil {
			vectors = append(vectors, vec)
		}
	}

	cfg := &PQConfig{
		Dim:    dimensions,
		M:      m,
		Ksub:   ksub,
		SubDim: dimensions / m,
	}

	enc, err := TrainPQEncoder(cfg, vectors, iterations)
	if err != nil {
		return err
	}

	h.SetPQEncoder(enc)

	// Encode existing vectors
	// This needs to be done under lock or carefully managed
	h.pqCodesMu.Lock()
	defer h.pqCodesMu.Unlock()

	// Resize codes slice
	if cap(h.pqCodes) < count {
		newCodes := make([][]uint8, count)
		copy(newCodes, h.pqCodes)
		h.pqCodes = newCodes
	} else {
		h.pqCodes = h.pqCodes[:count]
	}

	for i := 0; i < count; i++ {
		vec := h.getVector(VectorID(i))
		if vec != nil {
			h.pqCodes[i] = enc.Encode(vec)
		}
	}

	return nil
}

// GetDistanceFunc returns the distance function.
// If PQ is enabled, it returns the SDC-accelerated PQ distance.
func (h *HNSWIndex) GetDistanceFunc() func(a, b []float32) float32 {
	h.pqCodesMu.RLock()
	defer h.pqCodesMu.RUnlock()
	return h.getDistanceFuncNoLock()
}

func (h *HNSWIndex) getDistanceFuncNoLock() func(a, b []float32) float32 {
	pqEnabled := h.pqEnabled
	encoder := h.pqEncoder

	if pqEnabled && encoder != nil {
		// PQ SDC Distance Function with safety check for mixed graph states.
		packedLen := (h.pqEncoder.CodeSize() + 3) / 4

		return func(a, b []float32) float32 {
			// Fast path: both packed (common case after full indexing)
			if len(a) == packedLen && len(b) == packedLen {
				return h.pqEncoder.SDCDistancePacked(a, b)
			}

			// Fallback: mixed raw/packed
			var codesA, codesB []uint8

			// Process A
			if len(a) == packedLen {
				codesA = UnpackFloat32sToBytes(a, h.pqEncoder.CodeSize())
			} else {
				codesA = h.pqEncoder.Encode(a)
			}

			// Process B
			if len(b) == packedLen {
				codesB = UnpackFloat32sToBytes(b, h.pqEncoder.CodeSize())
			} else {
				codesB = h.pqEncoder.Encode(b)
			}

			return h.pqEncoder.SDCDistance(codesA, codesB)
		}
	}

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
	metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "read").Observe(time.Since(indexLockStart1).Seconds())
	loc, ok := h.locationStore.Get(id)
	if !ok {
		h.mu.RUnlock()
		return nil
	}
	h.mu.RUnlock()

	// Lock the dataset to safely access records
	indexLockStart2 := time.Now()
	h.dataset.dataMu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "read").Observe(time.Since(indexLockStart2).Seconds())
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

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
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
	defer func(start time.Time) {
		metrics.VectorSearchLatencySeconds.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}(time.Now())

	// PQ Encoding for query
	var graphQuery []float32 = query
	// Use RLock for config check to avoid race
	h.pqCodesMu.RLock()
	// Capture locals to avoid holding lock too long if encoding is slow?
	// Encoding is fast enough.
	if h.pqEnabled && h.pqEncoder != nil {
		codes := h.pqEncoder.Encode(query)
		graphQuery = PackBytesToFloat32s(codes)
	}
	h.pqCodesMu.RUnlock()

	// coder/hnsw library requires synchronization between Search and Add
	// Use global RLock for graph search (multiple concurrent searches OK)
	h.mu.RLock()
	neighbors := h.Graph.Search(graphQuery, k)
	h.mu.RUnlock()

	res := make([]VectorID, len(neighbors))
	for i, n := range neighbors {
		res[i] = n.Key
	}
	return res
}

// SearchVectors performs k-NN search returning full results with scores (distances).
// Uses striped locks for location access to reduce contention in result processing.
func (h *HNSWIndex) SearchVectors(query []float32, k int, filters []Filter) []SearchResult {
	defer func(start time.Time) {
		metrics.VectorSearchLatencySeconds.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}(time.Now())

	// Post-filtering approach:
	// 1. Search for K * factor candidates
	// 2. Filter candidates
	// 3. Keep top K

	limit := k
	if len(filters) > 0 {
		limit = k * 10 // Oversample for filtering. TODO: Adaptive or configurable factor
	}

	// PQ Encoding for query
	var graphQuery []float32 = query
	h.pqCodesMu.RLock()
	if h.pqEnabled && h.pqEncoder != nil {
		codes := h.pqEncoder.Encode(query)
		graphQuery = PackBytesToFloat32s(codes)
	}
	h.pqCodesMu.RUnlock()

	// Graph search requires global lock (coder/hnsw library requirement)
	// Multiple concurrent searches can hold RLock simultaneously
	h.mu.RLock()
	neighbors := h.Graph.Search(graphQuery, limit)
	h.mu.RUnlock()

	// Use parallel processing for large result sets
	cfg := h.getParallelSearchConfig()
	if cfg.Enabled && len(neighbors) >= cfg.Threshold {
		return h.processResultsParallel(query, neighbors, k, filters)
	}

	// Fall back to serial processing for small result sets (original implementation)
	distFunc := h.GetDistanceFunc()

	// Pre-process filters once per search (Item 6)
	var evaluator *FilterEvaluator
	if len(filters) > 0 {
		h.dataset.dataMu.RLock()
		if len(h.dataset.Records) > 0 {
			evaluator, _ = NewFilterEvaluator(h.dataset.Records[0], filters)
		}
		h.dataset.dataMu.RUnlock()
	}

	res := make([]SearchResult, 0, len(neighbors))

	// Batch configuration
	// 32 seems like a good balance for instruction cache and loop overhead
	const batchSize = 32
	batchIDs := make([]VectorID, batchSize)
	batchLocs := make([]Location, batchSize)

	count := 0
	for i := 0; i < len(neighbors); i += batchSize {
		if count >= k {
			break
		}

		end := i + batchSize
		if end > len(neighbors) {
			end = len(neighbors)
		}

		// 1. Prepare Batch
		batchLen := end - i
		for j := 0; j < batchLen; j++ {
			batchIDs[j] = neighbors[i+j].Key
		}

		// 2. Resolve Locations (Vectorized/Batched lookup)
		h.locationStore.GetBatch(batchIDs[:batchLen], batchLocs[:batchLen])

		// 3. Process Batch with single RLock section
		h.enterEpoch()
		h.dataset.dataMu.RLock()

		for j := 0; j < batchLen; j++ {
			if count >= k {
				break
			}

			id := batchIDs[j]
			loc := batchLocs[j]

			// Check Location Validity
			if loc.BatchIdx == -1 {
				continue
			}

			// Check Filters using optimized evaluator (Item 6)
			if evaluator != nil {
				if !evaluator.Matches(loc.RowIdx) {
					continue
				}
			}

			// Recompute distance using zero-copy / zero-lock path
			vec := h.getVectorLockedUnsafe(loc)
			if vec != nil {
				dist := distFunc(query, vec)
				res = append(res, SearchResult{ID: id, Score: dist})
				count++
			}
		}

		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
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
	metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "read").Observe(time.Since(indexLockStart5).Seconds())
	loc, ok := h.locationStore.Get(id)
	if !ok {
		h.mu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}
	h.mu.RUnlock()

	// NUMA instrumentation: check if we are accessing data from a remote node
	if h.numaTopology != nil && h.numaTopology.NumNodes > 1 && loc.BatchIdx != -1 {
		if loc.BatchIdx < len(h.dataset.BatchNodes) {
			dataNode := h.dataset.BatchNodes[loc.BatchIdx]
			if dataNode != -1 {
				currCPU := GetCurrentCPU()
				workerNode := h.numaTopology.GetNodeForCPU(currCPU)
				if workerNode != -1 && workerNode != dataNode {
					metrics.NUMACrossNodeAccessTotal.WithLabelValues(
						fmt.Sprintf("%d", workerNode),
						fmt.Sprintf("%d", dataNode),
					).Inc()
				}
			}
		}
	}

	indexLockStart6 := time.Now()
	h.dataset.dataMu.RLock()
	metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "read").Observe(time.Since(indexLockStart6).Seconds())

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
		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	if len(listArr.Data().Children()) == 0 {
		h.dataset.dataMu.RUnlock()
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
		h.dataset.dataMu.RUnlock()
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
func (h *HNSWIndex) Add(batchIdx, rowIdx int) (uint32, error) {
	// 1. Prepare location and update locations slice under global lock
	// and get vector while holding the lock to protect against slice reallocations.
	h.mu.Lock()
	id := h.locationStore.Append(Location{BatchIdx: batchIdx, RowIdx: rowIdx})
	h.mu.Unlock()

	// Check if we should trigger PQ training
	if h.pqTrainingEnabled && !h.pqEnabled && int(id) == h.pqTrainingThreshold {
		go func() {
			// Train with default params: M=8, K=256, Iter=10
			if h.dims > 0 && h.dims%8 == 0 {
				metrics.HNSWPQTrainingTriggered.WithLabelValues(h.dataset.Name).Inc()
				start := time.Now()
				err := h.TrainPQ(h.dims, 8, 256, 10)
				if err != nil {
					fmt.Printf("PQ Training failed: %v\n", err)
				}
				metrics.HNSWPQTrainingDuration.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
			}
		}()
	}

	// 3. Get vector (Safe to call as ID is reserved and location is set)
	vecRaw := h.getVector(id) // getVector now handles its own locks
	if vecRaw == nil {
		return 0, nil
	}
	vec := vecRaw

	// 4. Initialize dims for pool on first vector
	h.dimsOnce.Do(func() {
		h.dims = len(vec)
	})

	// 5. Add to HNSW graph - serialization is unfortunately required for coder/hnsw
	indexLockStart7 := time.Now()

	// PQ Encoding
	var nodeVec []float32 = vec
	h.pqCodesMu.RLock()
	pqEnabled := h.pqEnabled
	encoder := h.pqEncoder
	h.pqCodesMu.RUnlock()

	if pqEnabled && encoder != nil {
		codes := encoder.Encode(vec)
		h.pqCodesMu.Lock()
		// Resize storage if necessary
		if int(id) >= len(h.pqCodes) {
			// Grow slice to accommodate new ID
			targetLen := int(id) + 1
			if targetLen > cap(h.pqCodes) {
				newCap := targetLen * 2
				if newCap < 1024 {
					newCap = 1024
				}
				newCodes := make([][]uint8, targetLen, newCap)
				copy(newCodes, h.pqCodes)
				h.pqCodes = newCodes
			} else {
				h.pqCodes = h.pqCodes[:targetLen]
			}
		}
		h.pqCodes[id] = codes
		h.pqCodesMu.Unlock()

		// Pack codes into float32 slice for storage in Graph Node
		nodeVec = PackBytesToFloat32s(codes)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "write").Observe(time.Since(indexLockStart7).Seconds())

	h.Graph.Add(hnsw.MakeNode(id, nodeVec))

	// Track HNSW metrics
	metrics.HnswNodeCount.WithLabelValues(h.dataset.Name).Set(float64(h.nextVecID.Load()))
	nodeCount := float64(h.nextVecID.Load())
	if nodeCount > 1 {
		metrics.HnswGraphHeight.WithLabelValues(h.dataset.Name).Set(math.Log(nodeCount) / math.Log(4))
	}
	return uint32(id), nil
}

// AddSafe adds a vector using a direct record batch reference.
// It COPIES the vector to ensure it remains stable even if the record batch is released.
func (h *HNSWIndex) AddSafe(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	if rec == nil {
		return 0, fmt.Errorf("AddSafe: record is nil")
	}

	// 1. Allocate ID atomically
	id := VectorID(h.nextVecID.Add(1) - 1)

	// 2. Extract vector from record batch (Done outside global lock)
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
		return 0, fmt.Errorf("AddSafe: vector column not found")
	}

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		return 0, fmt.Errorf("AddSafe: invalid vector column format")
	}

	values := listArr.Data().Children()[0]
	floatArr := array.NewFloat32Data(values)
	defer floatArr.Release()

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := rowIdx * width
	end := start + width

	if start < 0 || end > floatArr.Len() {
		return 0, fmt.Errorf("AddSafe: row index out of bounds")
	}

	vec := floatArr.Float32Values()[start:end]

	// 3. Update locations under global lock
	h.mu.Lock()
	// The ID is already allocated by nextVecID.Add(1)-1.
	// We need to ensure locationStore has capacity for this ID.
	// Append handles this by growing if needed.
	// If we want to set a specific ID, we'd need a Set method on ChunkedLocationStore.
	// For now, we assume AddSafe is used for sequential additions, so Append is fine.
	// If IDs are not sequential, this needs a `Set(id, loc)` method.
	// Given `nextVecID.Add(1)-1`, IDs are sequential.
	h.locationStore.Append(Location{BatchIdx: batchIdx, RowIdx: rowIdx})
	h.mu.Unlock()

	// 4. Initialize dims
	h.dimsOnce.Do(func() {
		h.dims = len(vec)
	})

	// 5. Add to graph under global lock
	indexLockStart7 := time.Now()

	// PQ Encoding
	var nodeVec []float32 = vec
	h.pqCodesMu.RLock()
	pqEnabled := h.pqEnabled
	encoder := h.pqEncoder
	h.pqCodesMu.RUnlock()

	if pqEnabled && encoder != nil {
		codes := encoder.Encode(vec)
		h.pqCodesMu.Lock()
		// Resize storage if necessary
		if int(id) >= len(h.pqCodes) {
			targetLen := int(id) + 1
			if targetLen > cap(h.pqCodes) {
				newCap := targetLen * 2
				if newCap < 1024 {
					newCap = 1024
				}
				newCodes := make([][]uint8, targetLen, newCap)
				copy(newCodes, h.pqCodes)
				h.pqCodes = newCodes
			} else {
				h.pqCodes = h.pqCodes[:targetLen]
			}
		}
		h.pqCodes[id] = codes
		h.pqCodesMu.Unlock()
		nodeVec = PackBytesToFloat32s(codes)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "write").Observe(time.Since(indexLockStart7).Seconds())

	h.Graph.Add(hnsw.MakeNode(id, nodeVec))

	// Track HNSW metrics
	metrics.HnswNodeCount.WithLabelValues(h.dataset.Name).Set(float64(h.nextVecID.Load()))
	nodeCount := float64(h.nextVecID.Load())
	if nodeCount > 1 {
		metrics.HnswGraphHeight.WithLabelValues(h.dataset.Name).Set(math.Log(nodeCount) / math.Log(4))
	}

	return uint32(id), nil
}

// AddByLocation implements VectorIndex interface for HNSWIndex.
func (h *HNSWIndex) AddByLocation(batchIdx, rowIdx int) (uint32, error) {
	return h.Add(batchIdx, rowIdx)
}

// AddByRecord implements VectorIndex interface for HNSWIndex.
func (h *HNSWIndex) AddByRecord(rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	return h.AddSafe(rec, rowIdx, batchIdx)
}

// SearchVectorsWithBitmap returns k nearest neighbors filtered by a bitset.
func (h *HNSWIndex) SearchVectorsWithBitmap(query []float32, k int, filter *Bitset) []SearchResult {
	if filter == nil || filter.Count() == 0 {
		// Empty filter means NO results allowed? Or all?
		// Typically filter means "Must be in this set". Empty set = no results.
		// If filter is nil, we assume "No filtering"?? No, caller should pass nil if no filtering.
		// Interface signature: filter *Bitset.
		// If it's nil, we could treat as "Allow All", but explicit method implies filtering.
		// Let's assume filter is required.
		return []SearchResult{}
	}

	defer func(start time.Time) {
		metrics.VectorSearchLatencySeconds.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}(time.Now())

	// Heuristic: If filter is very restrictive (small count), Brute Force might be faster.
	// Check count.
	count := filter.Count()
	if count < 1000 {
		// Brute force: iterate set bits, compute distance.
		return h.searchBruteForceWithBitmap(query, k, filter)
	}

	// HNSW Post-Filtering with Oversampling
	// HNSW Post-Filtering with Oversampling
	limit := k * 10

	h.mu.RLock()
	neighbors := h.Graph.Search(query, limit)
	h.mu.RUnlock()

	distFunc := h.GetDistanceFunc()
	res := make([]SearchResult, 0, k)
	resultCount := 0

	// Batch configuration
	// 32 seems like a good balance for instruction cache and loop overhead
	const batchSize = 32
	batchIDs := make([]VectorID, batchSize)
	batchLocs := make([]Location, batchSize)

	for i := 0; i < len(neighbors); i += batchSize {
		if resultCount >= k {
			break
		}

		end := i + batchSize
		if end > len(neighbors) {
			end = len(neighbors)
		}

		// 1. Prepare Batch
		batchLen := end - i
		for j := 0; j < batchLen; j++ {
			batchIDs[j] = neighbors[i+j].Key
		}

		// 2. Resolve Locations (Vectorized/Batched lookup)
		h.locationStore.GetBatch(batchIDs[:batchLen], batchLocs[:batchLen])

		// 3. Process Batch with single RLock section
		h.enterEpoch()
		h.dataset.dataMu.RLock()

		for j := 0; j < batchLen; j++ {
			if resultCount >= k {
				break
			}

			id := batchIDs[j]
			loc := batchLocs[j]

			// Check Bitmap first (fastest rejection)
			if !filter.Contains(int(id)) {
				continue
			}

			// Check location validity
			if loc.BatchIdx == -1 {
				continue
			}

			// Recompute distance using zero-copy / zero-lock path
			vec := h.getVectorLockedUnsafe(loc)
			if vec != nil {
				dist := distFunc(query, vec)
				res = append(res, SearchResult{ID: id, Score: dist})
				resultCount++
			}
		}

		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
	}
	return res
}

// getVectorUnsafeWithLocation is an optimized variant that takes a known location.
// Caller MUST call release.
func (h *HNSWIndex) getVectorUnsafeWithLocation(id VectorID, loc Location) (vec []float32, release func()) {
	h.enterEpoch()

	// Skip Location Lookup (already done)

	// NUMA checks... (Skipped for brevity in hot path, or keep?)
	// Let's keep minimal or skip. For now, skip NUMA stats in this super-hot optimization?
	// Or maybe it's fine.

	indexLockStart6 := time.Now()
	h.dataset.dataMu.RLock()
	// Skip metrics for speed? metrics.IndexLockWaitDuration....
	// metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "read").Observe(time.Since(indexLockStart6).Seconds())
	_ = indexLockStart6

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

	// Optimization: Cache "vector" column index?
	// For now, iterate.
	// Actually, we can assume it's usually the same index for uniform datasets.
	// But let's stick to correctness first.
	var vecCol arrow.Array
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" {
			vecCol = rec.Column(i)
			break
		}
	}

	if vecCol == nil {
		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	if len(listArr.Data().Children()) == 0 {
		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}
	values := listArr.Data().Children()[0]
	// unsafe access via view
	// This allocation `NewFloat32Data` is also overhead!
	// Can we bypass `NewFloat32Data`?
	// It basically wraps `values` (ArrowData).
	// `values` has Buffers()[1] which is the raw bytes.

	// Fast Path: Direct access to buffer.
	// buffer := values.Buffers()[1]
	// bytes := buffer.Bytes()
	// But we need float32 slice header.

	// Let's stick to safe-ish standard way for now to minimize breakage risk,
	// unless we really need that last 5%.
	floatArr := array.NewFloat32Data(values)

	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	start := loc.RowIdx * width
	end := start + width

	if start < 0 || end > floatArr.Len() {
		floatArr.Release()
		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
		return nil, nil
	}

	// Return direct slice - caller must not hold reference after release
	vec = floatArr.Float32Values()[start:end]

	release = func() {
		floatArr.Release()
		h.dataset.dataMu.RUnlock()
		h.exitEpoch()
	}

	return vec, release
}

// getVectorLockedUnsafe returns a direct reference to the vector data.
// Caller MUST hold h.dataset.dataMu.RLock() AND h.enterEpoch().
// Provides true zero-copy access by bypassing Arrow array wrappers and RLock overhead.
func (h *HNSWIndex) getVectorLockedUnsafe(loc Location) []float32 {
	if h.dataset.Records == nil || loc.BatchIdx < 0 || loc.BatchIdx >= len(h.dataset.Records) {
		return nil
	}
	rec := h.dataset.Records[loc.BatchIdx]
	if rec == nil {
		return nil
	}

	colIdx := h.vectorColIdx.Load()
	if colIdx == -1 {
		// Resolve column index
		for i, field := range rec.Schema().Fields() {
			if field.Name == "vector" {
				colIdx = int32(i)
				h.vectorColIdx.Store(colIdx)
				break
			}
		}
	}

	if colIdx == -1 {
		return nil
	}

	vecCol := rec.Column(int(colIdx))
	listArr, ok := vecCol.(*array.FixedSizeList)
	if !ok {
		return nil
	}

	// Fast Path: Direct buffer access via unsafe
	values := listArr.ListValues()
	data := values.Data()
	if len(data.Buffers()) < 2 || data.Buffers()[1] == nil {
		return nil
	}

	buf := data.Buffers()[1].Bytes()
	width := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
	offset := loc.RowIdx * width * 4

	if offset+width*4 > len(buf) {
		return nil
	}

	ptr := unsafe.Pointer(&buf[offset])
	return unsafe.Slice((*float32)(ptr), width)
}

func (h *HNSWIndex) searchBruteForceWithBitmap(query []float32, k int, filter *Bitset) []SearchResult {
	// 1. Get all IDs from filter
	ids := filter.ToUint32Array()
	if len(ids) == 0 {
		return []SearchResult{}
	}

	// 2. Iterate and compute distances
	distFunc := h.GetDistanceFunc()
	results := make([]SearchResult, 0, len(ids))

	for _, id := range ids {
		vec, release := h.getVectorUnsafe(VectorID(id))
		if vec == nil {
			continue
		}

		dist := distFunc(query, vec)
		results = append(results, SearchResult{
			ID:    VectorID(id),
			Score: dist,
		})
		release()
	}

	// 3. Sort and truncate to K
	// Using simple sort for now, could use a heap for better performance if len(ids) is large
	// but this path is intended for highly selective filters.
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score < results[j].Score
	})

	if len(results) > k {
		results = results[:k]
	}

	return results
}

// SetIndexedColumns satisfies VectorIndex interface
func (h *HNSWIndex) SetIndexedColumns(cols []string) {
	// HNSW itself doesn't use these but the wrapper might
}

// Warmup implements VectorIndex interface.
func (h *HNSWIndex) Warmup() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.Graph.Len()
}

// getVectorDirectLocked retrieves the vector slice directly from Arrow memory without copy.
// Caller MUST hold h.mu.Lock or h.mu.RLock.
// It uses dataset.dataMu for safety but does NOT use epoch protection or return a release function,
// because the vector reference stored in the graph is tied to the Dataset lifecycle.
func (h *HNSWIndex) getVectorDirectLocked(id VectorID) []float32 {
	loc, ok := h.locationStore.Get(id)
	if !ok {
		return nil
	}

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
	if !ok {
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

	// Phase 1: Append all locations (Lock-free-ish / Reduced Lock)
	// We use the store's Append which locks internally per chunk creation, but it's fine.
	// Optimizing: We could implement AppendBatch.
	// For now, loop is safer during refactor.
	// CRITICAL: We must hold h.mu to ensure we get a contiguous block of IDs
	// so that baseID + i corresponds to locations[i].
	// Without this, concurrent AddBatchParallel calls would interleave IDs.
	h.mu.Lock()
	baseID := VectorID(h.locationStore.Len())
	for _, loc := range locations {
		h.locationStore.Append(loc)
	}
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
		metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "write").Observe(time.Since(indexLockStart9).Seconds())
		h.Graph.Add(hnsw.MakeNode(vd.id, vd.vec))
		h.mu.Unlock()
	}

	return nil
}

// Len returns the number of vectors in the index
func (h *HNSWIndex) Len() int {
	return h.locationStore.Len()
}

func (h *HNSWIndex) SearchByID(id VectorID, k int) []VectorID {
	defer func(start time.Time) {
		metrics.VectorSearchLatencySeconds.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}(time.Now())

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
	idx := 0
	for _, n := range neighbors {
		loc, ok := h.locationStore.Get(n.Key)
		if !ok {
			continue
		}
		if loc.BatchIdx != -1 {
			res[idx] = n.Key
			idx++
		}
	}
	return res[:idx]
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
	h.mu.Lock()
	defer h.mu.Unlock()
	h.Graph = nil
	h.locationStore.Reset() // Clear locations
	h.resultPool = nil
	h.dataset = nil
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
	defer func(start time.Time) {
		metrics.VectorSearchLatencySeconds.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}(time.Now())

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
	defer func(start time.Time) {
		metrics.VectorSearchLatencySeconds.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}(time.Now())

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
	idx := 0
	for _, n := range neighbors {
		loc, ok := h.locationStore.Get(n.Key)
		if !ok {
			return nil
		}
		if loc.BatchIdx != -1 {
			res[idx] = n.Key
			idx++
		}
	}
	return res[:idx]
}

// GetDimension returns the vector dimension for this index
func (h *HNSWIndex) GetDimension() uint32 {
	return uint32(h.dims)
}
