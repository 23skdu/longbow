package store

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/coder/hnsw"
)

// Add inserts a new vector location into the index and adds it to the graph.
func (h *HNSWIndex) Add(batchIdx, rowIdx int) (uint32, error) {
	// 1. Prepare location and update locations slice under global lock
	// and get vector while holding the lock to protect against slice reallocations.
	start := time.Now()
	h.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "write").Observe(time.Since(start).Seconds())
	id := h.locationStore.Append(Location{BatchIdx: batchIdx, RowIdx: rowIdx})
	h.mu.Unlock()

	// Check if we should trigger PQ training
	if h.pqTrainingEnabled && !h.pqEnabled && int(id) == h.pqTrainingThreshold {
		go func() {
			// Train with default params: M=8, K=256, Iter=10
			if h.dims > 0 && h.dims%8 == 0 {
				metrics.HNSWPQTrainingTriggered.WithLabelValues(h.dataset.Name).Inc()
				start := time.Now()

				// Sample vectors for training
				sampleSize := 10000
				if int(id) < sampleSize {
					sampleSize = int(id)
				}
				sample := make([][]float32, 0, sampleSize)
				step := int(id) / sampleSize
				if step == 0 {
					step = 1
				}
				for i := 0; i < int(id); i += step {
					v := h.getVector(VectorID(i))
					if v != nil {
						sample = append(sample, v)
					}
				}

				err := h.TrainPQ(sample)
				if err != nil {
					// PQ Training failed
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
	nodeVec := vec
	h.pqCodesMu.RLock()
	pqEnabled := h.pqEnabled
	encoder := h.pqEncoder
	h.pqCodesMu.RUnlock()

	if pqEnabled && encoder != nil {
		codes, _ := encoder.Encode(vec)
		startPQ := time.Now()
		h.pqCodesMu.Lock()
		metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "pq_write").Observe(time.Since(startPQ).Seconds())
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
		nodeVec = pq.PackBytesToFloat32s(codes)

		metrics.HNSWPQCompressedBytesTotal.WithLabelValues(h.dataset.Name).Add(float64(len(codes)))
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	h.metricLockWait.WithLabelValues("write").Observe(time.Since(indexLockStart7).Seconds())

	h.Graph.Add(hnsw.MakeNode(id, nodeVec))

	// Track HNSW metrics
	h.metricNodeCount.Set(float64(h.nextVecID.Load()))
	nodeCount := float64(h.nextVecID.Load())
	if nodeCount > 1 {
		h.metricGraphHeight.Set(math.Log(nodeCount) / math.Log(4))
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
	// Validate buffer capacity to prevent panics in NewFloat32Data
	if len(values.Buffers()) > 1 && values.Buffers()[1] != nil {
		bufLen := values.Buffers()[1].Len()
		// NewFloat32Data expects buffer to hold values.Len() floats
		needed := values.Len() * 4
		if bufLen < needed {
			return 0, fmt.Errorf("AddSafe: vector data buffer truncated (len=%d, needed=%d)", bufLen, needed)
		}
	}
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
	startLoc := time.Now()
	h.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "write").Observe(time.Since(startLoc).Seconds())
	h.locationStore.Append(Location{BatchIdx: batchIdx, RowIdx: rowIdx})
	h.mu.Unlock()

	// 4. Initialize dims
	h.dimsOnce.Do(func() {
		h.dims = len(vec)
	})

	// 5. Add to graph under global lock
	indexLockStart7 := time.Now()

	// PQ Encoding
	var nodeVec = vec
	h.pqCodesMu.RLock()
	pqEnabled := h.pqEnabled
	encoder := h.pqEncoder
	h.pqCodesMu.RUnlock()

	if pqEnabled && encoder != nil {
		codes, _ := encoder.Encode(vec)
		startPQSafe := time.Now()
		h.pqCodesMu.Lock()
		metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "pq_write").Observe(time.Since(startPQSafe).Seconds())
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
		nodeVec = pq.PackBytesToFloat32s(codes)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "write").Observe(time.Since(indexLockStart7).Seconds())

	h.Graph.Add(hnsw.MakeNode(id, nodeVec))

	// Track HNSW metrics
	h.metricNodeCount.Set(float64(h.nextVecID.Load()))
	nodeCount := float64(h.nextVecID.Load())
	if nodeCount > 1 {
		h.metricGraphHeight.Set(math.Log(nodeCount) / math.Log(4))
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

// AddBatch implements VectorIndex interface for HNSWIndex.
func (h *HNSWIndex) AddBatch(recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	if len(recs) == 0 {
		return nil, nil
	}

	start := time.Now()
	defer func() {
		h.metricIndexBuildDuration.Observe(time.Since(start).Seconds())
	}()

	n := len(recs)
	ids := make([]uint32, n)
	vectors := make([][]float32, n)

	// 1. Extract vectors (Done outside h.mu lock)
	for i := 0; i < n; i++ {
		vec, err := h.extractVector(recs[i], rowIdxs[i])
		if err != nil {
			return nil, err
		}
		vectors[i] = vec
	}

	// 2. Allocate IDs and update locations efficiently
	// We use BatchAppend to update location store in one go and get the base ID
	locs := make([]Location, n)
	for i := 0; i < n; i++ {
		locs[i] = Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]}
	}

	// This is now atomic and efficient
	baseID := h.locationStore.BatchAppend(locs)

	// Populate return IDs
	for i := 0; i < n; i++ {
		ids[i] = uint32(baseID) + uint32(i)
	}

	// Update nextVecID atomically to reflect new count
	h.nextVecID.Store(uint32(h.locationStore.Len()))

	// 3. Initialize dims once
	if n > 0 && vectors[0] != nil {
		h.dimsOnce.Do(func() {
			h.dims = len(vectors[0])
		})
	}

	// 4. PQ Encoding (Done outside h.mu lock, parallelized)
	encodedVectors := make([][]float32, n)
	h.pqCodesMu.RLock()
	pqEnabled := h.pqEnabled
	encoder := h.pqEncoder
	h.pqCodesMu.RUnlock()

	// Parallel PQ encoding for large batches
	if pqEnabled && encoder != nil {
		// Just use a simple loop if batch is small, spread if large
		if n < 100 {
			for i := 0; i < n; i++ {
				codes, _ := encoder.Encode(vectors[i])
				encodedVectors[i] = pq.PackBytesToFloat32s(codes)
			}
		} else {
			var wg sync.WaitGroup
			chunkSize := (n + 8 - 1) / 8 // 8 workers appropriate for encoding
			for i := 0; i < 8; i++ {
				start := i * chunkSize
				end := start + chunkSize
				if end > n {
					end = n
				}
				if start >= end {
					break
				}
				wg.Add(1)
				go func(s, e int) {
					defer wg.Done()
					for j := s; j < e; j++ {
						codes, _ := encoder.Encode(vectors[j])
						encodedVectors[j] = pq.PackBytesToFloat32s(codes)
					}
				}(start, end)
			}
			wg.Wait()
		}

		// Store PQ codes safely
		startPQ := time.Now()
		h.pqCodesMu.Lock()
		metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "pq_write").Observe(time.Since(startPQ).Seconds())
		targetLen := int(baseID) + n
		if len(h.pqCodes) < targetLen {
			// Resize
			newCap := targetLen * 2 // Aggressive growth
			if newCap < 1024 {
				newCap = 1024
			}
			newCodes := make([][]uint8, targetLen, newCap)
			copy(newCodes, h.pqCodes)
			h.pqCodes = newCodes
		} else {
			h.pqCodes = h.pqCodes[:targetLen]
		}

		// Fill codes (we have to re-encode or unpack, optimizing: should return codes from parallel loop)
		for i := 0; i < n; i++ {
			id := int(baseID) + i
			// UnpackFloat32sToBytes is robust
			h.pqCodes[id] = pq.UnpackFloat32sToBytes(encodedVectors[i], encoder.CodeSize())
		}
		h.pqCodesMu.Unlock()

	} else {
		for i := 0; i < n; i++ {
			encodedVectors[i] = vectors[i]
		}
	}

	// 5. Add to graph (Batched Locking)
	// Use extremely fine-grained locking to allow Searches to interleave fairly.
	const lockBatchSize = 1

	for i := 0; i < n; i += lockBatchSize {
		end := i + lockBatchSize
		if end > n {
			end = n
		}

		h.mu.Lock()
		for j := i; j < end; j++ {
			id := baseID + VectorID(j)
			h.Graph.Add(hnsw.MakeNode(id, encodedVectors[j]))
		}
		h.mu.Unlock()
	}

	// Update metrics
	h.metricNodeCount.Set(float64(h.nextVecID.Load()))

	return ids, nil
}

// extractor helper to avoid code duplication
func (h *HNSWIndex) extractVector(rec arrow.RecordBatch, rowIdx int) ([]float32, error) {
	if rec == nil {
		return nil, fmt.Errorf("extractVector: record is nil")
	}

	// Use cached vectorColIdx if available
	colIdx := int(h.vectorColIdx.Load())
	// Verify if cached index is valid and points to correct column
	valid := false
	if colIdx >= 0 && colIdx < int(rec.NumCols()) {
		name := rec.Schema().Field(colIdx).Name
		if name == "vector" || name == "embedding" {
			valid = true
		}
	}

	if !valid {
		// Scan for vector column
		found := false
		for i, field := range rec.Schema().Fields() {
			if field.Name == "vector" || field.Name == "embedding" {
				colIdx = i
				h.vectorColIdx.Store(int32(i))
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("extractVector: vector column not found")
		}
	}

	// Extract using shared helper (handles Float16/Float32 conversions)
	// ExtractVectorFromArrow returns a zero-copy slice (or converted slice).
	vecView, err := ExtractVectorFromArrow(rec, rowIdx, colIdx)
	if err != nil {
		return nil, fmt.Errorf("extractVector: %v", err)
	}

	// Must copy because HNSW (legacy) might outlive record batch and we need stable memory
	vec := make([]float32, len(vecView))
	copy(vec, vecView)

	// Track HNSW allocation metrics
	h.metricVectorAllocations.Inc()
	h.metricVectorAllocatedBytes.Add(float64(len(vec) * 4))

	return vec, nil
}

// vectorData holds vector ID and data for parallel processing
type vectorData struct {
	id  VectorID
	vec []float32
}

// AddBatchParallel adds multiple vectors in parallel using worker goroutines.
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
	start := time.Now()
	h.mu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "write").Observe(time.Since(start).Seconds())
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
		h.metricLockWait.WithLabelValues("write").Observe(time.Since(indexLockStart9).Seconds())
		h.Graph.Add(hnsw.MakeNode(vd.id, vd.vec))
		h.mu.Unlock()
	}

	return nil
}

// SetPQEncoder enables product quantization with the provided encoder.
func (h *HNSWIndex) SetPQEncoder(encoder *pq.PQEncoder) {
	start := time.Now()
	h.pqCodesMu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "pq_write").Observe(time.Since(start).Seconds())
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
func (h *HNSWIndex) TrainPQ(vectors [][]float32) error {
	start := time.Now()
	defer func() {
		metrics.HNSWPQTrainingDuration.WithLabelValues(h.dataset.Name).Observe(time.Since(start).Seconds())
	}()

	if len(vectors) == 0 {
		return fmt.Errorf("empty training vectors")
	}

	dims := len(vectors[0])
	m := 8 // Default
	if dims%8 != 0 {
		m = 4 // Fallback
		if dims%4 != 0 {
			m = 1
		}
	}
	k := 256

	enc, err := pq.NewPQEncoder(dims, m, k)
	if err != nil {
		return err
	}

	if err := enc.Train(vectors); err != nil {
		return err
	}

	h.SetPQEncoder(enc)

	// Encode existing vectors
	count := int(h.nextVecID.Load())
	startPQ := time.Now()
	h.pqCodesMu.Lock()
	metrics.IndexLockWaitDuration.WithLabelValues(h.dataset.Name, "pq_write").Observe(time.Since(startPQ).Seconds())
	defer h.pqCodesMu.Unlock()

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
			codes, _ := enc.Encode(vec)
			h.pqCodes[i] = codes
		}
	}

	return nil
}

func (h *HNSWIndex) GetPQEncoder() *pq.PQEncoder {
	h.pqCodesMu.RLock()
	defer h.pqCodesMu.RUnlock()
	return h.pqEncoder
}
