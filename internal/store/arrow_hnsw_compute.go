package store

import (
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// HNSWDistanceComputer abstracts the loop for computing distances for a batch of IDs.
type HNSWDistanceComputer interface {
	Compute(ids []uint32, dists []float32) error
	ComputeSingle(id uint32) (float32, error)
	Prefetch(id uint32)
}

// float32Computer handles standard Float32 vectors.
type float32Computer struct {
	data          *GraphData
	q             []float32
	dims          int
	paddedDims    int
	distFunc      DistanceFunc      // Use defined types from metric.go
	batchDistFunc BatchDistanceFunc // Use defined types from metric.go
	ctx           *ArrowSearchContext
}

func (c *float32Computer) Compute(ids []uint32, dists []float32) error {
	primaryVecs := c.data.Vectors
	dims := c.dims
	paddedDims := c.paddedDims

	// FAST PATH: Small batches to avoid gathering vecs slice
	if len(ids) <= 4 {
		for i, id := range ids {
			cID := chunkID(id)
			if int(cID) < len(primaryVecs) {
				chunk := c.data.GetVectorsChunk(cID)
				if chunk != nil {
					start := int(chunkOffset(id)) * paddedDims
					if start+dims <= len(chunk) {
						d, _ := c.distFunc(c.q, chunk[start:start+dims])
						dists[i] = d
						continue
					}
				}
			}
			dists[i] = math.MaxFloat32
		}
		return nil
	}

	// resize scratch buffer
	if cap(c.ctx.scratchVecs) < len(ids) {
		c.ctx.scratchVecs = make([][]float32, len(ids))
	}
	c.ctx.scratchVecs = c.ctx.scratchVecs[:len(ids)]
	vecs := c.ctx.scratchVecs

	// Use scratchNeighbors to track missing indices (safe to reuse here)
	missingIndices := c.ctx.scratchNeighbors[:0]

	// Gather vectors
	for i, id := range ids {
		cID := chunkID(id)
		if int(cID) < len(primaryVecs) {
			chunk := c.data.GetVectorsChunk(cID)
			if chunk != nil {
				start := int(chunkOffset(id)) * paddedDims
				if start+dims <= len(chunk) {
					vecs[i] = chunk[start : start+dims]
					continue
				}
			}
		}
		// Sentinel: use query as dummy (distance 0), fix up later
		vecs[i] = c.q
		missingIndices = append(missingIndices, uint32(i))
	}

	// Compute batch
	if err := c.batchDistFunc(c.q, vecs, dists); err != nil {
		return err
	}

	// Post-check: Fixup missing
	for _, i := range missingIndices {
		dists[i] = math.MaxFloat32
	}
	return nil
}

func (c *float32Computer) ComputeSingle(id uint32) (float32, error) {
	cID := chunkID(id)
	chunk := c.data.GetVectorsChunk(cID)
	if chunk != nil {
		start := int(chunkOffset(id)) * c.paddedDims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return c.distFunc(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func (c *float32Computer) Prefetch(id uint32) {
	cID := chunkID(id)
	chunk := c.data.GetVectorsChunk(cID)
	if chunk != nil {
		start := int(chunkOffset(id)) * c.paddedDims
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start]))
		}
	}
}

// sq8Computer handles SQ8 quantized vectors.
type sq8Computer struct {
	data       *GraphData
	disk       *DiskGraph // Optional fallback
	querySQ8   []byte
	quantizer  *ScalarQuantizer
	dims       int
	paddedDims int
	scale      float32
	ctx        *ArrowSearchContext
}

func (c *sq8Computer) Compute(ids []uint32, dists []float32) error {
	// Gather vectors and prefetch
	if cap(c.ctx.scratchVecsSQ8) < len(ids) {
		c.ctx.scratchVecsSQ8 = make([][]byte, len(ids))
	}
	c.ctx.scratchVecsSQ8 = c.ctx.scratchVecsSQ8[:len(ids)]
	vecs := c.ctx.scratchVecsSQ8

	// Batch prefetch
	for _, id := range ids {
		c.Prefetch(id)
	}

	for i, id := range ids {
		cID := chunkID(id)
		chunk := c.data.GetVectorsSQ8Chunk(cID)
		if chunk != nil {
			start := int(chunkOffset(id)) * c.paddedDims
			if start+c.dims <= len(chunk) {
				vecs[i] = chunk[start : start+c.dims]
				continue
			}
		}
		// Fallback: will be handled by EuclideanDistanceSQ8Batch if we use dummy,
		// but currently it doesn't handle nil. Let's use ComputeSingle for rare fallbacks.
		d, _ := c.ComputeSingle(id)
		dists[i] = d
		vecs[i] = nil // Mark as already computed
	}

	// Filter out already computed (disk fallbacks)
	// For simplicity, if most are in memory, we can just compute the whole batch
	// but nil checks are safer.
	// Actually, let's just use the batch function for everything in memory.
	err := simd.EuclideanDistanceSQ8Batch(c.querySQ8, vecs, dists)
	if err != nil {
		return err
	}

	// Apply scale
	for i := range dists {
		if vecs[i] != nil { // Only if computed by batch
			dists[i] *= c.scale
		}
	}

	return nil
}

func (c *sq8Computer) ComputeSingle(id uint32) (float32, error) {
	cID := chunkID(id)
	chunk := c.data.GetVectorsSQ8Chunk(cID)
	if chunk != nil {
		start := int(chunkOffset(id)) * c.paddedDims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			d, err := simd.EuclideanDistanceSQ8(c.querySQ8, v)
			return float32(d) * c.scale, err
		}
	}
	// Fallback to Disk
	if c.disk != nil && int(id) < c.disk.Size() {
		v := c.disk.GetVectorSQ8(id)
		if v != nil && len(v) == c.dims {
			d, err := simd.EuclideanDistanceSQ8(c.querySQ8, v)
			return float32(d) * c.scale, err
		}
	}
	return math.MaxFloat32, nil
}

func (c *sq8Computer) Prefetch(id uint32) {
	cID := chunkID(id)
	chunk := c.data.GetVectorsSQ8Chunk(cID)
	if chunk != nil {
		start := int(chunkOffset(id)) * c.paddedDims
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start]))
		}
	}
}

// pqComputer handles Product Quantization (ADC).
type pqComputer struct {
	data         *GraphData
	disk         *DiskGraph // Optional fallback
	adcTable     []float32
	pqEncoder    *pq.PQEncoder
	scratchCodes []byte
	pqM          int
}

func (c *pqComputer) Compute(ids []uint32, dists []float32) error {
	batchSize := len(ids)
	requiredSize := batchSize * c.pqM
	if cap(c.scratchCodes) < requiredSize {
		// This should be rare as the pool context should eventually stabilize
		c.scratchCodes = make([]byte, requiredSize)
	}
	codes := c.scratchCodes[:requiredSize]

	for i, id := range ids {
		v := c.data.GetVectorPQ(id)
		if v == nil && c.disk != nil && int(id) < c.disk.Size() {
			v = c.disk.GetVectorPQ(id)
		}

		if v != nil {
			copy(codes[i*c.pqM:(i+1)*c.pqM], v)
		} else {
			end := (i + 1) * c.pqM
			for j := i * c.pqM; j < end; j++ {
				codes[j] = 0
			}
		}
	}
	return c.pqEncoder.ADCDistanceBatch(c.adcTable, codes, dists)
}

func (c *pqComputer) ComputeSingle(id uint32) (float32, error) {
	v := c.data.GetVectorPQ(id)
	if v == nil && c.disk != nil && int(id) < c.disk.Size() {
		v = c.disk.GetVectorPQ(id)
	}
	if v != nil {
		return c.pqEncoder.ADCDistance(c.adcTable, v)
	}
	return math.MaxFloat32, nil
}

func (c *pqComputer) Prefetch(id uint32) {
	cID := chunkID(id)
	chunk := c.data.GetVectorsPQChunk(cID)
	if chunk != nil {
		pqM := c.pqM
		start := int(chunkOffset(id)) * pqM
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start]))
		}
	}
}

// float16Computer handles Float16 vectors.
type float16Computer struct {
	data       *GraphData
	q          []float16.Num
	dims       int
	paddedDims int
	ctx        *ArrowSearchContext
}

func (c *float16Computer) Compute(ids []uint32, dists []float32) error {
	if cap(c.ctx.scratchVecsF16) < len(ids) {
		c.ctx.scratchVecsF16 = make([][]float16.Num, len(ids))
	}
	c.ctx.scratchVecsF16 = c.ctx.scratchVecsF16[:len(ids)]
	vecs := c.ctx.scratchVecsF16

	// Batch prefetch
	for _, id := range ids {
		c.Prefetch(id)
	}

	for i, id := range ids {
		cID := chunkID(id)
		chunk := c.data.GetVectorsF16Chunk(cID)
		if chunk != nil {
			start := int(chunkOffset(id)) * c.paddedDims
			if start+c.dims <= len(chunk) {
				vecs[i] = chunk[start : start+c.dims]
				continue
			}
		}
		// Fallback
		d, _ := c.ComputeSingle(id)
		dists[i] = d
		vecs[i] = nil
	}

	return simd.EuclideanDistanceF16Batch(c.q, vecs, dists)
}

func (c *float16Computer) ComputeSingle(id uint32) (float32, error) {
	cID := chunkID(id)
	chunk := c.data.GetVectorsF16Chunk(cID)
	if chunk != nil {
		start := int(chunkOffset(id)) * c.paddedDims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return simd.EuclideanDistanceF16(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func (c *float16Computer) Prefetch(id uint32) {
	cID := chunkID(id)
	chunk := c.data.GetVectorsF16Chunk(cID)
	if chunk != nil {
		start := int(chunkOffset(id)) * c.paddedDims
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start]))
		}
	}
}

// genericComputer handles any storage type by converting to float32 on the fly.
// This is slower but correct for new/unsupported types like Int8.
type genericComputer struct {
	data       *GraphData
	q          []float32
	dims       int
	distFunc   func([]float32, []float32) (float32, error)
	scratchVec []float32
	ctx        *ArrowSearchContext
}

func (c *genericComputer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		d, err := c.ComputeSingle(id)
		if err != nil {
			return err
		}
		dists[i] = d
	}
	return nil
}

func (c *genericComputer) ComputeSingle(id uint32) (float32, error) {
	// Use GetVectorAsFloat32Into to avoid allocation
	v, err := c.data.GetVectorAsFloat32Into(id, c.scratchVec)
	if err == nil {
		return c.distFunc(c.q, v)
	}
	return math.MaxFloat32, nil
}

func (c *genericComputer) Prefetch(id uint32) {
	// Generic prefetch is hard without knowing underlying type.
	// Can optimize later if needed.
}

// resolveHNSWComputer selects the appropriate computer and encodes the query if necessary.
func (h *ArrowHNSW) resolveHNSWComputer(data *GraphData, ctx *ArrowSearchContext, queryVec any, forceFullPrecision bool) HNSWDistanceComputer {
	// Defaults if we can't determine dimensions from query logic below (fallback)
	// dims := int(h.dims.Load()) // Unused
	disk := h.diskGraph.Load() // Load disk backend

	// 1. Float16 Native
	if qF16, ok := queryVec.([]float16.Num); ok {
		// Native Float16 query
		metrics.HNSWPolymorphicSearchCount.WithLabelValues("float16").Inc()
		return &float16Computer{
			data:       data,
			q:          qF16,
			dims:       data.Dims,
			paddedDims: data.GetPaddedDimsForType(VectorTypeFloat16),
			ctx:        ctx,
		}
	}

	// Float16 Enabled but using Float32 query (conversion required)
	// Skip if forceFullPrecision is true and data is Float32
	if (h.config.Float16Enabled && !forceFullPrecision) || data.Type == VectorTypeFloat16 {
		if qF32, ok := queryVec.([]float32); ok {
			dims := len(qF32)
			if cap(ctx.queryF16) < dims {
				ctx.queryF16 = make([]float16.Num, dims)
			}
			ctx.queryF16 = ctx.queryF16[:dims]

			// Convert query: float32 -> float16
			for i, v := range qF32 {
				ctx.queryF16[i] = float16.New(v)
			}

			metrics.HNSWPolymorphicSearchCount.WithLabelValues("float16").Inc()
			return &float16Computer{
				data:       data,
				q:          ctx.queryF16,
				dims:       data.Dims,
				paddedDims: data.GetPaddedDimsForType(VectorTypeFloat16),
				ctx:        ctx,
			}
		}
	}

	// 2. Float64 Native
	if data.Type == VectorTypeFloat64 {
		if qF64, ok := queryVec.([]float64); ok {
			metrics.HNSWPolymorphicSearchCount.WithLabelValues("float64").Inc()
			return &float64Computer{
				data: data,
				q:    qF64,
				dims: data.Dims,
			}
		}
		if qF32, ok := queryVec.([]float32); ok {
			// Convert query: float32 -> float64
			qF64 := make([]float64, len(qF32))
			for i, v := range qF32 {
				qF64[i] = float64(v)
			}
			metrics.HNSWPolymorphicSearchCount.WithLabelValues("float64").Inc()
			return &float64Computer{
				data: data,
				q:    qF64,
				dims: data.Dims,
			}
		}
	}

	// 3. Complex Types
	if data.Type == VectorTypeComplex64 {
		if qC64, ok := queryVec.([]complex64); ok {
			return &complex64Computer{
				data: data,
				q:    qC64,
				dims: data.Dims,
			}
		}
		if qF32, ok := queryVec.([]float32); ok {
			dims := len(qF32)
			// Convert Query slice qF32 (float32 interleaved) to complex64
			qC := make([]complex64, dims/2)
			for i := 0; i < dims/2; i++ {
				qC[i] = complex(qF32[2*i], qF32[2*i+1])
			}
			return &complex64Computer{
				data: data,
				q:    qC,
				dims: data.Dims,
			}
		}
	}
	if data.Type == VectorTypeComplex128 {
		if qC128, ok := queryVec.([]complex128); ok {
			return &complex128Computer{
				data: data,
				q:    qC128,
				dims: data.Dims,
			}
		}
		if qF32, ok := queryVec.([]float32); ok {
			dims := len(qF32)
			qC := make([]complex128, dims/2)
			for i := 0; i < dims/2; i++ {
				qC[i] = complex(float64(qF32[2*i]), float64(qF32[2*i+1]))
			}
			return &complex128Computer{
				data: data,
				q:    qC,
				dims: data.Dims,
			}
		}
	}

	// ... continue with existing logic ...

	if !forceFullPrecision {
		// 3. PQ (reordered index for diff)
		if h.config.PQEnabled && h.pqEncoder != nil {
			if qF32, ok := queryVec.([]float32); ok {
				// Prepare ADC table
				table, err := h.pqEncoder.BuildADCTable(qF32)
				if err == nil {
					metrics.HNSWPolymorphicSearchCount.WithLabelValues("pq").Inc()
					ctx.adcTable = table
					return &pqComputer{
						data:         data,
						disk:         disk,
						adcTable:     ctx.adcTable,
						pqEncoder:    h.pqEncoder,
						scratchCodes: ctx.scratchPQCodes,
						pqM:          data.PQDims, // Use data.PQDims (loaded from graph)
					}
				}
			}
		}

		// 4. SQ8
		// Only supports float32 input for now as SQ8 encoding expects float32
		if h.config.SQ8Enabled && h.quantizer != nil && h.metric == MetricEuclidean {
			if qF32, ok := queryVec.([]float32); ok {
				dims := len(qF32)
				metrics.HNSWPolymorphicSearchCount.WithLabelValues("sq8").Inc()
				if cap(ctx.querySQ8) < dims {
					ctx.querySQ8 = make([]byte, dims)
				}
				ctx.querySQ8 = ctx.querySQ8[:dims]
				h.quantizer.Encode(qF32, ctx.querySQ8)

				return &sq8Computer{
					data:       data,
					disk:       disk,
					querySQ8:   ctx.querySQ8,
					quantizer:  h.quantizer,
					dims:       data.Dims,
					paddedDims: (data.Dims + 63) & ^63,
					scale:      h.quantizer.L2Scale(),
					ctx:        ctx,
				}
			}
		}
	}

	// 5. Default Float32
	metrics.HNSWPolymorphicSearchCount.WithLabelValues("float32").Inc()

	// Handle Float32 input (or convert others)
	var qF32 []float32
	var dims int

	// Helper for buffer management
	ensureF32 := func(l int) {
		if cap(ctx.queryF32) < l {
			ctx.queryF32 = make([]float32, l)
		}
		ctx.queryF32 = ctx.queryF32[:l]
	}

	switch q := queryVec.(type) {
	case []float32:
		qF32 = q
		dims = len(q)
	case []float64:
		dims = len(q)
		ensureF32(dims)
		for i, v := range q {
			ctx.queryF32[i] = float32(v)
		}
		qF32 = ctx.queryF32
	case []int64:
		dims = len(q)
		ensureF32(dims)
		for i, v := range q {
			ctx.queryF32[i] = float32(v)
		}
		qF32 = ctx.queryF32
	case []int32:
		dims = len(q)
		ensureF32(dims)
		for i, v := range q {
			ctx.queryF32[i] = float32(v)
		}
		qF32 = ctx.queryF32
	case []int16:
		dims = len(q)
		ensureF32(dims)
		for i, v := range q {
			ctx.queryF32[i] = float32(v)
		}
		qF32 = ctx.queryF32
	case []int8:
		dims = len(q)
		ensureF32(dims)
		for i, v := range q {
			ctx.queryF32[i] = float32(v)
		}
		qF32 = ctx.queryF32
	case []uint64:
		dims = len(q)
		ensureF32(dims)
		for i, v := range q {
			ctx.queryF32[i] = float32(v)
		}
		qF32 = ctx.queryF32
	case []uint32:
		dims = len(q)
		ensureF32(dims)
		for i, v := range q {
			ctx.queryF32[i] = float32(v)
		}
		qF32 = ctx.queryF32
	case []uint16:
		dims = len(q)
		ensureF32(dims)
		for i, v := range q {
			ctx.queryF32[i] = float32(v)
		}
		qF32 = ctx.queryF32
	case []uint8:
		dims = len(q)
		ensureF32(dims)
		for i, v := range q {
			ctx.queryF32[i] = float32(v)
		}
		qF32 = ctx.queryF32
	case []complex64:
		dims = len(q) * 2
		ensureF32(dims)
		for i, v := range q {
			ctx.queryF32[2*i] = real(v)
			ctx.queryF32[2*i+1] = imag(v)
		}
		qF32 = ctx.queryF32
	case []complex128:
		dims = len(q) * 2
		ensureF32(dims)
		for i, v := range q {
			ctx.queryF32[2*i] = float32(real(v))
			ctx.queryF32[2*i+1] = float32(imag(v))
		}
		qF32 = ctx.queryF32
	default:
		// Unrecoverable type mismatch
		// We return a generic computer (likely to fail if q is nil, but better than panic)
		return &float32Computer{data: data, dims: data.Dims, ctx: ctx}
	}

	// Select distance function with potential blocked optimization
	var distFunc func([]float32, []float32) (float32, error)
	var batchDistFunc func([]float32, [][]float32, []float32) error

	switch h.metric {
	case MetricDotProduct:
		if dims > 1024 {
			distFunc = simd.DotProductFloat32Blocked
		} else {
			distFunc = simd.DotProduct
		}
		batchDistFunc = simd.DotProductTiledBatch
	case MetricCosine:
		// Potential: CosineBlocked (Not already implemented, fallback)
		distFunc = simd.CosineDistance
		batchDistFunc = simd.CosineDistanceBatch
	default:
		// Euclidean
		if dims > 1024 {
			distFunc = simd.L2Float32Blocked
		} else {
			distFunc = simd.EuclideanDistance
		}
		batchDistFunc = simd.EuclideanDistanceTiledBatch
	}

	// Use GenericComputer if storage is not Float32 and we don't have specialized computer
	// Note: Float16, Float64, Complex, PQ, SQ8 handled above.
	// If we are here, we have Float32 Query (converted) but Unknown Storage Type (e.g. Int8)
	// OR we have Float32 Storage.
	if data.Type != VectorTypeFloat32 {
		return &genericComputer{
			data:       data,
			q:          qF32,
			dims:       data.Dims,
			distFunc:   distFunc,
			scratchVec: make([]float32, data.Dims),
			ctx:        ctx,
		}
	}

	return &float32Computer{
		data:          data,
		q:             qF32,
		dims:          data.Dims,
		paddedDims:    data.GetPaddedDims(),
		distFunc:      distFunc,
		batchDistFunc: batchDistFunc,
		ctx:           ctx,
	}
}
