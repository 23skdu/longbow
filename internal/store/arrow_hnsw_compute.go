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
	Compute(ids []uint32, dists []float32)
	ComputeSingle(id uint32) float32
	Prefetch(id uint32)
}

// float32Computer handles standard Float32 vectors.
type float32Computer struct {
	data       *GraphData
	q          []float32
	dims       int
	paddedDims int
}

func (c *float32Computer) Compute(ids []uint32, dists []float32) {
	vecs := c.data.Vectors // Primary array
	for i, id := range ids {
		cID := chunkID(id)
		if int(cID) < len(vecs) {
			chunk := c.data.GetVectorsChunk(cID)
			if chunk != nil {
				start := int(chunkOffset(id)) * c.paddedDims
				if start+c.dims <= len(chunk) {
					v := chunk[start : start+c.dims]
					dists[i] = simd.DistFunc(c.q, v)
					continue
				}
			}
		}
		dists[i] = math.MaxFloat32
	}
}

func (c *float32Computer) ComputeSingle(id uint32) float32 {
	cID := chunkID(id)
	chunk := c.data.GetVectorsChunk(cID)
	if chunk != nil {
		start := int(chunkOffset(id)) * c.paddedDims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return simd.DistFunc(c.q, v)
		}
	}
	return math.MaxFloat32
}

func (c *float32Computer) Prefetch(id uint32) {
}

// float16Computer handles Float16 vectors.
type float16Computer struct {
	data       *GraphData
	q          []float16.Num
	dims       int
	paddedDims int
}

func (c *float16Computer) Compute(ids []uint32, dists []float32) {
	for i, id := range ids {
		cID := chunkID(id)
		chunk := c.data.GetVectorsF16Chunk(cID)
		if chunk != nil {
			start := int(chunkOffset(id)) * c.paddedDims
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				dists[i] = simd.EuclideanDistanceF16(c.q, v)
				continue
			}
		}
		dists[i] = math.MaxFloat32
	}
}

func (c *float16Computer) ComputeSingle(id uint32) float32 {
	cID := chunkID(id)
	chunk := c.data.GetVectorsF16Chunk(cID)
	if chunk != nil {
		start := int(chunkOffset(id)) * c.paddedDims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return simd.EuclideanDistanceF16(c.q, v)
		}
	}
	return math.MaxFloat32
}

func (c *float16Computer) Prefetch(id uint32) {
}

// complex64Computer handles Complex64 vectors.
type complex64Computer struct {
	data       *GraphData
	q          []complex64
	dims       int
	paddedDims int
}

func (c *complex64Computer) Compute(ids []uint32, dists []float32) {
	for i, id := range ids {
		cID := chunkID(id)
		chunk := c.data.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			start := int(chunkOffset(id)) * c.paddedDims
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				dists[i] = simd.EuclideanDistanceComplex64(c.q, v)
				metrics.HNSWComplexOpsTotal.WithLabelValues("complex64").Inc()
				continue
			}
		}
		dists[i] = math.MaxFloat32
	}
}

func (c *complex64Computer) ComputeSingle(id uint32) float32 {
	cID := chunkID(id)
	chunk := c.data.GetVectorsComplex64Chunk(cID)
	if chunk != nil {
		start := int(chunkOffset(id)) * c.paddedDims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			metrics.HNSWComplexOpsTotal.WithLabelValues("complex64").Inc()
			return simd.EuclideanDistanceComplex64(c.q, v)
		}
	}
	return math.MaxFloat32
}

func (c *complex64Computer) Prefetch(id uint32) {
}

// complex128Computer handles Complex128 vectors.
type complex128Computer struct {
	data       *GraphData
	q          []complex128
	dims       int
	paddedDims int
}

func (c *complex128Computer) Compute(ids []uint32, dists []float32) {
	for i, id := range ids {
		cID := chunkID(id)
		chunk := c.data.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			start := int(chunkOffset(id)) * c.paddedDims
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				dists[i] = simd.EuclideanDistanceComplex128(c.q, v)
				metrics.HNSWComplexOpsTotal.WithLabelValues("complex128").Inc()
				continue
			}
		}
		dists[i] = math.MaxFloat32
	}
}

func (c *complex128Computer) ComputeSingle(id uint32) float32 {
	cID := chunkID(id)
	chunk := c.data.GetVectorsComplex128Chunk(cID)
	if chunk != nil {
		start := int(chunkOffset(id)) * c.paddedDims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			metrics.HNSWComplexOpsTotal.WithLabelValues("complex128").Inc()
			return simd.EuclideanDistanceComplex128(c.q, v)
		}
	}
	return math.MaxFloat32
}

func (c *complex128Computer) Prefetch(id uint32) {
}

// sq8Computer handles SQ8 quantized vectors.
type sq8Computer struct {
	data      *GraphData
	disk      *DiskGraph // Optional fallback
	querySQ8  []byte
	quantizer *ScalarQuantizer
	dims      int
	scale     float32
}

func (c *sq8Computer) Compute(ids []uint32, dists []float32) {
	for i, id := range ids {
		dists[i] = c.ComputeSingle(id)
	}
}

func (c *sq8Computer) ComputeSingle(id uint32) float32 {
	cID := chunkID(id)
	chunk := c.data.GetVectorsSQ8Chunk(cID)
	if chunk != nil {
		sq8Stride := (c.dims + 63) & ^63
		start := int(chunkOffset(id)) * sq8Stride
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return float32(simd.EuclideanDistanceSQ8(c.querySQ8, v)) * c.scale
		}
	}
	// Fallback to Disk
	if c.disk != nil && int(id) < c.disk.Size() {
		v := c.disk.GetVectorSQ8(id)
		if v != nil && len(v) == c.dims {
			return float32(simd.EuclideanDistanceSQ8(c.querySQ8, v)) * c.scale
		}
	}
	return math.MaxFloat32
}

func (c *sq8Computer) Prefetch(id uint32) {
	cID := chunkID(id)
	chunk := c.data.GetVectorsSQ8Chunk(cID)
	if chunk != nil {
		sq8Stride := (c.dims + 63) & ^63
		start := int(chunkOffset(id)) * sq8Stride
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

func (c *pqComputer) Compute(ids []uint32, dists []float32) {
	batchSize := len(ids)
	if len(c.scratchCodes) < batchSize*c.pqM {
		dists[0] = math.MaxFloat32
		return
	}
	codes := c.scratchCodes[:batchSize*c.pqM]

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
	c.pqEncoder.ADCDistanceBatch(c.adcTable, codes, dists)
}

func (c *pqComputer) ComputeSingle(id uint32) float32 {
	v := c.data.GetVectorPQ(id)
	if v == nil && c.disk != nil && int(id) < c.disk.Size() {
		v = c.disk.GetVectorPQ(id)
	}
	if v != nil {
		d, _ := c.pqEncoder.ADCDistance(c.adcTable, v)
		return d
	}
	return math.MaxFloat32
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

// resolveHNSWComputer selects the appropriate computer and encodes the query if necessary.
func (h *ArrowHNSW) resolveHNSWComputer(data *GraphData, ctx *ArrowSearchContext, q []float32) HNSWDistanceComputer {
	dims := len(q)
	disk := h.diskGraph.Load() // Load disk backend

	// ...

	// 3. PQ
	if h.config.PQEnabled && h.pqEncoder != nil {
		// Prepare ADC table
		table, err := h.pqEncoder.BuildADCTable(q)
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

	// 4. SQ8
	if h.config.SQ8Enabled && h.quantizer != nil && h.metric == MetricEuclidean {
		metrics.HNSWPolymorphicSearchCount.WithLabelValues("sq8").Inc()
		if cap(ctx.querySQ8) < dims {
			ctx.querySQ8 = make([]byte, dims)
		}
		ctx.querySQ8 = ctx.querySQ8[:dims]
		h.quantizer.Encode(q, ctx.querySQ8)

		return &sq8Computer{
			data:      data,
			disk:      disk,
			querySQ8:  ctx.querySQ8,
			quantizer: h.quantizer,
			dims:      data.Dims,
			scale:     h.quantizer.L2Scale(),
		}
	}

	// 5. Default Float32
	metrics.HNSWPolymorphicSearchCount.WithLabelValues("float32").Inc()
	return &float32Computer{
		data:       data,
		q:          q,
		dims:       data.Dims,
		paddedDims: data.GetPaddedDims(),
	}
}
