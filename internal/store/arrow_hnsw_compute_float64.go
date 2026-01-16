package store

import (
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
)

// float64Computer handles Float64 vectors
type float64Computer struct {
	data *GraphData
	q    []float64
	dims int
}

func (c *float64Computer) Compute(ids []uint32, dists []float32) {
	stride := c.data.GetPaddedDimsForType(VectorTypeFloat64)
	for i, id := range ids {
		cID := chunkID(id)
		chunk := c.data.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			start := int(chunkOffset(id)) * stride
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				dists[i] = simd.EuclideanDistanceFloat64(c.q, v)
				continue
			}
		}
		dists[i] = math.MaxFloat32
	}
}

func (c *float64Computer) ComputeSingle(id uint32) float32 {
	cID := chunkID(id)
	chunk := c.data.GetVectorsFloat64Chunk(cID)
	if chunk != nil {
		stride := c.data.GetPaddedDimsForType(VectorTypeFloat64)
		start := int(chunkOffset(id)) * stride
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return simd.EuclideanDistanceFloat64(c.q, v)
		}
	}
	return math.MaxFloat32
}

func (c *float64Computer) Prefetch(id uint32) {
	cID := chunkID(id)
	chunk := c.data.GetVectorsFloat64Chunk(cID)
	if chunk != nil {
		stride := c.data.GetPaddedDimsForType(VectorTypeFloat64)
		start := int(chunkOffset(id)) * stride
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start]))
		}
	}
}
