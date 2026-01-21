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

func (c *float64Computer) Compute(ids []uint32, dists []float32) error {
	stride := c.data.GetPaddedDimsForType(VectorTypeFloat64)
	for i, id := range ids {
		cID := chunkID(id)
		chunk := c.data.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			start := int(chunkOffset(id)) * stride
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				d, err := simd.EuclideanDistanceFloat64(c.q, v)
				if err != nil {
					return err
				}
				dists[i] = float32(d)
				continue
			}
		}
		dists[i] = math.MaxFloat32
	}
	return nil
}

func (c *float64Computer) ComputeSingle(id uint32) (float32, error) {
	cID := chunkID(id)
	chunk := c.data.GetVectorsFloat64Chunk(cID)
	if chunk != nil {
		stride := c.data.GetPaddedDimsForType(VectorTypeFloat64)
		start := int(chunkOffset(id)) * stride
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			d, err := simd.EuclideanDistanceFloat64(c.q, v)
			return float32(d), err
		}
	}
	return math.MaxFloat32, nil
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
