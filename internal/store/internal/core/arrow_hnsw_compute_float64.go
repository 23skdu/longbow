package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
)

// float64Computer handles Float64 vectors
type float64Computer struct {
	data *types.GraphData
	q    []float64
	dims int
}

func (c *float64Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsFloat64Chunk(cID)
		if chunk != nil {
			cOff := int(id) % types.ChunkSize
			start := cOff * c.data.Dims
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
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsFloat64Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		start := cOff * c.data.Dims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			d, err := simd.EuclideanDistanceFloat64(c.q, v)
			return float32(d), err
		}
	}
	return math.MaxFloat32, nil
}

func (c *float64Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsFloat64Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		start := cOff * c.data.Dims
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}
