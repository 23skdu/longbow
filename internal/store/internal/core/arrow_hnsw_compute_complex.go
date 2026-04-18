package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
)

// complex64Computer handles Complex64 vectors
type complex64Computer struct {
	data *types.GraphData
	q    []complex64
	dims int
}

func (c *complex64Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			cOff := int(id) % types.ChunkSize
			start := cOff * c.data.Dims
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				d, err := simd.EuclideanDistanceComplex64(c.q, v)
				if err != nil {
					return err
				}
				dists[i] = d
				continue
			}
		}
		dists[i] = math.MaxFloat32
	}
	return nil
}

func (c *complex64Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsComplex64Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		start := cOff * c.data.Dims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return simd.EuclideanDistanceComplex64(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func (c *complex64Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsComplex64Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		start := cOff * c.data.Dims
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start]))
		}
	}
}

// complex128Computer handles Complex128 vectors
type complex128Computer struct {
	data *types.GraphData
	q    []complex128
	dims int
}

func (c *complex128Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			cOff := int(id) % types.ChunkSize
			start := cOff * c.data.Dims
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				d, err := simd.EuclideanDistanceComplex128(c.q, v)
				if err != nil {
					return err
				}
				dists[i] = d
				continue
			}
		}
		dists[i] = math.MaxFloat32
	}
	return nil
}

func (c *complex128Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsComplex128Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		start := cOff * c.data.Dims
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return simd.EuclideanDistanceComplex128(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func (c *complex128Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsComplex128Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		start := cOff * c.data.Dims
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start]))
		}
	}
}
