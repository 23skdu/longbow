package store

import (
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
)

// complex64Computer handles Complex64 vectors
type complex64Computer struct {
	data *GraphData
	q    []complex64
	dims int
}

func (c *complex64Computer) Compute(ids []uint32, dists []float32) {
	for i, id := range ids {
		cID := chunkID(id)
		chunk := c.data.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			start := int(chunkOffset(id)) * c.data.GetPaddedDims()
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				dists[i] = simd.EuclideanDistanceComplex64(c.q, v)
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
		start := int(chunkOffset(id)) * c.data.GetPaddedDims()
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return simd.EuclideanDistanceComplex64(c.q, v)
		}
	}
	return math.MaxFloat32
}

func (c *complex64Computer) Prefetch(id uint32) {
	cID := chunkID(id)
	chunk := c.data.GetVectorsComplex64Chunk(cID)
	if chunk != nil {
		start := int(chunkOffset(id)) * c.data.GetPaddedDims()
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start]))
		}
	}
}

// complex128Computer handles Complex128 vectors
type complex128Computer struct {
	data *GraphData
	q    []complex128
	dims int
}

func (c *complex128Computer) Compute(ids []uint32, dists []float32) {
	for i, id := range ids {
		cID := chunkID(id)
		chunk := c.data.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			start := int(chunkOffset(id)) * c.data.GetPaddedDims()
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				dists[i] = simd.EuclideanDistanceComplex128(c.q, v)
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
		start := int(chunkOffset(id)) * c.data.GetPaddedDims()
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return simd.EuclideanDistanceComplex128(c.q, v)
		}
	}
	return math.MaxFloat32
}

func (c *complex128Computer) Prefetch(id uint32) {
	cID := chunkID(id)
	chunk := c.data.GetVectorsComplex128Chunk(cID)
	if chunk != nil {
		start := int(chunkOffset(id)) * c.data.GetPaddedDims()
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start]))
		}
	}
}
