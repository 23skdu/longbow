package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
)

// complex64Computer handles Complex64 vectors
type complex64Computer struct {
	data      *types.GraphData
	q         []complex64
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
}

func (c *complex64Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsComplex64Chunk(cID)
		if chunk != nil {
			cOff := int(id) % types.ChunkSize
			pd := c.data.GetPaddedDimsForType(types.VectorTypeComplex64)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				d, err := c.h.distFuncC64(c.q, v)
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
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id)
	if err == nil {
		if v, ok := vecAny.([]complex64); ok {
			return c.h.distFuncC64(c.q, v)
		}
	}

	// Fallback to direct chunk access
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsComplex64Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeComplex64)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncC64(c.q, chunk[start:start+c.dims])
		}
	}

	return math.MaxFloat32, nil
}

func (c *complex64Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsComplex64Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeComplex64)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

// complex128Computer handles Complex128 vectors
type complex128Computer struct {
	data      *types.GraphData
	q         []complex128
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
}

func (c *complex128Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsComplex128Chunk(cID)
		if chunk != nil {
			cOff := int(id) % types.ChunkSize
			pd := c.data.GetPaddedDimsForType(types.VectorTypeComplex128)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				d, err := c.h.distFuncC128(c.q, v)
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
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id)
	if err == nil {
		if v, ok := vecAny.([]complex128); ok {
			return c.h.distFuncC128(c.q, v)
		}
	}

	// Fallback to direct chunk access
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsComplex128Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeComplex128)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncC128(c.q, chunk[start:start+c.dims])
		}
	}

	return math.MaxFloat32, nil
}

func (c *complex128Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsComplex128Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeComplex128)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}
