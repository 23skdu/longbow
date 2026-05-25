package index

import (
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/store/types"

	"github.com/23skdu/longbow/internal/simd"
)

// complex64Computer handles Complex64 vectors
type complex64Computer struct {
	data      *types.GraphData
	q         []complex64
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	batchVecs [][]complex64
}

func (c *complex64Computer) Compute(ids []uint32, dists []float32) error {
	var lastCID int = -1
	var lastChunk []complex64

	for i, id := range ids {
		cID := int(types.ChunkID(id))
		if cID != lastCID {
			lastChunk = c.data.GetVectorsComplex64ChunkWithGen(cID, c.maxGen)
			lastCID = cID
		}
		
		if lastChunk != nil {
			cOff := int(id) % types.ChunkSize
			pd := c.data.GetPaddedDimsForType(types.VectorTypeComplex64)
			start := cOff * pd
			if start+c.dims <= len(lastChunk) {
				v := lastChunk[start : start+c.dims]
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
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]complex64); ok {
			return c.h.distFuncC64(c.q, v)
		}
	}

	// Fallback to direct chunk access
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsComplex64ChunkWithGen(int(cID), c.maxGen)
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

func (c *complex64Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}

	if cap(c.batchVecs) < len(ids) {
		c.batchVecs = make([][]complex64, len(ids))
	}
	c.batchVecs = c.batchVecs[:len(ids)]

	var lastCID int = -1
	var lastChunk []complex64
	for i, id := range ids {
		cID := int(types.ChunkID(id))
		if cID != lastCID {
			lastChunk = c.data.GetVectorsComplex64ChunkWithGen(cID, c.maxGen)
			lastCID = cID
		}
		if lastChunk != nil {
			cOff := int(id) % types.ChunkSize
			pd := c.data.GetPaddedDimsForType(types.VectorTypeComplex64)
			start := cOff * pd
			if start+c.dims <= len(lastChunk) {
				c.batchVecs[i] = lastChunk[start : start+c.dims]
				continue
			}
		}
		dst[i] = math.MaxFloat32
	}
	return dst, c.Compute(ids, dst)
}

func (c *complex64Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsComplex64ChunkFast(int(cID))
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
	maxGen    uint64
	batchVecs [][]complex128
}

func (c *complex128Computer) Compute(ids []uint32, dists []float32) error {
	var lastCID int = -1
	var lastChunk []complex128

	for i, id := range ids {
		cID := int(types.ChunkID(id))
		if cID != lastCID {
			lastChunk = c.data.GetVectorsComplex128ChunkWithGen(cID, c.maxGen)
			lastCID = cID
		}
		
		if lastChunk != nil {
			cOff := int(id) % types.ChunkSize
			pd := c.data.GetPaddedDimsForType(types.VectorTypeComplex128)
			start := cOff * pd
			if start+c.dims <= len(lastChunk) {
				v := lastChunk[start : start+c.dims]
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
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]complex128); ok {
			return c.h.distFuncC128(c.q, v)
		}
	}

	// Fallback to direct chunk access
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsComplex128ChunkWithGen(int(cID), c.maxGen)
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

func (c *complex128Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}

	if cap(c.batchVecs) < len(ids) {
		c.batchVecs = make([][]complex128, len(ids))
	}
	c.batchVecs = c.batchVecs[:len(ids)]

	var lastCID int = -1
	var lastChunk []complex128
	for i, id := range ids {
		cID := int(types.ChunkID(id))
		if cID != lastCID {
			lastChunk = c.data.GetVectorsComplex128ChunkWithGen(cID, c.maxGen)
			lastCID = cID
		}
		if lastChunk != nil {
			cOff := int(id) % types.ChunkSize
			pd := c.data.GetPaddedDimsForType(types.VectorTypeComplex128)
			start := cOff * pd
			if start+c.dims <= len(lastChunk) {
				c.batchVecs[i] = lastChunk[start : start+c.dims]
				continue
			}
		}
		dst[i] = math.MaxFloat32
	}
	return dst, c.Compute(ids, dst)
}

func (c *complex128Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsComplex128ChunkFast(int(cID))
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeComplex128)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}
