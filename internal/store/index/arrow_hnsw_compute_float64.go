package index

import (
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/store/types"

	"github.com/23skdu/longbow/internal/simd"
)

// float64Computer handles Float64 vectors
type float64Computer struct {
	data      *types.GraphData
	q         []float64
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
}

func (c *float64Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		cID := types.ChunkID(id)
		var chunk []float64
		if c.maxGen == 18446744073709551615 {
			chunk = c.data.GetVectorsFloat64ChunkFast(int(cID))
		} else {
			chunk = c.data.GetVectorsFloat64ChunkWithGen(int(cID), c.maxGen)
		}
		if chunk != nil {
			cOff := int(id) % types.ChunkSize
			pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat64)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				d, err := c.h.distFuncF64(c.q, v)
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

func (c *float64Computer) ComputeSingle(id uint32) (float32, error) {
	// Fast path: direct chunk access (same order as float32 computer)
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsFloat64ChunkWithGen(int(cID), c.maxGen)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat64)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncF64(c.q, chunk[start:start+c.dims])
		}
	}

	// Fallback to disk-backed vector extraction
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]float64); ok {
			return c.h.distFuncF64(c.q, v)
		}
	}

	return math.MaxFloat32, nil
}

func (c *float64Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}

	for i, id := range ids {
		cID := types.ChunkID(id)
		var chunk []float64
		if c.maxGen == 18446744073709551615 {
			chunk = c.data.GetVectorsFloat64ChunkFast(int(cID))
		} else {
			chunk = c.data.GetVectorsFloat64ChunkWithGen(int(cID), c.maxGen)
		}
		if chunk != nil {
			cOff := int(id) % types.ChunkSize
			pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat64)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				d, err := c.h.distFuncF64(c.q, chunk[start:start+c.dims])
				if err != nil {
					dst[i] = math.MaxFloat32
					continue
				}
				dst[i] = d
				continue
			}
		}
		vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
		if err != nil {
			dst[i] = math.MaxFloat32
			continue
		}
		v, ok := vecAny.([]float64)
		if !ok {
			dst[i] = math.MaxFloat32
			continue
		}
		d, err := c.h.distFuncF64(c.q, v)
		if err != nil {
			dst[i] = math.MaxFloat32
			continue
		}
		dst[i] = d
	}

	return dst, nil
}

func (c *float64Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsFloat64ChunkWithGen(int(cID), c.maxGen)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat64)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}
