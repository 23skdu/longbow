package core

import (
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

type float16Computer struct {
	data      *types.GraphData
	q         []float16.Num
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
}

func (c *float16Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsF16Chunk(cID)
		if chunk != nil {
			cOff := int(id) % types.ChunkSize
			pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat16)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				v := chunk[start : start+c.dims]
				d, err := c.h.distFuncF16(c.q, v)
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

func (c *float16Computer) ComputeSingle(id uint32) (float32, error) {
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id)
	if err == nil {
		if v, ok := vecAny.([]float16.Num); ok {
			return c.h.distFuncF16(c.q, v)
		}
	}

	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsF16Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat16)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncF16(c.q, chunk[start:start+c.dims])
		}
	}

	return math.MaxFloat32, nil
}

func (c *float16Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsF16Chunk(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat16)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start]))
		}
	}
}