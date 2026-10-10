package index

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
	maxGen    uint64
}

func (c *float16Computer) Compute(ids []uint32, dists []float32) error {
	_, err := c.ComputeBatch(ids, dists)
	return err
}

func (c *float16Computer) ComputeSingle(id uint32) (float32, error) {
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]float16.Num); ok {
			return c.h.distFuncF16(c.q, v)
		}
	}

	cID := types.ChunkID(id)
	var chunk []float16.Num
	if c.maxGen == math.MaxUint64 {
		chunk = c.data.GetVectorsF16ChunkFast(int(cID))
	} else {
		chunk = c.data.GetVectorsF16ChunkWithGen(int(cID), c.maxGen)
	}
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

func (c *float16Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}
	if len(ids) == 0 {
		return dst, nil
	}

	pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat16)
	var lastChunkID int32 = -1
	var chunk []float16.Num

	for i, id := range ids {
		cID := int32(types.ChunkID(id)) // #nosec G115
		if cID != lastChunkID {
			if c.maxGen == math.MaxUint64 {
				chunk = c.data.GetVectorsF16ChunkFast(int(cID))
			} else {
				chunk = c.data.GetVectorsF16ChunkWithGen(int(cID), c.maxGen)
			}
			lastChunkID = cID
		}
		if chunk != nil {
			cOff := int(id) % types.ChunkSize
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				d, err := c.h.distFuncF16(c.q, chunk[start:start+c.dims])
				if err != nil {
					return nil, err
				}
				dst[i] = d
				continue
			}
		}
		// Fallback for this vector
		d, err := c.ComputeSingle(id)
		if err != nil {
			return nil, err
		}
		dst[i] = d
	}
	return dst, nil
}

func (c *float16Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsF16ChunkWithGen(int(cID), c.maxGen)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat16)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103 -- intentional unsafe for performance
		}
	}
}
