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
	batchVecs [][]float64
	// Reusable float32 conversion buffer for SIMD batch
	batchF32  [][]float32
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
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]float64); ok {
			return c.h.distFuncF64(c.q, v)
		}
	}

	// Fallback to direct chunk access
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

	return math.MaxFloat32, nil
}

func (c *float64Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}

	if cap(c.batchVecs) < len(ids) {
		c.batchVecs = make([][]float64, len(ids))
	}
	c.batchVecs = c.batchVecs[:len(ids)]
	if cap(c.batchF32) < len(ids) {
		c.batchF32 = make([][]float32, len(ids))
	}
	c.batchF32 = c.batchF32[:len(ids)]

	var needsFallback bool
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
				c.batchVecs[i] = chunk[start : start+c.dims]
				continue
			}
		}
		vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
		if err != nil {
			needsFallback = true
			break
		}
		if v, ok := vecAny.([]float64); ok {
			c.batchVecs[i] = v
		} else {
			needsFallback = true
			break
		}
	}

	if needsFallback {
		return dst, c.Compute(ids, dst)
	}

	// Convert all float64 vectors to float32 and use SIMD batch
	for i, v := range c.batchVecs {
		if len(c.batchF32[i]) < len(v) {
			c.batchF32[i] = make([]float32, len(v))
		} else {
			c.batchF32[i] = c.batchF32[i][:len(v)]
		}
		for j, val := range v {
			c.batchF32[i][j] = float32(val)
		}
	}

	qF32 := make([]float32, len(c.q))
	for i, val := range c.q {
		qF32[i] = float32(val)
	}

	err := simd.EuclideanDistanceBatch(qF32, c.batchF32, dst)
	return dst, err
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
