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
	_, err := c.ComputeBatch(ids, dists)
	return err
}

func (c *float64Computer) ComputeSingle(id uint32) (float32, error) {
	// Fast path: direct chunk access (same order as float32 computer)
	cID := types.ChunkID(id)
	var chunk []float64
	if c.maxGen == math.MaxUint64 {
		chunk = c.data.GetVectorsFloat64ChunkFast(int(cID))
	} else {
		chunk = c.data.GetVectorsFloat64ChunkWithGen(int(cID), c.maxGen)
	}
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
	n := len(ids)
	if cap(dst) < n {
		dst = make([]float32, n)
	} else {
		dst = dst[:n]
	}
	if n == 0 {
		return dst, nil
	}

	const blockSize = 64
	for blockStart := 0; blockStart < n; blockStart += blockSize {
		blockEnd := blockStart + blockSize
		if blockEnd > n {
			blockEnd = n
		}

		// Cache-blocking optimization: prefetch next 64-vector block while computing current block
		if blockEnd < n {
			nextEnd := blockEnd + blockSize
			if nextEnd > n {
				nextEnd = n
			}
			for _, nextID := range ids[blockEnd:nextEnd] {
				c.Prefetch(nextID)
			}
		}

		// Process current 64-vector block
		for i := blockStart; i < blockEnd; i++ {
			id := ids[i]
			cID := types.ChunkID(id)
			var chunk []float64
			if c.maxGen == math.MaxUint64 {
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
	}

	return dst, nil
}

func (c *float64Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	var chunk []float64
	if c.maxGen == math.MaxUint64 {
		chunk = c.data.GetVectorsFloat64ChunkFast(int(cID))
	} else {
		chunk = c.data.GetVectorsFloat64ChunkWithGen(int(cID), c.maxGen)
	}
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat64)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			base := unsafe.Pointer(&chunk[start]) // #nosec G103
			byteLen := uintptr(c.dims * 8)
			for off := uintptr(0); off < byteLen; off += 64 {
				simd.Prefetch(unsafe.Add(base, off)) // #nosec G103
			}
		}
	}
}
