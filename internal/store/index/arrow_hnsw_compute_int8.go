package index

import (
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

type int8Computer struct {
	squared   bool
	data      *types.GraphData
	q         []uint8
	qInt8     []int8
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	isUint8   bool
	sctx      *ArrowSearchContext
}

func (c *int8Computer) uint8Mode() bool {
	return c.isUint8 || (c.data != nil && c.data.Type == types.VectorTypeUint8)
}

func (c *int8Computer) ComputeSingle(id uint32) (float32, error) {
	if c.sctx != nil {
		v8 := c.sctx.GetVectorInt8(c.data, id, c.dims, c.maxGen)
		if v8 != nil {
			if c.uint8Mode() {
				u8 := *(*[]uint8)(unsafe.Pointer(&v8)) // #nosec G103
				if c.squared && c.h.distFuncUint8Squared != nil {
					return c.h.distFuncUint8Squared(c.q, u8)
				}
				return c.h.distFuncUint8(c.q, u8)
			}
			if c.squared && c.h.distFuncInt8Squared != nil {
				return c.h.distFuncInt8Squared(c.qInt8, v8)
			}
			return c.h.distFuncInt8(c.qInt8, v8)
		}
		vSQ8 := c.sctx.GetVectorSQ8(c.data, id, c.dims, c.maxGen)
		if vSQ8 != nil {
			if c.uint8Mode() {
				if c.squared && c.h.distFuncUint8Squared != nil {
					return c.h.distFuncUint8Squared(c.q, vSQ8)
				}
				return c.h.distFuncUint8(c.q, vSQ8)
			}
			if c.squared && c.h.distFuncInt8Squared != nil {
				return c.h.distFuncInt8Squared(c.qInt8, *(*[]int8)(unsafe.Pointer(&vSQ8))) // #nosec G103
			}
			return c.h.distFuncInt8(c.qInt8, *(*[]int8)(unsafe.Pointer(&vSQ8))) // #nosec G103
		}
	} else {
		cID := types.ChunkID(id)
		var chunk []int8
		if c.maxGen == 18446744073709551615 {
			chunk = c.data.GetVectorsInt8ChunkFast(cID)
		} else {
			chunk = c.data.GetVectorsInt8ChunkWithGen(cID, c.maxGen)
		}
		if chunk != nil {
			cOff := int(id) % types.ChunkSize
			pd := c.data.GetPaddedDimsForType(types.VectorTypeInt8)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				v8 := chunk[start : start+c.dims]
				if c.uint8Mode() {
					u8 := *(*[]uint8)(unsafe.Pointer(&v8)) // #nosec G103
					if c.squared && c.h.distFuncUint8Squared != nil {
						return c.h.distFuncUint8Squared(c.q, u8)
					}
					return c.h.distFuncUint8(c.q, u8)
				}
				if c.squared && c.h.distFuncInt8Squared != nil {
					return c.h.distFuncInt8Squared(c.qInt8, v8)
				}
				return c.h.distFuncInt8(c.qInt8, v8)
			}
		}
		var chunkSQ8 []byte
		if c.maxGen == 18446744073709551615 {
			chunkSQ8 = c.data.GetVectorsSQ8ChunkFast(cID)
		} else {
			chunkSQ8 = c.data.GetVectorsSQ8ChunkWithGen(cID, c.maxGen)
		}
		if chunkSQ8 != nil {
			cOff := int(id) % types.ChunkSize
			pd := c.data.GetPaddedDimsForType(types.VectorTypeUint8)
			start := cOff * pd
			if start+c.dims <= len(chunkSQ8) {
				v8 := chunkSQ8[start : start+c.dims]
				if c.uint8Mode() {
					if c.squared && c.h.distFuncUint8Squared != nil {
						return c.h.distFuncUint8Squared(c.q, v8)
					}
					return c.h.distFuncUint8(c.q, v8)
				}
				if c.squared && c.h.distFuncInt8Squared != nil {
					return c.h.distFuncInt8Squared(c.qInt8, *(*[]int8)(unsafe.Pointer(&v8))) // #nosec G103
				}
				return c.h.distFuncInt8(c.qInt8, *(*[]int8)(unsafe.Pointer(&v8))) // #nosec G103
			}
		}
	}

	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err != nil {
		return 0, err
	}
	switch v := vecAny.(type) {
	case []float32:
		if c.h.quantizer != nil && c.h.sq8Ready.Load() {
			minV, maxV := c.h.quantizer.Params()
			scale := (maxV - minV) / 255.0
			var sum float32
			for i, val := range c.q {
				deq := minV + float32(val)*scale
				diff := deq - v[i]
				sum += diff * diff
			}
			return float32(math.Sqrt(float64(sum))), nil
		}
		var sum float32
		if c.uint8Mode() {
			for i, val := range c.q {
				diff := float32(val) - v[i]
				sum += diff * diff
			}
		} else {
			for i, val := range c.qInt8 {
				diff := float32(val) - v[i]
				sum += diff * diff
			}
		}
		return float32(math.Sqrt(float64(sum))), nil
	case []int8:
		if c.uint8Mode() {
			u8 := *(*[]uint8)(unsafe.Pointer(&v)) // #nosec G103
			if c.squared && c.h.distFuncUint8Squared != nil {
				return c.h.distFuncUint8Squared(c.q, u8)
			}
			return c.h.distFuncUint8(c.q, u8)
		}
		if c.squared && c.h.distFuncInt8Squared != nil {
			return c.h.distFuncInt8Squared(c.qInt8, v)
		}
		return c.h.distFuncInt8(c.qInt8, v)
	case []uint8:
		v8 := v
		if c.h.quantizer != nil && c.h.sq8Ready.Load() {
			minV, maxV := c.h.quantizer.Params()
			scale := (maxV - minV) / 255.0
			var sum float32
			for i, val := range c.q {
				deqQ := minV + float32(val)*scale
				deqV := minV + float32(v8[i])*scale
				diff := deqQ - deqV
				sum += diff * diff
			}
			return float32(math.Sqrt(float64(sum))), nil
		}
		if c.uint8Mode() {
			if c.squared && c.h.distFuncUint8Squared != nil {
				return c.h.distFuncUint8Squared(c.q, v8)
			}
			return c.h.distFuncUint8(c.q, v8)
		}
		vI8 := *(*[]int8)(unsafe.Pointer(&v8)) // #nosec G103
		if c.squared && c.h.distFuncInt8Squared != nil {
			return c.h.distFuncInt8Squared(c.qInt8, vI8)
		}
		return c.h.distFuncInt8(c.qInt8, vI8)
	}
	return math.MaxFloat32, nil
}

func (c *int8Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}

	pd := c.data.GetPaddedDimsForType(types.VectorTypeInt8)
	var lastChunkID int32 = -1
	var chunk []int8
	batch := c.data.BeginInt8ChunkBatch(c.maxGen)

	for i, id := range ids {
		cID := int32(types.ChunkID(id)) // #nosec G115
		if cID != lastChunkID {
			lastChunkID = cID
			chunk = batch.Chunk(int(cID))
		}
		if chunk != nil {
			cOff := int(id) % types.ChunkSize
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				v8 := chunk[start : start+c.dims]
				if c.uint8Mode() {
					u8 := *(*[]uint8)(unsafe.Pointer(&v8)) // #nosec G103
					if c.squared && c.h.distFuncUint8Squared != nil {
						dst[i], _ = c.h.distFuncUint8Squared(c.q, u8)
					} else {
						dst[i], _ = c.h.distFuncUint8(c.q, u8)
					}
				} else {
					if c.squared && c.h.distFuncInt8Squared != nil {
						dst[i], _ = c.h.distFuncInt8Squared(c.qInt8, v8)
					} else {
						dst[i], _ = c.h.distFuncInt8(c.qInt8, v8)
					}
				}
				continue
			}
		}
		dist, err := c.ComputeSingle(id)
		if err != nil {
			return nil, err
		}
		dst[i] = dist
	}
	return dst, nil
}

func (c *int8Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt8ChunkFast(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeInt8)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

func euclideanDistanceInt16(a, b []int16) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat32
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

func euclideanDistanceUint16(a, b []uint16) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat32
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

// euclideanDistanceUint8 widens to float64 before subtracting. The unsigned
// element types cannot be differenced in their own width: it wraps around
// instead of going negative.
func euclideanDistanceUint8(a, b []uint8) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat32
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

func euclideanDistanceInt32(a, b []int32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat32
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

func euclideanDistanceInt64(a, b []int64) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat32
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

func euclideanDistanceUint64(a, b []uint64) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat32
	}
	var sum float64
	for i := range a {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum))
}

type sharedInt8Computer struct {
	squared   bool
	data      *types.GraphData
	q         []uint8
	qInt8     []int8
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	slices    [][]int8
	startID   uint32
	n         int
	isUint8   bool
}

func (c *sharedInt8Computer) uint8Mode() bool {
	return c.isUint8 || (c.data != nil && c.data.Type == types.VectorTypeUint8)
}

func (c *sharedInt8Computer) ComputeSingle(id uint32) (float32, error) {
	if id >= c.startID && id < c.startID+uint32(c.n) { // #nosec G115
		idx := int(id - c.startID)
		if idx < len(c.slices) {
			vec := c.slices[idx]
			if c.uint8Mode() {
				u8 := *(*[]uint8)(unsafe.Pointer(&vec)) // #nosec G103
				if c.squared && c.h.distFuncUint8Squared != nil {
					return c.h.distFuncUint8Squared(c.q, u8)
				}
				return c.h.distFuncUint8(c.q, u8)
			}
			if c.squared && c.h.distFuncInt8Squared != nil {
				return c.h.distFuncInt8Squared(c.qInt8, vec)
			}
			return c.h.distFuncInt8(c.qInt8, vec)
		}
	}

	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err != nil {
		return 0, err
	}
	switch v := vecAny.(type) {
	case []int8:
		if c.uint8Mode() {
			u8 := *(*[]uint8)(unsafe.Pointer(&v)) // #nosec G103
			if c.squared && c.h.distFuncUint8Squared != nil {
				return c.h.distFuncUint8Squared(c.q, u8)
			}
			return c.h.distFuncUint8(c.q, u8)
		}
		if c.squared && c.h.distFuncInt8Squared != nil {
			return c.h.distFuncInt8Squared(c.qInt8, v)
		}
		return c.h.distFuncInt8(c.qInt8, v)
	case []uint8:
		if c.uint8Mode() {
			if c.squared && c.h.distFuncUint8Squared != nil {
				return c.h.distFuncUint8Squared(c.q, v)
			}
			return c.h.distFuncUint8(c.q, v)
		}
		vI8 := *(*[]int8)(unsafe.Pointer(&v)) // #nosec G103
		if c.squared && c.h.distFuncInt8Squared != nil {
			return c.h.distFuncInt8Squared(c.qInt8, vI8)
		}
		return c.h.distFuncInt8(c.qInt8, vI8)
	}
	return math.MaxFloat32, nil
}

func (c *sharedInt8Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	dst = dst[:0]
	for _, id := range ids {
		dist, err := c.ComputeSingle(id)
		if err != nil {
			return nil, err
		}
		dst = append(dst, dist)
	}
	return dst, nil
}

func (c *sharedInt8Computer) Prefetch(id uint32) {
	if id >= c.startID && id < c.startID+uint32(c.n) { // #nosec G115
		idx := int(id - c.startID)
		if idx < len(c.slices) && len(c.slices[idx]) > 0 {
			simd.Prefetch(unsafe.Pointer(&c.slices[idx][0])) // #nosec G103
		}
	}
}
