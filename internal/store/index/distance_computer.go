package index

import (
	"math"
	"unsafe"

	basecore "github.com/23skdu/longbow/internal/core"
	lbcore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// DistanceComputer defines the interface for specialized distance computation.
type DistanceComputer interface {
	ComputeSingle(id uint32) (float32, error)
	ComputeBatch(ids []uint32, dst []float32) ([]float32, error)
	Prefetch(id uint32)
}

type pqComputer struct {
	data      *types.GraphData
	q         []float32
	table     any // Accepts []float32 or pq.ADCTable
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
}

func (c *pqComputer) ComputeSingle(id uint32) (float32, error) {
	code := c.data.GetVectorPQWithGen(id, c.maxGen)
	if code == nil {
		if c.diskGraph != nil {
			code = c.diskGraph.GetVectorPQ(id)
		} else {
			dg := c.h.diskGraph.Load()
			if dg != nil {
				code = dg.GetVectorPQ(id)
			}
		}
	}

	if code == nil {
		return math.MaxFloat32, nil
	}

	var distSq float32
	var err error
	switch enc := c.h.oopqEncoder.(type) {
	case *pq.PQEncoder:
		if t, ok := c.table.([]float32); ok {
			distSq, err = enc.ADCDistance(t, code)
		}
	case *pq.OPQEncoder:
		if t, ok := c.table.([]float32); ok {
			distSq, err = enc.ADCDistance(t, code)
		}
	}

	if err != nil {
		return math.MaxFloat32, nil
	}

	if c.h.config.Metric == basecore.MetricEuclidean {
		return float32(math.Sqrt(float64(distSq))), nil
	}
	return distSq, nil
}

func (c *pqComputer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
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

func (c *pqComputer) Prefetch(id uint32) {
	cID := int(id) / types.ChunkSize
	chunk := c.data.GetVectorsPQChunkFast(cID)
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		numWordsPerNode := (c.data.PQM + 7) / 8
		start := cOff * numWordsPerNode * 8
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

type tqComputer struct {
	data         *types.GraphData
	h            *ArrowHNSW
	rotatedQuery []float32
	diskGraph    *DiskGraph
	maxGen       uint64
}

func (c *tqComputer) ComputeSingle(id uint32) (float32, error) {
	return c.h.tqCompute.DistanceWithRotatedQueryAndDisk(id, c.rotatedQuery, c.diskGraph, c.maxGen)
}

func (c *tqComputer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
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

func (c *tqComputer) Prefetch(id uint32) {
	chunk := c.data.GetVectorsTQChunkFast(int(id) / types.ChunkSize)
	if chunk != nil {
		stride := c.data.PackedSize()
		cOff := int(id) % types.ChunkSize
		start := cOff * stride
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

type float32Computer struct {
	squared   bool
	data      *types.GraphData
	q         []float32
	dims      int
	h         *ArrowHNSW
	qF64      []float64
	qF16      []float16.Num
	qC64      []complex64
	qC128     []complex128
	diskGraph *DiskGraph
	maxGen    uint64
}

func (c *float32Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	var chunk []float32
	if c.maxGen == 18446744073709551615 {
		chunk = c.data.GetVectorsChunkFast(int(cID))
	} else {
		chunk = c.data.GetVectorsChunkWithGen(int(cID), c.maxGen)
	}
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat32)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			if c.squared {
				return c.h.distFuncSquared(c.q, v)
			}
			return c.h.distFunc(c.q, v)
		}
	}

	var chunkF16 []float16.Num
	if c.maxGen == 18446744073709551615 {
		chunkF16 = c.data.GetVectorsF16ChunkFast(int(cID))
	} else {
		chunkF16 = c.data.GetVectorsF16ChunkWithGen(int(cID), c.maxGen)
	}
	if chunkF16 != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat16)
		start := cOff * pd
		if start+c.dims <= len(chunkF16) {
			v := chunkF16[start : start+c.dims]
			var q16 []float16.Num
			if len(c.qF16) == len(c.q) {
				q16 = c.qF16
			} else {
				q16 = make([]float16.Num, len(c.q))
				for i, val := range c.q {
					q16[i] = float16.New(val)
				}
			}
			return c.h.distFuncF16(q16, v)
		}
	}

	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err != nil {
		return 0, err
	}
	switch v := vecAny.(type) {
	case []float32:
		if c.squared {
			return c.h.distFuncSquared(c.q, v)
		}
		return c.h.distFunc(c.q, v)
	case []int8, []uint8:
		var v8 []uint8
		if vi8, ok := v.([]int8); ok {
			v8 = *(*[]uint8)(unsafe.Pointer(&vi8)) // #nosec G103
		} else {
			v8 = v.([]uint8)
		}

		if c.h.quantizer != nil && c.h.sq8Ready.Load() {
			minV, maxV := c.h.quantizer.Params()
			scale := (maxV - minV) / 255.0
			var sum float32
			for i, val := range c.q {
				deq := minV + float32(v8[i])*scale
				diff := val - deq
				sum += diff * diff
			}
			return float32(math.Sqrt(float64(sum))), nil
		}
		var sum float32
		for i, val := range c.q {
			diff := val - float32(v8[i])
			sum += diff * diff
		}
		return float32(math.Sqrt(float64(sum))), nil
	case []complex64:
		if len(c.q) != len(v)*2 {
			return math.MaxFloat32, nil
		}
		vf := unsafe.Slice((*float32)(unsafe.Pointer(&v[0])), len(v)*2) // #nosec G103
		return c.h.distFunc(c.q, vf)
	case []complex128:
		if len(c.q) != len(v)*2 {
			return math.MaxFloat32, nil
		}
		var sum float64
		for i, val := range v {
			re := float64(real(val))
			im := float64(imag(val))
			diffRe := float64(c.q[i*2]) - re
			diffIm := float64(c.q[i*2+1]) - im
			sum += diffRe*diffRe + diffIm*diffIm
		}
		return float32(math.Sqrt(sum)), nil
	case []float64:
		if len(c.q) != len(v) {
			return math.MaxFloat32, nil
		}
		var q64 []float64
		if len(c.qF64) == len(c.q) {
			q64 = c.qF64
		} else {
			q64 = make([]float64, len(c.q))
			for i, val := range c.q {
				q64[i] = float64(val)
			}
		}
		return c.h.distFuncF64(q64, v)
	case []float16.Num:
		if len(c.q) != len(v) {
			return math.MaxFloat32, nil
		}
		var q16 []float16.Num
		if len(c.qF16) == len(c.q) {
			q16 = c.qF16
		} else {
			q16 = make([]float16.Num, len(c.q))
			for i, val := range c.q {
				q16[i] = float16.New(val)
			}
		}
		return c.h.distFuncF16(q16, v)
	}
	return math.MaxFloat32, nil
}

func (c *float32Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
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

func (c *float32Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsChunkFast(int(cID))
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat32)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

type float32ToFloat32Computer struct {
	squared   bool
	data      *types.GraphData
	q         []float32
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	batchVecs [][]float32
}

func (c *float32ToFloat32Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	var chunk []float32
	if c.maxGen == 18446744073709551615 {
		chunk = c.data.GetVectorsChunkFast(int(cID))
	} else {
		chunk = c.data.GetVectorsChunkWithGen(int(cID), c.maxGen)
	}
	if chunk == nil {
		vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
		if err != nil {
			return 0, err
		}
		if v, ok := vecAny.([]float32); ok {
			if c.squared {
				return c.h.distFuncSquared(c.q, v)
			}
			return c.h.distFunc(c.q, v)
		}
		return math.MaxFloat32, nil
	}

	cOff := int(id) % types.ChunkSize
	pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat32)
	start := cOff * pd
	if start+c.dims <= len(chunk) {
		v := chunk[start : start+c.dims]
		if c.squared {
			return c.h.distFuncSquared(c.q, v)
		}
		return c.h.distFunc(c.q, v)
	}
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]float32); ok {
			if c.squared {
				return c.h.distFuncSquared(c.q, v)
			}
			return c.h.distFunc(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func (c *float32ToFloat32Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if c.h.gpuEnabled && c.h.gpuIndex != nil && len(ids) >= 256 {
		gpuDists, err := c.h.ComputeDistancesBatch(c.q, ids)
		if err == nil {
			if cap(dst) < len(ids) {
				dst = make([]float32, len(ids))
			} else {
				dst = dst[:len(ids)]
			}
			copy(dst, gpuDists)
			return dst, nil
		}
	}

	if cap(c.batchVecs) < len(ids) {
		c.batchVecs = make([][]float32, len(ids))
	}
	c.batchVecs = c.batchVecs[:len(ids)]

	for i, id := range ids {
		cID := types.ChunkID(id)
		var chunk []float32
		if c.maxGen == 18446744073709551615 {
			chunk = c.data.GetVectorsChunkFast(int(cID))
		} else {
			chunk = c.data.GetVectorsChunkWithGen(int(cID), c.maxGen)
		}

		if chunk != nil {
			cOff := int(id) % types.ChunkSize
			pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat32)
			start := cOff * pd
			c.batchVecs[i] = chunk[start : start+len(c.q)]
		} else {
			vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
			if err == nil {
				c.batchVecs[i] = vecAny.([]float32)
			} else {
				c.batchVecs[i] = nil
			}
		}
	}

	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}
	var err error
	if c.squared {
		err = simd.L2SquaredDistanceBatch(c.q, c.batchVecs, dst)
	} else if c.h.config.Metric == lbcore.MetricCosine {
		err = simd.CosineDistanceBatch(c.q, c.batchVecs, dst)
	} else if c.h.config.Metric == lbcore.MetricDotProduct {
		err = simd.DotProductBatch(c.q, c.batchVecs, dst)
	} else {
		err = simd.EuclideanDistanceBatch(c.q, c.batchVecs, dst)
	}
	return dst, err
}

func (c *float32ToFloat32Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	var chunk []float32
	if c.maxGen == 18446744073709551615 {
		chunk = c.data.GetVectorsChunkFast(int(cID))
	} else {
		chunk = c.data.GetVectorsChunkWithGen(int(cID), c.maxGen)
	}
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat32)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

type int8Computer struct {
	squared   bool
	data      *types.GraphData
	q         []uint8
	qInt8     []int8
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
}

func (c *int8Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	var chunk []int8
	if c.maxGen == 18446744073709551615 {
		chunk = c.data.GetVectorsInt8ChunkFast(int(cID))
	} else {
		chunk = c.data.GetVectorsInt8ChunkWithGen(int(cID), c.maxGen)
	}
	if chunk != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeInt8)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			v8 := chunk[start : start+c.dims]
			if c.squared {
				return c.h.distFuncInt8Squared(c.qInt8, v8)
			}
			return c.h.distFuncInt8(c.qInt8, v8)
		}
	}
	var chunkSQ8 []byte
	if c.maxGen == 18446744073709551615 {
		chunkSQ8 = c.data.GetVectorsSQ8ChunkFast(int(cID))
	} else {
		chunkSQ8 = c.data.GetVectorsSQ8ChunkWithGen(int(cID), c.maxGen)
	}
	if chunkSQ8 != nil {
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint8)
		start := cOff * pd
		if start+c.dims <= len(chunkSQ8) {
			v8 := chunkSQ8[start : start+c.dims]
			return c.h.distFuncInt8(c.qInt8, *(*[]int8)(unsafe.Pointer(&v8))) // #nosec G103
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
		for i, val := range c.q {
			diff := float32(val) - v[i]
			sum += diff * diff
		}
		return float32(math.Sqrt(float64(sum))), nil
	case []int8:
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
		vI8 := *(*[]int8)(unsafe.Pointer(&v8)) // #nosec G103
		return c.h.distFuncInt8(c.qInt8, vI8)
	}
	return math.MaxFloat32, nil
}

func (c *int8Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
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

func (c *int8Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt8ChunkFast(int(cID))
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

// Ensure all implement DistanceComputer
var (
	_ DistanceComputer = (*pqComputer)(nil)
	_ DistanceComputer = (*tqComputer)(nil)
	_ DistanceComputer = (*float32Computer)(nil)
	_ DistanceComputer = (*float32ToFloat32Computer)(nil)
	_ DistanceComputer = (*int8Computer)(nil)
	_ DistanceComputer = (*sharedFloat32Computer)(nil)
	_ DistanceComputer = (*sharedInt8Computer)(nil)
	_ DistanceComputer = (*complex64Computer)(nil)
	_ DistanceComputer = (*complex128Computer)(nil)
	_ DistanceComputer = (*float16Computer)(nil)
	_ DistanceComputer = (*float64Computer)(nil)
	_ DistanceComputer = (*int32Computer)(nil)
	_ DistanceComputer = (*int16Computer)(nil)
	_ DistanceComputer = (*int64Computer)(nil)
	_ DistanceComputer = (*uint32Computer)(nil)
	_ DistanceComputer = (*uint16Computer)(nil)
	_ DistanceComputer = (*uint64Computer)(nil)
)

type sharedFloat32Computer struct {
	squared   bool
	data      *types.GraphData
	q         []float32
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	slices    [][]float32
	startID   uint32
	n         int
}

func (c *sharedFloat32Computer) ComputeSingle(id uint32) (float32, error) {
	if id >= c.startID && id < c.startID+uint32(c.n) {
		idx := int(id - c.startID)
		if idx < len(c.slices) {
			vec := c.slices[idx]
			if c.squared {
				return c.h.distFuncSquared(c.q, vec)
			}
			return c.h.distFunc(c.q, vec)
		}
	}

	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err != nil {
		return 0, err
	}
	if v, ok := vecAny.([]float32); ok {
		if c.squared {
			return c.h.distFuncSquared(c.q, v)
		}
		return c.h.distFunc(c.q, v)
	}
	return math.MaxFloat32, nil
}

func (c *sharedFloat32Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
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

func (c *sharedFloat32Computer) Prefetch(id uint32) {
	if id >= c.startID && id < c.startID+uint32(c.n) {
		idx := int(id - c.startID)
		if idx < len(c.slices) && len(c.slices[idx]) > 0 {
			simd.Prefetch(unsafe.Pointer(&c.slices[idx][0])) // #nosec G103
		}
	}
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
}

func (c *sharedInt8Computer) ComputeSingle(id uint32) (float32, error) {
	if id >= c.startID && id < c.startID+uint32(c.n) {
		idx := int(id - c.startID)
		if idx < len(c.slices) {
			vec := c.slices[idx]
			if c.squared {
				return c.h.distFuncInt8Squared(c.qInt8, vec)
			}
			return c.h.distFuncInt8(c.qInt8, vec)
		}
	}

	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err != nil {
		return 0, err
	}
	if v, ok := vecAny.([]int8); ok {
		if c.squared {
			return c.h.distFuncInt8Squared(c.qInt8, v)
		}
		return c.h.distFuncInt8(c.qInt8, v)
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
	if id >= c.startID && id < c.startID+uint32(c.n) {
		idx := int(id - c.startID)
		if idx < len(c.slices) && len(c.slices[idx]) > 0 {
			simd.Prefetch(unsafe.Pointer(&c.slices[idx][0])) // #nosec G103
		}
	}
}
