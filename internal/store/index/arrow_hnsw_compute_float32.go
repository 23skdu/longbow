package index

import (
	"math"
	"sync"
	"unsafe"

	lbcore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

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
	sctx      *ArrowSearchContext
}

func (c *float32Computer) ComputeSingle(id uint32) (float32, error) {
	if c.sctx != nil {
		if v := c.sctx.GetVectorF32(c.data, id, c.dims, c.maxGen); v != nil {
			if c.squared {
				return c.h.distFuncSquared(c.q, v)
			}
			return c.h.distFunc(c.q, v)
		}
		if v16 := c.sctx.GetVectorF16(c.data, id, c.dims, c.maxGen); v16 != nil {
			var q16 []float16.Num
			if len(c.qF16) == len(c.q) {
				q16 = c.qF16
			} else {
				q16 = make([]float16.Num, len(c.q))
				for i, val := range c.q {
					q16[i] = float16.New(val)
				}
			}
			return c.h.distFuncF16(q16, v16)
		}
	} else {
		cID := types.ChunkID(id)
		var chunk []float32
		if c.maxGen == 18446744073709551615 {
			chunk = c.data.GetVectorsChunkFast(cID)
		} else {
			chunk = c.data.GetVectorsChunkWithGen(cID, c.maxGen)
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
			chunkF16 = c.data.GetVectorsF16ChunkFast(cID)
		} else {
			chunkF16 = c.data.GetVectorsF16ChunkWithGen(cID, c.maxGen)
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
	case []int8:
		var sum float32
		for i, val := range c.q {
			diff := val - float32(v[i])
			sum += diff * diff
		}
		return float32(math.Sqrt(float64(sum))), nil
	case []uint8:
		v8 := v
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
	case []int16:
		if len(c.q) != len(v) {
			return math.MaxFloat32, nil
		}
		var sum float32
		for i, val := range c.q {
			diff := val - float32(v[i])
			sum += diff * diff
		}
		return float32(math.Sqrt(float64(sum))), nil
	case []uint16:
		if len(c.q) != len(v) {
			return math.MaxFloat32, nil
		}
		var sum float32
		for i, val := range c.q {
			diff := val - float32(v[i])
			sum += diff * diff
		}
		return float32(math.Sqrt(float64(sum))), nil
	case []int32:
		if len(c.q) != len(v) {
			return math.MaxFloat32, nil
		}
		var sum float32
		for i, val := range c.q {
			diff := val - float32(v[i])
			sum += diff * diff
		}
		return float32(math.Sqrt(float64(sum))), nil
	case []uint32:
		if len(c.q) != len(v) {
			return math.MaxFloat32, nil
		}
		var sum float32
		for i, val := range c.q {
			diff := val - float32(v[i])
			sum += diff * diff
		}
		return float32(math.Sqrt(float64(sum))), nil
	case []int64:
		if len(c.q) != len(v) {
			return math.MaxFloat32, nil
		}
		var sum float64
		for i, val := range c.q {
			diff := float64(val) - float64(v[i])
			sum += diff * diff
		}
		return float32(math.Sqrt(sum)), nil
	case []uint64:
		if len(c.q) != len(v) {
			return math.MaxFloat32, nil
		}
		var sum float64
		for i, val := range c.q {
			diff := float64(val) - float64(v[i])
			sum += diff * diff
		}
		return float32(math.Sqrt(sum)), nil
	}
	return math.MaxFloat32, nil
}

func (c *float32Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}

	// Resolve the arena slab table and the generation policy once for the whole
	// batch instead of once per vector.
	batch := c.data.BeginFloat32ChunkBatch(c.maxGen)
	for i, id := range ids {
		v := batch.Vector(types.ChunkID(id), int(id)%types.ChunkSize, c.dims)
		if v == nil {
			dist, err := c.ComputeSingle(id)
			if err != nil {
				return nil, err
			}
			dst[i] = dist
			continue
		}
		var dist float32
		var err error
		if c.squared {
			dist, err = c.h.distFuncSquared(c.q, v)
		} else {
			dist, err = c.h.distFunc(c.q, v)
		}
		if err != nil {
			return nil, err
		}
		dst[i] = dist
	}
	return dst, nil
}

func (c *float32Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsChunkFast(cID)
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
	// sctx, when set, lends the batch scratch to the pooled search context
	// instead of owning it. The computer itself is still per-search - two of
	// them can be live on one context at once, so a single reusable slot would
	// be wrong - but the scratch does not escape ComputeBatch, so it can be
	// shared. Same arrangement the complex128 computer already uses
	// (roadmap Item 33).
	sctx      *ArrowSearchContext
	batchVecs [][]float32
}

func (c *float32ToFloat32Computer) ComputeSingle(id uint32) (float32, error) {
	if c.sctx != nil {
		if v := c.sctx.GetVectorF32(c.data, id, c.dims, c.maxGen); v != nil {
			if c.squared {
				return c.h.distFuncSquared(c.q, v)
			}
			return c.h.distFunc(c.q, v)
		}
	} else {
		cID := types.ChunkID(id)
		var chunk []float32
		if c.maxGen == 18446744073709551615 {
			chunk = c.data.GetVectorsChunkFast(cID)
		} else {
			chunk = c.data.GetVectorsChunkWithGen(cID, c.maxGen)
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

	if c.sctx != nil {
		if cap(c.sctx.batchVecsFloat32) < len(ids) {
			c.sctx.batchVecsFloat32 = make([][]float32, len(ids))
		}
		c.batchVecs = c.sctx.batchVecsFloat32[:len(ids)]
	} else {
		if cap(c.batchVecs) < len(ids) {
			c.batchVecs = make([][]float32, len(ids))
		}
		c.batchVecs = c.batchVecs[:len(ids)]
	}

	// Collect indices of vectors that need to be loaded from disk
	var diskLoads []int
	var batch types.VectorChunkBatch[float32]
	if c.sctx == nil {
		batch = c.data.BeginFloat32ChunkBatch(c.maxGen)
	}
	for i, id := range ids {
		var v []float32
		if c.sctx != nil {
			v = c.sctx.GetVectorF32(c.data, id, len(c.q), c.maxGen)
		} else {
			v = batch.Vector(types.ChunkID(id), int(id)%types.ChunkSize, len(c.q))
		}
		if v != nil {
			c.batchVecs[i] = v
		} else {
			diskLoads = append(diskLoads, i)
		}
	}

	// Concurrently fetch paged out vectors
	if len(diskLoads) > 0 {
		var wg sync.WaitGroup
		for _, i := range diskLoads {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, ids[idx], c.maxGen)
				if err == nil {
					if v, ok := vecAny.([]float32); ok {
						c.batchVecs[idx] = v
					} else {
						c.batchVecs[idx] = nil
					}
				} else {
					c.batchVecs[idx] = nil
				}
			}(i)
		}
		wg.Wait()
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
	// Use the generation-checked path for race-safe reads. Prefetch is a
	// performance hint so we tolerate stale generation results gracefully.
	if c.maxGen == 18446744073709551615 {
		// Fast path: no generation isolation needed. The chunk bytes may be
		// concurrently written by SetVector — the race is benign (prefetch hint)
		// but we use the generation check to keep the race detector happy.
		if c.data.GetPaddedDimsForType(types.VectorTypeFloat32) == 0 {
			return
		}
		chunk := c.data.GetVectorsChunkFast(cID)
		if chunk == nil {
			return
		}
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat32)
		start := cOff * pd
		if start < len(chunk) {
			// #nosec G103 — Prefetch is a performance hint; benign read race.
			simd.Prefetch(unsafe.Pointer(&chunk[start]))
		}
	} else {
		chunk := c.data.GetVectorsChunkWithGen(cID, c.maxGen)
		if chunk == nil {
			return
		}
		cOff := int(id) % types.ChunkSize
		pd := c.data.GetPaddedDimsForType(types.VectorTypeFloat32)
		start := cOff * pd
		if start < len(chunk) {
			// #nosec G103 — Prefetch is a performance hint; benign read race.
			simd.Prefetch(unsafe.Pointer(&chunk[start]))
		}
	}
}

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
	if id >= c.startID && id < c.startID+uint32(c.n) { // #nosec G115
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
	if id >= c.startID && id < c.startID+uint32(c.n) { // #nosec G115
		idx := int(id - c.startID)
		if idx < len(c.slices) && len(c.slices[idx]) > 0 {
			simd.Prefetch(unsafe.Pointer(&c.slices[idx][0])) // #nosec G103
		}
	}
}
