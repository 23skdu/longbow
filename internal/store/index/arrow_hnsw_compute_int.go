package index

import (
	"math"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

// int16Computer handles Int16 vectors
type int16Computer struct {
	data      *types.GraphData
	q         []int16
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	batchVecs [][]int16
}

func (c *int16Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		d, err := c.ComputeSingle(id)
		if err != nil {
			return err
		}
		dists[i] = d
	}
	return nil
}

func (c *int16Computer) ComputeSingle(id uint32) (float32, error) {
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]int16); ok {
			return c.h.distFuncInt16(c.q, v)
		}
	}

	// Fallback to direct chunk access if type-specific GetVector fails (COW path)
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt16ChunkWithGen(int(cID), c.maxGen)
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeInt16)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			v := chunk[start : start+c.dims]
			return c.h.distFuncInt16(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func (c *int16Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt16ChunkFast(int(cID))
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeInt16)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

func (c *int16Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(c.batchVecs) < len(ids) {
		c.batchVecs = make([][]int16, len(ids))
	}
	c.batchVecs = c.batchVecs[:len(ids)]

	needsFallback := false
	for i, id := range ids {
		vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
		if err == nil {
			if v, ok := vecAny.([]int16); ok {
				c.batchVecs[i] = v
				continue
			}
		}
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsInt16ChunkWithGen(int(cID), c.maxGen)
		if chunk != nil {
			cOff := types.ChunkOffset(id)
			pd := c.data.GetPaddedDimsForType(types.VectorTypeInt16)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				c.batchVecs[i] = chunk[start : start+c.dims]
				continue
			}
		}
		needsFallback = true
		break
	}

	if needsFallback {
		dst = dst[:0]
		for _, id := range ids {
			d, err := c.ComputeSingle(id)
			if err != nil {
				return nil, err
			}
			dst = append(dst, d)
		}
		return dst, nil
	}

	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}
	for i, v := range c.batchVecs {
		d, err := c.h.distFuncInt16(c.q, v)
		if err != nil {
			return nil, err
		}
		dst[i] = d
	}
	return dst, nil
}

// uint16Computer handles Uint16 vectors
type uint16Computer struct {
	data      *types.GraphData
	q         []uint16
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	batchVecs [][]uint16
}

func (c *uint16Computer) ComputeSingle(id uint32) (float32, error) {
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]uint16); ok {
			return c.h.distFuncUint16(c.q, v)
		}
	}

	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint16ChunkWithGen(int(cID), c.maxGen)
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint16)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncUint16(c.q, chunk[start:start+c.dims])
		}
	}
	return math.MaxFloat32, nil
}

func (c *uint16Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(c.batchVecs) < len(ids) {
		c.batchVecs = make([][]uint16, len(ids))
	}
	c.batchVecs = c.batchVecs[:len(ids)]

	needsFallback := false
	for i, id := range ids {
		vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
		if err == nil {
			if v, ok := vecAny.([]uint16); ok {
				c.batchVecs[i] = v
				continue
			}
		}
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsUint16ChunkWithGen(int(cID), c.maxGen)
		if chunk != nil {
			cOff := types.ChunkOffset(id)
			pd := c.data.GetPaddedDimsForType(types.VectorTypeUint16)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				c.batchVecs[i] = chunk[start : start+c.dims]
				continue
			}
		}
		needsFallback = true
		break
	}

	if needsFallback {
		dst = dst[:0]
		for _, id := range ids {
			d, err := c.ComputeSingle(id)
			if err != nil {
				return nil, err
			}
			dst = append(dst, d)
		}
		return dst, nil
	}

	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}
	for i, v := range c.batchVecs {
		d, err := c.h.distFuncUint16(c.q, v)
		if err != nil {
			return nil, err
		}
		dst[i] = d
	}
	return dst, nil
}

func (c *uint16Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint16ChunkFast(int(cID))
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint16)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

// int32Computer handles Int32 vectors
type int32Computer struct {
	data      *types.GraphData
	q         []int32
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	batchVecs [][]int32
}

func (c *int32Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		d, err := c.ComputeSingle(id)
		if err != nil {
			return err
		}
		dists[i] = d
	}
	return nil
}

func (c *int32Computer) ComputeSingle(id uint32) (float32, error) {
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]int32); ok {
			return c.h.distFuncInt32(c.q, v)
		}
	}

	// Fallback to direct chunk access
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt32ChunkWithGen(int(cID), c.maxGen)
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeInt32)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncInt32(c.q, chunk[start:start+c.dims])
		}
	}
	return math.MaxFloat32, nil
}

func (c *int32Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt32ChunkFast(int(cID))
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeInt32)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

func (c *int32Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(c.batchVecs) < len(ids) {
		c.batchVecs = make([][]int32, len(ids))
	}
	c.batchVecs = c.batchVecs[:len(ids)]

	needsFallback := false
	for i, id := range ids {
		vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
		if err == nil {
			if v, ok := vecAny.([]int32); ok {
				c.batchVecs[i] = v
				continue
			}
		}
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsInt32ChunkWithGen(int(cID), c.maxGen)
		if chunk != nil {
			cOff := types.ChunkOffset(id)
			pd := c.data.GetPaddedDimsForType(types.VectorTypeInt32)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				c.batchVecs[i] = chunk[start : start+c.dims]
				continue
			}
		}
		needsFallback = true
		break
	}

	if needsFallback {
		dst = dst[:0]
		for _, id := range ids {
			d, err := c.ComputeSingle(id)
			if err != nil {
				return nil, err
			}
			dst = append(dst, d)
		}
		return dst, nil
	}

	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}
	for i, v := range c.batchVecs {
		d, err := c.h.distFuncInt32(c.q, v)
		if err != nil {
			return nil, err
		}
		dst[i] = d
	}
	return dst, nil
}

// uint32Computer handles Uint32 vectors
type uint32Computer struct {
	data      *types.GraphData
	q         []uint32
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	batchVecs [][]uint32
}

func (c *uint32Computer) ComputeSingle(id uint32) (float32, error) {
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]uint32); ok {
			return c.h.distFuncUint32(c.q, v)
		}
	}

	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint32ChunkWithGen(int(cID), c.maxGen)
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint32)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncUint32(c.q, chunk[start:start+c.dims])
		}
	}
	return math.MaxFloat32, nil
}

func (c *uint32Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(c.batchVecs) < len(ids) {
		c.batchVecs = make([][]uint32, len(ids))
	}
	c.batchVecs = c.batchVecs[:len(ids)]

	needsFallback := false
	for i, id := range ids {
		vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
		if err == nil {
			if v, ok := vecAny.([]uint32); ok {
				c.batchVecs[i] = v
				continue
			}
		}
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsUint32ChunkWithGen(int(cID), c.maxGen)
		if chunk != nil {
			cOff := types.ChunkOffset(id)
			pd := c.data.GetPaddedDimsForType(types.VectorTypeUint32)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				c.batchVecs[i] = chunk[start : start+c.dims]
				continue
			}
		}
		needsFallback = true
		break
	}

	if needsFallback {
		dst = dst[:0]
		for _, id := range ids {
			d, err := c.ComputeSingle(id)
			if err != nil {
				return nil, err
			}
			dst = append(dst, d)
		}
		return dst, nil
	}

	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}
	for i, v := range c.batchVecs {
		d, err := c.h.distFuncUint32(c.q, v)
		if err != nil {
			return nil, err
		}
		dst[i] = d
	}
	return dst, nil
}

func (c *uint32Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint32ChunkFast(int(cID))
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint32)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

// int64Computer handles Int64 vectors
type int64Computer struct {
	data      *types.GraphData
	q         []int64
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	batchVecs [][]int64
}

func (c *int64Computer) Compute(ids []uint32, dists []float32) error {
	for i, id := range ids {
		d, err := c.ComputeSingle(id)
		if err != nil {
			return err
		}
		dists[i] = d
	}
	return nil
}

func (c *int64Computer) ComputeSingle(id uint32) (float32, error) {
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]int64); ok {
			return c.h.distFuncInt64(c.q, v)
		}
	}

	// Fallback to direct chunk access
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt64ChunkWithGen(int(cID), c.maxGen)
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeInt64)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncInt64(c.q, chunk[start:start+c.dims])
		}
	}
	return math.MaxFloat32, nil
}

func (c *int64Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt64ChunkFast(int(cID))
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeInt64)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

func (c *int64Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(c.batchVecs) < len(ids) {
		c.batchVecs = make([][]int64, len(ids))
	}
	c.batchVecs = c.batchVecs[:len(ids)]

	needsFallback := false
	for i, id := range ids {
		vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
		if err == nil {
			if v, ok := vecAny.([]int64); ok {
				c.batchVecs[i] = v
				continue
			}
		}
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsInt64ChunkWithGen(int(cID), c.maxGen)
		if chunk != nil {
			cOff := types.ChunkOffset(id)
			pd := c.data.GetPaddedDimsForType(types.VectorTypeInt64)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				c.batchVecs[i] = chunk[start : start+c.dims]
				continue
			}
		}
		needsFallback = true
		break
	}

	if needsFallback {
		dst = dst[:0]
		for _, id := range ids {
			d, err := c.ComputeSingle(id)
			if err != nil {
				return nil, err
			}
			dst = append(dst, d)
		}
		return dst, nil
	}

	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}
	for i, v := range c.batchVecs {
		d, err := c.h.distFuncInt64(c.q, v)
		if err != nil {
			return nil, err
		}
		dst[i] = d
	}
	return dst, nil
}

// uint64Computer handles Uint64 vectors
type uint64Computer struct {
	data      *types.GraphData
	q         []uint64
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	batchVecs [][]uint64
}

func (c *uint64Computer) ComputeSingle(id uint32) (float32, error) {
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]uint64); ok {
			return c.h.distFuncUint64(c.q, v)
		}
	}

	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint64ChunkWithGen(int(cID), c.maxGen)
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint64)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncUint64(c.q, chunk[start:start+c.dims])
		}
	}
	return math.MaxFloat32, nil
}

func (c *uint64Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(c.batchVecs) < len(ids) {
		c.batchVecs = make([][]uint64, len(ids))
	}
	c.batchVecs = c.batchVecs[:len(ids)]

	needsFallback := false
	for i, id := range ids {
		vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
		if err == nil {
			if v, ok := vecAny.([]uint64); ok {
				c.batchVecs[i] = v
				continue
			}
		}
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsUint64ChunkWithGen(int(cID), c.maxGen)
		if chunk != nil {
			cOff := types.ChunkOffset(id)
			pd := c.data.GetPaddedDimsForType(types.VectorTypeUint64)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				c.batchVecs[i] = chunk[start : start+c.dims]
				continue
			}
		}
		needsFallback = true
		break
	}

	if needsFallback {
		dst = dst[:0]
		for _, id := range ids {
			d, err := c.ComputeSingle(id)
			if err != nil {
				return nil, err
			}
			dst = append(dst, d)
		}
		return dst, nil
	}

	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}
	for i, v := range c.batchVecs {
		d, err := c.h.distFuncUint64(c.q, v)
		if err != nil {
			return nil, err
		}
		dst[i] = d
	}
	return dst, nil
}

func (c *uint64Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint64ChunkFast(int(cID))
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint64)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}

// uint8Computer handles Uint8 vectors
type uint8Computer struct {
	data      *types.GraphData
	q         []uint8
	dims      int
	h         *ArrowHNSW
	diskGraph *DiskGraph
	maxGen    uint64
	batchVecs [][]uint8
}

func (c *uint8Computer) ComputeSingle(id uint32) (float32, error) {
	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]uint8); ok {
			return c.h.distFuncUint8(c.q, v)
		}
	}

	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint8ChunkWithGen(int(cID), c.maxGen)
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint8)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncUint8(c.q, chunk[start:start+c.dims])
		}
	}
	return math.MaxFloat32, nil
}

func (c *uint8Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	if cap(c.batchVecs) < len(ids) {
		c.batchVecs = make([][]uint8, len(ids))
	}
	c.batchVecs = c.batchVecs[:len(ids)]

	needsFallback := false
	for i, id := range ids {
		vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
		if err == nil {
			if v, ok := vecAny.([]uint8); ok {
				c.batchVecs[i] = v
				continue
			}
		}
		cID := types.ChunkID(id)
		chunk := c.data.GetVectorsUint8ChunkWithGen(int(cID), c.maxGen)
		if chunk != nil {
			cOff := types.ChunkOffset(id)
			pd := c.data.GetPaddedDimsForType(types.VectorTypeUint8)
			start := cOff * pd
			if start+c.dims <= len(chunk) {
				c.batchVecs[i] = chunk[start : start+c.dims]
				continue
			}
		}
		needsFallback = true
		break
	}

	if needsFallback {
		dst = dst[:0]
		for _, id := range ids {
			d, err := c.ComputeSingle(id)
			if err != nil {
				return nil, err
			}
			dst = append(dst, d)
		}
		return dst, nil
	}

	if cap(dst) < len(ids) {
		dst = make([]float32, len(ids))
	} else {
		dst = dst[:len(ids)]
	}
	for i, v := range c.batchVecs {
		d, err := c.h.distFuncUint8(c.q, v)
		if err != nil {
			return nil, err
		}
		dst[i] = d
	}
	return dst, nil
}

func (c *uint8Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint8ChunkFast(int(cID))
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint8)
		start := cOff * pd
		if start < len(chunk) {
			simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
		}
	}
}
