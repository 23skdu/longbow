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
	cID := types.ChunkID(id)
	var chunk []int16
	if c.maxGen == math.MaxUint64 {
		chunk = c.data.GetVectorsInt16ChunkFast(cID)
	} else {
		chunk = c.data.GetVectorsInt16ChunkWithGen(cID, c.maxGen)
	}
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeInt16)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncInt16(c.q, chunk[start:start+c.dims])
		}
	}

	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]int16); ok {
			return c.h.distFuncInt16(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func (c *int16Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt16ChunkFast(cID)
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
	n := len(ids)
	if cap(dst) < n {
		dst = make([]float32, n)
	} else {
		dst = dst[:n]
	}
	if n == 0 {
		return dst, nil
	}

	pd := c.data.GetPaddedDimsForType(types.VectorTypeInt16)
	var lastChunkID int32 = -1
	var chunk []int16
	batch := c.data.BeginInt16ChunkBatch(c.maxGen)

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
				d, err := c.h.distFuncInt16(c.q, chunk[start:start+c.dims])
				if err != nil {
					dst[i] = math.MaxFloat32
					continue
				}
				dst[i] = d
				continue
			}
		}
		dist, err := c.ComputeSingle(id)
		if err != nil {
			dst[i] = math.MaxFloat32
			continue
		}
		dst[i] = dist
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
}

func (c *uint16Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	var chunk []uint16
	if c.maxGen == math.MaxUint64 {
		chunk = c.data.GetVectorsUint16ChunkFast(cID)
	} else {
		chunk = c.data.GetVectorsUint16ChunkWithGen(cID, c.maxGen)
	}
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint16)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncUint16(c.q, chunk[start:start+c.dims])
		}
	}

	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]uint16); ok {
			return c.h.distFuncUint16(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func (c *uint16Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	n := len(ids)
	if cap(dst) < n {
		dst = make([]float32, n)
	} else {
		dst = dst[:n]
	}
	if n == 0 {
		return dst, nil
	}

	pd := c.data.GetPaddedDimsForType(types.VectorTypeUint16)
	var lastChunkID int32 = -1
	var chunk []uint16
	batch := c.data.BeginUint16ChunkBatch(c.maxGen)

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
				d, err := c.h.distFuncUint16(c.q, chunk[start:start+c.dims])
				if err != nil {
					dst[i] = math.MaxFloat32
					continue
				}
				dst[i] = d
				continue
			}
		}
		dist, err := c.ComputeSingle(id)
		if err != nil {
			dst[i] = math.MaxFloat32
			continue
		}
		dst[i] = dist
	}
	return dst, nil
}

func (c *uint16Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint16ChunkFast(cID)
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
	cID := types.ChunkID(id)
	var chunk []int32
	if c.maxGen == math.MaxUint64 {
		chunk = c.data.GetVectorsInt32ChunkFast(cID)
	} else {
		chunk = c.data.GetVectorsInt32ChunkWithGen(cID, c.maxGen)
	}
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeInt32)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncInt32(c.q, chunk[start:start+c.dims])
		}
	}

	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]int32); ok {
			return c.h.distFuncInt32(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func (c *int32Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsInt32ChunkFast(cID)
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
	n := len(ids)
	if cap(dst) < n {
		dst = make([]float32, n)
	} else {
		dst = dst[:n]
	}
	if n == 0 {
		return dst, nil
	}

	pd := c.data.GetPaddedDimsForType(types.VectorTypeInt32)
	var lastChunkID int32 = -1
	var chunk []int32
	batch := c.data.BeginInt32ChunkBatch(c.maxGen)

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
				d, err := c.h.distFuncInt32(c.q, chunk[start:start+c.dims])
				if err != nil {
					dst[i] = math.MaxFloat32
					continue
				}
				dst[i] = d
				continue
			}
		}
		dist, err := c.ComputeSingle(id)
		if err != nil {
			dst[i] = math.MaxFloat32
			continue
		}
		dst[i] = dist
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
}

func (c *uint32Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	var chunk []uint32
	if c.maxGen == math.MaxUint64 {
		chunk = c.data.GetVectorsUint32ChunkFast(cID)
	} else {
		chunk = c.data.GetVectorsUint32ChunkWithGen(cID, c.maxGen)
	}
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint32)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncUint32(c.q, chunk[start:start+c.dims])
		}
	}

	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]uint32); ok {
			return c.h.distFuncUint32(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func (c *uint32Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	n := len(ids)
	if cap(dst) < n {
		dst = make([]float32, n)
	} else {
		dst = dst[:n]
	}
	if n == 0 {
		return dst, nil
	}

	pd := c.data.GetPaddedDimsForType(types.VectorTypeUint32)
	var lastChunkID int32 = -1
	var chunk []uint32
	batch := c.data.BeginUint32ChunkBatch(c.maxGen)

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
				d, err := c.h.distFuncUint32(c.q, chunk[start:start+c.dims])
				if err != nil {
					dst[i] = math.MaxFloat32
					continue
				}
				dst[i] = d
				continue
			}
		}
		dist, err := c.ComputeSingle(id)
		if err != nil {
			dst[i] = math.MaxFloat32
			continue
		}
		dst[i] = dist
	}
	return dst, nil
}

func (c *uint32Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	chunk := c.data.GetVectorsUint32ChunkFast(cID)
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
	cID := types.ChunkID(id)
	var chunk []int64
	if c.maxGen == math.MaxUint64 {
		chunk = c.data.GetVectorsInt64ChunkFast(cID)
	} else {
		chunk = c.data.GetVectorsInt64ChunkWithGen(cID, c.maxGen)
	}
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeInt64)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncInt64(c.q, chunk[start:start+c.dims])
		}
	}

	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]int64); ok {
			return c.h.distFuncInt64(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func (c *int64Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	var chunk []int64
	if c.maxGen == math.MaxUint64 {
		chunk = c.data.GetVectorsInt64ChunkFast(cID)
	} else {
		chunk = c.data.GetVectorsInt64ChunkWithGen(cID, c.maxGen)
	}
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeInt64)
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

func (c *int64Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	n := len(ids)
	if cap(dst) < n {
		dst = make([]float32, n)
	} else {
		dst = dst[:n]
	}
	if n == 0 {
		return dst, nil
	}

	pd := c.data.GetPaddedDimsForType(types.VectorTypeInt64)
	var lastChunkID int32 = -1
	var chunk []int64
	batch := c.data.BeginInt64ChunkBatch(c.maxGen)

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
				d, err := c.h.distFuncInt64(c.q, chunk[start:start+c.dims])
				if err != nil {
					dst[i] = math.MaxFloat32
					continue
				}
				dst[i] = d
				continue
			}
		}
		dist, err := c.ComputeSingle(id)
		if err != nil {
			dst[i] = math.MaxFloat32
			continue
		}
		dst[i] = dist
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
}

func (c *uint64Computer) ComputeSingle(id uint32) (float32, error) {
	cID := types.ChunkID(id)
	var chunk []uint64
	if c.maxGen == math.MaxUint64 {
		chunk = c.data.GetVectorsUint64ChunkFast(cID)
	} else {
		chunk = c.data.GetVectorsUint64ChunkWithGen(cID, c.maxGen)
	}
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint64)
		start := cOff * pd
		if start+c.dims <= len(chunk) {
			return c.h.distFuncUint64(c.q, chunk[start:start+c.dims])
		}
	}

	vecAny, err := c.h.getVectorWithCachedDisk(c.data, c.diskGraph, id, c.maxGen)
	if err == nil {
		if v, ok := vecAny.([]uint64); ok {
			return c.h.distFuncUint64(c.q, v)
		}
	}
	return math.MaxFloat32, nil
}

func (c *uint64Computer) ComputeBatch(ids []uint32, dst []float32) ([]float32, error) {
	n := len(ids)
	if cap(dst) < n {
		dst = make([]float32, n)
	} else {
		dst = dst[:n]
	}
	if n == 0 {
		return dst, nil
	}

	pd := c.data.GetPaddedDimsForType(types.VectorTypeUint64)
	var lastChunkID int32 = -1
	var chunk []uint64
	batch := c.data.BeginUint64ChunkBatch(c.maxGen)

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
				d, err := c.h.distFuncUint64(c.q, chunk[start:start+c.dims])
				if err != nil {
					dst[i] = math.MaxFloat32
					continue
				}
				dst[i] = d
				continue
			}
		}
		dist, err := c.ComputeSingle(id)
		if err != nil {
			dst[i] = math.MaxFloat32
			continue
		}
		dst[i] = dist
	}
	return dst, nil
}

func (c *uint64Computer) Prefetch(id uint32) {
	cID := types.ChunkID(id)
	var chunk []uint64
	if c.maxGen == math.MaxUint64 {
		chunk = c.data.GetVectorsUint64ChunkFast(cID)
	} else {
		chunk = c.data.GetVectorsUint64ChunkWithGen(cID, c.maxGen)
	}
	if chunk != nil {
		cOff := types.ChunkOffset(id)
		pd := c.data.GetPaddedDimsForType(types.VectorTypeUint64)
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
