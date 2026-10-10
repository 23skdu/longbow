package index

import (
	"fmt"
	"math"
	"unsafe" // #nosec G103 -- address-only prefetch hint

	"github.com/23skdu/longbow/internal/simd"
	"github.com/23skdu/longbow/internal/store/types"
)

// TurboQuantCompute handles distance computations using TurboQuant encoding.
type TurboQuantCompute struct {
	h       *ArrowHNSW
	encoder *TurboQuantEncoder
}

// NewTurboQuantCompute creates a new TurboQuantCompute instance for the given HNSW index.
func NewTurboQuantCompute(h *ArrowHNSW) *TurboQuantCompute {
	data := h.data.Load()
	encoder := NewTurboQuantEncoder(data.Dims, data.TurboQuantBits, 42)
	return &TurboQuantCompute{
		h:       h,
		encoder: encoder,
	}
}

// Distance computes the distance between two vectors by their IDs using TurboQuant.
func (c *TurboQuantCompute) Distance(id1, id2 uint32) (float32, error) {
	if p := c.h.tqDecodeCache.Load(); p != nil {
		dim := int(c.h.dims.Load())
		if dim > 0 {
			off1 := int(id1) * dim
			off2 := int(id2) * dim
			if off1+dim <= len(p.data) && off2+dim <= len(p.data) {
				return c.h.distFunc(p.data[off1:off1+dim], p.data[off2:off2+dim])
			}
		}
	}
	vec1, err := c.getVector(id1)
	if err != nil {
		return 0, err
	}
	vec2, err := c.getVector(id2)
	if err != nil {
		return 0, err
	}
	return c.h.distFunc(vec1, vec2)
}

// DistanceWithVector computes the distance between a vector ID and a raw vector using TurboQuant.
func (c *TurboQuantCompute) DistanceWithVector(id uint32, vec []float32) (float32, error) {
	rotatedQuery := make([]float32, c.encoder.pow2)
	copy(rotatedQuery, vec)
	if err := simd.RandomRotation(rotatedQuery, c.encoder.params.Seed); err != nil {
		return 0, err
	}

	if p := c.h.tqDecodeCache.Load(); p != nil {
		dim := int(c.h.dims.Load())
		if dim > 0 && len(rotatedQuery) >= dim {
			offset := int(id) * dim
			if offset+dim <= len(p.data) {
				return c.h.distFunc(p.data[offset:offset+dim], rotatedQuery[:dim])
			}
		}
	}

	vec1, err := c.getVector(id)
	if err != nil {
		return 0, err
	}
	return c.h.distFunc(vec1, rotatedQuery)
}

// DistanceWithRotatedQuery computes the distance between a vector ID and a pre-rotated query vector.
func (c *TurboQuantCompute) DistanceWithRotatedQuery(id uint32, rotatedQuery []float32) (float32, error) {
	return c.DistanceWithRotatedQueryAndDisk(id, rotatedQuery, nil, math.MaxUint64)
}

// DistanceWithRotatedQueryAndDisk computes the distance using a pre-rotated query, allowing fallback to a DiskGraph.
func (c *TurboQuantCompute) DistanceWithRotatedQueryAndDisk(id uint32, rotatedQuery []float32, dg *DiskGraph, maxGen uint64) (float32, error) {
	if p := c.h.tqDecodeCache.Load(); p != nil {
		dim := int(c.h.dims.Load())
		if dim > 0 && len(rotatedQuery) >= dim {
			offset := int(id) * dim
			if offset+dim <= len(p.data) {
				return c.h.distFunc(p.data[offset:offset+dim], rotatedQuery[:dim])
			}
		}
	}
	vec1, err := c.getVectorWithDisk(id, dg, maxGen)
	if err != nil {
		return 0, err
	}
	return c.h.distFunc(vec1, rotatedQuery)
}

// DistanceDirect computes L2 distance directly from TQ codes using the rotated query.
// Avoids the decode+distFunc path by using SIMD lookup-table reconstruction fused with L2.
// Uses a sync.Pool for scratch buffers to eliminate per-call heap allocations.
func (c *TurboQuantCompute) DistanceDirect(id uint32, rotatedQuery []float32, dg *DiskGraph, maxGen uint64) (float32, error) {
	tqCode, err := c.getTQBytes(id, dg, maxGen)
	if err != nil {
		return 0, err
	}
	fn := simd.GetTurboQuantDistanceFunc()
	if fn == nil {
		return 0, fmt.Errorf("no TurboQuant distance function available")
	}
	return fn(rotatedQuery, tqCode, c.encoder.dims, c.encoder.pow2, c.encoder.params.BitsPerAngle)
}

// DistanceDirectCodes computes the same value as DistanceDirect for a code slice
// the caller has already resolved out of a TurboQuant chunk.
//
// This is the inner loop of a graph search: resolving the code slice needs a
// slab-table load, a slab pointer chase and a generation comparison, which
// together cost several times more than the distance itself. A caller that
// evaluates a whole block of candidates opens one batch with BeginTQChunkBatch
// and then calls this per candidate, so that work is paid once per block.
func (c *TurboQuantCompute) DistanceDirectCodes(rotatedQuery []float32, tqCode []byte) (float32, error) {
	fn := simd.GetTurboQuantDistanceFunc()
	if fn == nil {
		return 0, fmt.Errorf("no TurboQuant distance function available")
	}
	return fn(rotatedQuery, tqCode, c.encoder.dims, c.encoder.pow2, c.encoder.params.BitsPerAngle)
}

// DistanceDirectCodesBatch computes distances directly from multiple TQ code slices using the rotated query.
func (c *TurboQuantCompute) DistanceDirectCodesBatch(rotatedQuery []float32, codes [][]byte, dst []float32) error {
	fn := simd.GetTurboQuantDistanceBatchFunc()
	if fn == nil {
		fn = simd.TurboQuantDistanceBatch
	}
	return fn(rotatedQuery, codes, dst, c.encoder.dims, c.encoder.pow2, c.encoder.params.BitsPerAngle)
}

// PrefetchChunk issues a read hint for the bytes a TurboQuant chunk starts at.
// Callers that already hold a batch-scoped view should slice it themselves;
// this exists for callers that only have an id.
func (c *TurboQuantCompute) PrefetchChunk(chunk []byte, index int) {
	if chunk == nil || index < 0 {
		return
	}
	stride := c.h.data.Load().PackedSize()
	start := index * stride
	if start < len(chunk) {
		simd.Prefetch(unsafe.Pointer(&chunk[start])) // #nosec G103
	}
}

// PrecomputeRotatedQuery applies the random rotation to a query vector for faster subsequent searches.
func (c *TurboQuantCompute) PrecomputeRotatedQuery(vec []float32, output []float32) error {
	if len(output) < c.encoder.pow2 {
		output = make([]float32, c.encoder.pow2)
	}
	clear(output)
	copy(output, vec)
	return simd.RandomRotation(output, c.encoder.params.Seed)
}

func (c *TurboQuantCompute) getVector(id uint32) ([]float32, error) {
	return c.getVectorWithDisk(id, nil, math.MaxUint64)
}

func (c *TurboQuantCompute) getVectorWithDisk(id uint32, dg *DiskGraph, maxGen uint64) ([]float32, error) {
	if p := c.h.tqDecodeCache.Load(); p != nil {
		dim := int(c.h.dims.Load())
		if dim > 0 {
			offset := int(id) * dim
			if offset+dim <= len(p.data) {
				res := make([]float32, dim)
				copy(res, p.data[offset:offset+dim])
				return res, nil
			}
		}
	}
	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)
	data := c.h.data.Load()
	var chunk []byte
	if maxGen == 18446744073709551615 {
		chunk = data.GetVectorsTQChunkFast(int(cID))
	} else {
		chunk = data.GetVectorsTQChunkWithGen(int(cID), maxGen)
	}

	var tqCode []byte
	var stride int

	if chunk != nil {
		stride = data.PackedSize()
		start := cOff * stride
		if start+stride <= len(chunk) {
			tqCode = chunk[start : start+stride]
		}
	}

	if tqCode == nil {
		// Fallback to DiskGraph
		if dg == nil {
			dg = c.h.diskGraph.Load()
		}
		if dg != nil {
			tqCode = dg.GetVectorTQ(id)
		}
	}

	if tqCode == nil {
		return nil, fmt.Errorf("tq vector %d not found", id)
	}

	return c.encoder.Decode(tqCode)
}

// getTQBytes returns the raw TurboQuant byte codes for a vector ID.
func (c *TurboQuantCompute) getTQBytes(id uint32, dg *DiskGraph, maxGen uint64) ([]byte, error) {
	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)
	data := c.h.data.Load()
	var chunk []byte
	if maxGen == 18446744073709551615 {
		chunk = data.GetVectorsTQChunkFast(int(cID))
	} else {
		chunk = data.GetVectorsTQChunkWithGen(int(cID), maxGen)
	}

	if chunk != nil {
		stride := data.PackedSize()
		start := cOff * stride
		if start+stride <= len(chunk) {
			return chunk[start : start+stride], nil
		}
	}

	if dg == nil {
		dg = c.h.diskGraph.Load()
	}
	if dg != nil {
		if tqCode := dg.GetVectorTQ(id); tqCode != nil {
			return tqCode, nil
		}
	}

	return nil, fmt.Errorf("tq vector %d not found", id)
}

// GetRadius extracts the radius information for a TurboQuant encoded vector.
func (c *TurboQuantCompute) GetRadius(id uint32, dg *DiskGraph, maxGen uint64) (float32, error) {
	cID := types.ChunkID(id)
	cOff := types.ChunkOffset(id)
	data := c.h.data.Load()
	var chunk []byte
	if maxGen == 18446744073709551615 {
		chunk = data.GetVectorsTQChunkFast(int(cID))
	} else {
		chunk = data.GetVectorsTQChunkWithGen(int(cID), maxGen)
	}

	var tqCode []byte
	var stride int

	if chunk != nil {
		stride = data.PackedSize()
		start := cOff * stride
		if start+stride <= len(chunk) {
			tqCode = chunk[start : start+stride]
		}
	}

	if tqCode == nil {
		if dg == nil {
			dg = c.h.diskGraph.Load()
		}
		if dg != nil {
			tqCode = dg.GetVectorTQ(id)
		}
	}

	if tqCode == nil {
		return 0, fmt.Errorf("tq vector %d not found for radius extraction", id)
	}

	return c.encoder.GetRadius(tqCode), nil
}
