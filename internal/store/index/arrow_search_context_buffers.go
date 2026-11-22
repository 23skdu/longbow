package index

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow/float16"

	"github.com/23skdu/longbow/internal/store/types"
)

// GetChunkF32 returns the chunk f32.
func (ctx *ArrowSearchContext) GetChunkF32(data *types.GraphData, cID int, maxGen uint64) []float32 {
	if cID < 0 || data == nil {
		return nil
	}
	if cID < len(ctx.cachedChunksF32) && ctx.cachedChunksF32[cID].epoch == ctx.chunkEpoch {
		return ctx.cachedChunksF32[cID].slice
	}
	return ctx.resolveChunkF32(data, cID, maxGen)
}

func (ctx *ArrowSearchContext) resolveChunkF32(data *types.GraphData, cID int, maxGen uint64) []float32 {
	var chunk []float32
	if maxGen == math.MaxUint64 {
		chunk = data.GetVectorsChunkFast(cID)
	} else {
		chunk = data.GetVectorsChunkWithGen(cID, maxGen)
	}
	if chunk == nil {
		return nil
	}
	if cID >= len(ctx.cachedChunksF32) {
		newLen := cID + 1
		if newLen < 64 {
			newLen = 64
		}
		if newLen < len(ctx.cachedChunksF32)*2 {
			newLen = len(ctx.cachedChunksF32) * 2
		}
		newBuf := make([]cachedChunkF32, newLen)
		copy(newBuf, ctx.cachedChunksF32)
		ctx.cachedChunksF32 = newBuf
	}
	ctx.cachedChunksF32[cID] = cachedChunkF32{
		slice: chunk,
		epoch: ctx.chunkEpoch,
	}
	return chunk
}

// GetVectorF32 retrieves the dims-length vector for node id from cached chunks.
func (ctx *ArrowSearchContext) GetVectorF32(data *types.GraphData, id uint32, dims int, maxGen uint64) []float32 {
	cID := int(id) / types.ChunkSize
	chunk := ctx.GetChunkF32(data, cID, maxGen)
	if chunk == nil {
		return nil
	}
	cOff := int(id) % types.ChunkSize
	pd := data.GetPaddedDimsForType(types.VectorTypeFloat32)
	start := cOff * pd
	if start+dims <= len(chunk) {
		return chunk[start : start+dims]
	}
	return nil
}

// GetChunkF64 returns the cached float64 chunk slice for cID under maxGen.
func (ctx *ArrowSearchContext) GetChunkF64(data *types.GraphData, cID int, maxGen uint64) []float64 {
	if cID < 0 || data == nil {
		return nil
	}
	if cID < len(ctx.cachedChunksF64) && ctx.cachedChunksF64[cID].epoch == ctx.chunkEpoch {
		return ctx.cachedChunksF64[cID].slice
	}
	return ctx.resolveChunkF64(data, cID, maxGen)
}

func (ctx *ArrowSearchContext) resolveChunkF64(data *types.GraphData, cID int, maxGen uint64) []float64 {
	var chunk []float64
	if maxGen == math.MaxUint64 {
		chunk = data.GetVectorsFloat64ChunkFast(cID)
	} else {
		chunk = data.GetVectorsFloat64ChunkWithGen(cID, maxGen)
	}
	if chunk == nil {
		return nil
	}
	if cID >= len(ctx.cachedChunksF64) {
		newLen := cID + 1
		if newLen < 32 {
			newLen = 32
		}
		if newLen < len(ctx.cachedChunksF64)*2 {
			newLen = len(ctx.cachedChunksF64) * 2
		}
		newBuf := make([]cachedChunkF64, newLen)
		copy(newBuf, ctx.cachedChunksF64)
		ctx.cachedChunksF64 = newBuf
	}
	ctx.cachedChunksF64[cID] = cachedChunkF64{
		slice: chunk,
		epoch: ctx.chunkEpoch,
	}
	return chunk
}

// GetVectorF64 retrieves the dims-length vector for node id from cached chunks.
func (ctx *ArrowSearchContext) GetVectorF64(data *types.GraphData, id uint32, dims int, maxGen uint64) []float64 {
	cID := int(id) / types.ChunkSize
	chunk := ctx.GetChunkF64(data, cID, maxGen)
	if chunk == nil {
		return nil
	}
	cOff := int(id) % types.ChunkSize
	pd := data.GetPaddedDimsForType(types.VectorTypeFloat64)
	start := cOff * pd
	if start+dims <= len(chunk) {
		return chunk[start : start+dims]
	}
	return nil
}

// GetChunkInt8 returns the cached int8 chunk slice for cID under maxGen.
func (ctx *ArrowSearchContext) GetChunkInt8(data *types.GraphData, cID int, maxGen uint64) []int8 {
	if cID < 0 || data == nil {
		return nil
	}
	if cID < len(ctx.cachedChunksInt8) && ctx.cachedChunksInt8[cID].epoch == ctx.chunkEpoch {
		return ctx.cachedChunksInt8[cID].slice
	}
	return ctx.resolveChunkInt8(data, cID, maxGen)
}

func (ctx *ArrowSearchContext) resolveChunkInt8(data *types.GraphData, cID int, maxGen uint64) []int8 {
	var chunk []int8
	if maxGen == math.MaxUint64 {
		chunk = data.GetVectorsInt8ChunkFast(cID)
	} else {
		chunk = data.GetVectorsInt8ChunkWithGen(cID, maxGen)
	}
	if chunk == nil {
		return nil
	}
	if cID >= len(ctx.cachedChunksInt8) {
		newLen := cID + 1
		if newLen < 32 {
			newLen = 32
		}
		if newLen < len(ctx.cachedChunksInt8)*2 {
			newLen = len(ctx.cachedChunksInt8) * 2
		}
		newBuf := make([]cachedChunkInt8, newLen)
		copy(newBuf, ctx.cachedChunksInt8)
		ctx.cachedChunksInt8 = newBuf
	}
	ctx.cachedChunksInt8[cID] = cachedChunkInt8{
		slice: chunk,
		epoch: ctx.chunkEpoch,
	}
	return chunk
}

// GetVectorInt8 retrieves the dims-length vector for node id from cached chunks.
func (ctx *ArrowSearchContext) GetVectorInt8(data *types.GraphData, id uint32, dims int, maxGen uint64) []int8 {
	cID := int(id) / types.ChunkSize
	chunk := ctx.GetChunkInt8(data, cID, maxGen)
	if chunk == nil {
		return nil
	}
	cOff := int(id) % types.ChunkSize
	pd := data.GetPaddedDimsForType(types.VectorTypeInt8)
	start := cOff * pd
	if start+dims <= len(chunk) {
		return chunk[start : start+dims]
	}
	return nil
}

// GetChunkSQ8 returns the cached SQ8 chunk slice for cID under maxGen.
func (ctx *ArrowSearchContext) GetChunkSQ8(data *types.GraphData, cID int, maxGen uint64) []byte {
	if cID < 0 || data == nil {
		return nil
	}
	if cID < len(ctx.cachedChunksSQ8) && ctx.cachedChunksSQ8[cID].epoch == ctx.chunkEpoch {
		return ctx.cachedChunksSQ8[cID].slice
	}
	return ctx.resolveChunkSQ8(data, cID, maxGen)
}

func (ctx *ArrowSearchContext) resolveChunkSQ8(data *types.GraphData, cID int, maxGen uint64) []byte {
	var chunk []byte
	if maxGen == math.MaxUint64 {
		chunk = data.GetVectorsSQ8ChunkFast(cID)
	} else {
		chunk = data.GetVectorsSQ8ChunkWithGen(cID, maxGen)
	}
	if chunk == nil {
		return nil
	}
	if cID >= len(ctx.cachedChunksSQ8) {
		newLen := cID + 1
		if newLen < 32 {
			newLen = 32
		}
		if newLen < len(ctx.cachedChunksSQ8)*2 {
			newLen = len(ctx.cachedChunksSQ8) * 2
		}
		newBuf := make([]cachedChunkBytes, newLen)
		copy(newBuf, ctx.cachedChunksSQ8)
		ctx.cachedChunksSQ8 = newBuf
	}
	ctx.cachedChunksSQ8[cID] = cachedChunkBytes{
		slice: chunk,
		epoch: ctx.chunkEpoch,
	}
	return chunk
}

// GetVectorSQ8 retrieves the dims-length SQ8 vector for node id from cached chunks.
func (ctx *ArrowSearchContext) GetVectorSQ8(data *types.GraphData, id uint32, dims int, maxGen uint64) []byte {
	cID := int(id) / types.ChunkSize
	chunk := ctx.GetChunkSQ8(data, cID, maxGen)
	if chunk == nil {
		return nil
	}
	cOff := int(id) % types.ChunkSize
	pd := data.GetPaddedDimsForType(types.VectorTypeUint8)
	start := cOff * pd
	if start+dims <= len(chunk) {
		return chunk[start : start+dims]
	}
	return nil
}

// GetChunkF16 returns the cached float16 chunk slice for cID under maxGen.
func (ctx *ArrowSearchContext) GetChunkF16(data *types.GraphData, cID int, maxGen uint64) []float16.Num {
	if cID < 0 || data == nil {
		return nil
	}
	if cID < len(ctx.cachedChunksF16) && ctx.cachedChunksF16[cID].epoch == ctx.chunkEpoch {
		return ctx.cachedChunksF16[cID].slice
	}
	return ctx.resolveChunkF16(data, cID, maxGen)
}

func (ctx *ArrowSearchContext) resolveChunkF16(data *types.GraphData, cID int, maxGen uint64) []float16.Num {
	var chunk []float16.Num
	if maxGen == math.MaxUint64 {
		chunk = data.GetVectorsF16ChunkFast(cID)
	} else {
		chunk = data.GetVectorsF16ChunkWithGen(cID, maxGen)
	}
	if chunk == nil {
		return nil
	}
	if cID >= len(ctx.cachedChunksF16) {
		newLen := cID + 1
		if newLen < 32 {
			newLen = 32
		}
		if newLen < len(ctx.cachedChunksF16)*2 {
			newLen = len(ctx.cachedChunksF16) * 2
		}
		newBuf := make([]cachedChunkF16, newLen)
		copy(newBuf, ctx.cachedChunksF16)
		ctx.cachedChunksF16 = newBuf
	}
	ctx.cachedChunksF16[cID] = cachedChunkF16{
		slice: chunk,
		epoch: ctx.chunkEpoch,
	}
	return chunk
}

// GetVectorF16 retrieves the dims-length vector for node id from cached chunks.
func (ctx *ArrowSearchContext) GetVectorF16(data *types.GraphData, id uint32, dims int, maxGen uint64) []float16.Num {
	cID := int(id) / types.ChunkSize
	chunk := ctx.GetChunkF16(data, cID, maxGen)
	if chunk == nil {
		return nil
	}
	cOff := int(id) % types.ChunkSize
	pd := data.GetPaddedDimsForType(types.VectorTypeFloat16)
	start := cOff * pd
	if start+dims <= len(chunk) {
		return chunk[start : start+dims]
	}
	return nil
}

// GetChunkC128 returns the cached complex128 chunk slice for cID under maxGen.
func (ctx *ArrowSearchContext) GetChunkC128(data *types.GraphData, cID int, maxGen uint64) []complex128 {
	if cID < 0 || data == nil {
		return nil
	}
	if cID < len(ctx.cachedChunksC128) && ctx.cachedChunksC128[cID].epoch == ctx.chunkEpoch {
		return ctx.cachedChunksC128[cID].slice
	}
	return ctx.resolveChunkC128(data, cID, maxGen)
}

func (ctx *ArrowSearchContext) resolveChunkC128(data *types.GraphData, cID int, maxGen uint64) []complex128 {
	var chunk []complex128
	if maxGen == math.MaxUint64 {
		chunk = data.GetVectorsComplex128ChunkFast(cID)
	} else {
		chunk = data.GetVectorsComplex128ChunkWithGen(cID, maxGen)
	}
	if chunk == nil {
		return nil
	}
	if cID >= len(ctx.cachedChunksC128) {
		newLen := cID + 1
		if newLen < 32 {
			newLen = 32
		}
		if newLen < len(ctx.cachedChunksC128)*2 {
			newLen = len(ctx.cachedChunksC128) * 2
		}
		newBuf := make([]cachedChunkC128, newLen)
		copy(newBuf, ctx.cachedChunksC128)
		ctx.cachedChunksC128 = newBuf
	}
	ctx.cachedChunksC128[cID] = cachedChunkC128{
		slice: chunk,
		epoch: ctx.chunkEpoch,
	}
	return chunk
}

// GetVectorC128 retrieves the dims-length vector for node id from cached chunks.
func (ctx *ArrowSearchContext) GetVectorC128(data *types.GraphData, id uint32, dims int, maxGen uint64) []complex128 {
	cID := int(id) / types.ChunkSize
	chunk := ctx.GetChunkC128(data, cID, maxGen)
	if chunk == nil {
		return nil
	}
	cOff := int(id) % types.ChunkSize
	pd := data.GetPaddedDimsForType(types.VectorTypeComplex128)
	start := cOff * pd
	if start+dims <= len(chunk) {
		return chunk[start : start+dims]
	}
	return nil
}
