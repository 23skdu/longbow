package index

import (
	"math"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/memory"
	types "github.com/23skdu/longbow/internal/store/types"
)

// EnableTurboQuant dynamically promotes the index to use TurboQuant encoding.
func (h *ArrowHNSW) EnableTurboQuant(bits int) {
	h.growMu.Lock()
	defer h.growMu.Unlock()
	h.config.TurboQuantEnabled = true
	h.config.TurboQuantBits = bits
	h.config.DataType = types.VectorTypeTQ
	gd := h.data.Load()
	if gd == nil {
		return
	}
	gd.TurboQuantEnabled = true
	gd.TurboQuantBits = bits
	h.publishTQ(int(h.dims.Load()), bits)

	// Allocate and compress existing in-memory float32 vectors!
	stride := gd.PackedSize()
	paddedDims := gd.GetPaddedDimsForType(types.VectorTypeFloat32)

	for cID := 0; cID < len(gd.VectorsF32); cID++ {
		offsetF32 := atomic.LoadUint64(&gd.VectorsF32[cID])
		if offsetF32 == 0 {
			continue
		}

		// Ensure the TQ slice is allocated for this chunk
		_ = gd.EnsureChunk(cID, 0, gd.Dims)

		// Get the float32 chunk data
		f32Chunk := gd.Float32Arena.Get(memory.SliceRef{
			Offset: offsetF32,
			Len:    uint32(types.ChunkSize * paddedDims), // #nosec G115
			Cap:    uint32(types.ChunkSize * paddedDims), // #nosec G115
		})

		// Get the TQ chunk slice ref/offset
		offsetTQ := atomic.LoadUint64(&gd.VectorsTQ[cID])
		tqChunk := gd.Uint8Arena.Get(memory.SliceRef{
			Offset: offsetTQ,
			Len:    uint32(types.ChunkSize * stride), // #nosec G115
			Cap:    uint32(types.ChunkSize * stride), // #nosec G115
		})

		// Compress each vector in the chunk
		for i := 0; i < types.ChunkSize; i++ {
			vF32 := f32Chunk[i*paddedDims : i*paddedDims+gd.Dims]
			code, err := h.tq().encoder.Encode(vF32)
			if err == nil {
				dest := tqChunk[i*stride : (i+1)*stride]
				copy(dest, code)
			}
		}
	}

	// Release original Float32Arena slabs to reclaim memory immediately!
	for cID := 0; cID < len(gd.VectorsF32); cID++ {
		gd.ReleaseFloat32Chunk(cID)
	}
}

// tqRuntime is the TurboQuant encoder and its distance computer, published as
// one immutable unit.
//
// They are always replaced together and are only ever replaced when the
// dimension they are bound to changes, which happens while the index is still
// empty. Publishing them as a pair rather than as two independent pointers means
// a concurrent reader cannot observe a new encoder beside a stale computer, and
// one atomic load per use replaces two unsynchronised field reads on a path that
// runs once per candidate.
type tqRuntime struct {
	encoder *TurboQuantEncoder
	compute *TurboQuantCompute
}

// tq returns the current TurboQuant runtime, or nil when TurboQuant is not
// engaged. The result is a consistent snapshot; it is never mutated in place.
func (h *ArrowHNSW) tq() *tqRuntime {
	return h.tqRuntime.Load()
}

// tqEncoderOf returns the TurboQuant encoder, or nil when TurboQuant is not
// engaged, for call sites that only encode or decode and never compute a
// distance.
func tqEncoderOf(h *ArrowHNSW) *TurboQuantEncoder {
	if tq := h.tq(); tq != nil {
		return tq.encoder
	}
	return nil
}

// ensureTQComponents binds the TurboQuant encoder, the TurboQuant distance
// computer and the scratch arena to dims, creating whatever is missing.
func (h *ArrowHNSW) ensureTQComponents(dims int) {
	if dims <= 0 {
		return
	}
	if h.config.DataType != types.VectorTypeTQ && !h.config.TurboQuantEnabled {
		return
	}
	if gd := h.data.Load(); gd == nil || gd.Dims != dims {
		return
	}
	cur := h.tq()
	if cur == nil || cur.encoder.dims != dims {
		bits := h.config.TurboQuantBits
		if bits <= 0 || bits > 8 {
			bits = 8
		}
		h.publishTQ(dims, bits)
	} else if cur.compute == nil || cur.compute.encoder.dims != dims {
		h.tqRuntime.Store(&tqRuntime{encoder: cur.encoder, compute: NewTurboQuantCompute(h)})
	}
	h.EnableTQScratchArena()
}

// publishTQ builds and publishes the TurboQuant runtime for dims.
func (h *ArrowHNSW) publishTQ(dims, bits int) {
	if bits <= 0 || bits > 8 {
		bits = 4
	}
	enc := NewTurboQuantEncoder(dims, bits, 42)
	h.tqRuntime.Store(&tqRuntime{encoder: enc, compute: NewTurboQuantCompute(h)})
}

// GetTQScratchArena returns the active ephemeral TurboQuant scratch arena, or nil if none.
func (h *ArrowHNSW) GetTQScratchArena() *TQScratchArena {
	return h.tqArena.Load()
}

// tqVectorAt returns the reconstructed float32 vector for id, preferring the
// scratch arena and reconstructing it on a miss.
func (h *ArrowHNSW) tqVectorAt(arena *TQScratchArena, id uint32, dim int) ([]float32, bool) {
	tq := h.tq()
	if arena == nil || dim <= 0 || tq == nil || tq.compute == nil {
		return nil, false
	}
	if v, ok := arena.Get(id); ok && len(v) >= dim {
		return v, true
	}
	v, err := arena.GetOrDecode(id, func(_ uint32, dst []float32) error {
		code, err := tq.compute.getTQBytes(id, nil, math.MaxUint64)
		if err != nil {
			return err
		}
		return tq.compute.encoder.DecodeInto(code, dst)
	})
	if err != nil || len(v) < dim {
		return nil, false
	}
	return v, true
}

// EnableTQScratchArena initializes and activates the ephemeral scratch arena for indexing.
func (h *ArrowHNSW) EnableTQScratchArena() *TQScratchArena {
	if existing := h.tqArena.Load(); existing != nil && existing.IsEnabled() {
		if existing.Dims() == h.arenaDims() {
			return existing
		}
	}
	arena := NewTQScratchArena(h.arenaDims())
	h.tqArena.Store(arena)
	return arena
}

// arenaDims is the dimension the scratch arena should be bound to: the live
// index dimension, falling back to the configured one before the dimension has
// been discovered from the first ingested record.
func (h *ArrowHNSW) arenaDims() int {
	dims := int(h.dims.Load())
	if dims <= 0 {
		if gd := h.data.Load(); gd != nil {
			dims = gd.Dims
		}
	}
	if dims <= 0 {
		dims = h.config.Dims
	}
	if dims <= 0 {
		return 0
	}
	return dims
}

// ReleaseTQScratchArena deallocates and releases the ephemeral scratch arena after indexing.
func (h *ArrowHNSW) ReleaseTQScratchArena() {
	if arena := h.tqArena.Swap(nil); arena != nil {
		arena.Release()
	}
}
