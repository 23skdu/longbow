package index

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/store/types"
)

const tqArenaChunkSize = types.ChunkSize // 1024 vectors per chunk

// arenaChunk represents an ephemeral chunk of reconstructed float32 vectors.
type arenaChunk struct {
	data  []float32      // tqArenaChunkSize * dims
	valid []atomic.Uint64 // bitmask tracking valid vector slots: (tqArenaChunkSize + 63) / 64 = 16 uint64s
	mu    sync.Mutex      // per-chunk lock for decode and populate
}

func newArenaChunk(dims int) *arenaChunk {
	numWords := (tqArenaChunkSize + 63) / 64
	return &arenaChunk{
		data:  make([]float32, tqArenaChunkSize*dims),
		valid: make([]atomic.Uint64, numWords),
	}
}

func (c *arenaChunk) isValid(offset int) bool {
	word := offset / 64
	bit := uint64(1) << (offset % 64)
	return (c.valid[word].Load() & bit) != 0
}

func (c *arenaChunk) setValid(offset int) {
	word := offset / 64
	bit := uint64(1) << (offset % 64)
	for {
		old := c.valid[word].Load()
		if old&bit != 0 {
			return
		}
		if c.valid[word].CompareAndSwap(old, old|bit) {
			return
		}
	}
}

// TQScratchArena provides an ephemeral, concurrent-safe chunked scratch arena for caching
// reconstructed (decoded) float32 vectors during the HNSW indexing phase.
//
// Background & Motivation:
// TurboQuant encodes vectors in polar coordinates with QJL randomized rotations.
// While compressed vectors yield high memory density, distance evaluation against
// compressed codes requires trigonometric lookups and dequantization. During HNSW
// graph construction, each node's neighborhood is evaluated across dozens of candidate
// hops, causing the same vector to be dequantized hundreds of times.
//
// TQScratchArena eliminates redundant dequantization:
// 1. Decoded float32 vectors are cached chunk-by-chunk on demand or in batches.
// 2. Chunks align with types.ChunkSize (1024), allocated lazily as vectors are accessed.
// 3. Thread-safe lock-free bitmask tracking ensures concurrent worker goroutines
//    (e.g., 8 workers in AddConnectionsBatch) share decoded vectors without race conditions.
// 4. Once indexing completes, the arena is released via Release(), reclaiming the float32
//    working set and preserving TurboQuant's small memory footprint during query serving.
type TQScratchArena struct {
	dims        int
	chunksMu    sync.RWMutex
	chunks      []*arenaChunk
	chunkCount  atomic.Int64
	hitCount    atomic.Int64
	missCount   atomic.Int64
	decodeCount atomic.Int64
	enabled     atomic.Bool
}

// NewTQScratchArena creates a new scratch arena for the specified vector dimension.
func NewTQScratchArena(dims int) *TQScratchArena {
	a := &TQScratchArena{
		dims:   dims,
		chunks: make([]*arenaChunk, 0, 64),
	}
	a.enabled.Store(true)
	return a
}

// IsEnabled returns true if the scratch arena is active.
func (a *TQScratchArena) IsEnabled() bool {
	return a != nil && a.enabled.Load()
}

// ensureChunk retrieves or allocates the chunk for a given chunk index.
func (a *TQScratchArena) ensureChunk(chunkID int) *arenaChunk {
	a.chunksMu.RLock()
	if chunkID < len(a.chunks) && a.chunks[chunkID] != nil {
		c := a.chunks[chunkID]
		a.chunksMu.RUnlock()
		return c
	}
	a.chunksMu.RUnlock()

	a.chunksMu.Lock()
	defer a.chunksMu.Unlock()
	if chunkID < len(a.chunks) && a.chunks[chunkID] != nil {
		return a.chunks[chunkID]
	}
	if chunkID >= len(a.chunks) {
		newChunks := make([]*arenaChunk, chunkID+1)
		copy(newChunks, a.chunks)
		a.chunks = newChunks
	}
	c := newArenaChunk(a.dims)
	a.chunks[chunkID] = c
	a.chunkCount.Add(1)
	return c
}

// Get returns the cached reconstructed float32 vector for id if present.
func (a *TQScratchArena) Get(id uint32) ([]float32, bool) {
	if a == nil || !a.enabled.Load() {
		return nil, false
	}
	chunkID := int(id) / tqArenaChunkSize
	offset := int(id) % tqArenaChunkSize

	a.chunksMu.RLock()
	if chunkID >= len(a.chunks) {
		a.chunksMu.RUnlock()
		a.missCount.Add(1)
		return nil, false
	}
	c := a.chunks[chunkID]
	a.chunksMu.RUnlock()

	if c == nil || !c.isValid(offset) {
		a.missCount.Add(1)
		return nil, false
	}

	a.hitCount.Add(1)
	start := offset * a.dims
	return c.data[start : start+a.dims], true
}

// Put caches an already-reconstructed float32 vector for id.
func (a *TQScratchArena) Put(id uint32, vec []float32) {
	if a == nil || !a.enabled.Load() || len(vec) < a.dims {
		return
	}
	chunkID := int(id) / tqArenaChunkSize
	offset := int(id) % tqArenaChunkSize
	c := a.ensureChunk(chunkID)

	start := offset * a.dims
	copy(c.data[start:start+a.dims], vec[:a.dims])
	c.setValid(offset)
}

// PutBatch populates a contiguous sequence of float32 vectors starting at startID.
func (a *TQScratchArena) PutBatch(startID uint32, vecs [][]float32) {
	if a == nil || !a.enabled.Load() {
		return
	}
	for i, v := range vecs {
		if len(v) >= a.dims {
			a.Put(startID+uint32(i), v)
		}
	}
}

// GetOrDecode retrieves the vector from cache or decodes it into the arena using decodeFn.
func (a *TQScratchArena) GetOrDecode(id uint32, decodeFn func(id uint32, dst []float32) error) ([]float32, error) {
	if a == nil {
		return nil, fmt.Errorf("scratch arena is nil")
	}
	if !a.enabled.Load() {
		dst := make([]float32, a.dims)
		err := decodeFn(id, dst)
		return dst, err
	}

	if v, ok := a.Get(id); ok {
		return v, nil
	}

	chunkID := int(id) / tqArenaChunkSize
	offset := int(id) % tqArenaChunkSize
	c := a.ensureChunk(chunkID)

	c.mu.Lock()
	defer c.mu.Unlock()

	start := offset * a.dims
	if c.isValid(offset) {
		a.hitCount.Add(1)
		return c.data[start : start+a.dims], nil
	}

	target := c.data[start : start+a.dims]
	if err := decodeFn(id, target); err != nil {
		return nil, err
	}
	c.setValid(offset)
	a.decodeCount.Add(1)
	return target, nil
}

// Release clears and releases all allocated arena chunks, freeing the ephemeral memory.
func (a *TQScratchArena) Release() {
	if a == nil {
		return
	}
	a.enabled.Store(false)
	a.chunksMu.Lock()
	defer a.chunksMu.Unlock()
	a.chunks = nil
	a.chunkCount.Store(0)
}

// Stats returns hit count, miss count, dequantization count, and number of active chunks.
func (a *TQScratchArena) Stats() (hits, misses, decodes, chunks int64) {
	if a == nil {
		return 0, 0, 0, 0
	}
	return a.hitCount.Load(), a.missCount.Load(), a.decodeCount.Load(), a.chunkCount.Load()
}
