package core

import (
	"sync"
	"sync/atomic"

	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
)

const (
	// LocationChunkSize is the number of locations per chunk.
	// 1024 * 8 bytes (types.Location) = 8KB. Fits well in L1/L2.
	LocationChunkSize = 1024
	// ReverseShards is the number of shards for the reverse index to reduce contention.
	ReverseShards = 64
)

// locationChunk holds a fixed block of locations.
type locationChunk struct {
	data [LocationChunkSize]atomic.Uint64
}

// packLocation packs a types.Location into a uint64.
func packLocation(loc types.Location) uint64 {
	return basecore.PackLocation(loc)
}

// unpackLocation unpacks a uint64 into a types.Location.
func unpackLocation(val uint64) types.Location {
	return basecore.UnpackLocation(val)
}

type reverseShard struct {
	mu   sync.RWMutex
	data map[uint64]types.VectorID
}

// ChunkedLocationStore manages vector locations using chunks to avoid
// global locking during reads and massive reallocations during growth.
type ChunkedLocationStore struct {
	mu     sync.Mutex                   // Protects growth (appending chunks)
	chunks atomic.Pointer[[]*locationChunk] // Stores []*locationChunk
	size   atomic.Uint32                // Total number of locations (simulates len)
	
	// reverseMap is sharded to reduce contention during parallel ingestion.
	reverseShards [ReverseShards]reverseShard
}

// NewChunkedLocationStore creates a new store.
func NewChunkedLocationStore() *ChunkedLocationStore {
	s := &ChunkedLocationStore{}
	emptyChunks := make([]*locationChunk, 0)
	s.chunks.Store(&emptyChunks)
	for i := 0; i < ReverseShards; i++ {
		s.reverseShards[i].data = make(map[uint64]types.VectorID)
	}
	return s
}

func (s *ChunkedLocationStore) getShard(packed uint64) *reverseShard {
	return &s.reverseShards[packed%uint64(ReverseShards)]
}

// Get returns the location for the given ID.
// It is safe for concurrent access and lock-free for reads.
func (s *ChunkedLocationStore) Get(id types.VectorID) (types.Location, bool) {
	if uint32(id) >= s.size.Load() {
		return types.Location{}, false
	}

	chunksPtr := s.chunks.Load()
	if chunksPtr == nil {
		return types.Location{}, false
	}
	chunks := *chunksPtr
	idx := int(id)
	chunkIdx := idx / LocationChunkSize
	offset := idx % LocationChunkSize

	if chunkIdx >= len(chunks) {
		return types.Location{}, false
	}
	
	packed := chunks[chunkIdx].data[offset].Load()
	if packed == 0 {
		return types.Location{}, false
	}
	return unpackLocation(packed), true
}

// GetID returns the ID for a given location using the reverse index.
// Returns (id, true) if found, (0, false) otherwise.
func (s *ChunkedLocationStore) GetID(loc types.Location) (types.VectorID, bool) {
	packed := packLocation(loc)
	shard := s.getShard(packed)
	shard.mu.RLock()
	id, ok := shard.data[packed]
	shard.mu.RUnlock()
	return id, ok
}

// GetBatch retrieves locations for multiple IDs efficiently.
func (s *ChunkedLocationStore) GetBatch(ids []types.VectorID, results []types.Location) {
	chunksPtr := s.chunks.Load()
	if chunksPtr == nil {
		for i := range results {
			results[i] = types.Location{BatchIdx: -1, RowIdx: -1}
		}
		return
	}
	chunks := *chunksPtr
	maxSize := s.size.Load()

	for i, id := range ids {
		if uint32(id) >= maxSize {
			results[i] = types.Location{BatchIdx: -1, RowIdx: -1}
			continue
		}

		idx := int(id)
		chunkIdx := idx / LocationChunkSize
		offset := idx % LocationChunkSize

		if chunkIdx >= len(chunks) {
			results[i] = types.Location{BatchIdx: -1, RowIdx: -1}
			continue
		}

		packed := chunks[chunkIdx].data[offset].Load()
		if packed == 0 {
			results[i] = types.Location{BatchIdx: -1, RowIdx: -1}
		} else {
			results[i] = unpackLocation(packed)
		}
	}
}

// Set updates the location for a given ID.
func (s *ChunkedLocationStore) Set(id types.VectorID, loc types.Location) {
	chunksPtr := s.chunks.Load()
	if chunksPtr == nil {
		return
	}
	chunks := *chunksPtr
	idx := int(id)
	chunkIdx := idx / LocationChunkSize
	offset := idx % LocationChunkSize

	packed := packLocation(loc)

	if chunkIdx < len(chunks) {
		// Update data
		oldPacked := chunks[chunkIdx].data[offset].Swap(packed)
		
		// Update reverse map
		if oldPacked != 0 {
			oldShard := s.getShard(oldPacked)
			oldShard.mu.Lock()
			delete(oldShard.data, oldPacked)
			oldShard.mu.Unlock()
		}
		
		newShard := s.getShard(packed)
		newShard.mu.Lock()
		newShard.data[packed] = id
		newShard.mu.Unlock()
	}
}

// Delete removes the location for a given ID.
func (s *ChunkedLocationStore) Delete(id types.VectorID) {
	chunksPtr := s.chunks.Load()
	if chunksPtr == nil {
		return
	}
	chunks := *chunksPtr
	idx := int(id)
	chunkIdx := idx / LocationChunkSize
	offset := idx % LocationChunkSize

	if chunkIdx < len(chunks) {
		packed := chunks[chunkIdx].data[offset].Swap(0)
		if packed != 0 {
			shard := s.getShard(packed)
			shard.mu.Lock()
			delete(shard.data, packed)
			shard.mu.Unlock()
		}
	}
}

// Append adds a new location and returns its ID.
func (s *ChunkedLocationStore) Append(loc types.Location) types.VectorID {
	// 1. Reserve ID
	id := types.VectorID(s.size.Add(1) - 1)
	
	// 2. Ensure capacity (thread-safe growth)
	s.EnsureCapacity(id)
	
	// 3. Update data
	chunks := *s.chunks.Load()
	chunkIdx := int(id) / LocationChunkSize
	offset := int(id) % LocationChunkSize
	
	packed := packLocation(loc)
	chunks[chunkIdx].data[offset].Store(packed)
	
	// 4. Update reverse map
	shard := s.getShard(packed)
	shard.mu.Lock()
	shard.data[packed] = id
	shard.mu.Unlock()
	
	return id
}

// BatchAppend adds multiple locations efficiently.
func (s *ChunkedLocationStore) BatchAppend(locs []types.Location) (startID types.VectorID) {
	if len(locs) == 0 {
		return types.VectorID(s.size.Load())
	}

	// 1. Reserve block of IDs
	count := uint32(len(locs))
	startIDVal := s.size.Add(count) - count
	startID = types.VectorID(startIDVal)
	
	// 2. Ensure capacity for the whole batch
	s.EnsureCapacity(types.VectorID(startIDVal + count - 1))
	
	// 3. Update data and reverse maps
	chunks := *s.chunks.Load()
	for i, loc := range locs {
		currID := types.VectorID(startIDVal + uint32(i))
		idx := int(currID)
		chunkIdx := idx / LocationChunkSize
		offset := idx % LocationChunkSize
		
		packed := packLocation(loc)
		chunks[chunkIdx].data[offset].Store(packed)
		
		shard := s.getShard(packed)
		shard.mu.Lock()
		shard.data[packed] = currID
		shard.mu.Unlock()
	}
	
	return startID
}

// Len returns the number of items.
func (s *ChunkedLocationStore) Len() int {
	return int(s.size.Load())
}

// MaxID returns the maximum types.VectorID currently stored.
func (s *ChunkedLocationStore) MaxID() uint32 {
	return s.size.Load()
}

// EnsureCapacity ensures the store can hold the given types.VectorID.
func (s *ChunkedLocationStore) EnsureCapacity(id types.VectorID) {
	idx := int(id)
	chunkIdx := idx / LocationChunkSize

	// Optimistic check
	chunksPtr := s.chunks.Load()
	if chunksPtr != nil && chunkIdx < len(*chunksPtr) {
		return
	}

	// Slow path: growth with lock
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Re-check under lock
	oldChunksPtr := s.chunks.Load()
	oldChunks := *oldChunksPtr
	if chunkIdx < len(oldChunks) {
		return
	}
	
	neededChunks := chunkIdx + 1
	newChunks := make([]*locationChunk, neededChunks)
	copy(newChunks, oldChunks)
	for i := len(oldChunks); i < neededChunks; i++ {
		newChunks[i] = &locationChunk{}
	}
	s.chunks.Store(&newChunks)
}

// UpdateSize ensures size is at least id+1.
func (s *ChunkedLocationStore) UpdateSize(id types.VectorID) {
	newSize := uint32(id) + 1
	for {
		curr := s.size.Load()
		if curr >= newSize {
			return
		}
		if s.size.CompareAndSwap(curr, newSize) {
			return
		}
	}
}

// Reset clears the store.
func (s *ChunkedLocationStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.size.Store(0)
	emptyChunks := make([]*locationChunk, 0)
	s.chunks.Store(&emptyChunks)
	for i := 0; i < ReverseShards; i++ {
		s.reverseShards[i].mu.Lock()
		s.reverseShards[i].data = make(map[uint64]types.VectorID)
		s.reverseShards[i].mu.Unlock()
	}
}

// IterateMutable iterates over all locations, allowing atomic modification.
func (s *ChunkedLocationStore) IterateMutable(fn func(id types.VectorID, val *atomic.Uint64)) {
	chunksPtr := s.chunks.Load()
	if chunksPtr == nil {
		return
	}
	chunks := *chunksPtr
	currentSize := int(s.size.Load())

	for i, chunk := range chunks {
		baseID := i * LocationChunkSize
		if baseID >= currentSize {
			break
		}

		limit := LocationChunkSize
		if baseID+limit > currentSize {
			limit = currentSize - baseID
		}

		for j := 0; j < limit; j++ {
			fn(types.VectorID(baseID+j), &chunk.data[j])
		}
	}
}

