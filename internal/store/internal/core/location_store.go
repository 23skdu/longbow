package core

import (
	"sync"
	"sync/atomic"

	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
)

const (
	// types.LocationChunkSize is the number of locations per chunk.
	// 1024 * 8 bytes (types.Location) = 8KB. Fits well in L1/L2.
	LocationChunkSize = 1024
)

// locationChunk holds a fixed block of locations.
type locationChunk struct {
	data [LocationChunkSize]atomic.Uint64
}

// packLocation packs a types.Location into a uint64.
// Assumes BatchIdx and RowIdx fit in int32.
func packLocation(loc types.Location) uint64 {
	return basecore.PackLocation(loc)
}

// unpackLocation unpacks a uint64 into a types.Location.
func unpackLocation(val uint64) types.Location {
	return basecore.UnpackLocation(val)
}

// ChunkedLocationStore manages vector locations using chunks to avoid
// global locking during reads and massive reallocations during growth.
type ChunkedLocationStore struct {
	mu     sync.RWMutex // Protects growth (appending chunks)
	chunks atomic.Pointer[[]*locationChunk]
	size   atomic.Uint32 // Total number of locations (simulates len)
	// reverseMap maps packed location (uint64) to types.VectorID for O(1) reverse lookup.
	reverseMap map[uint64]types.VectorID
}

// NewChunkedLocationStore creates a new store.
func NewChunkedLocationStore() *ChunkedLocationStore {
	s := &ChunkedLocationStore{
		reverseMap: make(map[uint64]types.VectorID),
	}
	// Initialize with empty slice
	empty := make([]*locationChunk, 0)
	s.chunks.Store(&empty)
	return s
}

// Get returns the location for the given ID.
// It is safe for concurrent access and lock-free for reads.
func (s *ChunkedLocationStore) Get(id types.VectorID) (types.Location, bool) {
	// Check against valid size to avoid reading uninitialized data in allocated chunks
	if uint32(id) >= s.size.Load() {
		return types.Location{}, false
	}

	chunks := *s.chunks.Load()
	idx := int(id)
	chunkIdx := idx / types.LocationChunkSize
	offset := idx % types.LocationChunkSize

	if chunkIdx >= len(chunks) {
		return types.Location{}, false
	}
	// Note: concurrent writes to the same ID are not guarded here,
	// but types.VectorID allocation is unique.
	packed := chunks[chunkIdx].data[offset].Load()
	return unpackLocation(packed), true
}

// GetID returns the ID for a given location using the reverse index.
// Returns (id, true) if found, (0, false) otherwise.
func (s *ChunkedLocationStore) GetID(loc types.Location) (types.VectorID, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	packed := packLocation(loc)
	id, ok := s.reverseMap[packed]
	return id, ok
}

// GetBatch retrieves locations for multiple IDs efficiently.
// results must be at least len(ids).
// Returns the number of found locations. types.Locations not found are not written to results (or zeroed).
// Actually, to keep index alignment, we should probably output found bools or use a structure.
// For our prefetch usecase: we want to map id -> location for checking.
// Simpler: Just fill results slice. If not found, use types.Location{-1, -1}.
func (s *ChunkedLocationStore) GetBatch(ids []types.VectorID, results []types.Location) {
	chunks := *s.chunks.Load()
	maxSize := uint32(s.size.Load())

	for i, id := range ids {
		if uint32(id) >= maxSize {
			results[i] = types.Location{BatchIdx: -1, RowIdx: -1}
			continue
		}

		idx := int(id)
		chunkIdx := idx / types.LocationChunkSize
		offset := idx % types.LocationChunkSize

		if chunkIdx >= len(chunks) {
			results[i] = types.Location{BatchIdx: -1, RowIdx: -1}
			continue
		}

		packed := chunks[chunkIdx].data[offset].Load()
		results[i] = unpackLocation(packed)
	}
}

// Set updates the location for a given ID.
// NOTE: This does not grow the store. Use Append for new IDs.
func (s *ChunkedLocationStore) Set(id types.VectorID, loc types.Location) {
	chunks := *s.chunks.Load()
	idx := int(id)
	chunkIdx := idx / types.LocationChunkSize
	offset := idx % types.LocationChunkSize

	packed := packLocation(loc)

	if chunkIdx < len(chunks) {
		// Update reverse map
		s.mu.Lock()
		chunks[chunkIdx].data[offset].Store(packed)
		s.reverseMap[packed] = id
		s.mu.Unlock()
	}
}

// Delete removes the location for a given ID.
func (s *ChunkedLocationStore) Delete(id types.VectorID) {
	chunks := *s.chunks.Load()
	idx := int(id)
	chunkIdx := idx / types.LocationChunkSize
	offset := idx % types.LocationChunkSize

	if chunkIdx < len(chunks) {
		s.mu.Lock()
		defer s.mu.Unlock()
		// Get old packed value to remove from reverse map
		packed := chunks[chunkIdx].data[offset].Load()
		delete(s.reverseMap, packed)

		// Mark as deleted/invalid (e.g. 0 or specific sentinel)
		// Assuming 0 is invalid or we just rely on reverseMap removal.
		chunks[chunkIdx].data[offset].Store(0)
	}
}

// Append adds a new location and returns its ID.
// This requires a lock but only during chunk creation.
func (s *ChunkedLocationStore) Append(loc types.Location) types.VectorID {
	// We optimistically check if we have space in the current tail chunk
	// However, since we need to return a unique ID and ensure existence,
	// we simplify by taking the lock.
	// Optimizing this to be lock-free is possible (CAS on index) but complex for resizing.
	s.mu.Lock()
	defer s.mu.Unlock()

	currentID := s.size.Load()
	idx := int(currentID)
	chunkIdx := idx / types.LocationChunkSize
	offset := idx % types.LocationChunkSize

	// Get current chunks
	oldChunksPtr := s.chunks.Load()
	oldChunks := *oldChunksPtr

	var currentChunk *locationChunk

	// Check if we need to grow chunks
	if chunkIdx >= len(oldChunks) {
		// Create new chunk
		newChunk := &locationChunk{}
		// Create new slice with appended chunk (Copy-On-Write for the slice header)
		newChunks := make([]*locationChunk, len(oldChunks)+1)
		copy(newChunks, oldChunks)
		newChunks[len(oldChunks)] = newChunk

		s.chunks.Store(&newChunks)
		currentChunk = newChunk
	} else {
		// Point to existing chunk
		currentChunk = oldChunks[chunkIdx]
	}

	packed := packLocation(loc)
	currentChunk.data[offset].Store(packed)
	s.reverseMap[packed] = types.VectorID(currentID) // Update reverse map
	s.size.Add(1)
	return types.VectorID(currentID)
}

// BatchAppend adds multiple locations efficiently, resizing chunks once.
func (s *ChunkedLocationStore) BatchAppend(locs []types.Location) (startID types.VectorID) {
	if len(locs) == 0 {
		return types.VectorID(s.size.Load())
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	currentID := int(s.size.Load())
	startID = types.VectorID(currentID) // #nosec G115
	targetEnd := currentID + len(locs)

	oldChunksPtr := s.chunks.Load()
	oldChunks := *oldChunksPtr

	neededChunks := (targetEnd + types.LocationChunkSize - 1) / types.LocationChunkSize

	// Resize if necessary
	var currentChunks []*locationChunk
	if neededChunks > len(oldChunks) {
		currentChunks = make([]*locationChunk, neededChunks)
		copy(currentChunks, oldChunks)
		for i := len(oldChunks); i < neededChunks; i++ {
			currentChunks[i] = &locationChunk{}
		}
		s.chunks.Store(&currentChunks)
	} else {
		currentChunks = oldChunks
	}

	// Fill data
	for i, loc := range locs {
		absIdx := currentID + i
		cIdx := absIdx / types.LocationChunkSize
		off := absIdx % types.LocationChunkSize
		packed := packLocation(loc)
		currentChunks[cIdx].data[off].Store(packed)
		s.reverseMap[packed] = types.VectorID(absIdx) // #nosec G115
	}

	s.size.Store(uint32(targetEnd)) // #nosec G115
	return startID
}

// Len returns the number of items.
func (s *ChunkedLocationStore) Len() int {
	return int(s.size.Load())
}

// MaxID returns the maximum types.VectorID currently stored (size).
// This is useful for sizing bitsets.
func (s *ChunkedLocationStore) MaxID() uint32 {
	return s.size.Load()
}

// EnsureCapacity ensures the store can hold the given types.VectorID.
// It uses an optimistic check to avoid locking if capacity is sufficient.
func (s *ChunkedLocationStore) EnsureCapacity(id types.VectorID) {
	idx := int(id)
	chunkIdx := idx / types.LocationChunkSize

	// Always ensure size is at least id+1
	currentSize := s.size.Load()
	if uint32(id) >= currentSize {
		s.UpdateSize(id)
	}

	// Optimistic check for chunks
	chunksPtr := s.chunks.Load()
	if chunkIdx < len(*chunksPtr) {
		return
	}

	// Slow path: Lock and grow
	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-check under lock
	chunksPtr = s.chunks.Load()
	chunks := *chunksPtr
	if chunkIdx < len(chunks) {
		// Someone else grew it
		return
	}

	// Grow
	neededChunks := chunkIdx + 1
	newChunks := make([]*locationChunk, neededChunks)
	copy(newChunks, chunks)
	for i := len(chunks); i < neededChunks; i++ {
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
	empty := make([]*locationChunk, 0)
	s.chunks.Store(&empty)
	// Clear map
	s.reverseMap = make(map[uint64]types.VectorID)
}

// IterateMutable iterates over all locations, allowing atomic modification.
// The callback receives the ID and a pointer to the atomic storage.
// Note: This is not thread-safe with respect to concurrent remapping,
// but RemapLocations is a stop-the-world operation anyway.
func (s *ChunkedLocationStore) IterateMutable(fn func(id types.VectorID, val *atomic.Uint64)) {
	chunks := *s.chunks.Load()
	currentSize := int(s.size.Load())

	for i, chunk := range chunks {
		baseID := i * types.LocationChunkSize
		if baseID >= currentSize {
			break
		}

		limit := types.LocationChunkSize
		// If this is the last relevant chunk, cap limit
		if baseID+limit > currentSize {
			limit = currentSize - baseID
		}

		for j := 0; j < limit; j++ {
			fn(types.VectorID(baseID+j), &chunk.data[j])
		}
	}
}
