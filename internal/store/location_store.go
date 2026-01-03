package store

import (
	"sync"
	"sync/atomic"
)

const (
	// LocationChunkSize is the number of locations per chunk.
	// 1024 * 8 bytes (Location) = 8KB. Fits well in L1/L2.
	LocationChunkSize = 1024
)

// locationChunk holds a fixed block of locations.
type locationChunk struct {
	data [LocationChunkSize]atomic.Uint64
}

// packLocation packs a Location into a uint64.
// Assumes BatchIdx and RowIdx fit in int32.
func packLocation(loc Location) uint64 {
	return uint64(uint32(loc.BatchIdx))<<32 | uint64(uint32(loc.RowIdx))
}

// unpackLocation unpacks a uint64 into a Location.
func unpackLocation(val uint64) Location {
	return Location{
		BatchIdx: int(int32(val >> 32)),
		RowIdx:   int(int32(val)),
	}
}

// ChunkedLocationStore manages vector locations using chunks to avoid
// global locking during reads and massive reallocations during growth.
type ChunkedLocationStore struct {
	mu     sync.RWMutex // Protects growth (appending chunks)
	chunks atomic.Pointer[[]*locationChunk]
	size   atomic.Uint32 // Total number of locations (simulates len)
}

// NewChunkedLocationStore creates a new store.
func NewChunkedLocationStore() *ChunkedLocationStore {
	s := &ChunkedLocationStore{}
	// Initialize with empty slice
	empty := make([]*locationChunk, 0)
	s.chunks.Store(&empty)
	return s
}

// Get returns the location for the given ID.
// It is safe for concurrent access and lock-free for reads.
func (s *ChunkedLocationStore) Get(id VectorID) (Location, bool) {
	// Check against valid size to avoid reading uninitialized data in allocated chunks
	if uint32(id) >= s.size.Load() {
		return Location{}, false
	}

	chunks := *s.chunks.Load()
	idx := int(id)
	chunkIdx := idx / LocationChunkSize
	offset := idx % LocationChunkSize

	if chunkIdx >= len(chunks) {
		return Location{}, false
	}
	// Note: concurrent writes to the same ID are not guarded here,
	// but VectorID allocation is unique.
	packed := chunks[chunkIdx].data[offset].Load()
	return unpackLocation(packed), true
}

// GetBatch retrieves locations for multiple IDs efficiently.
// results must be at least len(ids).
// Returns the number of found locations. Locations not found are not written to results (or zeroed).
// Actually, to keep index alignment, we should probably output found bools or use a structure.
// For our prefetch usecase: we want to map id -> location for checking.
// Simpler: Just fill results slice. If not found, use Location{-1, -1}.
func (s *ChunkedLocationStore) GetBatch(ids []VectorID, results []Location) {
	chunks := *s.chunks.Load()
	maxSize := uint32(s.size.Load())

	for i, id := range ids {
		if uint32(id) >= maxSize {
			results[i] = Location{BatchIdx: -1, RowIdx: -1}
			continue
		}

		idx := int(id)
		chunkIdx := idx / LocationChunkSize
		offset := idx % LocationChunkSize

		if chunkIdx >= len(chunks) {
			results[i] = Location{BatchIdx: -1, RowIdx: -1}
			continue
		}

		packed := chunks[chunkIdx].data[offset].Load()
		results[i] = unpackLocation(packed)
	}
}

// Set updates the location for a given ID.
// NOTE: This does not grow the store. Use Append for new IDs.
func (s *ChunkedLocationStore) Set(id VectorID, loc Location) {
	chunks := *s.chunks.Load()
	idx := int(id)
	chunkIdx := idx / LocationChunkSize
	offset := idx % LocationChunkSize

	if chunkIdx < len(chunks) {
		chunks[chunkIdx].data[offset].Store(packLocation(loc))
	}
}

// Append adds a new location and returns its ID.
// This requires a lock but only during chunk creation.
func (s *ChunkedLocationStore) Append(loc Location) VectorID {
	// We optimistically check if we have space in the current tail chunk
	// However, since we need to return a unique ID and ensure existence,
	// we simplify by taking the lock.
	// Optimizing this to be lock-free is possible (CAS on index) but complex for resizing.
	s.mu.Lock()
	defer s.mu.Unlock()

	currentID := s.size.Load()
	idx := int(currentID)
	chunkIdx := idx / LocationChunkSize
	offset := idx % LocationChunkSize

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

	currentChunk.data[offset].Store(packLocation(loc))
	s.size.Add(1)
	return VectorID(currentID)
}

// BatchAppend adds multiple locations efficiently, resizing chunks once.
func (s *ChunkedLocationStore) BatchAppend(locs []Location) (startID VectorID) {
	if len(locs) == 0 {
		return VectorID(s.size.Load())
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	currentID := int(s.size.Load())
	startID = VectorID(currentID)
	targetEnd := currentID + len(locs)

	oldChunksPtr := s.chunks.Load()
	oldChunks := *oldChunksPtr

	neededChunks := (targetEnd + LocationChunkSize - 1) / LocationChunkSize

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
		cIdx := absIdx / LocationChunkSize
		off := absIdx % LocationChunkSize
		currentChunks[cIdx].data[off].Store(packLocation(loc))
	}

	s.size.Store(uint32(targetEnd))
	return startID
}

// Len returns the number of items.
func (s *ChunkedLocationStore) Len() int {
	return int(s.size.Load())
}

// MaxID returns the maximum VectorID currently stored (size).
// This is useful for sizing bitsets.
func (s *ChunkedLocationStore) MaxID() uint32 {
	return s.size.Load()
}

// EnsureCapacity ensures the store can hold the given VectorID.
// It uses an optimistic check to avoid locking if capacity is sufficient.
func (s *ChunkedLocationStore) EnsureCapacity(id VectorID) {
	idx := int(id)
	chunkIdx := idx / LocationChunkSize

	// Optimistic check
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

	// We also need to ensure 'size' reflects this growth?
	// 'size' tracks 'Len'. If we just reserve capacity, size might lag if we store sparsely?
	// But ShardedHNSW uses sequential IDs.
	// We'll leave 'size' management to Append or explicit SetSize if needed.
	// For Get() to work, 'size' check is performed: `if uint32(id) >= s.size.Load()`.
	// So we MUST update size if we intend to allow Get(id) to succeed immediately.
	// But `Set` doesn't update size.
	// If we use sequential allocation in ShardedHNSW, we should update size there or here.
	// Let's allow updating size if id >= size.

	currentSize := s.size.Load()
	if uint32(id) >= currentSize {
		// Loop to CAS loop?
		// Since we hold lock, we can store?
		// But size is atomic.
		// Let's just update size strictly if we are extending.
		// Wait, multiple threads calling EnsureCapacity(id) might race on size update.
		// But we hold 'mu' for growth. Is 'mu' protecting size? No, size is atomic.
		// Maybe we shouldn't couple Capacity with Size.
		// Let Get() rely on chunk existence, removing the size check?
		// Or update size lazily?
		// The existing Get() implementation: `if uint32(id) >= s.size.Load() { return false }`
		// This prevents reading "unallocated" slots.
		// In ShardedHNSW, we assign ID -> atomic increment size -> Set Location.
		// So we should handle size update.
	}
}

// UpdateSize ensures size is at least id+1.
func (s *ChunkedLocationStore) UpdateSize(id VectorID) {
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
}

// IterateMutable iterates over all locations, allowing atomic modification.
// The callback receives the ID and a pointer to the atomic storage.
// Note: This is not thread-safe with respect to concurrent remapping,
// but RemapLocations is a stop-the-world operation anyway.
func (s *ChunkedLocationStore) IterateMutable(fn func(id VectorID, val *atomic.Uint64)) {
	chunks := *s.chunks.Load()
	currentSize := int(s.size.Load())

	for i, chunk := range chunks {
		baseID := i * LocationChunkSize
		if baseID >= currentSize {
			break
		}

		limit := LocationChunkSize
		// If this is the last relevant chunk, cap limit
		if baseID+limit > currentSize {
			limit = currentSize - baseID
		}

		for j := 0; j < limit; j++ {
			fn(VectorID(baseID+j), &chunk.data[j])
		}
	}
}
