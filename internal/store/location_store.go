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

// Len returns the number of items.
func (s *ChunkedLocationStore) Len() int {
	return int(s.size.Load())
}

// Grow ensures the store has capacity for at least n items.
func (s *ChunkedLocationStore) Grow(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentLen := int(s.size.Load())
	targetLen := currentLen + n

	currentChunksPtr := s.chunks.Load()
	currentChunks := *currentChunksPtr

	neededChunks := (targetLen + LocationChunkSize - 1) / LocationChunkSize
	if neededChunks > len(currentChunks) {
		newChunks := make([]*locationChunk, neededChunks)
		copy(newChunks, currentChunks)
		for i := len(currentChunks); i < neededChunks; i++ {
			newChunks[i] = &locationChunk{}
		}
		s.chunks.Store(&newChunks)
	}
	// We don't verify s.size here, assuming caller will fill them
	// Actually, just allocating capacity doesn't change Len() semantically in a slice,
	// but here we are mixing concepts.
	// For HNSW 'Grow' usually means reserve capacity.
	// We'll leave size as is, but EnsureCapacity is better naming.
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
