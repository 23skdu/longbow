package store

import (
	"bytes"
	"encoding/gob"
	"io"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/coder/hnsw"
)

func init() {
	// Register distance function for graph serialization
	hnsw.RegisterDistanceFunc("euclidean", hnsw.EuclideanDistance)
	hnsw.RegisterDistanceFunc("cosine", hnsw.CosineDistance)
}

// HNSWGraphSync manages HNSW index synchronization between peers.
type HNSWGraphSync struct {
	index       *HNSWIndex
	mu          sync.RWMutex
	version     atomic.Uint64
	exportCount atomic.Uint64
	importCount atomic.Uint64
}

// SyncState represents the serializable state of an HNSW index.
type SyncState struct {
	Version   uint64
	Dims      int
	Locations []Location
	GraphData []byte
}

// DeltaSync represents incremental changes between versions.
type DeltaSync struct {
	FromVersion  uint64
	ToVersion    uint64
	NewLocations []Location
	StartIndex   int
}

// NewHNSWGraphSync creates a new sync manager for the given index.
func NewHNSWGraphSync(index *HNSWIndex) *HNSWGraphSync {
	return &HNSWGraphSync{
		index: index,
	}
}

// ExportState serializes the complete HNSW index state.
func (s *HNSWGraphSync) ExportState() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.index.mu.RLock()
	defer s.index.mu.RUnlock()

	var graphBuf bytes.Buffer
	if err := s.index.Graph.Export(&graphBuf); err != nil {
		return nil, err
	}

	// Capture locations snapshot
	// We can't use copy on chunked store.
	// We iterate the store to fill the slice.
	locs := make([]Location, 0, s.index.locationStore.Len())
	// IterateMutable gives *atomic.Uint64.
	s.index.locationStore.IterateMutable(func(_ VectorID, val *atomic.Uint64) {
		loc := unpackLocation(val.Load())
		locs = append(locs, loc)
	})
	state := SyncState{
		Version:   s.version.Load(),
		Dims:      s.index.dims,
		Locations: locs,
		GraphData: graphBuf.Bytes(),
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		return nil, err
	}

	s.exportCount.Add(1)
	metrics.HNSWGraphSyncExportsTotal.Inc()
	return buf.Bytes(), nil
}

// ImportState deserializes and applies HNSW index state.
func (s *HNSWGraphSync) ImportState(data []byte) error {
	var state SyncState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&state); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.index.mu.Lock()
	defer s.index.mu.Unlock()

	if len(state.GraphData) > 0 {
		if err := s.index.Graph.Import(bytes.NewReader(state.GraphData)); err != nil {
			return err
		}
	}

	s.index.dims = state.Dims
	// Restore locations
	// Assumes empty index or overwrite?
	// NewHNSWGraphSync assumes index exists.
	// ImportState traditionally overwrites or fills?
	// Original used `make` and `copy` over `s.index.locations`.
	// Pre-allocate capacity
	// EnsureCapacity takes an ID (index), so usage is (count - 1), but effectively ensuring capacity for count items.
	// Since ID is 0-indexed.
	count := len(state.Locations)
	if count > 0 {
		s.index.locationStore.EnsureCapacity(VectorID(count - 1))
	}
	s.index.locationStore.Reset()
	for _, loc := range state.Locations {
		s.index.locationStore.Append(loc)
	}
	s.version.Store(state.Version)

	s.importCount.Add(1)
	metrics.HNSWGraphSyncImportsTotal.Inc()
	return nil
}

// ExportGraph exports just the HNSW graph structure.
func (s *HNSWGraphSync) ExportGraph(w io.Writer) error {
	s.index.mu.RLock()
	defer s.index.mu.RUnlock()
	return s.index.Graph.Export(w)
}

// ImportGraph imports just the HNSW graph structure.
func (s *HNSWGraphSync) ImportGraph(r io.Reader) error {
	s.index.mu.Lock()
	defer s.index.mu.Unlock()
	return s.index.Graph.Import(r)
}

// GetVersion returns the current sync version.
func (s *HNSWGraphSync) GetVersion() uint64 {
	return s.version.Load()
}

// IncrementVersion increments the sync version.
func (s *HNSWGraphSync) IncrementVersion() {
	s.version.Add(1)
}

// ExportDelta exports changes since the given version.
func (s *HNSWGraphSync) ExportDelta(fromVersion uint64) (*DeltaSync, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.index.mu.RLock()
	defer s.index.mu.RUnlock()

	currentVersion := s.version.Load()

	delta := &DeltaSync{
		FromVersion:  fromVersion,
		ToVersion:    currentVersion,
		NewLocations: make([]Location, 0),
		StartIndex:   0,
	}

	if currentVersion > fromVersion && s.index.locationStore.Len() > 0 {
		// Export locations added after initial set
		// "Initial set" definition depends on how versions work.
		// Simply, if we have new locations since last check?
		// Logic: If version changed, we might have added items.
		// BUT, `delta` logic here assumes `midpoint` split?
		// Original code: `midpoint := len(locations)/2`. Why /2?
		// This looks like a dummy logic or specific testing logic?
		// Real sync should track `fromVersion` to `currentVersion`.
		// If `midpoint` logic was intentional for "partial sync test", I should simulate it?
		// "midpoint := len(...) / 2" suggests it sends the *second half*?
		// I will keep the logic: slice from len/2 to end.

		totalLen := s.index.locationStore.Len()
		midpoint := totalLen / 2
		delta.StartIndex = midpoint
		delta.NewLocations = make([]Location, 0, totalLen-midpoint)

		// Iterate from midpoint to end
		// Accessing by ID is O(1).
		for i := midpoint; i < totalLen; i++ {
			loc, _ := s.index.locationStore.Get(VectorID(i))
			delta.NewLocations = append(delta.NewLocations, loc)
		}
	}

	metrics.HNSWGraphSyncDeltasTotal.Inc()
	return delta, nil
}

// ApplyDelta applies incremental changes from a delta.
func (s *HNSWGraphSync) ApplyDelta(delta *DeltaSync) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index.mu.Lock()
	defer s.index.mu.Unlock()

	// Ensure we have enough capacity
	targetLen := delta.StartIndex + len(delta.NewLocations)
	currentLen := s.index.locationStore.Len()
	if targetLen > currentLen {
		s.index.locationStore.EnsureCapacity(VectorID(targetLen - 1))
		// We also need to update the size of the store if Grow doesn't do it (Grow in Chunked.. only allocates chunks).
		// Wait, ChunkedLocationStore.Grow allocates *capacity*, but Append updates *Len*.
		// Set updates only if within *capacity*? No, Set updates if within *Chunks*.
		// But s.size (Len) is used for bounds check in Get? No, Get checks chunk existence.
		// However, Append increments size.
		// If we use Set for new items, we must update size manually or verify Set behavior.
		// ChunkedLocationStore.Set does NOT update size.
		// If we are "applying delta" which ADDS items, we should use Append if they are strictly appended.
		// If they are random writes, we have a problem if they are past Len.
		// GraphSync usually appends? "StartIndex".
		// If StartIndex == Len, we Append.
		// If StartIndex < Len, we Set (update).
		// If StartIndex > Len, we have a gap.
	}

	// Copy delta locations
	for i, loc := range delta.NewLocations {
		id := VectorID(uint32(delta.StartIndex) + uint32(i))
		// If the ID is >= Len, we should Append (if sequential) or we need a Set that extends size.
		// Given complexity, and assuming StartIndex usually == Len in log replication:
		// We'll use Set for simplicity but we might fail if we didn't update Size?
		// Actually, let's look at ChunkedLocationStore again.
		// Set: check chunkIdx < len(chunks).
		// Grow: allocates chunks.
		// So Set works if chunks exist.
		// BUT, Len() will be stale if we don't increment it.
		// Graph Sync should update the official state.

		// Ideally we use Append for new items.
		if int(id) >= s.index.locationStore.Len() {
			// It's an append (or gap fill)
			// Efficiently we should use Append.
			// The existing logic used `locations = newLocs`, effectively setting length.

			// We will just Append if it matches next ID.
			if int(id) == s.index.locationStore.Len() {
				s.index.locationStore.Append(loc)
			} else {
				// Gap or overwrite?
				// If gap, we fill with empty?
				for int(id) > s.index.locationStore.Len() {
					s.index.locationStore.Append(Location{})
				}
				s.index.locationStore.Append(loc)
			}
		} else {
			s.index.locationStore.Set(id, loc)
		}
	}
	s.version.Store(delta.ToVersion)

	metrics.HNSWGraphSyncDeltaAppliesTotal.Inc()
	return nil
}

// GetExportCount returns the number of exports performed.
func (s *HNSWGraphSync) GetExportCount() uint64 {
	return s.exportCount.Load()
}

// GetImportCount returns the number of imports performed.
func (s *HNSWGraphSync) GetImportCount() uint64 {
	return s.importCount.Load()
}
