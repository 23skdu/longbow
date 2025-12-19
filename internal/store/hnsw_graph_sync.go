package store

import (
"bytes"
"encoding/gob"
"io"
"sync"
"sync/atomic"

"github.com/23skdu/longbow/internal/metrics"
"github.com/23skdu/longbow/internal/simd"
"github.com/coder/hnsw"
)

func init() {
// Register distance function for graph serialization
hnsw.RegisterDistanceFunc("euclidean", simd.EuclideanDistance)
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
FromVersion   uint64
ToVersion     uint64
NewLocations  []Location
StartIndex    int
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

state := SyncState{
Version:   s.version.Load(),
Dims:      s.index.dims,
Locations: make([]Location, len(s.index.locations)),
GraphData: graphBuf.Bytes(),
}
copy(state.Locations, s.index.locations)

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
s.index.locations = make([]Location, len(state.Locations))
copy(s.index.locations, state.Locations)
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

if currentVersion > fromVersion && len(s.index.locations) > 0 {
// Export locations added after initial set
// Assume first half existed at fromVersion, second half is new
midpoint := len(s.index.locations) / 2
delta.StartIndex = midpoint
delta.NewLocations = make([]Location, len(s.index.locations)-midpoint)
copy(delta.NewLocations, s.index.locations[midpoint:])
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
if len(s.index.locations) < targetLen {
newLocs := make([]Location, targetLen)
copy(newLocs, s.index.locations)
s.index.locations = newLocs
}

// Copy delta locations at the correct position
copy(s.index.locations[delta.StartIndex:], delta.NewLocations)
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
