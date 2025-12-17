package store

import (
"sync"
"sync/atomic"

"github.com/apache/arrow-go/v18/arrow"
)

// DatasetMetadata holds lightweight metadata for ListFlights
// without requiring locks on the main Dataset struct.
type DatasetMetadata struct {
Name       string
Schema     *arrow.Schema
TotalRows  int64
BatchCount int
}

// COWMetadataMap provides a Copy-On-Write map for dataset metadata.
// Reads are lock-free via atomic pointer; writes copy the entire map.
// This eliminates lock contention in ListFlights.
type COWMetadataMap struct {
data atomic.Pointer[map[string]DatasetMetadata]
mu   sync.Mutex // Only for writes
}

// NewCOWMetadataMap creates a new Copy-On-Write metadata map.
func NewCOWMetadataMap() *COWMetadataMap {
c := &COWMetadataMap{}
emptyMap := make(map[string]DatasetMetadata)
c.data.Store(&emptyMap)
return c
}

// Snapshot returns the current map for iteration.
// The returned map is immutable and safe to iterate without locks.
func (c *COWMetadataMap) Snapshot() map[string]DatasetMetadata {
return *c.data.Load()
}

// Get returns metadata for a dataset (lock-free read).
func (c *COWMetadataMap) Get(name string) (DatasetMetadata, bool) {
current := *c.data.Load()
meta, ok := current[name]
return meta, ok
}

// Set adds or updates metadata for a dataset (copy-on-write).
func (c *COWMetadataMap) Set(name string, meta DatasetMetadata) {
c.mu.Lock()
defer c.mu.Unlock()

// Copy current map
current := *c.data.Load()
newMap := make(map[string]DatasetMetadata, len(current)+1)
for k, v := range current {
newMap[k] = v
}
newMap[name] = meta
c.data.Store(&newMap)
}

// Delete removes metadata for a dataset (copy-on-write).
func (c *COWMetadataMap) Delete(name string) {
c.mu.Lock()
defer c.mu.Unlock()

current := *c.data.Load()
if _, exists := current[name]; !exists {
return // Nothing to delete
}

newMap := make(map[string]DatasetMetadata, len(current))
for k, v := range current {
if k != name {
newMap[k] = v
}
}
c.data.Store(&newMap)
}

// IncrementStats atomically increments row and batch counts.
func (c *COWMetadataMap) IncrementStats(name string, rows int64, batches int) {
c.mu.Lock()
defer c.mu.Unlock()

current := *c.data.Load()
meta, exists := current[name]
if !exists {
return // Dataset not found
}

newMap := make(map[string]DatasetMetadata, len(current))
for k, v := range current {
newMap[k] = v
}
meta.TotalRows += rows
meta.BatchCount += batches
newMap[name] = meta
c.data.Store(&newMap)
}

// UpdateFromRecords updates metadata from a slice of record batches.
func (c *COWMetadataMap) UpdateFromRecords(name string, batches []arrow.RecordBatch) {
if len(batches) == 0 {
return
}

var totalRows int64
for _, b := range batches {
totalRows += b.NumRows()
}

meta := DatasetMetadata{
Name:       name,
Schema:     batches[0].Schema(),
TotalRows:  totalRows,
BatchCount: len(batches),
}
c.Set(name, meta)
}

// Len returns the number of datasets in the map.
func (c *COWMetadataMap) Len() int {
return len(*c.data.Load())
}
