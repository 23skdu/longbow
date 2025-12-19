package store

import (
"sort"
"sync"
"sync/atomic"
"time"
"unsafe"

"github.com/apache/arrow-go/v18/arrow"

	"github.com/23skdu/longbow/internal/metrics"
)

// ============================================================================
// Per-Record Eviction System - Zero-Copy Design
// ============================================================================
//
// Design principles:
// 1. Pointer-Identity Keys: Use unsafe.Pointer from Arrow records as cache keys
//    (no data copying, O(1) lookup via sync.Map)
// 2. Atomic Operations: Use atomic.Int64 for access counts/times (zero-lock reads)
// 3. Metadata Separation: Store eviction metadata separately from record data
// 4. Lazy Eviction: Check TTL on access or periodic sweep, not background scanning
//
// ============================================================================

// RecordMetadata tracks per-record eviction information using atomics for zero-lock reads
type RecordMetadata struct {
CreatedAt   int64         // Unix nanos - immutable after creation
LastAccess  atomic.Int64  // Unix nanos - updated atomically on access
AccessCount atomic.Int64  // Frequency counter - incremented atomically
TTL         time.Duration // 0 = no expiration
}

// NewRecordMetadata creates metadata for a record with optional TTL
func NewRecordMetadata(ttl time.Duration) *RecordMetadata {
now := time.Now().UnixNano()
m := &RecordMetadata{
CreatedAt: now,
TTL:       ttl,
}
m.LastAccess.Store(now)
m.AccessCount.Store(0)
return m
}

// IsExpired returns true if TTL has elapsed since creation
func (m *RecordMetadata) IsExpired() bool {
if m.TTL <= 0 {
return false // No expiration
}
elapsed := time.Since(time.Unix(0, m.CreatedAt))
return elapsed > m.TTL
}

// RecordAccess updates last access time and increments access count atomically
func (m *RecordMetadata) RecordAccess() {
m.LastAccess.Store(time.Now().UnixNano())
m.AccessCount.Add(1)
}

// GetLastAccess returns the last access time
func (m *RecordMetadata) GetLastAccess() time.Time {
return time.Unix(0, m.LastAccess.Load())
}

// GetAccessCount returns the total access count
func (m *RecordMetadata) GetAccessCount() int64 {
return m.AccessCount.Load()
}

// ============================================================================
// RecordEvictionManager - manages per-record eviction with pointer-identity keys
// ============================================================================

// RecordEvictionManager tracks eviction metadata for individual records
// Uses unsafe.Pointer as keys for zero-copy pointer-identity lookups
type RecordEvictionManager struct {
metadata      sync.Map      // uintptr -> *RecordMetadata
evictionCount atomic.Int64  // Total evicted records counter
mu            sync.RWMutex  // Protects bulk operations like SelectVictims
}

// NewRecordEvictionManager creates a new per-record eviction manager
func NewRecordEvictionManager() *RecordEvictionManager {
return &RecordEvictionManager{}
}

// evictionRecordKey extracts pointer identity from an Arrow record (zero-copy)
// Uses interface data pointer pattern for stable identity
func evictionRecordKey(rec arrow.Record) uintptr { //nolint:staticcheck
if rec == nil {
return 0
}
// Extract data pointer from interface for stable identity
type iface struct {
typ  uintptr
data uintptr
}
return (*iface)(unsafe.Pointer(&rec)).data
}

// Register adds a record to the eviction manager with optional TTL
func (m *RecordEvictionManager) Register(rec arrow.Record, ttl time.Duration) { //nolint:staticcheck
key := evictionRecordKey(rec)
meta := NewRecordMetadata(ttl)
m.metadata.Store(key, meta)
}

// Get retrieves metadata for a record by pointer identity
func (m *RecordEvictionManager) Get(rec arrow.Record) *RecordMetadata { //nolint:staticcheck
key := evictionRecordKey(rec)
if val, ok := m.metadata.Load(key); ok {
return val.(*RecordMetadata)
}
return nil
}

// Unregister removes a record from the eviction manager
func (m *RecordEvictionManager) Unregister(rec arrow.Record) { //nolint:staticcheck
key := evictionRecordKey(rec)
m.metadata.Delete(key)
}

// EvictExpired removes all expired records and returns their pointers
func (m *RecordEvictionManager) EvictExpired() []uintptr {
var expired []uintptr

m.metadata.Range(func(key, value any) bool {
meta := value.(*RecordMetadata)
if meta.IsExpired() {
expired = append(expired, key.(uintptr))
}
return true
})

// Remove expired entries
for _, ptr := range expired {
m.metadata.Delete(ptr)
m.evictionCount.Add(1)
}

return expired
}

// recordWithMeta pairs a pointer with its metadata for sorting
type recordWithMeta struct {
ptr  uintptr
meta *RecordMetadata
}

// SelectLRUVictims returns pointers to the N least recently accessed records
func (m *RecordEvictionManager) SelectLRUVictims(count int) []uintptr {
m.mu.RLock()
defer m.mu.RUnlock()

var records []recordWithMeta
m.metadata.Range(func(key, value any) bool {
records = append(records, recordWithMeta{
ptr:  key.(uintptr),
meta: value.(*RecordMetadata),
})
return true
})

// Sort by last access time (oldest first)
sort.Slice(records, func(i, j int) bool {
return records[i].meta.LastAccess.Load() < records[j].meta.LastAccess.Load()
})

// Return up to count victims
if count > len(records) {
count = len(records)
}

victims := make([]uintptr, count)
for i := 0; i < count; i++ {
victims[i] = records[i].ptr
}

return victims
}

// SelectLFUVictims returns pointers to the N least frequently accessed records
func (m *RecordEvictionManager) SelectLFUVictims(count int) []uintptr {
m.mu.RLock()
defer m.mu.RUnlock()

var records []recordWithMeta
m.metadata.Range(func(key, value any) bool {
records = append(records, recordWithMeta{
ptr:  key.(uintptr),
meta: value.(*RecordMetadata),
})
return true
})

// Sort by access count (lowest first)
sort.Slice(records, func(i, j int) bool {
return records[i].meta.AccessCount.Load() < records[j].meta.AccessCount.Load()
})

// Return up to count victims
if count > len(records) {
count = len(records)
}

victims := make([]uintptr, count)
for i := 0; i < count; i++ {
victims[i] = records[i].ptr
}

return victims
}

// GetEvictionCount returns total number of records evicted
func (m *RecordEvictionManager) GetEvictionCount() int64 {
return m.evictionCount.Load()
}

// Count returns the number of tracked records
func (m *RecordEvictionManager) Count() int {
count := 0
m.metadata.Range(func(_, _ any) bool {
count++
return true
})
return count
}


// ============================================================================
// Dataset Integration for Per-Record Eviction
// ============================================================================

// EvictExpiredRecords removes TTL-expired records from the dataset
// Returns the number of records evicted
//nolint:staticcheck // arrow.Record usage for consistency
func (d *Dataset) EvictExpiredRecords() []arrow.Record {
if d.recordEviction == nil {
return nil
}

d.mu.Lock()
defer d.mu.Unlock()

// Get expired record pointers
expiredPtrs := d.recordEviction.EvictExpired()
if len(expiredPtrs) == 0 {
return nil
}

// Build set of expired pointers for O(1) lookup
expiredSet := make(map[uintptr]bool, len(expiredPtrs))
for _, ptr := range expiredPtrs {
expiredSet[ptr] = true
}

// Filter out expired records (zero-copy - just updating slice)
var evicted []arrow.Record //nolint:staticcheck
var remaining []arrow.RecordBatch

for _, rec := range d.Records {
ptr := evictionRecordKey(rec)
if expiredSet[ptr] {
evicted = append(evicted, rec)
} else {
remaining = append(remaining, rec)
}
}

d.Records = remaining
return evicted
}

// InitRecordEviction initializes the per-record eviction manager for a dataset
func (d *Dataset) InitRecordEviction() {
if d.recordEviction == nil {
d.recordEviction = NewRecordEvictionManager()
}
}

// RegisterRecordWithTTL registers a record for per-record TTL eviction
func (d *Dataset) RegisterRecordWithTTL(rec arrow.Record, ttl time.Duration) { //nolint:staticcheck
if d.recordEviction == nil {
d.recordEviction = NewRecordEvictionManager()
}
d.recordEviction.Register(rec, ttl)
}

// GetRecordMetadata retrieves eviction metadata for a record
func (d *Dataset) GetRecordMetadata(rec arrow.Record) *RecordMetadata { //nolint:staticcheck
if d.recordEviction == nil {
return nil
}
return d.recordEviction.Get(rec)
}

// ============================================================================
// Prometheus Metrics Integration
// ============================================================================

// IncrementRecordAccess should be called when a record is accessed (for metrics)
func IncrementRecordAccess() {
metrics.RecordAccessTotal.Inc()
}

// UpdateRecordMetadataGauge updates the gauge with current tracked record count
func (m *RecordEvictionManager) UpdateRecordMetadataGauge() {
metrics.RecordMetadataEntries.Set(float64(m.Count()))
}
