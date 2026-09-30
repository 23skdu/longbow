package store

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// VersionedVector represents a specific version of a vector with its metadata and timestamp.
type VersionedVector struct {
	ID        uint64
	Vector    []float32
	Norm      float32 // Pre-calculated norm
	Timestamp int64
	Metadata  []byte
	Version   int
}

// versionedEntity is the writable per-id record: the versions in insertion
// order, plus the timestamp column the sorted search reads.
//
// vers is authoritative for ordering, because insertion order is what breaks
// ties between versions that share a timestamp. ts is a parallel copy of those
// timestamps, kept only while they are non-decreasing. The first out-of-order
// insert clears the column and drops sorted, which forces the search back onto
// the reverse scan: for a group whose timestamps are not ascending, "the last
// version inserted at or before t" is not a monotone function of t and no
// timestamp-sorted array can answer it.
type versionedEntity struct {
	ts     []int64
	vers   []VersionedVector
	sorted bool
}

// timestamps returns the group's timestamp column aligned with vers. The column
// is only meaningful when sorted is set; for an out-of-order group it is the
// insertion-order copy, which nothing searches.
func (e *versionedEntity) timestamps() []int64 {
	if e.sorted {
		return e.ts
	}
	ts := make([]int64, len(e.vers))
	for i := range e.vers {
		ts[i] = e.vers[i].Timestamp
	}
	return ts
}

// active returns the position in vers of the version active at timestamp, or -1
// when the id has no version at or before it.
//
// "Active" is the version with the greatest timestamp <= timestamp, and among
// versions sharing that timestamp the last one in insertion order. For a sorted
// group that is the element below an upper bound; for an out-of-order group it
// is the last element the reverse scan reaches, which is the same rule the
// pre-index implementation used for every group.
func (e *versionedEntity) active(timestamp int64) int {
	if e.sorted {
		i := upperBoundScalar(e.ts, timestamp) - 1
		if i < 0 {
			return -1
		}
		return i
	}
	for i := len(e.vers) - 1; i >= 0; i-- {
		if e.vers[i].Timestamp <= timestamp {
			return i
		}
	}
	return -1
}

// VersionHistory manages the storage and retrieval of multiple versions for each vector ID.
type VersionHistory struct {
	// The published index is read on every batch and written under the build
	// mutex; they lead so the atomics they share are not split by mu.
	index      atomic.Pointer[versionHistoryIndex]
	indexStale atomic.Int64
	indexMiss  atomic.Int64
	versions   atomic.Int64

	mu          sync.RWMutex
	indexBuild  sync.Mutex
	indexDirty  atomic.Bool
	maxVersions int
	retention   time.Duration
	entities    map[uint64]*versionedEntity
}

// VersionHistoryConfig defines the retention settings for version history.
type VersionHistoryConfig struct {
	// MaxVersions is the maximum number of versions to keep per vector ID.
	MaxVersions int
	// RetentionPeriod is the duration for which older versions are kept.
	RetentionPeriod time.Duration
}

// DefaultVersionHistoryConfig returns a default configuration for version history.
func DefaultVersionHistoryConfig() VersionHistoryConfig {
	return VersionHistoryConfig{
		MaxVersions:     10,
		RetentionPeriod: 7 * 24 * time.Hour,
	}
}

// NewVersionHistory creates a new VersionHistory instance with the provided configuration.
func NewVersionHistory(cfg VersionHistoryConfig) *VersionHistory {
	return &VersionHistory{
		maxVersions: cfg.MaxVersions,
		retention:   cfg.RetentionPeriod,
		entities:    make(map[uint64]*versionedEntity),
	}
}

// Add inserts a new version for a vector ID, pruning old versions if necessary.
func (vh *VersionHistory) Add(id uint64, vector []float32, norm float32, timestamp int64, metadata []byte) {
	vh.mu.Lock()
	defer vh.mu.Unlock()

	vh.markIndexDirty()

	ent, ok := vh.entities[id]
	if !ok {
		ent = &versionedEntity{sorted: true}
		vh.entities[id] = ent
	}

	newVersion := 1
	if len(ent.vers) > 0 {
		newVersion = ent.vers[len(ent.vers)-1].Version + 1
	}

	ent.vers = append(ent.vers, VersionedVector{
		ID:        id,
		Vector:    vector,
		Norm:      norm,
		Timestamp: timestamp,
		Metadata:  metadata,
		Version:   newVersion,
	})
	vh.versions.Add(1)

	if ent.sorted {
		if len(ent.vers) > 1 && timestamp < ent.vers[len(ent.vers)-2].Timestamp {
			ent.sorted = false
			ent.ts = nil
		} else {
			ent.ts = append(ent.ts, timestamp)
		}
	}

	if len(ent.vers) > vh.maxVersions {
		drop := len(ent.vers) - vh.maxVersions
		ent.vers = ent.vers[drop:]
		if ent.sorted {
			ent.ts = ent.ts[drop:]
		}
		vh.versions.Add(-int64(drop))
	}
}

// GetVersion retrieves a specific version of a vector by its ID and version number.
func (vh *VersionHistory) GetVersion(id uint64, version int) (*VersionedVector, error) {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	ent, ok := vh.entities[id]
	if !ok || len(ent.vers) == 0 {
		return nil, fmt.Errorf("vector %d not found", id)
	}

	for i := len(ent.vers) - 1; i >= 0; i-- {
		if ent.vers[i].Version == version {
			return &ent.vers[i], nil
		}
	}

	return nil, fmt.Errorf("version %d not found for vector %d", version, id)
}

// GetVersionAt retrieves the version of a vector that was active at a specific timestamp.
func (vh *VersionHistory) GetVersionAt(id uint64, timestamp int64) (*VersionedVector, error) {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	ent, ok := vh.entities[id]
	if !ok || len(ent.vers) == 0 {
		return nil, fmt.Errorf("vector %d not found", id)
	}

	if i := ent.active(timestamp); i >= 0 {
		return &ent.vers[i], nil
	}

	return nil, fmt.Errorf("no version found at or before timestamp %d for vector %d", timestamp, id)
}

// GetVersionsAtBatch retrieves active versions for multiple IDs at a specific timestamp.
func (vh *VersionHistory) GetVersionsAtBatch(ids []uint64, timestamp int64, out map[uint64]VersionedVector) {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	idx := vh.versionIndex()
	if idx == nil {
		vh.batchFromEntities(ids, timestamp, out)
		return
	}

	for _, id := range ids {
		slot, ok := idx.slotOf(id)
		if !ok {
			continue
		}
		pos, ok := idx.activeSlot(slot, timestamp)
		if !ok {
			vh.batchOne(id, timestamp, out)
			continue
		}
		if pos >= 0 {
			out[id] = idx.vers[pos]
		}
	}
}

// batchFromEntities answers a batch from the writable entities, which is the
// path taken while the published index is stale and the miss budget is not yet
// spent.
func (vh *VersionHistory) batchFromEntities(ids []uint64, timestamp int64, out map[uint64]VersionedVector) {
	for _, id := range ids {
		ent, ok := vh.entities[id]
		if !ok {
			continue
		}
		if i := ent.active(timestamp); i >= 0 {
			out[id] = ent.vers[i]
		}
	}
}

// batchOne answers a single id from the writable entities, which the index path
// falls back to for the groups whose timestamps were not inserted in order.
func (vh *VersionHistory) batchOne(id uint64, timestamp int64, out map[uint64]VersionedVector) {
	ent, ok := vh.entities[id]
	if !ok {
		return
	}
	if i := ent.active(timestamp); i >= 0 {
		out[id] = ent.vers[i]
	}
}

// GetHistory returns the entire version history for a specific vector ID.
func (vh *VersionHistory) GetHistory(id uint64) []VersionedVector {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	ent, ok := vh.entities[id]
	if !ok {
		return nil
	}

	result := make([]VersionedVector, len(ent.vers))
	copy(result, ent.vers)
	return result
}

// GetLatestVersion retrieves the most recent version for a vector ID.
func (vh *VersionHistory) GetLatestVersion(id uint64) (*VersionedVector, error) {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	ent, ok := vh.entities[id]
	if !ok || len(ent.vers) == 0 {
		return nil, fmt.Errorf("vector %d not found", id)
	}

	return &ent.vers[len(ent.vers)-1], nil
}

// Prune removes all versions with timestamps before the specified value.
func (vh *VersionHistory) Prune(ctx context.Context, beforeTimestamp int64) int {
	vh.mu.Lock()
	defer vh.mu.Unlock()

	vh.markIndexDirty()

	pruned := 0
	for id, ent := range vh.entities {
		kept := make([]VersionedVector, 0, len(ent.vers))
		for _, v := range ent.vers {
			if v.Timestamp > beforeTimestamp {
				kept = append(kept, v)
			} else {
				pruned++
			}
		}
		if len(kept) == 0 {
			delete(vh.entities, id)
			continue
		}
		ent.vers = kept
		if ent.sorted {
			ts := make([]int64, 0, len(kept))
			for i := range kept {
				ts = append(ts, kept[i].Timestamp)
			}
			ent.ts = ts
		}
	}
	vh.versions.Add(-int64(pruned))

	return pruned
}

// Size returns the number of unique vector IDs tracked in the version history.
func (vh *VersionHistory) Size() int {
	vh.mu.RLock()
	defer vh.mu.RUnlock()
	return len(vh.entities)
}

// TotalVersions returns the total number of versions across all vector IDs.
func (vh *VersionHistory) TotalVersions() int {
	return int(vh.versions.Load())
}

// versionHistoryIndex is an immutable, column-oriented snapshot of a
// VersionHistory, laid out like temporalColumnarIndex:
//
//	entOff   entOff[slot]..entOff[slot+1] index ts/vers (uint32 prefix sum)
//	ts       per-version timestamps, aligned with vers, ascending per group
//	vers     per-version records
//	slot     id -> entity slot, for ids outside the dense column
//	dense    id - denseBase -> entity slot, for ids inside it
//	ordered  one bit per slot, set when the group's ts column is ascending
//
// Grouping every version into two flat columns turns a lookup into one id
// resolution plus a binary search over a cache-dense int64 column, and the
// 56-byte slice header the map used to hold disappears: the map value is now
// either a slot or a pointer to a small writable record. entOff is a prefix
// sum, so a group is a pair of subtractions and the record is read from one
// place.
//
// A snapshot is published through an atomic pointer and never mutated
// afterwards, so a reader needs no lock for the columns themselves.
type versionHistoryIndex struct {
	entOff    []uint32
	ts        []int64
	vers      []VersionedVector
	slot      map[uint64]uint32
	dense     []uint32
	denseBase uint64
	ordered   []uint64
}

const (
	// versionIndexRebuildMin is the smallest number of writes that justifies a
	// snapshot rebuild.
	versionIndexRebuildMin = 64
	// versionIndexRebuildFactor spreads the rebuild cost over a 1/8 growth
	// window of the history.
	versionIndexRebuildFactor = 8
	// versionIndexMissBudget caps how many batches fall back to the per-entity
	// scan after a write before the next one rebuilds regardless of staleness.
	versionIndexMissBudget = 64
	// versionIndexDenseFactor is the largest id span, per tracked id, that the
	// dense column is allowed to cover.
	versionIndexDenseFactor = 16
	// versionIndexDenseMaxSpan caps the dense column at 64 MiB of slots.
	versionIndexDenseMaxSpan = 1 << 24
)

// versionSlotAbsent marks an id the dense column does not track.
const versionSlotAbsent = math.MaxUint32

// upperBoundScalar returns the index of the first element of the ascending
// slice ts that is > x, or len(ts) when every element is <= x.
//
// It is lowerBoundScalar with the comparison flipped, so it keeps the same
// schedule: the candidate answer stays in the window [base, base+cnt] and each
// level costs exactly one comparison and one dependent load.
func upperBoundScalar(ts []int64, x int64) int {
	cnt := len(ts)
	if cnt == 0 {
		return 0
	}
	if ts[0] > x {
		return 0
	}
	base := 0
	for cnt > 1 {
		half := cnt >> 1
		if ts[base+half-1] <= x {
			base += half
			cnt -= half
		} else {
			cnt = half
		}
	}
	if ts[base] <= x {
		return base + 1
	}
	return base
}

// markIndexDirty invalidates the published index. It must run before the
// entities are mutated, and it is only ever called with the write lock held.
func (vh *VersionHistory) markIndexDirty() {
	vh.indexDirty.Store(true)
	vh.indexStale.Add(1)
}

// versionIndex returns a snapshot that covers every insert performed so far, or
// nil when the caller must fall back to the per-entity scan. A nil result is
// always correct to treat as "no snapshot"; a stale snapshot never is, which is
// why inserts flip indexDirty before touching an entity and why the rebuild
// below runs under the read lock the caller already holds.
//
// The caller must hold the read lock, so no writer can mutate an entity while
// the columns are built. Taking the write lock instead would stall every other
// reader of the history for the length of an O(versions) build.
func (vh *VersionHistory) versionIndex() *versionHistoryIndex {
	if !vh.indexDirty.Load() {
		return vh.index.Load()
	}

	vh.indexBuild.Lock()
	defer vh.indexBuild.Unlock()

	if !vh.indexDirty.Load() {
		return vh.index.Load()
	}

	threshold := int64(versionIndexRebuildMin)
	if n := vh.versions.Load(); n > threshold*versionIndexRebuildFactor {
		threshold = n / versionIndexRebuildFactor
	}
	if vh.indexStale.Load() < threshold && vh.indexMiss.Load() < versionIndexMissBudget {
		vh.indexMiss.Add(1)
		return nil
	}

	idx := buildVersionHistoryIndex(vh)
	vh.index.Store(idx)
	vh.indexStale.Store(0)
	vh.indexMiss.Store(0)
	vh.indexDirty.Store(false)
	return idx
}

// rebuildVersionIndex forces a fresh snapshot and publishes it. It exists for
// tests and benchmarks that need the indexed path without first paying the
// miss budget.
func (vh *VersionHistory) rebuildVersionIndex() *versionHistoryIndex {
	vh.mu.RLock()
	defer vh.mu.RUnlock()

	vh.indexBuild.Lock()
	defer vh.indexBuild.Unlock()

	idx := buildVersionHistoryIndex(vh)
	vh.index.Store(idx)
	vh.indexStale.Store(0)
	vh.indexMiss.Store(0)
	vh.indexDirty.Store(false)
	return idx
}

// buildVersionHistoryIndex flattens the entities into columns, sorted by id so
// that a batch of ascending ids walks the columns forwards. The caller must
// hold a lock that excludes writers.
func buildVersionHistoryIndex(vh *VersionHistory) *versionHistoryIndex {
	ids := make([]uint64, 0, len(vh.entities))
	versions := 0
	for id, ent := range vh.entities {
		if len(ent.vers) == 0 {
			continue
		}
		ids = append(ids, id)
		versions += len(ent.vers)
	}
	slices.Sort(ids)

	idx := &versionHistoryIndex{
		entOff:  make([]uint32, 1, len(ids)+1),
		ts:      make([]int64, 0, versions),
		vers:    make([]VersionedVector, 0, versions),
		slot:    make(map[uint64]uint32, len(ids)),
		ordered: make([]uint64, (len(ids)+63)/64),
	}

	for i, id := range ids {
		ent := vh.entities[id]
		idx.ts = append(idx.ts, ent.timestamps()...)
		idx.vers = append(idx.vers, ent.vers...)
		idx.entOff = append(idx.entOff, uint32(len(idx.vers))) // #nosec G115
		idx.slot[id] = uint32(i)                               // #nosec G115
		if ent.sorted {
			idx.ordered[i/64] |= 1 << (i % 64)
		}
	}

	idx.buildDense(ids)
	return idx
}

// buildDense adds the contiguous id -> slot column when the tracked ids are
// dense enough to make it cheaper than the map. Vector ids are handed out
// sequentially, so a live store almost always takes this path: the batch then
// resolves an id with one bounds check and one load from a column the hardware
// prefetcher walks, with no hashing at all.
func (idx *versionHistoryIndex) buildDense(ids []uint64) {
	if len(ids) == 0 {
		return
	}
	base := ids[0]
	span := ids[len(ids)-1] - base + 1
	if span > versionIndexDenseMaxSpan || span > versionIndexDenseFactor*uint64(len(ids))+64 {
		return
	}

	dense := make([]uint32, span)
	for i := range dense {
		dense[i] = versionSlotAbsent
	}
	for slot, id := range ids {
		dense[id-base] = uint32(slot) // #nosec G115
	}
	idx.dense = dense
	idx.denseBase = base
}

// slotOf returns the entity slot for id, or false when the id is not tracked.
func (idx *versionHistoryIndex) slotOf(id uint64) (uint32, bool) {
	if dense := idx.dense; dense != nil {
		if d := id - idx.denseBase; d < uint64(len(dense)) {
			slot := dense[d]
			return slot, slot != versionSlotAbsent
		}
	}
	slot, ok := idx.slot[id]
	return slot, ok
}

// activeSlot returns the position in vers of the version active at timestamp,
// or -1 when the group has no version at or before it. ok is false when the
// group's timestamps were not inserted in order, which means the caller has to
// answer from the writable entity instead.
func (idx *versionHistoryIndex) activeSlot(slot uint32, timestamp int64) (pos int, ok bool) {
	if idx.ordered[slot/64]&(1<<(slot%64)) == 0 {
		return 0, false
	}
	lo := int(idx.entOff[slot])
	i := upperBoundScalar(idx.ts[lo:int(idx.entOff[slot+1])], timestamp) - 1
	if i < 0 {
		return -1, true
	}
	return lo + i, true
}
