package store

import (
	"github.com/23skdu/longbow/internal/metrics"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/memory"
)

type TemporalEntry struct {
	ID   uint64
	Norm float32
}

// TemporalNode represents a set of vector IDs sharing a specific timestamp.
type TemporalNode struct {
	Timestamp int64
	Offset    uint32 // Offset into entryArena
	Len       uint32 // Number of entries for this timestamp
	Cap       uint32 // Capacity of the allocated slice in entryArena
}

type temporalLeaf struct {
	Nodes [nodesPerChunk]TemporalNode
	Len   uint32
}

type temporalLeafRef struct {
	MinTs int64
	MaxTs int64
	Ref   memory.SliceRef
}

// TemporalTree is a memory-efficient index structure for geographic or temporal vectors.
type TemporalTree struct {
	mu         sync.RWMutex
	leafArena  *memory.TypedArena[temporalLeaf]
	entryArena *memory.TypedArena[TemporalEntry]
	leafRefs   []temporalLeafRef
	minTs      int64
	maxTs      int64
	nodeCount  atomic.Uint32

	// columnar holds the immutable struct-of-arrays snapshot that backs the
	// range/predicate queries. columnarDirty is set before any arena mutation
	// so a snapshot is never served while it is missing an insert; see
	// temporal_columnar.go.
	columnar       atomic.Pointer[temporalColumnarIndex]
	columnarDirty  atomic.Bool
	columnarStale  atomic.Int64
	columnarMisses atomic.Int64
	columnarBuild  sync.Mutex
	columnarOff    atomic.Bool
}

// NewTemporalTree creates a new TemporalTree instance with an optional arena.
func NewTemporalTree(arena *memory.SlabArena) *TemporalTree {
	if arena == nil {
		arena = memory.NewSlabArena(16 * 1024 * 1024) // 16MB default
	}
	return &TemporalTree{
		leafArena:  memory.NewTypedArena[temporalLeaf](arena),
		entryArena: memory.NewTypedArena[TemporalEntry](arena),
		leafRefs:   make([]temporalLeafRef, 0),
		minTs:      math.MaxInt64,
		maxTs:      math.MinInt64,
	}
}

// Release deallocates the arenas used by the temporal tree.
func (tt *TemporalTree) Release() {
	if tt.leafArena != nil {
		tt.leafArena.Release()
	}
	if tt.entryArena != nil {
		tt.entryArena.Release()
	}
}

// Insert adds a vector ID and its norm to the tree at the specified timestamp.
func (tt *TemporalTree) Insert(timestamp int64, id uint64, norm float32) {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	tt.insertEntryNoLock(timestamp, TemporalEntry{ID: id, Norm: norm}, nil)
}

// insertEntryNoLock inserts a single entry assuming the write lock is already held.
// cursor, if non-nil, provides a hint for the leaf index to start searching from
// and is updated to the leaf where the entry was placed.
func (tt *TemporalTree) insertEntryNoLock(timestamp int64, entry TemporalEntry, cursor *int) {
	tt.markColumnarDirty()

	if timestamp < tt.minTs {
		tt.minTs = timestamp
	}
	if timestamp > tt.maxTs {
		tt.maxTs = timestamp
	}

	// Start search from cursor hint when entries are processed in sorted order
	lo := 0
	if cursor != nil && *cursor >= 0 && *cursor < len(tt.leafRefs) && tt.leafRefs[*cursor].MinTs <= timestamp {
		lo = *cursor
	}

	idx := sort.Search(len(tt.leafRefs)-lo, func(i int) bool {
		return tt.leafRefs[lo+i].MaxTs >= timestamp
	})
	idx += lo

	if idx >= len(tt.leafRefs) {
		if len(tt.leafRefs) > 0 && tt.leafRefs[len(tt.leafRefs)-1].MaxTs < timestamp {
			lastLeaf := &tt.leafRefs[len(tt.leafRefs)-1]
			leaf := &tt.leafArena.Get(lastLeaf.Ref)[0]
			if leaf.Len < nodesPerChunk {
				tt.insertInLeaf(leaf, timestamp, entry)
				lastLeaf.MaxTs = leaf.Nodes[leaf.Len-1].Timestamp
				tt.nodeCount.Add(1)
				metrics.TemporalTreeNodesTotal.Set(float64(tt.nodeCount.Load()))
				if cursor != nil {
					*cursor = len(tt.leafRefs) - 1
				}
				return
			}
		}

		ref, _ := tt.leafArena.AllocSlice(1)
		leaf := &tt.leafArena.Get(ref)[0]
		tt.insertInLeaf(leaf, timestamp, entry)
		tt.leafRefs = append(tt.leafRefs, temporalLeafRef{
			MinTs: timestamp,
			MaxTs: timestamp,
			Ref:   ref,
		})
		tt.nodeCount.Add(1)
		metrics.TemporalTreeNodesTotal.Set(float64(tt.nodeCount.Load()))
		if cursor != nil {
			*cursor = len(tt.leafRefs) - 1
		}
		return
	}

	leafRef := &tt.leafRefs[idx]
	leaf := &tt.leafArena.Get(leafRef.Ref)[0]

	nodeIdx := sort.Search(int(leaf.Len), func(i int) bool {
		return leaf.Nodes[i].Timestamp >= timestamp
	})

	if nodeIdx < int(leaf.Len) && leaf.Nodes[nodeIdx].Timestamp == timestamp {
		tt.appendEntryToNode(&leaf.Nodes[nodeIdx], entry)
		if cursor != nil {
			*cursor = idx
		}
		return
	}

	if leaf.Len < nodesPerChunk {
		tt.insertInLeaf(leaf, timestamp, entry)
		leafRef.MinTs = leaf.Nodes[0].Timestamp
		leafRef.MaxTs = leaf.Nodes[leaf.Len-1].Timestamp
		tt.nodeCount.Add(1)
		metrics.TemporalTreeNodesTotal.Set(float64(tt.nodeCount.Load()))
		if cursor != nil {
			*cursor = idx
		}
		return
	}

	tt.splitAndInsert(idx, timestamp, entry)
	tt.nodeCount.Add(1)
	metrics.TemporalTreeNodesTotal.Set(float64(tt.nodeCount.Load()))
	stats := tt.leafArena.Slab().Stats()
	metrics.TemporalTreeAllocatedBytesTotal.Set(float64(stats.TotalCapacity))
	if cursor != nil {
		*cursor = 0
	}
}

func (tt *TemporalTree) appendEntryToNode(node *TemporalNode, entry TemporalEntry) {
	if node.Len >= node.Cap {
		oldRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Cap}
		oldEntries := tt.entryArena.Get(oldRef)

		newCap := node.Cap * 2
		if newCap == 0 {
			newCap = 2
		}

		newRef, _ := tt.entryArena.AllocSlice(int(newCap))
		newEntries := tt.entryArena.Get(newRef)
		copy(newEntries, oldEntries)

		if newRef.Offset > math.MaxUint32 {
			panic(fmt.Sprintf("temporal tree entry offset overflow: %d exceeds MaxUint32", newRef.Offset))
		}
		node.Offset = uint32(newRef.Offset) // #nosec G115
		node.Cap = newCap
	}

	ref := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Cap, Cap: node.Cap}
	entries := tt.entryArena.Get(ref)
	entries[node.Len] = entry
	node.Len++
}

func (tt *TemporalTree) insertInLeaf(leaf *temporalLeaf, timestamp int64, entry TemporalEntry) {
	nodeIdx := sort.Search(int(leaf.Len), func(i int) bool {
		return leaf.Nodes[i].Timestamp >= timestamp
	})

	entryRef, _ := tt.entryArena.AllocSlice(1)
	tt.entryArena.Get(entryRef)[0] = entry

	if entryRef.Offset > math.MaxUint32 {
		panic(fmt.Sprintf("temporal tree entry offset overflow: %d exceeds MaxUint32", entryRef.Offset))
	}
	newNode := TemporalNode{
		Timestamp: timestamp,
		Offset:    uint32(entryRef.Offset), // #nosec G115
		Len:       1,
		Cap:       1,
	}

	copy(leaf.Nodes[nodeIdx+1:leaf.Len+1], leaf.Nodes[nodeIdx:leaf.Len])
	leaf.Nodes[nodeIdx] = newNode
	leaf.Len++
}

func (tt *TemporalTree) splitAndInsert(idx int, timestamp int64, entry TemporalEntry) {
	oldLeafRef := tt.leafRefs[idx]
	oldLeaf := tt.leafArena.Get(oldLeafRef.Ref)[0]

	// Create new leaf
	newRef, _ := tt.leafArena.AllocSlice(1)
	newLeaf := &tt.leafArena.Get(newRef)[0]

	splitIdx := nodesPerChunk / 2
	copy(newLeaf.Nodes[:], oldLeaf.Nodes[splitIdx:])
	newLeaf.Len = uint32(nodesPerChunk - splitIdx)

	// Update old leaf (in place)
	// Since TypedArena returns a slice, we can modify the element directly if it's a pointer or we write it back.
	// In our case tt.leafArena.Get(oldLeafRef.Ref)[0] gives us the value. We need to update it in the arena.
	// Wait! I'll get a pointer instead.

	leafPtr := &tt.leafArena.Get(oldLeafRef.Ref)[0]
	leafPtr.Len = uint32(splitIdx)

	// Create new leaf ref
	newLeafRef := temporalLeafRef{
		MinTs: newLeaf.Nodes[0].Timestamp,
		MaxTs: newLeaf.Nodes[newLeaf.Len-1].Timestamp,
		Ref:   newRef,
	}

	// Insert into refs
	tt.leafRefs = append(tt.leafRefs, temporalLeafRef{})
	copy(tt.leafRefs[idx+2:], tt.leafRefs[idx+1:])
	tt.leafRefs[idx+1] = newLeafRef

	// Update old ref
	tt.leafRefs[idx].MaxTs = leafPtr.Nodes[leafPtr.Len-1].Timestamp

	// Now insert the new node into the correct half
	if timestamp <= tt.leafRefs[idx].MaxTs {
		tt.insertInLeaf(leafPtr, timestamp, entry)
		tt.leafRefs[idx].MaxTs = leafPtr.Nodes[leafPtr.Len-1].Timestamp
	} else {
		tt.insertInLeaf(newLeaf, timestamp, entry)
		tt.leafRefs[idx+1].MaxTs = newLeaf.Nodes[newLeaf.Len-1].Timestamp
		tt.leafRefs[idx+1].MinTs = newLeaf.Nodes[0].Timestamp
	}
}

// InsertBatch adds multiple vector IDs and their norms to the tree.
// It sorts entries by timestamp, locks once, and uses a cursor-based search
// to avoid repeated O(log n) binary searches and lock acquisitions.
func (tt *TemporalTree) InsertBatch(timestamps []int64, ids []uint64, norms []float32) {
	if len(timestamps) == 0 {
		return
	}

	type timedEntry struct {
		ts   int64
		id   uint64
		norm float32
	}

	entries := make([]timedEntry, len(timestamps))
	for i := range timestamps {
		entries[i] = timedEntry{ts: timestamps[i], id: ids[i], norm: norms[i]}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ts < entries[j].ts
	})

	tt.mu.Lock()

	cursor := -1
	for _, e := range entries {
		tt.insertEntryNoLock(e.ts, TemporalEntry{ID: e.id, Norm: e.norm}, &cursor)
	}
	tt.mu.Unlock()

	// A batch is the natural amortization point for a columnar snapshot rebuild,
	// so refresh eagerly instead of leaving the tree on the chunked path until
	// the lazy staleness budget runs out.
	tt.rebuildColumnarIndex()
}

// getRangeChunked is the reference chunk walk served when no fresh columnar
// snapshot is available. GetRange is the public entry point.
func (tt *TemporalTree) getRangeChunked(start, end int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if len(tt.leafRefs) == 0 {
		return nil
	}

	startIdx := sort.Search(len(tt.leafRefs), func(i int) bool {
		return tt.leafRefs[i].MaxTs >= start
	})

	var results []uint64
	for i := startIdx; i < len(tt.leafRefs); i++ {
		leafRef := tt.leafRefs[i]
		if leafRef.MinTs > end {
			break
		}

		leaf := tt.leafArena.Get(leafRef.Ref)[0]
		nodeIdx := 0
		if i == startIdx {
			nodeIdx = sort.Search(int(leaf.Len), func(j int) bool {
				return leaf.Nodes[j].Timestamp >= start
			})
		}

		for j := nodeIdx; j < int(leaf.Len); j++ {
			node := &leaf.Nodes[j]
			if node.Timestamp > end {
				return results
			}

			metrics.TemporalQueryScannedNodesTotal.Add(1)

			entryRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Len}
			entries := tt.entryArena.Get(entryRef)
			for _, e := range entries {
				results = append(results, e.ID)
			}
		}
	}
	return results
}

// getRangeReversedChunked is the reference chunk walk served when no fresh
// columnar snapshot is available.
func (tt *TemporalTree) getRangeReversedChunked(start, end int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if len(tt.leafRefs) == 0 {
		return nil
	}

	// Find the chunk containing 'end'
	idx := sort.Search(len(tt.leafRefs), func(i int) bool {
		return tt.leafRefs[i].MaxTs >= end
	})
	if idx == len(tt.leafRefs) {
		idx--
	}

	var results []uint64
	for i := idx; i >= 0; i-- {
		leafRef := tt.leafRefs[i]
		if leafRef.MaxTs < start {
			break
		}

		leaf := tt.leafArena.Get(leafRef.Ref)[0]

		// Find end in leaf
		nodeIdx := int(leaf.Len) - 1
		if i == idx {
			nodeIdx = sort.Search(int(leaf.Len), func(j int) bool {
				return leaf.Nodes[j].Timestamp > end
			}) - 1
		}

		for j := nodeIdx; j >= 0; j-- {
			node := &leaf.Nodes[j]
			if node.Timestamp < start {
				return results
			}

			metrics.TemporalQueryScannedNodesTotal.Add(1)

			entryRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Len}
			entries := tt.entryArena.Get(entryRef)
			for k := len(entries) - 1; k >= 0; k-- {
				results = append(results, entries[k].ID)
			}
		}
	}
	return results
}

// getUniqueIDsInRangeChunked is the reference chunk walk served when no fresh
// columnar snapshot is available.
func (tt *TemporalTree) getUniqueIDsInRangeChunked(start, end int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if len(tt.leafRefs) == 0 {
		return nil
	}

	idx := sort.Search(len(tt.leafRefs), func(i int) bool {
		return tt.leafRefs[i].MaxTs >= end
	})
	if idx == len(tt.leafRefs) {
		idx--
	}

	uniqueIDs := temporalIDMapPool.Get().(map[uint64]struct{})
	defer func() {
		clear(uniqueIDs)
		temporalIDMapPool.Put(uniqueIDs)
	}()

	var results []uint64
	for i := idx; i >= 0; i-- {
		leafRef := tt.leafRefs[i]
		if leafRef.MaxTs < start {
			break
		}

		leaf := tt.leafArena.Get(leafRef.Ref)[0]
		nodeIdx := int(leaf.Len) - 1
		if i == idx {
			nodeIdx = sort.Search(int(leaf.Len), func(j int) bool {
				return leaf.Nodes[j].Timestamp > end
			}) - 1
		}

		for j := nodeIdx; j >= 0; j-- {
			node := &leaf.Nodes[j]
			if node.Timestamp < start {
				return results
			}

			metrics.TemporalQueryScannedNodesTotal.Add(1)

			entryRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Len}
			entries := tt.entryArena.Get(entryRef)
			for k := len(entries) - 1; k >= 0; k-- {
				id := entries[k].ID
				if _, exists := uniqueIDs[id]; !exists {
					results = append(results, id)
					uniqueIDs[id] = struct{}{}
				}
			}
		}
	}
	return results
}

// GetBefore returns all vector IDs with timestamps before the specified value.
func (tt *TemporalTree) GetBefore(timestamp int64) []uint64 {
	return tt.GetRange(0, timestamp-1)
}

// GetAfter returns all vector IDs with timestamps after the specified value.
func (tt *TemporalTree) GetAfter(timestamp int64) []uint64 {
	return tt.GetRange(timestamp+1, math.MaxInt64)
}

// getEarliestChunked is the reference chunk walk served when no fresh columnar
// snapshot is available.
func (tt *TemporalTree) getEarliestChunked(n int) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if len(tt.leafRefs) == 0 {
		return nil
	}

	var results []uint64
	remaining := n
	for i := 0; i < len(tt.leafRefs) && remaining > 0; i++ {
		leaf := tt.leafArena.Get(tt.leafRefs[i].Ref)[0]
		for j := 0; j < int(leaf.Len) && remaining > 0; j++ {
			node := &leaf.Nodes[j]
			metrics.TemporalQueryScannedNodesTotal.Add(1)

			entryRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Len}
			entries := tt.entryArena.Get(entryRef)
			for _, e := range entries {
				results = append(results, e.ID)
				remaining--
				if remaining <= 0 {
					break
				}
			}
		}
	}
	return results
}

// getLatestChunked is the reference chunk walk served when no fresh columnar
// snapshot is available.
func (tt *TemporalTree) getLatestChunked(n int) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if len(tt.leafRefs) == 0 {
		return nil
	}

	var results []uint64
	remaining := n
	for i := len(tt.leafRefs) - 1; i >= 0 && remaining > 0; i-- {
		leaf := tt.leafArena.Get(tt.leafRefs[i].Ref)[0]
		for j := int(leaf.Len) - 1; j >= 0 && remaining > 0; j-- {
			node := &leaf.Nodes[j]
			metrics.TemporalQueryScannedNodesTotal.Add(1)

			entryRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Len}
			entries := tt.entryArena.Get(entryRef)
			for k := len(entries) - 1; k >= 0; k-- {
				results = append(results, entries[k].ID)
				remaining--
				if remaining <= 0 {
					break
				}
			}
		}
	}
	return results
}

// getUniqueLatestChunked is the reference chunk walk served when no fresh
// columnar snapshot is available.
func (tt *TemporalTree) getUniqueLatestChunked(n int) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if len(tt.leafRefs) == 0 {
		return nil
	}

	uniqueIDs := temporalIDMapPool.Get().(map[uint64]struct{})
	defer func() {
		clear(uniqueIDs)
		temporalIDMapPool.Put(uniqueIDs)
	}()

	var results []uint64
	for i := len(tt.leafRefs) - 1; i >= 0 && len(results) < n; i-- {
		leaf := tt.leafArena.Get(tt.leafRefs[i].Ref)[0]
		for j := int(leaf.Len) - 1; j >= 0 && len(results) < n; j-- {
			node := &leaf.Nodes[j]
			metrics.TemporalQueryScannedNodesTotal.Add(1)

			entryRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Len}
			entries := tt.entryArena.Get(entryRef)
			for k := len(entries) - 1; k >= 0; k-- {
				id := entries[k].ID
				if _, exists := uniqueIDs[id]; !exists {
					results = append(results, id)
					uniqueIDs[id] = struct{}{}
					if len(results) >= n {
						break
					}
				}
			}
		}
	}
	return results
}

// Len returns the number of unique timestamps in the tree.
func (tt *TemporalTree) Len() int {
	return int(tt.nodeCount.Load())
}

// GetBounds returns the min and max timestamps in the tree.
func (tt *TemporalTree) GetBounds() (int64, int64) {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	return tt.minTs, tt.maxTs
}

// GetBounds returns the min and max timestamps in the index.
