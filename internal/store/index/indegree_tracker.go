package index

import (
	"sync"
	"sync/atomic"
)

const (
	inDegreeChunkShift = 12
	inDegreeChunkSize  = 1 << inDegreeChunkShift
	inDegreeChunkMask  = inDegreeChunkSize - 1
)

// inDegreeChunk holds the inbound-edge count for inDegreeChunkSize consecutive
// node ids.
type inDegreeChunk = [inDegreeChunkSize]atomic.Int32

// inDegreeTracker records how many layer-0 edges point at each node, so that
// pruning can refuse to drop a node's last inbound edge (roadmap R26).
//
// Reads are lock-free. The directory is an immutable slice of chunk pointers
// republished through an atomic pointer on growth, so a reader sees either the
// pre-growth or the post-growth directory and never a torn view; only the first
// touch of a chunk takes the mutex, and growth fills every chunk up to the new
// high-water mark so an in-range entry is never nil.
//
// The previous implementation used a sync.Map directory and paid a map load per
// operation. The prune path performs one lookup per dropped candidate per
// contended add, which put the bookkeeping in the same order of magnitude as
// the distance computation it is guarding, and it sits on the graph-build hot
// path where nothing is amortised.
type inDegreeTracker struct {
	mu  sync.Mutex
	dir atomic.Pointer[[]*inDegreeChunk]
}

// chunk returns the chunk holding id, or nil when the id is beyond the
// high-water mark and create is false.
func (t *inDegreeTracker) chunk(id uint32, create bool) *inDegreeChunk {
	idx := int(id >> inDegreeChunkShift)
	if d := t.dir.Load(); d != nil && idx < len(*d) {
		return (*d)[idx]
	}
	if !create {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Re-read under the lock: another writer may have grown past us.
	if d := t.dir.Load(); d != nil && idx < len(*d) {
		return (*d)[idx]
	}
	old := t.dir.Load()
	var grown []*inDegreeChunk
	if old != nil {
		grown = make([]*inDegreeChunk, idx+1)
		copy(grown, *old)
	} else {
		grown = make([]*inDegreeChunk, idx+1)
	}
	for i := range grown {
		if grown[i] == nil {
			grown[i] = new(inDegreeChunk)
		}
	}
	t.dir.Store(&grown)
	return grown[idx]
}

func (t *inDegreeTracker) Inc(id uint32) {
	c := t.chunk(id, true)
	if c == nil {
		return
	}
	c[id&inDegreeChunkMask].Add(1)
}

func (t *inDegreeTracker) Dec(id uint32) {
	c := t.chunk(id, false)
	if c == nil {
		return
	}
	c[id&inDegreeChunkMask].Add(-1)
}

// Get returns the number of layer-0 edges pointing at id. Ids the tracker has
// never seen report 0, which is the conservative answer for the pruning guard:
// an unknown node may have no inbound edge left to protect.
func (t *inDegreeTracker) Get(id uint32) int32 {
	c := t.chunk(id, false)
	if c == nil {
		return 0
	}
	return c[id&inDegreeChunkMask].Load()
}

func (t *inDegreeTracker) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dir.Store(nil)
}
