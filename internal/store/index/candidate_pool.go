package index

import (
	"sync"

	types "github.com/23skdu/longbow/internal/store/types"
)

// getCandidateSlice gets a candidate slice from the pool.
func (h *ArrowHNSW) getCandidateSlice(capacity int) []types.Candidate {
	return h.candidatePool.get(capacity)
}

// putCandidateSlice returns a slice obtained from getCandidateSlice to the pool.
func (h *ArrowHNSW) putCandidateSlice(s []types.Candidate) {
	if s == nil {
		return
	}
	h.candidatePool.put(s)
}

// candidateFreeList is a bounded free list of candidate slices.
type candidateFreeList struct {
	mu       sync.Mutex
	free     [][]types.Candidate
	maxItems int
	maxCap   int
}

const (
	// A candidate slice wider than this is one nobody asked for: keeping it
	// would retain memory for the rest of the process to save one allocation.
	candidateFreeListMaxCap = 8192
	// A sub-batch holds one candidate slice per node alive until it has been
	// linked - 4096 of them at layer 0 of a 250k build - so a list shorter
	// than a sub-batch is empty from the moment it starts and can only
	// recycle across sub-batches. This is sized for that: at efConstruction
	// 200 it retains a few MB.
	candidateFreeListMaxItems = 4096
)

func (f *candidateFreeList) get(capacity int) []types.Candidate {
	if capacity <= 0 {
		capacity = 1
	}
	f.mu.Lock()
	n := len(f.free)
	if n == 0 {
		f.mu.Unlock()
		return make([]types.Candidate, 0, capacity)
	}
	s := f.free[n-1]
	f.free[n-1] = nil
	f.free = f.free[:n-1]
	f.mu.Unlock()
	if cap(s) < capacity {
		return make([]types.Candidate, 0, capacity)
	}
	return s[:0]
}

func (f *candidateFreeList) put(s []types.Candidate) {
	if cap(s) > f.maxCap {
		return
	}
	f.mu.Lock()
	if len(f.free) < f.maxItems {
		f.free = append(f.free, s[:0])
	}
	f.mu.Unlock()
}
