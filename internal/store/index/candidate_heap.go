package index

import (
	"github.com/23skdu/longbow/internal/store/types"
)

type MinCandidateHeap []types.Candidate

func (h MinCandidateHeap) Len() int           { return len(h) }
func (h MinCandidateHeap) Less(i, j int) bool { return h[i].Dist < h[j].Dist }
func (h MinCandidateHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push adds a candidate to the heap.
func (h *MinCandidateHeap) Push(x any) { *h = append(*h, x.(types.Candidate)) }

// Pop removes the closest candidate from the heap.
func (h *MinCandidateHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MinCandidateHeapAdapter makes a []types.Candidate a Min-Heap (closest on top)
type MinCandidateHeapAdapter []types.Candidate

func (h MinCandidateHeapAdapter) Len() int           { return len(h) }
func (h MinCandidateHeapAdapter) Less(i, j int) bool { return h[i].Dist < h[j].Dist }
func (h MinCandidateHeapAdapter) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Zero-allocation Push
func (h *MinCandidateHeapAdapter) PushCandidate(c types.Candidate) {
	*h = append(*h, c)
	h.up(len(*h) - 1)
}

// Zero-allocation Pop
func (h *MinCandidateHeapAdapter) PopCandidate() types.Candidate {
	n := len(*h)
	c := (*h)[0]
	(*h)[0] = (*h)[n-1]
	*h = (*h)[:n-1]
	if len(*h) > 0 {
		h.down(0, len(*h))
	}
	return c
}

func (h *MinCandidateHeapAdapter) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *MinCandidateHeapAdapter) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

// MaxCandidateHeapAdapter makes a []types.Candidate a Max-Heap (furthest on top)
type MaxCandidateHeapAdapter []types.Candidate

func (h MaxCandidateHeapAdapter) Len() int           { return len(h) }
func (h MaxCandidateHeapAdapter) Less(i, j int) bool { return h[i].Dist > h[j].Dist }
func (h MaxCandidateHeapAdapter) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Zero-allocation Push
func (h *MaxCandidateHeapAdapter) PushCandidate(c types.Candidate) {
	*h = append(*h, c)
	h.up(len(*h) - 1)
}

// Zero-allocation Pop
func (h *MaxCandidateHeapAdapter) PopCandidate() types.Candidate {
	n := len(*h)
	c := (*h)[0]
	(*h)[0] = (*h)[n-1]
	*h = (*h)[:n-1]
	if len(*h) > 0 {
		h.down(0, len(*h))
	}
	return c
}

func (h *MaxCandidateHeapAdapter) up(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *MaxCandidateHeapAdapter) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}
