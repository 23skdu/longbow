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

// Push adds a types.Candidate to the heap.
func (h *MinCandidateHeapAdapter) Push(x any) { *h = append(*h, x.(types.Candidate)) }

// Pop removes and returns the smallest types.Candidate from the heap.
func (h *MinCandidateHeapAdapter) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// MaxCandidateHeapAdapter makes a []types.Candidate a Max-Heap (furthest on top)
type MaxCandidateHeapAdapter []types.Candidate

func (h MaxCandidateHeapAdapter) Len() int           { return len(h) }
func (h MaxCandidateHeapAdapter) Less(i, j int) bool { return h[i].Dist > h[j].Dist }
func (h MaxCandidateHeapAdapter) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push adds a types.Candidate to the heap.
func (h *MaxCandidateHeapAdapter) Push(x any) { *h = append(*h, x.(types.Candidate)) }

// Pop removes and returns the largest types.Candidate from the heap.
func (h *MaxCandidateHeapAdapter) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
