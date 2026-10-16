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

// MinCandidateHeapAdapter makes a []types.Candidate a Min-Heap (closest on top).
// The flat backing slice is sifted as a 4-ary heap: parent (j-1)/4, children 4*i+1..4*i+4.
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
	s := *h
	n := len(s)
	c := s[0]
	s[0] = s[n-1]
	*h = s[:n-1]
	if n > 1 {
		h.down(0, n-1)
	}
	return c
}

// ReplaceCandidate replaces the root candidate with c and restores the heap property in a single pass.
func (h *MinCandidateHeapAdapter) ReplaceCandidate(c types.Candidate) {
	s := *h
	if len(s) == 0 {
		*h = append(s, c)
		return
	}
	s[0] = c
	h.down(0, len(s))
}

func (h *MinCandidateHeapAdapter) up(j int) {
	s := *h
	if j <= 0 || j >= len(s) {
		return
	}
	target := s[j]
	targetDist := target.Dist
	for j > 0 {
		parent := (j - 1) / 4
		if s[parent].Dist <= targetDist {
			break
		}
		s[j] = s[parent]
		j = parent
	}
	s[j] = target
}

func (h *MinCandidateHeapAdapter) down(i0, n int) bool {
	s := *h
	if i0 < 0 || i0 >= n || n > len(s) {
		return false
	}
	target := s[i0]
	targetDist := target.Dist
	i := i0
	for {
		j1 := 4*i + 1
		if j1 >= n {
			break
		}
		var best int
		var bestDist float32
		if j1+3 < n {
			// All 4 children exist. Branchless/unrolled 4-way minimum.
			d0 := s[j1].Dist
			d1 := s[j1+1].Dist
			d2 := s[j1+2].Dist
			d3 := s[j1+3].Dist

			b0 := j1
			if d1 < d0 {
				b0 = j1 + 1
				d0 = d1
			}
			b1 := j1 + 2
			if d3 < d2 {
				b1 = j1 + 3
				d2 = d3
			}
			best = b0
			bestDist = d0
			if d2 < d0 {
				best = b1
				bestDist = d2
			}
		} else {
			// 1, 2, or 3 trailing children.
			best = j1
			bestDist = s[j1].Dist
			k := n - j1
			if k > 1 {
				if d1 := s[j1+1].Dist; d1 < bestDist {
					best = j1 + 1
					bestDist = d1
				}
				if k > 2 {
					if d2 := s[j1+2].Dist; d2 < bestDist {
						best = j1 + 2
						bestDist = d2
					}
				}
			}
		}

		if targetDist <= bestDist {
			break
		}
		s[i] = s[best]
		i = best
	}
	s[i] = target
	return i > i0
}

// MaxCandidateHeapAdapter makes a []types.Candidate a Max-Heap (furthest on top).
// The flat backing slice is sifted as a 4-ary heap: parent (j-1)/4, children 4*i+1..4*i+4.
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
	s := *h
	n := len(s)
	c := s[0]
	s[0] = s[n-1]
	*h = s[:n-1]
	if n > 1 {
		h.down(0, n-1)
	}
	return c
}

// ReplaceCandidate replaces the root candidate with c and restores the heap property in a single pass.
func (h *MaxCandidateHeapAdapter) ReplaceCandidate(c types.Candidate) {
	s := *h
	if len(s) == 0 {
		*h = append(s, c)
		return
	}
	s[0] = c
	h.down(0, len(s))
}

// InsertOrReplace inserts c if len < limit, or replaces the root if c.Dist < root.Dist.
// Returns true if c was added/retained, false if discarded.
func (h *MaxCandidateHeapAdapter) InsertOrReplace(c types.Candidate, limit int) bool {
	s := *h
	if len(s) < limit {
		h.PushCandidate(c)
		return true
	}
	if c.Dist < s[0].Dist {
		s[0] = c
		h.down(0, len(s))
		return true
	}
	return false
}

func (h *MaxCandidateHeapAdapter) up(j int) {
	s := *h
	if j <= 0 || j >= len(s) {
		return
	}
	target := s[j]
	targetDist := target.Dist
	for j > 0 {
		parent := (j - 1) / 4
		if s[parent].Dist >= targetDist {
			break
		}
		s[j] = s[parent]
		j = parent
	}
	s[j] = target
}

func (h *MaxCandidateHeapAdapter) down(i0, n int) bool {
	s := *h
	if i0 < 0 || i0 >= n || n > len(s) {
		return false
	}
	target := s[i0]
	targetDist := target.Dist
	i := i0
	for {
		j1 := 4*i + 1
		if j1 >= n {
			break
		}
		var best int
		var bestDist float32
		if j1+3 < n {
			// All 4 children exist. Branchless/unrolled 4-way maximum.
			d0 := s[j1].Dist
			d1 := s[j1+1].Dist
			d2 := s[j1+2].Dist
			d3 := s[j1+3].Dist

			b0 := j1
			if d1 > d0 {
				b0 = j1 + 1
				d0 = d1
			}
			b1 := j1 + 2
			if d3 > d2 {
				b1 = j1 + 3
				d2 = d3
			}
			best = b0
			bestDist = d0
			if d2 > d0 {
				best = b1
				bestDist = d2
			}
		} else {
			// 1, 2, or 3 trailing children.
			best = j1
			bestDist = s[j1].Dist
			k := n - j1
			if k > 1 {
				if d1 := s[j1+1].Dist; d1 > bestDist {
					best = j1 + 1
					bestDist = d1
				}
				if k > 2 {
					if d2 := s[j1+2].Dist; d2 > bestDist {
						best = j1 + 2
						bestDist = d2
					}
				}
			}
		}

		if targetDist >= bestDist {
			break
		}
		s[i] = s[best]
		i = best
	}
	s[i] = target
	return i > i0
}
