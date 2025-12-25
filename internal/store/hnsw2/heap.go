package hnsw2

// Candidate represents a search candidate with its ID and distance.
type Candidate struct {
	ID   uint32
	Dist float32
}

// FixedHeap is a fixed-capacity min-heap for managing search candidates.
// It avoids allocations by using a pre-allocated array.
type FixedHeap struct {
	items []Candidate
	size  int
	cap   int
}

// NewFixedHeap creates a new fixed-capacity heap.
func NewFixedHeap(capacity int) *FixedHeap {
	return &FixedHeap{
		items: make([]Candidate, capacity),
		size:  0,
		cap:   capacity,
	}
}

// Push adds a candidate to the heap. Returns false if heap is full.
func (h *FixedHeap) Push(c Candidate) bool {
	if h.size >= h.cap {
		return false
	}
	
	// Add to end
	h.items[h.size] = c
	h.size++
	
	// Bubble up
	h.bubbleUp(h.size - 1)
	return true
}

// Pop removes and returns the minimum element (smallest distance).
func (h *FixedHeap) Pop() (Candidate, bool) {
	if h.size == 0 {
		return Candidate{}, false
	}
	
	minItem := h.items[0]
	h.size--
	
	if h.size > 0 {
		h.items[0] = h.items[h.size]
		h.bubbleDown(0)
	}
	
	return minItem, true
}

// Peek returns the minimum element without removing it.
func (h *FixedHeap) Peek() (Candidate, bool) {
	if h.size == 0 {
		return Candidate{}, false
	}
	return h.items[0], true
}

// Len returns the current number of elements in the heap.
func (h *FixedHeap) Len() int {
	return h.size
}

// Clear resets the heap to empty.
func (h *FixedHeap) Clear() {
	h.size = 0
}

// Items returns a slice of all items in the heap (not sorted).
func (h *FixedHeap) Items() []Candidate {
	return h.items[:h.size]
}

func (h *FixedHeap) bubbleUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if h.items[idx].Dist >= h.items[parent].Dist {
			break
		}
		h.items[idx], h.items[parent] = h.items[parent], h.items[idx]
		idx = parent
	}
}

func (h *FixedHeap) bubbleDown(idx int) {
	for {
		left := 2*idx + 1
		right := 2*idx + 2
		smallest := idx
		
		if left < h.size && h.items[left].Dist < h.items[smallest].Dist {
			smallest = left
		}
		if right < h.size && h.items[right].Dist < h.items[smallest].Dist {
			smallest = right
		}
		
		if smallest == idx {
			break
		}
		
		h.items[idx], h.items[smallest] = h.items[smallest], h.items[idx]
		idx = smallest
	}
}
