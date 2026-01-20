package store

// MaxHeap is a fixed-capacity max-heap for managing result sets.
// Unlike FixedHeap (min-heap), this keeps the largest distance at the root.
type MaxHeap struct {
	items []Candidate
	size  int
	cap   int
}

// NewMaxHeap creates a new fixed-capacity max-heap.
func NewMaxHeap(capacity int) *MaxHeap {
	return &MaxHeap{
		items: make([]Candidate, capacity),
		size:  0,
		cap:   capacity,
	}
}

// Grow ensures the heap has at least newCap capacity.
func (h *MaxHeap) Grow(newCap int) {
	if newCap <= h.cap {
		return
	}
	newItems := make([]Candidate, newCap)
	copy(newItems, h.items[:h.size])
	h.items = newItems
	h.cap = newCap
}

// Push adds a candidate to the heap.
func (h *MaxHeap) Push(c Candidate) bool {
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

// Pop removes and returns the maximum element (largest distance).
func (h *MaxHeap) Pop() (Candidate, bool) {
	if h.size == 0 {
		return Candidate{}, false
	}

	maxItem := h.items[0]
	h.size--

	if h.size > 0 {
		h.items[0] = h.items[h.size]
		h.bubbleDown(0)
	}

	return maxItem, true
}

// Peek returns the maximum element without removing it.
func (h *MaxHeap) Peek() (Candidate, bool) {
	if h.size == 0 {
		return Candidate{}, false
	}
	return h.items[0], true
}

// Len returns the current number of elements in the heap.
func (h *MaxHeap) Len() int {
	return h.size
}

// Clear resets the heap to empty.
func (h *MaxHeap) Clear() {
	h.size = 0
}

// Items returns a slice of all items in the heap (not sorted).
func (h *MaxHeap) Items() []Candidate {
	return h.items[:h.size]
}

func (h *MaxHeap) bubbleUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		// Max-heap: parent should be >= child
		if h.items[idx].Dist <= h.items[parent].Dist {
			break
		}
		h.items[idx], h.items[parent] = h.items[parent], h.items[idx]
		idx = parent
	}
}

func (h *MaxHeap) bubbleDown(idx int) {
	for {
		left := 2*idx + 1
		right := 2*idx + 2
		largest := idx

		// Max-heap: find largest among parent and children
		if left < h.size && h.items[left].Dist > h.items[largest].Dist {
			largest = left
		}
		if right < h.size && h.items[right].Dist > h.items[largest].Dist {
			largest = right
		}

		if largest == idx {
			break
		}

		h.items[idx], h.items[largest] = h.items[largest], h.items[idx]
		idx = largest
	}
}

// ReplaceTop replaces the maximum element with a new candidate and reorders the heap.
// This is more efficient than Pop followed by Push.
func (h *MaxHeap) ReplaceTop(c Candidate) {
	if h.size == 0 {
		h.Push(c)
		return
	}
	h.items[0] = c
	h.bubbleDown(0)
}
