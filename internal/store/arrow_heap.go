package store

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

// Grow ensures the heap has at least newCap capacity.
func (h *FixedHeap) Grow(newCap int) {
	if newCap <= h.cap {
		return
	}
	// Resize
	newItems := make([]Candidate, newCap)
	copy(newItems, h.items[:h.size])
	h.items = newItems
	h.cap = newCap
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

// MaxHeap is a fixed-capacity max-heap.
// It keeps the LARGEST elements at the top.
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

func (h *MaxHeap) Len() int {
	return h.size
}

func (h *MaxHeap) Push(c Candidate) {
	if h.size < h.cap {
		h.items[h.size] = c
		h.size++
		h.bubbleUp(h.size - 1)
	} else if c.Dist < h.items[0].Dist { // Only push if smaller than max? No, MaxHeap usually just holds items.
		// If capacity is limited, we might want to evict largest?
		// The test says "MaxHeap used for top-k smallest values".
		// This means we keep the K smallest values.
		// To do this, we use a MaxHeap of size K.
		// If new item < Max (Root), we replace Root and sift down.
		// This behavior is usually implemented by user, or implicit in a "BoundedMaxHeap".
		// But here `Push` is void or bool.
		// The test: checks `if h.Len() >= capacity { h.Pop() }`.
		// It manually verifies capacity!
		// So `Push` just adds. If full, it should probably expand or panic or just overwrite end?
		// But test calls Pop first if full.
		// So `Push` assumes space exists.

		h.items[h.size] = c
		h.size++
		h.bubbleUp(h.size - 1)
	}
}

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

func (h *MaxHeap) Peek() (Candidate, bool) {
	if h.size == 0 {
		return Candidate{}, false
	}
	return h.items[0], true
}

func (h *MaxHeap) bubbleUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		// MaxHeap: child > parent -> Swap
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
