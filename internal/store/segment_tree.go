package store

import (
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
)

// SegmentNode represents a node in the dynamic segment tree.
type SegmentNode struct {
	Left  *SegmentNode
	Right *SegmentNode
	IDs   *roaring.Bitmap
}

// SegmentTree is a high-performance dynamic segment tree for storing intervals
// and querying overlaps, returning roaring bitmaps of vector IDs.
type SegmentTree struct {
	mu   sync.RWMutex
	root *SegmentNode
	min  int64
	max  int64
}

// NewSegmentTree creates a new dynamic segment tree capable of storing intervals
// within the [min, max] range.
func NewSegmentTree(min, max int64) *SegmentTree {
	return &SegmentTree{
		root: &SegmentNode{IDs: roaring.New()},
		min:  min,
		max:  max,
	}
}

// Insert adds a vector ID to the interval [start, end].
func (st *SegmentTree) Insert(start, end int64, id uint32) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.insert(st.root, st.min, st.max, start, end, id)
}

func (st *SegmentTree) insert(node *SegmentNode, l, r, start, end int64, id uint32) {
	if start <= l && r <= end {
		node.IDs.Add(id)
		return
	}
	mid := l + (r-l)/2
	if start <= mid {
		if node.Left == nil {
			node.Left = &SegmentNode{IDs: roaring.New()}
		}
		st.insert(node.Left, l, mid, start, end, id)
	}
	if end > mid {
		if node.Right == nil {
			node.Right = &SegmentNode{IDs: roaring.New()}
		}
		st.insert(node.Right, mid+1, r, start, end, id)
	}
}

// Remove deletes a vector ID from the interval [start, end].
func (st *SegmentTree) Remove(start, end int64, id uint32) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.remove(st.root, st.min, st.max, start, end, id)
}

func (st *SegmentTree) remove(node *SegmentNode, l, r, start, end int64, id uint32) {
	if node == nil {
		return
	}
	if start <= l && r <= end {
		node.IDs.Remove(id)
		return
	}
	mid := l + (r-l)/2
	if start <= mid {
		st.remove(node.Left, l, mid, start, end, id)
	}
	if end > mid {
		st.remove(node.Right, mid+1, r, start, end, id)
	}
}

// Query returns a bitmap of all vector IDs whose intervals overlap with the given point.
func (st *SegmentTree) Query(point int64) *roaring.Bitmap {
	st.mu.RLock()
	defer st.mu.RUnlock()
	result := roaring.New()
	st.queryPoint(st.root, st.min, st.max, point, result)
	return result
}

func (st *SegmentTree) queryPoint(node *SegmentNode, l, r, point int64, result *roaring.Bitmap) {
	if node == nil {
		return
	}
	if !node.IDs.IsEmpty() {
		result.Or(node.IDs)
	}
	if l == r {
		return
	}
	mid := l + (r-l)/2
	if point <= mid {
		st.queryPoint(node.Left, l, mid, point, result)
	} else {
		st.queryPoint(node.Right, mid+1, r, point, result)
	}
}

// QueryRange returns a bitmap of all vector IDs whose intervals overlap with [start, end].
func (st *SegmentTree) QueryRange(start, end int64) *roaring.Bitmap {
	st.mu.RLock()
	defer st.mu.RUnlock()
	result := roaring.New()
	st.queryRange(st.root, st.min, st.max, start, end, result)
	return result
}

func (st *SegmentTree) queryRange(node *SegmentNode, l, r, start, end int64, result *roaring.Bitmap) {
	if node == nil {
		return
	}
	// If the current node interval [l, r] is completely covered by [start, end],
	// or we overlap in any way, the node's IDs are valid.
	if start <= r && end >= l {
		if !node.IDs.IsEmpty() {
			result.Or(node.IDs)
		}
	}
	if l == r {
		return
	}
	mid := l + (r-l)/2
	if start <= mid {
		st.queryRange(node.Left, l, mid, start, end, result)
	}
	if end > mid {
		st.queryRange(node.Right, mid+1, r, start, end, result)
	}
}
