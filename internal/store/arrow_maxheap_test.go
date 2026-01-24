package store

import (
	"testing"
)

func TestMaxHeap(t *testing.T) {
	capacity := 10
	h := NewMaxHeap(capacity)

	// Push random values
	values := []float32{1.0, 5.0, 2.0, 8.0, 3.0, 9.0, 4.0, 7.0, 6.0, 0.5, 10.0, 0.1}

	// Expect heap to keep SMALLEST values?
	// No, MaxHeap used for top-k smallest values keeps LARGEST at root so we can evict it.
	// So if we have capacity 10. We want to keep 10 smallest.
	// We push. If full, we pop (remove largest) then push.

	for _, v := range values {
		if h.Len() >= capacity {
			h.Pop()
		}
		h.Push(Candidate{ID: 0, Dist: v})
	}

	if h.Len() != capacity {
		t.Errorf("Expected len %d, got %d", capacity, h.Len())
	}

	// Verify items are the 10 smallest
	// 0.1, 0.5, 1, 2, 3, 4, 5, 6, 7, 8
	// 9 and 10 should be evicted.

	// Peek should return the LARGEST of the smallest (i.e. 8.0)
	maxVal, ok := h.Peek()
	if !ok {
		t.Fatal("Peek failed")
	}
	if maxVal.Dist != 8.0 {
		t.Errorf("Expected max 8.0, got %f", maxVal.Dist)
	}

	// Pop all and verify order (should come out largest to smallest)
	expected := []float32{8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0, 0.5, 0.1}
	for i, exp := range expected {
		val, ok := h.Pop()
		if !ok {
			t.Fatal("Pop failed")
		}
		if val.Dist != exp {
			t.Errorf("Pop %d: expected %f, got %f", i, exp, val.Dist)
		}
	}
}
