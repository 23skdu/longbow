package hnsw2

import (
	"math/rand"
	"sort"
	"testing"
)

func TestFixedHeap_PushPop(t *testing.T) {
	h := NewFixedHeap(10)
	
	// Push some candidates
	h.Push(Candidate{ID: 1, Dist: 5.0})
	h.Push(Candidate{ID: 2, Dist: 2.0})
	h.Push(Candidate{ID: 3, Dist: 8.0})
	h.Push(Candidate{ID: 4, Dist: 1.0})
	
	if h.Len() != 4 {
		t.Errorf("expected length 4, got %d", h.Len())
	}
	
	// Pop should return in ascending distance order
	c, ok := h.Pop()
	if !ok || c.ID != 4 || c.Dist != 1.0 {
		t.Errorf("expected {4, 1.0}, got {%d, %f}", c.ID, c.Dist)
	}
	
	c, ok = h.Pop()
	if !ok || c.ID != 2 || c.Dist != 2.0 {
		t.Errorf("expected {2, 2.0}, got {%d, %f}", c.ID, c.Dist)
	}
	
	c, ok = h.Pop()
	if !ok || c.ID != 1 || c.Dist != 5.0 {
		t.Errorf("expected {1, 5.0}, got {%d, %f}", c.ID, c.Dist)
	}
	
	c, ok = h.Pop()
	if !ok || c.ID != 3 || c.Dist != 8.0 {
		t.Errorf("expected {3, 8.0}, got {%d, %f}", c.ID, c.Dist)
	}
	
	// Should be empty now
	_, ok = h.Pop()
	if ok {
		t.Error("expected Pop to fail on empty heap")
	}
}

func TestFixedHeap_Peek(t *testing.T) {
	h := NewFixedHeap(10)
	
	h.Push(Candidate{ID: 1, Dist: 5.0})
	h.Push(Candidate{ID: 2, Dist: 2.0})
	
	c, ok := h.Peek()
	if !ok || c.Dist != 2.0 {
		t.Errorf("expected min distance 2.0, got %f", c.Dist)
	}
	
	// Peek should not remove
	if h.Len() != 2 {
		t.Error("Peek should not remove elements")
	}
}

func TestFixedHeap_Clear(t *testing.T) {
	h := NewFixedHeap(10)
	
	h.Push(Candidate{ID: 1, Dist: 1.0})
	h.Push(Candidate{ID: 2, Dist: 2.0})
	
	h.Clear()
	
	if h.Len() != 0 {
		t.Error("Clear should empty the heap")
	}
	
	_, ok := h.Pop()
	if ok {
		t.Error("Pop should fail after Clear")
	}
}

func TestFixedHeap_Capacity(t *testing.T) {
	h := NewFixedHeap(3)
	
	// Fill to capacity
	h.Push(Candidate{ID: 1, Dist: 1.0})
	h.Push(Candidate{ID: 2, Dist: 2.0})
	h.Push(Candidate{ID: 3, Dist: 3.0})
	
	// Should reject when full
	ok := h.Push(Candidate{ID: 4, Dist: 4.0})
	if ok {
		t.Error("Push should fail when heap is full")
	}
	
	if h.Len() != 3 {
		t.Errorf("expected length 3, got %d", h.Len())
	}
}

func TestFixedHeap_RandomOrder(t *testing.T) {
	h := NewFixedHeap(100)
	
	// Push random distances
	rng := rand.New(rand.NewSource(42))
	expected := make([]float32, 100)
	for i := 0; i < 100; i++ {
		dist := rng.Float32() * 100
		expected[i] = dist
		h.Push(Candidate{ID: uint32(i), Dist: dist})
	}
	
	// Sort expected
	sort.Slice(expected, func(i, j int) bool {
		return expected[i] < expected[j]
	})
	
	// Pop all and verify sorted order
	for i := 0; i < 100; i++ {
		c, ok := h.Pop()
		if !ok {
			t.Fatalf("Pop failed at index %d", i)
		}
		if c.Dist != expected[i] {
			t.Errorf("at index %d: expected dist %f, got %f", i, expected[i], c.Dist)
		}
	}
}

func BenchmarkFixedHeap_Push(b *testing.B) {
	h := NewFixedHeap(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Clear()
		for j := 0; j < 1000; j++ {
			h.Push(Candidate{ID: uint32(j), Dist: float32(j)})
		}
	}
}

func BenchmarkFixedHeap_Pop(b *testing.B) {
	h := NewFixedHeap(1000)
	for j := 0; j < 1000; j++ {
		h.Push(Candidate{ID: uint32(j), Dist: float32(j)})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Pop()
		if h.Len() == 0 {
			// Refill
			for j := 0; j < 1000; j++ {
				h.Push(Candidate{ID: uint32(j), Dist: float32(j)})
			}
		}
	}
}
