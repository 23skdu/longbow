//go:build linux

package memory

import (
	"testing"
)

func TestParseCPUList_SingleCPU(t *testing.T) {
	res, err := parseCPUList("0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 1 || res[0] != 0 {
		t.Errorf("expected [0], got %v", res)
	}
}

func TestParseCPUList_CommaSeparated(t *testing.T) {
	res, err := parseCPUList("0,2,4")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{0, 2, 4}
	if len(res) != len(expected) {
		t.Fatalf("length mismatch: got %d, want %d", len(res), len(expected))
	}
	for i, v := range expected {
		if res[i] != v {
			t.Errorf("index %d: got %d, want %d", i, res[i], v)
		}
	}
}

func TestParseCPUList_Range(t *testing.T) {
	res, err := parseCPUList("0-3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{0, 1, 2, 3}
	if len(res) != len(expected) {
		t.Fatalf("length mismatch: got %d, want %d", len(res), len(expected))
	}
	for i, v := range expected {
		if res[i] != v {
			t.Errorf("index %d: got %d, want %d", i, res[i], v)
		}
	}
}

func TestParseCPUList_Mixed(t *testing.T) {
	res, err := parseCPUList("0,2-4,7")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := []int{0, 2, 3, 4, 7}
	if len(res) != len(expected) {
		t.Fatalf("length mismatch: got %d, want %d", len(res), len(expected))
	}
	for i, v := range expected {
		if res[i] != v {
			t.Errorf("index %d: got %d, want %d", i, res[i], v)
		}
	}
}

func TestParseCPUList_Empty(t *testing.T) {
	res, err := parseCPUList("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 0 {
		t.Errorf("expected empty slice, got %v", res)
	}
}

func TestParseCPUList_Invalid(t *testing.T) {
	res, err := parseCPUList("abc")
	if err == nil {
		t.Errorf("expected error for invalid input, got nil")
	}
	if len(res) != 0 {
		t.Errorf("expected empty slice for invalid input, got %v", res)
	}
}

func TestNUMATopology_Direct(t *testing.T) {
	topo := &NUMATopology{
		NumNodes: 4,
		CPUs:     [][]int{{0, 1}, {2, 3}, {4, 5}, {6, 7}},
	}
	if topo.NumNodes != 4 {
		t.Errorf("NumNodes: got %d, want 4", topo.NumNodes)
	}
	if len(topo.CPUs) != 4 {
		t.Errorf("CPUs length: got %d, want 4", len(topo.CPUs))
	}
}

func TestNUMAAllocator_Basic(t *testing.T) {
	topo := &NUMATopology{
		NumNodes: 2,
		CPUs:     [][]int{{0, 1, 2, 3}, {4, 5, 6, 7}},
	}
	alloc := NewNUMAAllocator(topo, 0)

	buf := alloc.Allocate(100)
	if len(buf) != 100 {
		t.Errorf("Allocate: got %d, want 100", len(buf))
	}
	alloc.Free(buf)
}

func TestNUMAAllocator_MultipleNodes(t *testing.T) {
	topo := &NUMATopology{
		NumNodes: 2,
		CPUs:     [][]int{{0, 1}, {2, 3}},
	}

	alloc0 := NewNUMAAllocator(topo, 0)
	alloc1 := NewNUMAAllocator(topo, 1)

	buf0 := alloc0.Allocate(50)
	buf1 := alloc1.Allocate(50)

	if len(buf0) != 50 || len(buf1) != 50 {
		t.Errorf("allocation sizes incorrect")
	}

	alloc0.Free(buf0)
	alloc1.Free(buf1)
}
