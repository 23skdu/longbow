package store

import (
"testing"
)

// Extension tests for NUMA allocator coverage gaps

func TestParseCPUList_SingleCPU(t *testing.T) {
res := parseCPUList("0")
if len(res) != 1 || res[0] != 0 {
t.Errorf("expected [0], got %v", res)
}
}

func TestParseCPUList_CommaSeparated(t *testing.T) {
res := parseCPUList("0,2,4")
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
res := parseCPUList("0-3")
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
res := parseCPUList("0,2-4,7")
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
res := parseCPUList("")
if len(res) != 0 {
t.Errorf("expected empty slice, got %v", res)
}
}

func TestParseCPUList_Invalid(t *testing.T) {
res := parseCPUList("abc")
if len(res) == 0 {
t.Errorf("parseCPUList returns default for invalid: got %v", res)
}
}

func TestNUMATopology_NumNodes_Direct(t *testing.T) {
topo := &NUMATopology{
nodes: []int{0, 1, 2, 3},
numCPUs:  16,
nodeCPUs: map[int][]int{0: {0, 1}, 1: {2, 3}, 2: {4, 5}, 3: {6, 7}},
cpuNode:  map[int]int{0: 0, 1: 0, 2: 1, 3: 1, 4: 2, 5: 2, 6: 3, 7: 3},
}
if n := topo.NumNodes(); n != 4 {
t.Errorf("NumNodes: got %d, want 4", n)
}
}

func TestNUMATopology_NumCPUs_Direct(t *testing.T) {
topo := &NUMATopology{
nodes: []int{0, 1},
numCPUs:  8,
nodeCPUs: map[int][]int{0: {0, 1, 2, 3}, 1: {4, 5, 6, 7}},
cpuNode:  map[int]int{},
}
if n := topo.NumCPUs(); n != 8 {
t.Errorf("NumCPUs: got %d, want 8", n)
}
}

func TestNUMATopology_GetCPUNode_Direct(t *testing.T) {
topo := &NUMATopology{
nodes: []int{0, 1},
numCPUs:  4,
nodeCPUs: map[int][]int{0: {0, 1}, 1: {2, 3}},
cpuNode:  map[int]int{0: 0, 1: 0, 2: 1, 3: 1},
}
if node := topo.GetCPUNode(2); node != 1 {
t.Errorf("GetCPUNode(2): got %d, want 1", node)
}
if node := topo.GetCPUNode(99); node != 0 {
t.Errorf("GetCPUNode(99) for missing key: got %d", node)
}
}

func TestNUMAAllocator_NumNodes_Enabled(t *testing.T) {
cfg := NewNUMAConfig()
cfg.Enabled = true
alloc, err := NewNUMAAllocator(*cfg)
if err != nil {
t.Skipf("NUMA not available: %v", err)
}
if alloc.IsEnabled() {
nodes := alloc.NumNodes()
if nodes < 1 {
t.Errorf("NumNodes should be >= 1, got %d", nodes)
}
}
}

func TestNUMAAllocator_NextNode_RoundRobin(t *testing.T) {
// Test with disabled allocator (simpler path)
cfg := NewNUMAConfig()
cfg.Enabled = false
alloc, _ := NewNUMAAllocator(*cfg)

// When disabled, NextNode should return 0
for i := 0; i < 5; i++ {
node := alloc.NextNode()
if node != 0 {
t.Errorf("NextNode (disabled) iteration %d: got %d, want 0", i, node)
}
}
}
