package main

import (
	"fmt"
	"sort"
	"testing"
)

func TestNewRing(t *testing.T) {
	r := NewRing(10)
	if r == nil {
		t.Fatal("NewRing returned nil")
	}
	if r.vnodes != 10 {
		t.Errorf("expected vnodes=10, got %d", r.vnodes)
	}
	if len(r.nodes) != 0 {
		t.Errorf("expected empty nodes, got %d", len(r.nodes))
	}
	if len(r.sortedNodes) != 0 {
		t.Errorf("expected empty sortedNodes, got %d", len(r.sortedNodes))
	}
}

func TestAddNode(t *testing.T) {
	r := NewRing(5)
	r.AddNode("node1")

	if len(r.nodes) != 5 {
		t.Errorf("expected 5 entries in nodes, got %d", len(r.nodes))
	}
	if len(r.sortedNodes) != 5 {
		t.Errorf("expected 5 entries in sortedNodes, got %d", len(r.sortedNodes))
	}

	// Verify sortedNodes is actually sorted
	if !sort.SliceIsSorted(r.sortedNodes, func(i, j int) bool {
		return r.sortedNodes[i] < r.sortedNodes[j]
	}) {
		t.Error("sortedNodes is not sorted")
	}

	// All virtual nodes should map to "node1"
	for _, h := range r.sortedNodes {
		if r.nodes[h] != "node1" {
			t.Errorf("expected node1 for hash %d, got %s", h, r.nodes[h])
		}
	}
}

func TestAddMultipleNodes(t *testing.T) {
	r := NewRing(10)
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("c")

	totalEntries := len(r.sortedNodes)
	if totalEntries != 30 {
		t.Errorf("expected 30 sortedNodes, got %d", totalEntries)
	}
	if len(r.nodes) != 30 {
		t.Errorf("expected 30 nodes entries, got %d", len(r.nodes))
	}
}

func TestRemoveNode(t *testing.T) {
	r := NewRing(5)
	r.AddNode("node1")
	r.AddNode("node2")

	r.RemoveNode("node1")

	if len(r.sortedNodes) != 5 {
		t.Errorf("expected 5 sortedNodes after removal, got %d", len(r.sortedNodes))
	}

	// All remaining entries should be node2
	for _, h := range r.sortedNodes {
		if r.nodes[h] != "node2" {
			t.Errorf("expected node2 for hash %d, got %s", h, r.nodes[h])
		}
	}
}

func TestRemoveNonexistentNode(t *testing.T) {
	r := NewRing(5)
	r.AddNode("node1")
	originalLen := len(r.sortedNodes)

	r.RemoveNode("nonexistent")

	if len(r.sortedNodes) != originalLen {
		t.Errorf("removing nonexistent node changed sortedNodes from %d to %d", originalLen, len(r.sortedNodes))
	}
}

func TestGetNodeEmptyRing(t *testing.T) {
	r := NewRing(10)
	if got := r.GetNode("any-key"); got != "" {
		t.Errorf("GetNode on empty ring returned %q, want empty string", got)
	}
}

func TestGetNodeDeterministic(t *testing.T) {
	r := NewRing(10)
	r.AddNode("node1")
	r.AddNode("node2")

	key := "test-key"
	first := r.GetNode(key)
	second := r.GetNode(key)
	if first != second {
		t.Errorf("GetNode not deterministic: %q != %q", first, second)
	}
}

func TestGetNodeReturnsKnownNode(t *testing.T) {
	r := NewRing(10)
	r.AddNode("node1")
	r.AddNode("node2")
	r.AddNode("node3")

	known := map[string]bool{"node1": true, "node2": true, "node3": true}
	for i := 0; i < 1000; i++ {
		node := r.GetNode(fmt.Sprintf("key-%d", i))
		if !known[node] {
			t.Errorf("GetNode returned unknown node %q", node)
		}
	}
}

func TestGetNodeUniformDistribution(t *testing.T) {
	r := NewRing(20)
	nodes := []string{"a", "b", "c", "d", "e"}
	for _, n := range nodes {
		r.AddNode(n)
	}

	counts := make(map[string]int)
	total := 10000
	for i := 0; i < total; i++ {
		counts[r.GetNode(fmt.Sprintf("key-%d", i))]++
	}

	for node, count := range counts {
		pct := float64(count) / float64(total) * 100
		if pct < 10 || pct > 40 {
			t.Errorf("node %s got %.1f%% of keys (expected ~20%%)", node, pct)
		}
	}
}

func TestGetPreferenceListEmptyRing(t *testing.T) {
	r := NewRing(10)
	result := r.GetPreferenceList("key", 3)
	if result != nil {
		t.Errorf("expected nil for empty ring, got %v", result)
	}
}

func TestGetPreferenceListDistinctNodes(t *testing.T) {
	r := NewRing(10)
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("c")

	result := r.GetPreferenceList("key", 3)
	if len(result) != 3 {
		t.Fatalf("expected 3 distinct nodes, got %d: %v", len(result), result)
	}

	seen := make(map[string]bool)
	for _, n := range result {
		if seen[n] {
			t.Errorf("duplicate node in preference list: %s", n)
		}
		seen[n] = true
	}
}

func TestGetPreferenceListWraps(t *testing.T) {
	r := NewRing(20)
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("c")
	r.AddNode("d")
	r.AddNode("e")

	result := r.GetPreferenceList("some-key", 4)
	if len(result) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(result))
	}

	seen := make(map[string]bool)
	for _, n := range result {
		if seen[n] {
			t.Errorf("duplicate node: %s", n)
		}
		seen[n] = true
	}
}

func TestGetPreferenceListMoreThanAvailable(t *testing.T) {
	r := NewRing(10)
	r.AddNode("only-one")

	result := r.GetPreferenceList("key", 10)
	if len(result) != 1 {
		t.Errorf("expected at most 1 node when only 1 exists, got %d", len(result))
	}
	if result[0] != "only-one" {
		t.Errorf("expected 'only-one', got %s", result[0])
	}
}

func TestHashConsistency(t *testing.T) {
	r := NewRing(1)
	h1 := r.hash("test")
	h2 := r.hash("test")
	if h1 != h2 {
		t.Errorf("hash not consistent: %d != %d", h1, h2)
	}
}

func TestHashDifferentInputs(t *testing.T) {
	r := NewRing(1)
	h1 := r.hash("test1")
	h2 := r.hash("test2")
	if h1 == h2 {
		t.Error("different inputs produced same hash")
	}
}

func TestNodeAfterRemovalNotReturned(t *testing.T) {
	r := NewRing(20)
	r.AddNode("keep")
	r.AddNode("remove")

	r.RemoveNode("remove")

	for i := 0; i < 1000; i++ {
		node := r.GetNode(fmt.Sprintf("key-%d", i))
		if node == "remove" {
			t.Errorf("removed node 'remove' was returned for key %d", i)
			return
		}
	}
}

func TestPreferenceListConsistency(t *testing.T) {
	r := NewRing(10)
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("c")

	key := "consistent-key"
	first := r.GetPreferenceList(key, 2)
	second := r.GetPreferenceList(key, 2)

	if len(first) != len(second) {
		t.Fatalf("preference list length not consistent: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Errorf("preference list not consistent at index %d: %s vs %s", i, first[i], second[i])
		}
	}
}

func BenchmarkGetNode(b *testing.B) {
	r := NewRing(20)
	for i := 0; i < 100; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.GetNode(fmt.Sprintf("key-%d", i))
	}
}

func BenchmarkGetPreferenceList(b *testing.B) {
	r := NewRing(20)
	for i := 0; i < 100; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.GetPreferenceList(fmt.Sprintf("key-%d", i), 10)
	}
}
