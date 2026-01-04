package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

// Ring manages consistent hashing
type Ring struct {
	nodes       map[uint64]string // hash -> nodeID
	sortedNodes []uint64          // sorted hashes
	vnodes      int               // virtual nodes per real node
}

func NewRing(vnodes int) *Ring {
	return &Ring{
		nodes:  make(map[uint64]string),
		vnodes: vnodes,
	}
}

func (r *Ring) AddNode(id string) {
	for i := 0; i < r.vnodes; i++ {
		h := r.hash(fmt.Sprintf("%s:%d", id, i))
		r.nodes[h] = id
		r.sortedNodes = append(r.sortedNodes, h)
	}
	sort.Slice(r.sortedNodes, func(i, j int) bool {
		return r.sortedNodes[i] < r.sortedNodes[j]
	})
}

func (r *Ring) RemoveNode(id string) {
	newNodes := make([]uint64, 0, len(r.sortedNodes))
	for _, h := range r.sortedNodes {
		if r.nodes[h] != id {
			newNodes = append(newNodes, h)
		} else {
			delete(r.nodes, h)
		}
	}
	r.sortedNodes = newNodes
}

func (r *Ring) GetNode(key string) string {
	if len(r.sortedNodes) == 0 {
		return ""
	}
	h := r.hash(key)
	idx := sort.Search(len(r.sortedNodes), func(i int) bool {
		return r.sortedNodes[i] >= h
	})
	if idx == len(r.sortedNodes) {
		idx = 0
	}
	return r.nodes[r.sortedNodes[idx]]
}

// GetPreferenceList returns N distinct nodes for replication
func (r *Ring) GetPreferenceList(key string, n int) []string {
	if len(r.sortedNodes) == 0 {
		return nil
	}

	h := r.hash(key)
	idx := sort.Search(len(r.sortedNodes), func(i int) bool {
		return r.sortedNodes[i] >= h
	})

	seen := make(map[string]bool)
	result := make([]string, 0, n)

	// Walk the ring
	start := idx
	if start == len(r.sortedNodes) {
		start = 0
	}

	for i := 0; i < len(r.sortedNodes); i++ {
		currIdx := (start + i) % len(r.sortedNodes)
		nodeID := r.nodes[r.sortedNodes[currIdx]]
		if !seen[nodeID] {
			result = append(result, nodeID)
			seen[nodeID] = true
		}
		if len(result) == n {
			break
		}
	}

	return result
}

func (r *Ring) hash(s string) uint64 {
	h := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(h[:8])
}

func main() {
	// rand.Seed(time.Now().UnixNano()) // Deprecated in Go 1.20

	ring := NewRing(20) // 20 vnodes per node

	nodes := []string{"node1", "node2", "node3", "node4", "node5"}
	for _, n := range nodes {
		ring.AddNode(n)
	}

	const numKeys = 100000
	distribution := make(map[string]int)
	initialAssignment := make(map[string]string)

	// Assign keys
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := ring.GetNode(key)
		distribution[node]++
		initialAssignment[key] = node
	}

	fmt.Println("Initial Distribution:")
	for node, count := range distribution {
		fmt.Printf("%s: %d (%.2f%%)\n", node, count, float64(count)/float64(numKeys)*100)
	}

	// Simulate adding a node
	fmt.Println("\nAdding node6...")
	ring.AddNode("node6")

	moved := 0
	newDistribution := make(map[string]int)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := ring.GetNode(key)
		newDistribution[node]++
		if node != initialAssignment[key] {
			moved++
		}
	}

	fmt.Println("New Distribution:")
	for node, count := range newDistribution {
		fmt.Printf("%s: %d (%.2f%%)\n", node, count, float64(count)/float64(numKeys)*100)
	}

	fmt.Printf("\nKeys moved: %d (%.2f%%)\n", moved, float64(moved)/float64(numKeys)*100)
	fmt.Printf("Ideal move %%: %.2f%%\n", 100.0/6.0)
}
