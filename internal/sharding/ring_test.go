package sharding

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsistentHash_AddRemove(t *testing.T) {
	c := NewConsistentHash(20)
	c.AddNode("node1")
	c.AddNode("node2")

	node := c.GetNode("some-key")
	assert.Contains(t, []string{"node1", "node2"}, node)

	c.RemoveNode("node1")
	node = c.GetNode("some-key")
	assert.Equal(t, "node2", node)
}

func TestConsistentHash_Distribution(t *testing.T) {
	c := NewConsistentHash(20)
	nodes := []string{"node1", "node2", "node3", "node4", "node5"}
	for _, n := range nodes {
		c.AddNode(n)
	}

	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		node := c.GetNode(key)
		counts[node]++
	}

	// Expect roughly even distribution (w/ 20 vnodes, variance is acceptable but no node should be empty)
	for _, n := range nodes {
		assert.Greater(t, counts[n], 0, "Node %s received 0 keys", n)
	}
}

func TestConsistentHash_GetPreferenceList(t *testing.T) {
	c := NewConsistentHash(20)
	nodes := []string{"node1", "node2", "node3", "node4", "node5"}
	for _, n := range nodes {
		c.AddNode(n)
	}

	key := "test-key"
	replicas := c.GetPreferenceList(key, 3)
	assert.Len(t, replicas, 3)
	assert.Equal(t, replicas[0], c.GetNode(key)) // First replica is always the owner

	// Verify uniqueness
	seen := make(map[string]bool)
	for _, r := range replicas {
		assert.False(t, seen[r], "Duplicate replica %s", r)
		seen[r] = true
	}
}
