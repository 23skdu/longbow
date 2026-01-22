package sharding

import (
	"fmt"
	"testing"
	"time"

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

func TestConsistentHash_EmptyRing(t *testing.T) {
	c := NewConsistentHash(10)
	node := c.GetNode("any-key")
	assert.Equal(t, "", node)
}

func TestConsistentHash_SingleNode(t *testing.T) {
	c := NewConsistentHash(50)
	c.AddNode("single")

	// All keys should go to single node
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		assert.Equal(t, "single", c.GetNode(key))
	}
}

func TestConsistentHash_RemoveLastNode(t *testing.T) {
	c := NewConsistentHash(20)
	c.AddNode("only-node")

	c.RemoveNode("only-node")

	node := c.GetNode("key")
	assert.Equal(t, "", node)
}

func TestConsistentHash_GetNodeDeterministic(t *testing.T) {
	c := NewConsistentHash(50)
	c.AddNode("node1")
	c.AddNode("node2")

	key := "deterministic-key-123"

	node1 := c.GetNode(key)
	node2 := c.GetNode(key)

	assert.Equal(t, node1, node2, "Same key should always return same node")
}

func TestConsistentHash_PreferenceListExhaustion(t *testing.T) {
	c := NewConsistentHash(100)
	c.AddNode("node1")
	c.AddNode("node2")

	// Request more replicas than available distinct nodes
	replicas := c.GetPreferenceList("key", 10)
	assert.Len(t, replicas, 2) // Only 2 distinct nodes exist
}

func TestConsistentHash_PreferenceListUniqueness(t *testing.T) {
	c := NewConsistentHash(50)
	c.AddNode("node1")
	c.AddNode("node2")
	c.AddNode("node3")

	key := "test-key"
	replicas := c.GetPreferenceList(key, 100) // Request more than exists

	// All replicas should be unique
	seen := make(map[string]int)
	for _, r := range replicas {
		seen[r]++
	}
	for _, count := range seen {
		assert.Equal(t, 1, count, "Each node should appear at most once in preference list")
	}
}

func BenchmarkConsistentHash_GetNode(b *testing.B) {
	c := NewConsistentHash(100)
	for i := 0; i < 10; i++ {
		c.AddNode(fmt.Sprintf("node%d", i))
	}

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			c.GetNode(key)
		}
	}
}

func BenchmarkConsistentHash_GetPreferenceList(b *testing.B) {
	c := NewConsistentHash(100)
	for i := 0; i < 10; i++ {
		c.AddNode(fmt.Sprintf("node%d", i))
	}

	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			c.GetPreferenceList(key, 3)
		}
	}
}

func BenchmarkConsistentHash_AddNode(b *testing.B) {
	c := NewConsistentHash(100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		c.AddNode(fmt.Sprintf("node%d", i))
	}
}

func TestConsistentHash_CacheBasic(t *testing.T) {
	c := NewConsistentHash(20)
	c.AddNode("node1")
	c.AddNode("node2")

	// First call should miss cache
	key := "cache-test-key"

	node1 := c.GetNode(key)
	assert.Contains(t, []string{"node1", "node2"}, node1)

	metrics := c.GetMetrics()
	assert.Equal(t, int64(1), metrics.CacheMisses)

	// Second call should hit cache
	node2 := c.GetNode(key)
	assert.Equal(t, node1, node2)

	metrics = c.GetMetrics()
	assert.Equal(t, int64(1), metrics.CacheHits)
}

func TestConsistentHash_CacheInvalidation(t *testing.T) {
	c := NewConsistentHash(20)
	c.AddNode("node1")

	key := "test-key"

	// First call - miss
	node1 := c.GetNode(key)
	assert.Equal(t, "node1", node1)

	metrics := c.GetMetrics()
	assert.Equal(t, int64(0), metrics.CacheHits)
	assert.Equal(t, int64(1), metrics.CacheMisses)

	// Second call - hit
	node1b := c.GetNode(key)
	assert.Equal(t, node1, node1b)

	metrics = c.GetMetrics()
	assert.Equal(t, int64(1), metrics.CacheHits)
	assert.Equal(t, int64(1), metrics.CacheMisses)

	// Add new node - should invalidate cache
	c.AddNode("node2")

	// Cache should be invalidated, so this call will miss
	node2 := c.GetNode(key)
	assert.Contains(t, []string{"node1", "node2"}, node2)

	metrics = c.GetMetrics()
	// Hit count unchanged, miss count incremented
	assert.Equal(t, int64(1), metrics.CacheHits)
	assert.Equal(t, int64(2), metrics.CacheMisses)
}

func TestConsistentHash_CacheClear(t *testing.T) {
	c := NewConsistentHash(20)
	c.AddNode("node1")

	// Fill cache with unique keys
	for i := 0; i < 50; i++ {
		c.GetNode(fmt.Sprintf("key-%d", i))
	}

	metrics := c.GetMetrics()
	// All unique keys, so all should be misses
	assert.Equal(t, int64(50), metrics.CacheMisses)

	// Clear cache
	c.ClearCache()

	metrics = c.GetMetrics()
	// ClearCache doesn't reset metrics
	assert.Equal(t, int64(50), metrics.CacheMisses)
	assert.Equal(t, 0, metrics.CacheSize)
}

func TestConsistentHash_CacheExpiry(t *testing.T) {
	// Create ring with very short TTL
	c := NewConsistentHashWithCache(20, 100, 10*time.Millisecond)
	c.AddNode("node1")

	key := "expiry-test-key"
	c.GetNode(key)

	metrics := c.GetMetrics()
	assert.Equal(t, int64(1), metrics.CacheMisses)

	// Wait for cache to expire
	time.Sleep(20 * time.Millisecond)

	// Should be a cache miss now
	c.GetNode(key)

	metrics = c.GetMetrics()
	assert.Equal(t, int64(2), metrics.CacheMisses)
}

func TestConsistentHash_CacheCapacity(t *testing.T) {
	// Create ring with small cache
	c := NewConsistentHashWithCache(20, 50, time.Hour)
	c.AddNode("node1")

	// Add more than capacity entries
	for i := 0; i < 100; i++ {
		c.GetNode(fmt.Sprintf("key-%d", i))
	}

	metrics := c.GetMetrics()
	assert.Equal(t, 50, metrics.CacheSize, "Cache should be at capacity")
}

func TestConsistentHash_GetNodeUncached(t *testing.T) {
	c := NewConsistentHash(20)
	c.AddNode("node1")
	c.AddNode("node2")

	key := "uncached-test-key"

	// First call via cached path - should miss cache
	node1 := c.GetNode(key)

	metrics := c.GetMetrics()
	// First call is a miss, then it populates cache
	assert.Equal(t, int64(0), metrics.CacheHits)
	assert.Equal(t, int64(1), metrics.CacheMisses)

	// Second call via cached path - should hit cache
	node2 := c.GetNode(key)
	assert.Equal(t, node1, node2)

	metrics = c.GetMetrics()
	assert.Equal(t, int64(1), metrics.CacheHits)

	// GetNodeUncached doesn't use or update cache
	node3 := c.GetNodeUncached(key)
	assert.Equal(t, node1, node3)

	// Metrics should be unchanged
	metrics = c.GetMetrics()
	assert.Equal(t, int64(1), metrics.CacheHits)
	assert.Equal(t, int64(1), metrics.CacheMisses)
}

func TestConsistentHash_NodeCount(t *testing.T) {
	c := NewConsistentHash(20)
	assert.Equal(t, 0, c.NodeCount())

	c.AddNode("node1")
	assert.Equal(t, 1, c.NodeCount())

	c.AddNode("node2")
	assert.Equal(t, 2, c.NodeCount())

	c.RemoveNode("node1")
	assert.Equal(t, 1, c.NodeCount())
}

func TestConsistentHash_VirtualNodeCount(t *testing.T) {
	c := NewConsistentHash(100)
	assert.Equal(t, 100, c.VirtualNodeCount())
}

func TestConsistentHash_TotalHashCount(t *testing.T) {
	c := NewConsistentHash(50)
	assert.Equal(t, 0, c.TotalHashCount())

	c.AddNode("node1")
	assert.Equal(t, 50, c.TotalHashCount())

	c.AddNode("node2")
	assert.Equal(t, 100, c.TotalHashCount())
}

func BenchmarkConsistentHash_GetNodeWithCache(b *testing.B) {
	c := NewConsistentHash(100)
	for i := 0; i < 10; i++ {
		c.AddNode(fmt.Sprintf("node%d", i))
	}

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	// Warm up cache
	for _, key := range keys {
		c.GetNode(key)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			c.GetNode(key)
		}
	}
}

func BenchmarkConsistentHash_GetNodeWithoutCache(b *testing.B) {
	c := NewConsistentHash(100)
	for i := 0; i < 10; i++ {
		c.AddNode(fmt.Sprintf("node%d", i))
	}

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			c.GetNodeUncached(key)
		}
	}
}

func BenchmarkConsistentHash_CacheHitRate(b *testing.B) {
	c := NewConsistentHash(100)
	for i := 0; i < 10; i++ {
		c.AddNode(fmt.Sprintf("node%d", i))
	}

	// Generate keys with some repetition (zipfian-like)
	keys := make([]string, 10000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i%100) // Only 100 unique keys
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			c.GetNode(key)
		}
	}
}
