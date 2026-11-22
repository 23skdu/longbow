package sharding

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestConsistentHash_AddRemove runs the test consistent hash_add remove operation.
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

// TestConsistentHash_Distribution runs the test consistent hash_distribution operation.
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

// TestConsistentHash_GetPreferenceList runs the test consistent hash_get preference list operation.
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

// TestConsistentHash_EmptyRing runs the test consistent hash_empty ring operation.
func TestConsistentHash_EmptyRing(t *testing.T) {
	c := NewConsistentHash(10)
	node := c.GetNode("any-key")
	assert.Equal(t, "", node)
}

// TestConsistentHash_SingleNode runs the test consistent hash_single node operation.
func TestConsistentHash_SingleNode(t *testing.T) {
	c := NewConsistentHash(50)
	c.AddNode("single")

	// All keys should go to single node
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		assert.Equal(t, "single", c.GetNode(key))
	}
}

// TestConsistentHash_RemoveLastNode runs the test consistent hash_remove last node operation.
func TestConsistentHash_RemoveLastNode(t *testing.T) {
	c := NewConsistentHash(20)
	c.AddNode("only-node")

	c.RemoveNode("only-node")

	node := c.GetNode("key")
	assert.Equal(t, "", node)
}

// TestConsistentHash_GetNodeDeterministic runs the test consistent hash_get node deterministic operation.
func TestConsistentHash_GetNodeDeterministic(t *testing.T) {
	c := NewConsistentHash(50)
	c.AddNode("node1")
	c.AddNode("node2")

	key := "deterministic-key-123"

	node1 := c.GetNode(key)
	node2 := c.GetNode(key)

	assert.Equal(t, node1, node2, "Same key should always return same node")
}

// TestConsistentHash_PreferenceListExhaustion runs the test consistent hash_preference list exhaustion operation.
func TestConsistentHash_PreferenceListExhaustion(t *testing.T) {
	c := NewConsistentHash(100)
	c.AddNode("node1")
	c.AddNode("node2")

	// Request more replicas than available distinct nodes
	replicas := c.GetPreferenceList("key", 10)
	assert.Len(t, replicas, 2) // Only 2 distinct nodes exist
}

// TestConsistentHash_PreferenceListUniqueness runs the test consistent hash_preference list uniqueness operation.
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

// BenchmarkConsistentHash_GetNode runs the benchmark consistent hash_get node operation.
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

// BenchmarkConsistentHash_GetPreferenceList runs the benchmark consistent hash_get preference list operation.
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

// BenchmarkConsistentHash_AddNode runs the benchmark consistent hash_add node operation.
func BenchmarkConsistentHash_AddNode(b *testing.B) {
	c := NewConsistentHash(100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		c.AddNode(fmt.Sprintf("node%d", i))
	}
}

// TestConsistentHash_CacheBasic runs the test consistent hash_cache basic operation.
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

// TestConsistentHash_CacheInvalidation runs the test consistent hash_cache invalidation operation.
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

// TestConsistentHash_CacheClear runs the test consistent hash_cache clear operation.
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

// TestConsistentHash_CacheExpiry runs the test consistent hash_cache expiry operation.
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

// TestConsistentHash_CacheCapacity runs the test consistent hash_cache capacity operation.
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

// TestConsistentHash_GetNodeUncached runs the test consistent hash_get node uncached operation.
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

// TestConsistentHash_NodeCount runs the test consistent hash_node count operation.
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

// TestConsistentHash_VirtualNodeCount runs the test consistent hash_virtual node count operation.
func TestConsistentHash_VirtualNodeCount(t *testing.T) {
	c := NewConsistentHash(100)
	assert.Equal(t, 100, c.VirtualNodeCount())
}

// TestConsistentHash_TotalHashCount runs the test consistent hash_total hash count operation.
func TestConsistentHash_TotalHashCount(t *testing.T) {
	c := NewConsistentHash(50)
	assert.Equal(t, 0, c.TotalHashCount())

	c.AddNode("node1")
	assert.Equal(t, 50, c.TotalHashCount())

	c.AddNode("node2")
	assert.Equal(t, 100, c.TotalHashCount())
}

// BenchmarkConsistentHash_GetNodeWithCache runs the benchmark consistent hash_get node with cache operation.
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

// BenchmarkConsistentHash_GetNodeWithoutCache runs the benchmark consistent hash_get node without cache operation.
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

// BenchmarkConsistentHash_CacheHitRate runs the benchmark consistent hash_cache hit rate operation.
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

// TestConsistentHash_UpdateVNodeCount runs the test consistent hash_update v node count operation.
func TestConsistentHash_UpdateVNodeCount(t *testing.T) {
	c := NewConsistentHash(10)
	c.AddNode("node1")
	c.AddNode("node2")

	// Calculate initial distribution
	counts1 := make(map[string]int)
	for i := 0; i < 1000; i++ {
		counts1[c.GetNode(fmt.Sprintf("key-%d", i))]++
	}

	// Update node1 to have many more vnodes
	c.UpdateVNodeCount("node1", 100)

	// Calculate new distribution
	counts2 := make(map[string]int)
	for i := 0; i < 1000; i++ {
		counts2[c.GetNode(fmt.Sprintf("key-%d", i))]++
	}

	// node1 should have gained a significant share of keys
	assert.Greater(t, counts2["node1"], counts1["node1"], "node1 should have more keys after increasing vnodes")
	assert.Less(t, counts2["node2"], counts1["node2"], "node2 should have fewer keys after node1 vnodes increased")
}
