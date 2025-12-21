package sharding

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// ConsistentHash implements a consistent hashing ring
type ConsistentHash struct {
	mu           sync.RWMutex
	nodes        map[uint64]string // hash -> nodeID
	sortedHashes []uint64          // sorted hash values
	vnodes       int               // virtual nodes per real node
}

// NewConsistentHash creates a new ring with specified vnodes
func NewConsistentHash(vnodes int) *ConsistentHash {
	return &ConsistentHash{
		nodes:  make(map[uint64]string),
		vnodes: vnodes,
	}
}

// AddNode adds a new node to the ring
func (c *ConsistentHash) AddNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < c.vnodes; i++ {
		h := c.hash(fmt.Sprintf("%s:%d", nodeID, i))
		c.nodes[h] = nodeID
		c.sortedHashes = append(c.sortedHashes, h)
	}
	sort.Slice(c.sortedHashes, func(i, j int) bool {
		return c.sortedHashes[i] < c.sortedHashes[j]
	})
}

// RemoveNode removes a node from the ring
func (c *ConsistentHash) RemoveNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	newHashes := make([]uint64, 0, len(c.sortedHashes))
	for _, h := range c.sortedHashes {
		if c.nodes[h] == nodeID {
			delete(c.nodes, h)
		} else {
			newHashes = append(newHashes, h)
		}
	}
	c.sortedHashes = newHashes
}

// GetNode returns the node responsible for the given key
func (c *ConsistentHash) GetNode(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.sortedHashes) == 0 {
		return ""
	}

	h := c.hash(key)
	idx := sort.Search(len(c.sortedHashes), func(i int) bool {
		return c.sortedHashes[i] >= h
	})

	if idx == len(c.sortedHashes) {
		idx = 0
	}

	return c.nodes[c.sortedHashes[idx]]
}

// GetPreferenceList returns the top N distinct nodes for a key (Replication Strategy)
func (c *ConsistentHash) GetPreferenceList(key string, n int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.sortedHashes) == 0 {
		return nil
	}

	h := c.hash(key)
	idx := sort.Search(len(c.sortedHashes), func(i int) bool {
		return c.sortedHashes[i] >= h
	})

	seen := make(map[string]bool)
	result := make([]string, 0, n)

	// Walk around the ring
	start := idx
	if start == len(c.sortedHashes) {
		start = 0
	}

	for i := 0; i < len(c.sortedHashes); i++ {
		currIdx := (start + i) % len(c.sortedHashes)
		nodeID := c.nodes[c.sortedHashes[currIdx]]
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

func (c *ConsistentHash) hash(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}
