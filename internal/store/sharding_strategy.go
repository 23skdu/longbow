package store

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// ShardingStrategy defines how vectors are assigned to shards.
type ShardingStrategy interface {
	// GetShard returns the shard index for a given vector ID.
	GetShard(id VectorID) int
	// ActiveShards returns the number of currently active shards.
	ActiveShards() int
}

// LinearSharding is the legacy strategy: ShardID = ID / Threshold.
type LinearSharding struct {
	threshold int
}

func NewLinearSharding(threshold int) *LinearSharding {
	if threshold <= 0 {
		threshold = 1
	}
	return &LinearSharding{threshold: threshold}
}

func (s *LinearSharding) GetShard(id VectorID) int {
	return int(uint64(id) / uint64(s.threshold)) // #nosec G115
}

func (s *LinearSharding) ActiveShards() int {
	// Linear sharding effectively has "infinite" shards, but active ones depend on max ID.
	// This method is less relevant for linear sharding's dynamic growth.
	return -1
}

// RingSharder implements consistent hashing for distributing VectorIDs across a fixed set of shards.
type RingSharder struct {
	mu           sync.RWMutex
	vnodes       int
	shardIDs     []int
	sortedHashes []uint32
	ring         map[uint32]int // hash -> shardID
}

// NewRingSharder creates a new RingSharder with the specified number of shards and vnodes per shard.
func NewRingSharder(numShards, vnodes int) *RingSharder {
	if vnodes <= 0 {
		vnodes = 20
	}
	rs := &RingSharder{
		vnodes: vnodes,
		ring:   make(map[uint32]int),
	}
	for i := 0; i < numShards; i++ {
		rs.addShard(i)
	}
	return rs
}

func (rs *RingSharder) addShard(shardID int) {
	for i := 0; i < rs.vnodes; i++ {
		h := rs.hash(shardID, i)
		rs.ring[h] = shardID
		rs.sortedHashes = append(rs.sortedHashes, h)
	}
	sort.Slice(rs.sortedHashes, func(i, j int) bool {
		return rs.sortedHashes[i] < rs.sortedHashes[j]
	})
	rs.shardIDs = append(rs.shardIDs, shardID)
}

// hash computes a lightweight hash (FNV-1a) for the shard+vnode key.
func (rs *RingSharder) hash(shardID, vnode int) uint32 {
	h := fnv.New32a()
	// nosec G104 - hash.Write on bytes never returns error
	_, _ = h.Write([]byte(strconv.Itoa(shardID))) // nosec G104
	_, _ = h.Write([]byte(":"))                   // nosec G104
	_, _ = h.Write([]byte(strconv.Itoa(vnode)))   // nosec G104
	return h.Sum32()
}

// hashID computes hash for VectorID.
func (rs *RingSharder) hashID(id VectorID) uint32 {
	h := fnv.New32a()
	// Simple int64 to byte conversion for speed
	var b [8]byte
	v := uint64(id)
	b[0] = byte(v) // #nosec G115
	b[1] = byte(v >> 8) // #nosec G115
	b[2] = byte(v >> 16) // #nosec G115
	b[3] = byte(v >> 24) // #nosec G115
	b[4] = byte(v >> 32) // #nosec G115
	b[5] = byte(v >> 40) // #nosec G115
	b[6] = byte(v >> 48) // #nosec G115
	b[7] = byte(v >> 56)
	_, _ = h.Write(b[:]) // nosec G104
	return h.Sum32()
}

func (rs *RingSharder) GetShard(id VectorID) int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if len(rs.sortedHashes) == 0 {
		return 0
	}

	h := rs.hashID(id)
	idx := sort.Search(len(rs.sortedHashes), func(i int) bool {
		return rs.sortedHashes[i] >= h
	})

	if idx == len(rs.sortedHashes) {
		idx = 0
	}

	return rs.ring[rs.sortedHashes[idx]]
}

func (rs *RingSharder) ActiveShards() int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return len(rs.shardIDs)
}
