package store

import (
	"context"
	"container/heap"
	"container/list"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"math"

	"github.com/23skdu/longbow/internal/simd"
	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
)

const (
	// TemporalShards is the number of shards for the temporal vector map.
	TemporalShards = 128
)

type temporalShard struct {
	mu   sync.RWMutex
	data map[uint64]*TemporalVector
}

var (
	temporalIDMapPool = sync.Pool{
		New: func() any {
			return make(map[uint64]struct{}, 1024)
		},
	}

	temporalVersionMapPool = sync.Pool{
		New: func() any {
			return make(map[uint64]VersionedVector, 1024)
		},
	}
)

type temporalScoredResult struct {
	id       uint64
	distance float32
}

// TemporalConfig defines the configuration for temporal indexing and retention.
type TemporalConfig struct {
	// Enabled indicates whether temporal features are active.
	Enabled            bool
	// VersionHistory indicates whether to keep a full history of vector updates.
	VersionHistory     bool
	// MaxVersions is the maximum number of versions to keep per vector.
	MaxVersions        int
	// RetentionPeriod is the duration for which temporal data is kept.
	RetentionPeriod    time.Duration
	// TTLEnabled indicates whether Time-To-Live (TTL) is active for vectors.
	TTLEnabled         bool
	// DefaultTTL is the default duration a vector remains valid.
	DefaultTTL         time.Duration
	// CleanupInterval is the frequency of background cleanup tasks.
	CleanupInterval    time.Duration
	// AggregationEnabled indicates whether temporal aggregation features are active.
	AggregationEnabled bool
	// MaxBuckets is the maximum number of buckets for temporal aggregation.
	MaxBuckets         int
}

// DefaultTemporalConfig returns a default configuration for temporal features.
func DefaultTemporalConfig() TemporalConfig {
	return TemporalConfig{
		Enabled:            false,
		VersionHistory:     false,
		MaxVersions:        10,
		RetentionPeriod:    7 * 24 * time.Hour,
		TTLEnabled:         false,
		DefaultTTL:         30 * 24 * time.Hour,
		CleanupInterval:    time.Hour,
		AggregationEnabled: false,
		MaxBuckets:         1000,
	}
}

// TemporalVector represents a vector with an associated timestamp for temporal search.
type TemporalVector struct {
	ID        uint64
	Vector    []float32
	Norm      float32 // Pre-calculated norm
	Timestamp int64
	Metadata  []byte
	Tombstone bool
}

// TemporalIndex provides efficient search and retrieval based on vector timestamps.
type TemporalIndex struct {
	mu           sync.Mutex
	dimension    int
	shards       [TemporalShards]temporalShard
	temporalTree atomic.Pointer[TemporalTree]
	history      *VersionHistory
	cache        *TemporalResultCache
	pointCount   atomic.Int64
	gpuIndex     atomic.Value // holds gputypes.Index
}

// TemporalResultCache provides LRU caching for temporal search results.
type TemporalResultCache struct {
	mu        sync.Mutex
	items     map[string]*list.Element
	evict     *list.List
	max       int
	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
}

type temporalCacheEntry struct {
	key     string
	results []lbtypes.SearchResult
	expiry  time.Time
}

// NewTemporalResultCache creates a new TemporalResultCache with the specified capacity.
func NewTemporalResultCache(size int) *TemporalResultCache {
	return &TemporalResultCache{
		items: make(map[string]*list.Element),
		evict: list.New(),
		max:   size,
	}
}

// Get retrieves search results from the cache if they exist and are not expired.
func (c *TemporalResultCache) Get(key string) ([]lbtypes.SearchResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.items[key]
	if !ok {
		c.misses.Add(1)
		return nil, false
	}

	entry := element.Value.(*temporalCacheEntry)
	if time.Now().After(entry.expiry) {
		c.evict.Remove(element)
		delete(c.items, key)
		c.evictions.Add(1)
		c.misses.Add(1)
		return nil, false
	}

	c.evict.MoveToFront(element)
	c.hits.Add(1)
	return entry.results, true
}

// Set adds search results to the cache with the specified TTL.
func (c *TemporalResultCache) Set(key string, results []lbtypes.SearchResult, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if element, ok := c.items[key]; ok {
		c.evict.MoveToFront(element)
		entry := element.Value.(*temporalCacheEntry)
		entry.results = results
		entry.expiry = time.Now().Add(ttl)
		return
	}

	entry := &temporalCacheEntry{
		key:     key,
		results: results,
		expiry:  time.Now().Add(ttl),
	}
	element := c.evict.PushFront(entry)
	c.items[key] = element

	if c.evict.Len() > c.max {
		oldest := c.evict.Back()
		if oldest != nil {
			c.evict.Remove(oldest)
			delete(c.items, oldest.Value.(*temporalCacheEntry).key)
			c.evictions.Add(1)
		}
	}
}

type TemporalTree struct {
	mu             sync.RWMutex
	nodeArena      *memory.TypedArena[TemporalNode]
	entryArena     *memory.TypedArena[TemporalEntry]
	nodeBaseOffset uint64
	nodeCount      atomic.Uint32
	minTs          int64
	maxTs          int64
	
	// Optional: Metadata for fast range skipping
	chunks      []temporalChunkMetadata
}

type temporalChunkMetadata struct {
	minTs int64
	maxTs int64
	start uint32
	end   uint32
}

const nodesPerChunk = 1024

// TemporalEntry represents a vector ID and its norm at a specific point in time.
type TemporalEntry struct {
	ID   uint64
	Norm float32
}

// TemporalNode represents a set of vector IDs sharing a specific timestamp.
type TemporalNode struct {
	Timestamp int64
	Offset    uint32 // Offset into entryArena
	Len       uint32 // Number of entries for this timestamp
}

// NewTemporalTree creates a new TemporalTree instance with an optional arena.
func NewTemporalTree(arena *memory.SlabArena) *TemporalTree {
	if arena == nil {
		arena = memory.NewSlabArena(4 * 1024 * 1024) // 4MB default
	}
	return &TemporalTree{
		nodeArena:  memory.NewTypedArena[TemporalNode](arena),
		entryArena: memory.NewTypedArena[TemporalEntry](arena),
		chunks:     make([]temporalChunkMetadata, 0),
		minTs:      math.MaxInt64,
		maxTs:      math.MinInt64,
	}
}

func (tt *TemporalTree) Release() {
	if tt.nodeArena != nil {
		tt.nodeArena.Release()
	}
	if tt.entryArena != nil {
		tt.entryArena.Release()
	}
}

// Insert adds a vector ID and its norm to the tree at the specified timestamp.
func (tt *TemporalTree) Insert(timestamp int64, id uint64, norm float32) {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	entry := TemporalEntry{ID: id, Norm: norm}
	
	// Update global min/max
	if timestamp < tt.minTs { tt.minTs = timestamp }
	if timestamp > tt.maxTs { tt.maxTs = timestamp }

	nodeCount := tt.nodeCount.Load()
	
	var nodes []TemporalNode
	if nodeCount > 0 {
		ref := memory.SliceRef{Offset: tt.nodeBaseOffset, Len: nodeCount, Cap: nodeCount}
		nodes = tt.nodeArena.Get(ref)
	}

	idx := sort.Search(int(nodeCount), func(i int) bool {
		return nodes[i].Timestamp >= timestamp
	})

	if idx < int(nodeCount) && nodes[idx].Timestamp == timestamp {
		// Existing node: append to entryArena
		oldOffset := nodes[idx].Offset
		oldLen := nodes[idx].Len
		
		newRef, _ := tt.entryArena.AllocSlice(int(oldLen + 1))
		newEntries := tt.entryArena.Get(newRef)
		oldEntriesRef := memory.SliceRef{Offset: uint64(oldOffset), Len: oldLen, Cap: oldLen}
		oldEntries := tt.entryArena.Get(oldEntriesRef)
		
		copy(newEntries, oldEntries)
		newEntries[oldLen] = entry
		
		// Update existing node in place (it's already in the arena)
		nodes[idx].Offset = uint32(newRef.Offset) // #nosec G115
		nodes[idx].Len = oldLen + 1
		return
	}

	// New node: must re-allocate entire node list to maintain contiguity in arena
	newEntryRef, _ := tt.entryArena.AllocSlice(1)
	tt.entryArena.Get(newEntryRef)[0] = entry
	
	newNode := TemporalNode{
		Timestamp: timestamp,
		Offset:    uint32(newEntryRef.Offset), // #nosec G115
		Len:       1,
	}

	// Re-allocate and copy all nodes
	ref, _ := tt.nodeArena.AllocSlice(int(nodeCount + 1))
	newNodes := tt.nodeArena.Get(ref)
	if nodeCount > 0 {
		copy(newNodes[:idx], nodes[:idx])
		newNodes[idx] = newNode
		copy(newNodes[idx+1:], nodes[idx:])
	} else {
		newNodes[0] = newNode
	}
	
	tt.nodeBaseOffset = ref.Offset
	tt.nodeCount.Add(1)
	
	metrics.TemporalTreeNodesTotal.Set(float64(tt.nodeCount.Load()))
	stats := tt.nodeArena.Slab().Stats()
	metrics.TemporalTreeAllocatedBytesTotal.Set(float64(stats.TotalCapacity))
	
	if tt.nodeCount.Load()%nodesPerChunk == 0 {
		tt.rebuildChunks()
	}
}

func (tt *TemporalTree) rebuildChunks() {
	count := tt.nodeCount.Load()
	numChunks := (count + nodesPerChunk - 1) / nodesPerChunk
	tt.chunks = make([]temporalChunkMetadata, numChunks)
	
	ref := memory.SliceRef{Offset: tt.nodeBaseOffset, Len: count, Cap: count}
	nodes := tt.nodeArena.Get(ref)
	
	for i := uint32(0); i < numChunks; i++ {
		start := i * nodesPerChunk
		end := start + nodesPerChunk
		if end > count { end = count }
		
		tt.chunks[i] = temporalChunkMetadata{
			minTs: nodes[start].Timestamp,
			maxTs: nodes[end-1].Timestamp,
			start: start,
			end:   end,
		}
	}
}

// InsertBatch adds multiple vector IDs and their norms to the tree.
func (tt *TemporalTree) InsertBatch(timestamps []int64, ids []uint64, norms []float32) {
	if len(timestamps) == 0 {
		return
	}
	// For simplicity and correctness with splits, we call Insert for each
	for i := range timestamps {
		tt.Insert(timestamps[i], ids[i], norms[i])
	}
}

// GetRange returns all vector IDs within the specified timestamp range.
func (tt *TemporalTree) GetRange(start, end int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	nodeCount := tt.nodeCount.Load()
	if nodeCount == 0 { return nil }

	ref := memory.SliceRef{Offset: tt.nodeBaseOffset, Len: nodeCount, Cap: nodeCount}
	nodes := tt.nodeArena.Get(ref)

	startIdx := sort.Search(int(nodeCount), func(i int) bool {
		return nodes[i].Timestamp >= start
	})

	var results []uint64
	for i := startIdx; i < int(nodeCount); i++ {
		node := &nodes[i]
		if node.Timestamp > end { break }
		
		metrics.TemporalQueryScannedNodesTotal.Add(1)
		
		entryRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Len}
		entries := tt.entryArena.Get(entryRef)
		for _, e := range entries {
			results = append(results, e.ID)
		}
	}
	return results
}

// GetRangeReversed returns all vector IDs within the specified timestamp range in descending order.
func (tt *TemporalTree) GetRangeReversed(start, end int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	nodeCount := tt.nodeCount.Load()
	if nodeCount == 0 { return nil }

	ref := memory.SliceRef{Offset: tt.nodeBaseOffset, Len: nodeCount, Cap: nodeCount}
	nodes := tt.nodeArena.Get(ref)

	// Find the first node > end, then go backwards
	idx := sort.Search(int(nodeCount), func(i int) bool {
		return nodes[i].Timestamp > end
	})

	var results []uint64
	for i := idx - 1; i >= 0; i-- {
		node := &nodes[i]
		if node.Timestamp < start { break }
		
		metrics.TemporalQueryScannedNodesTotal.Add(1)
		
		entryRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Len}
		entries := tt.entryArena.Get(entryRef)
		for j := len(entries) - 1; j >= 0; j-- {
			results = append(results, entries[j].ID)
		}
	}
	return results
}

// GetUniqueIDsInRange returns unique vector IDs within the specified timestamp range, 
// keeping only the most recent version of each ID.
func (tt *TemporalTree) GetUniqueIDsInRange(start, end int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	nodeCount := tt.nodeCount.Load()
	if nodeCount == 0 { return nil }

	ref := memory.SliceRef{Offset: tt.nodeBaseOffset, Len: nodeCount, Cap: nodeCount}
	nodes := tt.nodeArena.Get(ref)

	// Find the end point
	idx := sort.Search(int(nodeCount), func(i int) bool {
		return nodes[i].Timestamp > end
	})

	uniqueIDs := temporalIDMapPool.Get().(map[uint64]struct{})
	defer func() {
		clear(uniqueIDs)
		temporalIDMapPool.Put(uniqueIDs)
	}()

	var results []uint64
	for i := idx - 1; i >= 0; i-- {
		node := &nodes[i]
		if node.Timestamp < start { break }
		
		metrics.TemporalQueryScannedNodesTotal.Add(1)
		
		entryRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Len}
		entries := tt.entryArena.Get(entryRef)
		for j := len(entries) - 1; j >= 0; j-- {
			id := entries[j].ID
			if _, exists := uniqueIDs[id]; !exists {
				results = append(results, id)
				uniqueIDs[id] = struct{}{}
			}
		}
	}
	return results
}

// GetBefore returns all vector IDs with timestamps before the specified value.
func (tt *TemporalTree) GetBefore(timestamp int64) []uint64 {
	return tt.GetRange(0, timestamp-1)
}

// GetAfter returns all vector IDs with timestamps after the specified value.
func (tt *TemporalTree) GetAfter(timestamp int64) []uint64 {
	return tt.GetRange(timestamp+1, math.MaxInt64)
}


// GetEarliest returns the vector IDs from the first n timestamps.
func (tt *TemporalTree) GetEarliest(n int) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	nodeCount := tt.nodeCount.Load()
	if nodeCount == 0 { return nil }

	ref := memory.SliceRef{Offset: tt.nodeBaseOffset, Len: nodeCount, Cap: nodeCount}
	nodes := tt.nodeArena.Get(ref)

	var results []uint64
	remaining := n
	for i := 0; i < int(nodeCount) && remaining > 0; i++ {
		node := &nodes[i]
		metrics.TemporalQueryScannedNodesTotal.Add(1)
		
		entryRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Len}
		entries := tt.entryArena.Get(entryRef)
		for _, e := range entries {
			results = append(results, e.ID)
			remaining--
			if remaining <= 0 { break }
		}
	}
	return results
}

// GetLatest returns the vector IDs from the last n timestamps.
func (tt *TemporalTree) GetLatest(n int) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	nodeCount := tt.nodeCount.Load()
	if nodeCount == 0 { return nil }

	ref := memory.SliceRef{Offset: tt.nodeBaseOffset, Len: nodeCount, Cap: nodeCount}
	nodes := tt.nodeArena.Get(ref)

	var results []uint64
	remaining := n
	for i := int(nodeCount) - 1; i >= 0 && remaining > 0; i-- {
		node := &nodes[i]
		metrics.TemporalQueryScannedNodesTotal.Add(1)
		
		entryRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Len}
		entries := tt.entryArena.Get(entryRef)
		for j := len(entries) - 1; j >= 0; j-- {
			results = append(results, entries[j].ID)
			remaining--
			if remaining <= 0 { break }
		}
	}
	return results
}

// GetUniqueLatest returns the n most recent unique vector IDs.
func (tt *TemporalTree) GetUniqueLatest(n int) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	nodeCount := tt.nodeCount.Load()
	if nodeCount == 0 { return nil }

	ref := memory.SliceRef{Offset: tt.nodeBaseOffset, Len: nodeCount, Cap: nodeCount}
	nodes := tt.nodeArena.Get(ref)

	uniqueIDs := temporalIDMapPool.Get().(map[uint64]struct{})
	defer func() {
		clear(uniqueIDs)
		temporalIDMapPool.Put(uniqueIDs)
	}()

	var results []uint64
	for i := int(nodeCount) - 1; i >= 0 && len(results) < n; i-- {
		node := &nodes[i]
		metrics.TemporalQueryScannedNodesTotal.Add(1)
		
		entryRef := memory.SliceRef{Offset: uint64(node.Offset), Len: node.Len, Cap: node.Len}
		entries := tt.entryArena.Get(entryRef)
		for j := len(entries) - 1; j >= 0; j-- {
			id := entries[j].ID
			if _, exists := uniqueIDs[id]; !exists {
				results = append(results, id)
				uniqueIDs[id] = struct{}{}
				if len(results) >= n { break }
			}
		}
	}
	return results
}

// Len returns the number of unique timestamps in the tree.
func (tt *TemporalTree) Len() int {
	return int(tt.nodeCount.Load())
}

// NewTemporalIndex creates a new TemporalIndex instance.
func NewTemporalIndex(dimension int) *TemporalIndex {
	ti := &TemporalIndex{
		dimension: dimension,
		cache:     NewTemporalResultCache(1024),
		history:   NewVersionHistory(DefaultVersionHistoryConfig()),
	}
	for i := 0; i < TemporalShards; i++ {
		ti.shards[i].data = make(map[uint64]*TemporalVector)
	}
	arena := memory.NewSlabArena(1024 * 1024)
	ti.temporalTree.Store(NewTemporalTree(arena))
	return ti
}

// getShard returns the shard for a given ID.
func (ti *TemporalIndex) getShard(id uint64) *temporalShard {
	return &ti.shards[id%uint64(TemporalShards)]
}

// Close releases resources associated with the temporal index.
func (ti *TemporalIndex) Close() error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	// Clear the sharded maps
	for i := 0; i < TemporalShards; i++ {
		ti.shards[i].mu.Lock()
		ti.shards[i].data = nil
		ti.shards[i].mu.Unlock()
	}

	if gpuIdx := ti.gpuIndex.Load(); gpuIdx != nil {
		if gi, ok := gpuIdx.(gputypes.Index); ok {
			_ = gi.Close()
		}
		ti.gpuIndex.Store(nil)
	}

	ti.pointCount.Store(0)
	return nil
}

// Add inserts a new vector with a timestamp into the temporal index.
func (ti *TemporalIndex) Add(id uint64, vector []float32, timestamp int64, metadata []byte) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	if len(vector) != ti.dimension {
		return fmt.Errorf("dimension mismatch: expected %d, got %d", ti.dimension, len(vector))
	}

	norm := ti.computeNorm(vector)
	vec := &TemporalVector{
		ID:        id,
		Vector:    vector,
		Norm:      norm,
		Timestamp: timestamp,
		Metadata:  metadata,
		Tombstone: false,
	}

	shard := ti.getShard(id)
	shard.mu.Lock()
	shard.data[id] = vec
	shard.mu.Unlock()
	tree := ti.temporalTree.Load()
	if tree != nil {
		tree.Insert(timestamp, id, norm)
	}
	ti.pointCount.Add(1)

	if ti.history != nil {
		ti.history.Add(id, vector, norm, timestamp, metadata)
	}

	return nil
}

// AddBatch inserts multiple vectors into the TemporalIndex.
func (ti *TemporalIndex) AddBatch(ids []uint64, vectors [][]float32, timestamps []int64, metadata [][]byte) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	tree := ti.temporalTree.Load()
	norms := make([]float32, len(ids))
	for i := range ids {
		if len(vectors[i]) != ti.dimension {
			continue
		}

		var m []byte
		if i < len(metadata) {
			m = metadata[i]
		}
		
		norm := ti.computeNorm(vectors[i])
		norms[i] = norm
		vec := &TemporalVector{
			ID:        ids[i],
			Vector:    vectors[i],
			Norm:      norm,
			Timestamp: timestamps[i],
			Metadata:  m,
			Tombstone: false,
		}

		shard := ti.getShard(ids[i])
		shard.mu.Lock()
		shard.data[ids[i]] = vec
		shard.mu.Unlock()
		if ti.history != nil {
			ti.history.Add(ids[i], vectors[i], norm, timestamps[i], m)
		}
	}
	
	if tree != nil {
		tree.InsertBatch(timestamps, ids, norms)
	}
	ti.pointCount.Add(int64(len(ids)))

	return nil
}

// Delete marks a vector as deleted (tombstoned) in the temporal index.
func (ti *TemporalIndex) Delete(id uint64) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	shard := ti.getShard(id)
	shard.mu.RLock()
	vec, ok := shard.data[id]
	shard.mu.RUnlock()
	if !ok {
		return fmt.Errorf("vector id %d not found", id)
	}

	newVec := *vec
	newVec.Tombstone = true
	shard.mu.Lock()
	shard.data[id] = &newVec
	shard.mu.Unlock()
	// We don't decrement pointCount here because it's a tombstone
	return nil
}

// Update updates an existing vector and metadata at a new timestamp.
func (ti *TemporalIndex) Update(id uint64, vector []float32, timestamp int64, metadata []byte) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	shard := ti.getShard(id)
	shard.mu.RLock()
	oldVec, ok := shard.data[id]
	shard.mu.RUnlock()
	if !ok {
		return fmt.Errorf("vector id %d not found", id)
	}

	newOldVec := *oldVec
	newOldVec.Tombstone = true
	shard.mu.Lock()
	shard.data[id] = &newOldVec
	shard.mu.Unlock()

	norm := ti.computeNorm(vector)
	newVec := &TemporalVector{
		ID:        id,
		Vector:    vector,
		Norm:      norm,
		Timestamp: timestamp,
		Metadata:  metadata,
		Tombstone: false,
	}

	shard.mu.Lock()
	shard.data[id] = newVec
	shard.mu.Unlock()
	tree := ti.temporalTree.Load()
	if tree != nil {
		tree.Insert(timestamp, id, norm)
	}
	
	if ti.history != nil {
		ti.history.Add(id, vector, norm, timestamp, metadata)
	}

	return nil
}

// SearchAsOf performs a vector search considering only data available at a specific timestamp.
func (ti *TemporalIndex) SearchAsOf(ctx context.Context, timestamp int64, k int) ([]lbtypes.SearchResult, error) {
	cacheKey := fmt.Sprintf("asof:%d:%d", timestamp, k)
	if results, ok := ti.cache.Get(cacheKey); ok {
		return results, nil
	}

	tree := ti.temporalTree.Load()
	if tree == nil {
		return []lbtypes.SearchResult{}, nil
	}

	// Get latest version of each unique ID before or at timestamp
	validIDs := tree.GetUniqueIDsInRange(0, timestamp)
	if len(validIDs) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	// 2. Batch lookup versions from history
	versionMap := temporalVersionMapPool.Get().(map[uint64]VersionedVector)
	defer func() {
		clear(versionMap)
		temporalVersionMapPool.Put(versionMap)
	}()

	if ti.history != nil {
		ti.history.GetVersionsAtBatch(validIDs, timestamp, versionMap)
	}

	// 3. Select top-k using max-heap
	h := &temporalMaxHeap{}
	heap.Init(h)

	for _, id := range validIDs {
		var norm float32
		var found bool
		
		if v, ok := versionMap[id]; ok {
			norm = v.Norm
			found = true
		}
		
		// Fallback to latest norm if history lookup failed or disabled
		if !found {
			shard := ti.getShard(id)
			shard.mu.RLock()
			vec, ok := shard.data[id]
			shard.mu.RUnlock()
			if ok {
				if !vec.Tombstone {
					norm = vec.Norm
					found = true
				}
			}
		}
		
		if found {
			if h.Len() < k {
				heap.Push(h, temporalScoredResult{id: id, distance: norm})
			} else if norm < (*h)[0].distance {
				heap.Pop(h)
				heap.Push(h, temporalScoredResult{id: id, distance: norm})
			}
		}
	}

	limit := h.Len()
	searchResults := make([]lbtypes.SearchResult, limit)
	for i := limit - 1; i >= 0; i-- {
		res := heap.Pop(h).(temporalScoredResult)
		searchResults[i] = lbtypes.SearchResult{
			ID:       lbtypes.VectorID(res.id), // #nosec G115
			Distance: res.distance,
			Score:    1.0 / (1.0 + res.distance),
		}
	}

	ti.cache.Set(cacheKey, searchResults, 5*time.Minute)
	return searchResults, nil
}

// Prewarm populates the temporal cache with common search results.
func (ti *TemporalIndex) Prewarm(ctx context.Context) error {
	tree := ti.temporalTree.Load()
	if tree == nil {
		return nil
	}

	tree.mu.RLock()
	if len(tree.chunks) == 0 {
		tree.mu.RUnlock()
		return nil
	}
	latestTs := tree.maxTs
	tree.mu.RUnlock()

	// 1. Latest results
	_, _ = ti.SearchAsOf(ctx, latestTs, 100)

	// 2. Common windows (if they contain data)
	now := time.Now().UnixNano()
	windows := []time.Duration{
		5 * time.Minute,
		1 * time.Hour,
		24 * time.Hour,
	}

	for _, d := range windows {
		start := now - d.Nanoseconds()
		_, _ = ti.SearchRange(ctx, start, now, 100)
	}

	return nil
}

// SearchRange performs a vector search over data within a specific timestamp range.
func (ti *TemporalIndex) SearchRange(ctx context.Context, startTime, endTime int64, k int) ([]lbtypes.SearchResult, error) {
	tree := ti.temporalTree.Load()
	if tree == nil {
		return []lbtypes.SearchResult{}, nil
	}

	// Get unique IDs in range (most recent versions within range)
	validIDs := tree.GetUniqueIDsInRange(startTime, endTime)
	if len(validIDs) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	// 2. Batch lookup versions from history
	versionMap := temporalVersionMapPool.Get().(map[uint64]VersionedVector)
	defer func() {
		clear(versionMap)
		temporalVersionMapPool.Put(versionMap)
	}()

	if ti.history != nil {
		ti.history.GetVersionsAtBatch(validIDs, endTime, versionMap)
	}

	// 3. Select top-k using max-heap
	h := &temporalMaxHeap{}
	heap.Init(h)

	for _, id := range validIDs {
		var norm float32
		var found bool
		
		if v, ok := versionMap[id]; ok && v.Timestamp >= startTime {
			norm = v.Norm
			found = true
		}
		
		// Fallback to latest norm if historylookup failed or disabled
		if !found {
			shard := ti.getShard(id)
			shard.mu.RLock()
			vec, ok := shard.data[id]
			shard.mu.RUnlock()
			if ok {
				if !vec.Tombstone && vec.Timestamp >= startTime && vec.Timestamp <= endTime {
					norm = vec.Norm
					found = true
				}
			}
		}
		
		if found {
			if h.Len() < k {
				heap.Push(h, temporalScoredResult{id: id, distance: norm})
			} else if norm < (*h)[0].distance {
				heap.Pop(h)
				heap.Push(h, temporalScoredResult{id: id, distance: norm})
			}
		}
	}

	limit := h.Len()
	searchResults := make([]lbtypes.SearchResult, limit)
	for i := limit - 1; i >= 0; i-- {
		res := heap.Pop(h).(temporalScoredResult)
		searchResults[i] = lbtypes.SearchResult{
			ID:       lbtypes.VectorID(res.id), // #nosec G115
			Distance: res.distance,
			Score:    1.0 / (1.0 + res.distance),
		}
	}

	return searchResults, nil
}

// SearchSlidingWindow performs a search over the last n vector updates.
func (ti *TemporalIndex) SearchSlidingWindow(ctx context.Context, windowSize int, k int) ([]lbtypes.SearchResult, error) {
	tree := ti.temporalTree.Load()
	if tree == nil {
		return []lbtypes.SearchResult{}, nil
	}

	// Use optimized unique latest retrieval
	validIDs := tree.GetUniqueLatest(windowSize)
	if len(validIDs) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	// 2. Select top-k using max-heap
	h := &temporalMaxHeap{}
	heap.Init(h)

	for _, id := range validIDs {
		shard := ti.getShard(id)
		shard.mu.RLock()
		vec, ok := shard.data[id]
		shard.mu.RUnlock()
		if ok {
			if !vec.Tombstone {
				norm := vec.Norm
				if h.Len() < k {
					heap.Push(h, temporalScoredResult{id: id, distance: norm})
				} else if norm < (*h)[0].distance {
					heap.Pop(h)
					heap.Push(h, temporalScoredResult{id: id, distance: norm})
				}
			}
		}
	}

	limit := h.Len()
	searchResults := make([]lbtypes.SearchResult, limit)
	for i := limit - 1; i >= 0; i-- {
		res := heap.Pop(h).(temporalScoredResult)
		searchResults[i] = lbtypes.SearchResult{
			ID:       lbtypes.VectorID(res.id), // #nosec G115
			Distance: res.distance,
			Score:    1.0 / (1.0 + res.distance),
		}
	}

	return searchResults, nil
}

// SearchSlidingWindowByTime performs a search over vector updates from the last duration.
func (ti *TemporalIndex) SearchSlidingWindowByTime(ctx context.Context, duration time.Duration, k int) ([]lbtypes.SearchResult, error) {
	tree := ti.temporalTree.Load()
	if tree == nil {
		return []lbtypes.SearchResult{}, nil
	}

	now := time.Now().UnixNano()
	start := now - duration.Nanoseconds()

	validIDs := tree.GetRange(start, now)
	if len(validIDs) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	// 2. Select top-k using max-heap
	h := &temporalMaxHeap{}
	heap.Init(h)

	for _, id := range validIDs {
		shard := ti.getShard(id)
		shard.mu.RLock()
		vec, ok := shard.data[id]
		shard.mu.RUnlock()
		if ok {
			if !vec.Tombstone {
				norm := vec.Norm
				if h.Len() < k {
					heap.Push(h, temporalScoredResult{id: id, distance: norm})
				} else if norm < (*h)[0].distance {
					heap.Pop(h)
					heap.Push(h, temporalScoredResult{id: id, distance: norm})
				}
			}
		}
	}

	limit := h.Len()
	searchResults := make([]lbtypes.SearchResult, limit)
	for i := limit - 1; i >= 0; i-- {
		res := heap.Pop(h).(temporalScoredResult)
		searchResults[i] = lbtypes.SearchResult{
			ID:       lbtypes.VectorID(res.id), // #nosec G115
			Distance: res.distance,
			Score:    1.0 / (1.0 + res.distance),
		}
	}

	return searchResults, nil
}

// DeleteByTime removes or tombstones vectors with timestamps before the specified value.
func (ti *TemporalIndex) DeleteByTime(ctx context.Context, beforeTimestamp int64) (int, error) {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	tree := ti.temporalTree.Load()
	if tree == nil {
		return 0, nil
	}
	toDelete := tree.GetBefore(beforeTimestamp)
	deleted := 0

	for _, id := range toDelete {
		shard := ti.getShard(id)
		shard.mu.RLock()
		vec, ok := shard.data[id]
		shard.mu.RUnlock()
		if ok {
			newVec := *vec
			newVec.Tombstone = true
			shard.mu.Lock()
			shard.data[id] = &newVec
			shard.mu.Unlock()
			deleted++
		}
	}

	return deleted, nil
}

// GetVersion retrieves the vector data for a specific ID as it existed at a given timestamp.
func (ti *TemporalIndex) GetVersion(id uint64, timestamp int64) ([]float32, bool) {
	shard := ti.getShard(id)
	shard.mu.RLock()
	vec, ok := shard.data[id]
	shard.mu.RUnlock()
	if !ok {
		return nil, false
	}

	if vec.Timestamp > timestamp {
		return nil, false
	}

	return vec.Vector, true
}

// GetHistory returns the temporal version history for a specific vector ID.
func (ti *TemporalIndex) GetHistory(id uint64) []TemporalVector {
	shard := ti.getShard(id)
	shard.mu.RLock()
	vec, ok := shard.data[id]
	shard.mu.RUnlock()
	if !ok {
		return nil
	}

	if vec.Timestamp == 0 {
		return nil
	}

	return []TemporalVector{*vec}
}

// SetGPUIndex sets the GPU acceleration index for this TemporalIndex.
func (ti *TemporalIndex) SetGPUIndex(idx gputypes.Index) {
	ti.gpuIndex.Store(idx)
}

// Size returns the total number of vectors in the temporal index.
func (ti *TemporalIndex) Size() int {
	return int(ti.pointCount.Load())
}

// ActiveCount returns the number of non-tombstoned vectors in the index.
func (ti *TemporalIndex) ActiveCount() int {
	count := 0
	for i := 0; i < TemporalShards; i++ {
		ti.shards[i].mu.RLock()
		for _, v := range ti.shards[i].data {
			if !v.Tombstone {
				count++
			}
		}
		ti.shards[i].mu.RUnlock()
	}
	return count
}

func (ti *TemporalIndex) computeNorm(v []float32) float32 {
	if len(v) == 0 {
		return 0
	}
	norm, _ := simd.DotProduct(v, v)
	return norm
}

// VectorTimestamp pairs a vector with its creation/update timestamp for JSON export.
type VectorTimestamp struct {
	ID        uint64                 `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	Vector    []float32              `json:"vector,omitempty"`
	Metadata  []byte                 `json:"metadata,omitempty"`
}

// GetVectorsInRange retrieves all vectors within a timestamp range as VectorTimestamp objects.
func (ti *TemporalIndex) GetVectorsInRange(startTime, endTime int64) []VectorTimestamp {
	tree := ti.temporalTree.Load()
	if tree == nil {
		return nil
	}
	ids := tree.GetRange(startTime, endTime)

	results := make([]VectorTimestamp, 0, len(ids))
	for _, id := range ids {
		shard := ti.getShard(id)
		shard.mu.RLock()
		vec, ok := shard.data[id]
		shard.mu.RUnlock()
		if !ok {
			continue
		}
		if vec.Tombstone {
			continue
		}
		results = append(results, VectorTimestamp{
			ID:        vec.ID,
			Timestamp: time.Unix(0, vec.Timestamp),
			Vector:    vec.Vector,
			Metadata:  vec.Metadata,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp.Before(results[j].Timestamp)
	})

	return results
}

// MarshalJSON provides custom JSON serialization for TemporalIndex.
func (ti *TemporalIndex) MarshalJSON() ([]byte, error) {
	type TempVec struct {
		ID        uint64    `json:"id"`
		Vector    []float32 `json:"vector"`
		Timestamp int64     `json:"timestamp"`
		Metadata  []byte    `json:"metadata,omitempty"`
		Tombstone bool      `json:"tombstone,omitempty"`
	}

	var vectors []TempVec
	for i := 0; i < TemporalShards; i++ {
		ti.shards[i].mu.RLock()
		for _, v := range ti.shards[i].data {
			vectors = append(vectors, TempVec{
				ID:        v.ID,
				Vector:    v.Vector,
				Timestamp: v.Timestamp,
				Metadata:  v.Metadata,
				Tombstone: v.Tombstone,
			})
		}
		ti.shards[i].mu.RUnlock()
	}

	return json.Marshal(struct {
		Dimension int       `json:"dimension"`
		Vectors   []TempVec `json:"vectors"`
	}{
		Dimension: ti.dimension,
		Vectors:   vectors,
	})
}

// temporalMaxHeap implements heap.Interface for top-k temporal results (Max-Heap by Distance).
type temporalMaxHeap []temporalScoredResult

func (h temporalMaxHeap) Len() int           { return len(h) }
func (h temporalMaxHeap) Less(i, j int) bool { return h[i].distance > h[j].distance }
func (h temporalMaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *temporalMaxHeap) Push(x any) {
	*h = append(*h, x.(temporalScoredResult))
}

func (h *temporalMaxHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
