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
	internalcore "github.com/23skdu/longbow/internal/store/internal/core"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

var (
	temporalIDMapPool = sync.Pool{
		New: func() any {
			return make(map[uint64]struct{}, 1024)
		},
	}
	temporalScoredResultPool = sync.Pool{
		New: func() any {
			return make([]temporalScoredResult, 0, 1024)
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
	vectors      sync.Map
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
	mu     sync.RWMutex
	chunks []*temporalChunk
}

type temporalChunk struct {
	nodes []TemporalNode
	minTs int64
	maxTs int64
}

const maxNodesPerChunk = 1024

// TemporalNode represents a set of vector IDs sharing a specific timestamp.
// Aligned to 64 bytes to improve cache locality during range scans.
type TemporalNode struct {
	Timestamp int64
	VectorIDs []uint64
	_         [32]byte // Padding to exactly 64 bytes (8 + 24 + 32)
}

// NewTemporalTree creates a new TemporalTree instance.
func NewTemporalTree() *TemporalTree {
	return &TemporalTree{
		chunks: make([]*temporalChunk, 0),
	}
}

// Insert adds a vector ID to the tree at the specified timestamp.
func (tt *TemporalTree) Insert(timestamp int64, id uint64) {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	if len(tt.chunks) == 0 {
		tt.chunks = append(tt.chunks, &temporalChunk{
			nodes: []TemporalNode{{Timestamp: timestamp, VectorIDs: []uint64{id}}},
			minTs: timestamp,
			maxTs: timestamp,
		})
		return
	}

	// 1. Find the target chunk
	idx := sort.Search(len(tt.chunks), func(i int) bool {
		return tt.chunks[i].maxTs >= timestamp
	})

	if idx == len(tt.chunks) {
		idx = len(tt.chunks) - 1
	}

	chunk := tt.chunks[idx]
	
	// 2. Insert into chunk
	nodeIdx := sort.Search(len(chunk.nodes), func(i int) bool {
		return chunk.nodes[i].Timestamp >= timestamp
	})

	if nodeIdx < len(chunk.nodes) && chunk.nodes[nodeIdx].Timestamp == timestamp {
		chunk.nodes[nodeIdx].VectorIDs = append(chunk.nodes[nodeIdx].VectorIDs, id)
	} else {
		node := TemporalNode{
			Timestamp: timestamp,
			VectorIDs: []uint64{id},
		}
		
		if nodeIdx == len(chunk.nodes) {
			chunk.nodes = append(chunk.nodes, node)
		} else {
			chunk.nodes = append(chunk.nodes, TemporalNode{})
			copy(chunk.nodes[nodeIdx+1:], chunk.nodes[nodeIdx:])
			chunk.nodes[nodeIdx] = node
		}
		
		// Update chunk bounds
		if timestamp < chunk.minTs {
			chunk.minTs = timestamp
		}
		if timestamp > chunk.maxTs {
			chunk.maxTs = timestamp
		}

		// 3. Split chunk if too large
		if len(chunk.nodes) > maxNodesPerChunk {
			mid := len(chunk.nodes) / 2
			newChunk := &temporalChunk{
				nodes: chunk.nodes[mid:],
				minTs: chunk.nodes[mid].Timestamp,
				maxTs: chunk.nodes[len(chunk.nodes)-1].Timestamp,
			}
			chunk.nodes = chunk.nodes[:mid]
			chunk.maxTs = chunk.nodes[len(chunk.nodes)-1].Timestamp
			
			// Insert new chunk into tt.chunks
			tt.chunks = append(tt.chunks, nil)
			copy(tt.chunks[idx+2:], tt.chunks[idx+1:])
			tt.chunks[idx+1] = newChunk
		}
	}
}

// InsertBatch adds multiple vector IDs to the tree.
func (tt *TemporalTree) InsertBatch(timestamps []int64, ids []uint64) {
	if len(timestamps) == 0 {
		return
	}
	// For simplicity and correctness with splits, we call Insert for each
	for i := range timestamps {
		tt.Insert(timestamps[i], ids[i])
	}
}

// GetRange returns all vector IDs within the specified timestamp range.
func (tt *TemporalTree) GetRange(start, end int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if len(tt.chunks) == 0 {
		return nil
	}

	startChunkIdx := sort.Search(len(tt.chunks), func(i int) bool {
		return tt.chunks[i].maxTs >= start
	})

	var results []uint64
	for i := startChunkIdx; i < len(tt.chunks); i++ {
		chunk := tt.chunks[i]
		if chunk.minTs > end {
			break
		}
		
		nodeIdx := sort.Search(len(chunk.nodes), func(j int) bool {
			return chunk.nodes[j].Timestamp >= start
		})
		
		for j := nodeIdx; j < len(chunk.nodes); j++ {
			node := &chunk.nodes[j]
			if node.Timestamp > end {
				return results // Done with the whole range
			}
			results = append(results, node.VectorIDs...)
		}
	}
	return results
}

// GetRangeReversed returns all vector IDs within the specified timestamp range in descending order.
func (tt *TemporalTree) GetRangeReversed(start, end int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if len(tt.chunks) == 0 {
		return nil
	}

	// Find the end chunk
	endChunkIdx := sort.Search(len(tt.chunks), func(i int) bool {
		return tt.chunks[i].maxTs >= end
	})
	if endChunkIdx == len(tt.chunks) {
		endChunkIdx = len(tt.chunks) - 1
	}

	var results []uint64
	for i := endChunkIdx; i >= 0; i-- {
		chunk := tt.chunks[i]
		if chunk.maxTs < start {
			break
		}
		
		for j := len(chunk.nodes) - 1; j >= 0; j-- {
			ts := chunk.nodes[j].Timestamp
			if ts >= start && ts <= end {
				results = append(results, chunk.nodes[j].VectorIDs...)
			} else if ts < start {
				return results
			}
		}
	}
	return results
}

// GetUniqueIDsInRange returns unique vector IDs within the specified timestamp range, 
// keeping only the most recent version of each ID.
func (tt *TemporalTree) GetUniqueIDsInRange(start, end int64) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if len(tt.chunks) == 0 {
		return nil
	}

	// Find the end chunk
	endChunkIdx := sort.Search(len(tt.chunks), func(i int) bool {
		return tt.chunks[i].maxTs >= end
	})
	if endChunkIdx == len(tt.chunks) {
		endChunkIdx = len(tt.chunks) - 1
	}

	uniqueIDs := temporalIDMapPool.Get().(map[uint64]struct{})
	defer func() {
		clear(uniqueIDs)
		temporalIDMapPool.Put(uniqueIDs)
	}()

	var results []uint64
	
	for i := endChunkIdx; i >= 0; i-- {
		chunk := tt.chunks[i]
		if chunk.maxTs < start {
			break
		}
		
		for j := len(chunk.nodes) - 1; j >= 0; j-- {
			node := &chunk.nodes[j]
			if node.Timestamp >= start && node.Timestamp <= end {
				for _, id := range node.VectorIDs {
					if _, seen := uniqueIDs[id]; !seen {
						uniqueIDs[id] = struct{}{}
						results = append(results, id)
					}
				}
			} else if node.Timestamp < start {
				return results
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

	if len(tt.chunks) == 0 {
		return nil
	}

	var results []uint64
	remaining := n
	for i := 0; i < len(tt.chunks) && remaining > 0; i++ {
		chunk := tt.chunks[i]
		count := len(chunk.nodes)
		take := remaining
		if take > count {
			take = count
		}
		
		for j := 0; j < take; j++ {
			results = append(results, chunk.nodes[j].VectorIDs...)
		}
		remaining -= take
	}
	return results
}

// GetLatest returns the last n vector IDs added to the tree.
func (tt *TemporalTree) GetLatest(n int) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if len(tt.chunks) == 0 {
		return nil
	}

	var results []uint64
	remaining := n
	// Iterate backwards from the most recent chunk
	for i := len(tt.chunks) - 1; i >= 0 && remaining > 0; i-- {
		chunk := tt.chunks[i]
		nodes := chunk.nodes
		count := len(nodes)
		take := remaining
		if take > count {
			take = count
		}
		
		for j := count - 1; j >= count-take; j-- {
			results = append(results, nodes[j].VectorIDs...)
		}
		remaining -= take
	}
	return results
}

// GetUniqueLatest returns the last n unique vector IDs added to the tree.
func (tt *TemporalTree) GetUniqueLatest(n int) []uint64 {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if len(tt.chunks) == 0 {
		return nil
	}

	uniqueIDs := make(map[uint64]struct{})
	var results []uint64
	remaining := n
	
	for i := len(tt.chunks) - 1; i >= 0 && remaining > 0; i-- {
		chunk := tt.chunks[i]
		for j := len(chunk.nodes) - 1; j >= 0 && remaining > 0; j-- {
			for _, id := range chunk.nodes[j].VectorIDs {
				if _, seen := uniqueIDs[id]; !seen {
					uniqueIDs[id] = struct{}{}
					results = append(results, id)
					remaining--
					if remaining == 0 {
						break
					}
				}
			}
		}
	}
	return results
}

// Len returns the number of unique timestamps in the tree.
func (tt *TemporalTree) Len() int {
	tt.mu.RLock()
	defer tt.mu.RUnlock()
	total := 0
	for _, c := range tt.chunks {
		total += len(c.nodes)
	}
	return total
}

// NewTemporalIndex creates a new TemporalIndex instance.
func NewTemporalIndex(dimension int) *TemporalIndex {
	ti := &TemporalIndex{
		dimension: dimension,
		cache:     NewTemporalResultCache(1024),
		history:   NewVersionHistory(DefaultVersionHistoryConfig()),
	}
	ti.temporalTree.Store(NewTemporalTree())
	return ti
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

	ti.vectors.Store(id, vec)
	tree := ti.temporalTree.Load()
	if tree != nil {
		tree.Insert(timestamp, id)
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
	for i := range ids {
		if len(vectors[i]) != ti.dimension {
			continue
		}

		var m []byte
		if i < len(metadata) {
			m = metadata[i]
		}
		
		norm := ti.computeNorm(vectors[i])
		vec := &TemporalVector{
			ID:        ids[i],
			Vector:    vectors[i],
			Norm:      norm,
			Timestamp: timestamps[i],
			Metadata:  m,
			Tombstone: false,
		}

		ti.vectors.Store(ids[i], vec)
		if ti.history != nil {
			ti.history.Add(ids[i], vectors[i], norm, timestamps[i], m)
		}
	}
	
	if tree != nil {
		tree.InsertBatch(timestamps, ids)
	}
	ti.pointCount.Add(int64(len(ids)))

	return nil
}

// Delete marks a vector as deleted (tombstoned) in the temporal index.
func (ti *TemporalIndex) Delete(id uint64) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	val, ok := ti.vectors.Load(id)
	if !ok {
		return fmt.Errorf("vector id %d not found", id)
	}

	vec := val.(*TemporalVector)
	newVec := *vec
	newVec.Tombstone = true
	ti.vectors.Store(id, &newVec)
	// We don't decrement pointCount here because it's a tombstone
	return nil
}

// Update updates an existing vector and metadata at a new timestamp.
func (ti *TemporalIndex) Update(id uint64, vector []float32, timestamp int64, metadata []byte) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	val, ok := ti.vectors.Load(id)
	if !ok {
		return fmt.Errorf("vector id %d not found", id)
	}

	oldVec := val.(*TemporalVector)
	newOldVec := *oldVec
	newOldVec.Tombstone = true
	ti.vectors.Store(id, &newOldVec)

	norm := ti.computeNorm(vector)
	newVec := &TemporalVector{
		ID:        id,
		Vector:    vector,
		Norm:      norm,
		Timestamp: timestamp,
		Metadata:  metadata,
		Tombstone: false,
	}

	ti.vectors.Store(id, newVec)
	tree := ti.temporalTree.Load()
	if tree != nil {
		tree.Insert(timestamp, id)
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
			if val, ok := ti.vectors.Load(id); ok {
				vec := val.(*TemporalVector)
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
	lastChunk := tree.chunks[len(tree.chunks)-1]
	latestTs := lastChunk.nodes[len(lastChunk.nodes)-1].Timestamp
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
			if val, ok := ti.vectors.Load(id); ok {
				vec := val.(*TemporalVector)
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
		if val, ok := ti.vectors.Load(id); ok {
			vec := val.(*TemporalVector)
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

	// 2. Select top-k using max-heap
	h := &temporalMaxHeap{}
	heap.Init(h)

	results := make([]temporalScoredResult, 0, len(validIDs))
	var resMu sync.Mutex
	pool := internalcore.GetSharedPool()

	pool.ParallelFor(len(validIDs), 1024, func(start, end int) {
		localResults := make([]temporalScoredResult, 0, end-start)
		for i := start; i < end; i++ {
			id := validIDs[i]
			val, ok := ti.vectors.Load(id)
			if !ok {
				continue
			}
			vec := val.(*TemporalVector)
			if vec.Tombstone {
				continue
			}
			localResults = append(localResults, temporalScoredResult{id: id, distance: vec.Norm})
		}
		if len(localResults) > 0 {
			resMu.Lock()
			results = append(results, localResults...)
			resMu.Unlock()
		}
	})

	for _, res := range results {
		if h.Len() < k {
			heap.Push(h, res)
		} else if res.distance < (*h)[0].distance {
			heap.Pop(h)
			heap.Push(h, res)
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
		if val, ok := ti.vectors.Load(id); ok {
			vec := val.(*TemporalVector)
			newVec := *vec
			newVec.Tombstone = true
			ti.vectors.Store(id, &newVec)
			deleted++
		}
	}

	return deleted, nil
}

// GetVersion retrieves the vector data for a specific ID as it existed at a given timestamp.
func (ti *TemporalIndex) GetVersion(id uint64, timestamp int64) ([]float32, bool) {
	val, ok := ti.vectors.Load(id)
	if !ok {
		return nil, false
	}

	vec := val.(*TemporalVector)
	if vec.Timestamp > timestamp {
		return nil, false
	}

	return vec.Vector, true
}

// GetHistory returns the temporal version history for a specific vector ID.
func (ti *TemporalIndex) GetHistory(id uint64) []TemporalVector {
	val, ok := ti.vectors.Load(id)
	if !ok {
		return nil
	}

	vec := val.(*TemporalVector)
	if vec.Timestamp == 0 {
		return nil
	}

	return []TemporalVector{*vec}
}

// SetGPUIndex sets the GPU acceleration index for this TemporalIndex.
func (ti *TemporalIndex) SetGPUIndex(idx gputypes.Index) {
	ti.gpuIndex.Store(idx)
}

// Add adds a vector to the temporal index.
// Size returns the total number of vectors in the temporal index.
func (ti *TemporalIndex) Size() int {
	return int(ti.pointCount.Load())
}

// ActiveCount returns the number of non-tombstoned vectors in the index.
func (ti *TemporalIndex) ActiveCount() int {
	count := 0
	ti.vectors.Range(func(key, value any) bool {
		v := value.(*TemporalVector)
		if !v.Tombstone {
			count++
		}
		return true
	})
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
		val, ok := ti.vectors.Load(id)
		if !ok {
			continue
		}
		vec := val.(*TemporalVector)
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
	ti.vectors.Range(func(key, value any) bool {
		v := value.(*TemporalVector)
		vectors = append(vectors, TempVec{
			ID:        v.ID,
			Vector:    v.Vector,
			Timestamp: v.Timestamp,
			Metadata:  v.Metadata,
			Tombstone: v.Tombstone,
		})
		return true
	})

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
