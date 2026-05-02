package store

import (
	"context"
	"container/list"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/simd"
	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	internalcore "github.com/23skdu/longbow/internal/store/internal/core"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

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
	byTimestamp  sync.Map
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

// TemporalTree implements a specialized structure for range-based timestamp queries.
type TemporalTree struct {
	nodes  sync.Map
	sorted atomic.Pointer[[]int64]
}

// TemporalNode represents a set of vector IDs sharing a specific timestamp.
type TemporalNode struct {
	Timestamp int64
	VectorIDs []uint64
}

// NewTemporalTree creates a new TemporalTree instance.
func NewTemporalTree() *TemporalTree {
	tt := &TemporalTree{}
	empty := make([]int64, 0)
	tt.sorted.Store(&empty)
	return tt
}

// Insert adds a vector ID to the tree at the specified timestamp.
func (tt *TemporalTree) Insert(timestamp int64, id uint64) {
	val, ok := tt.nodes.Load(timestamp)
	if ok {
		node := val.(*TemporalNode)
		newNode := *node
		newNode.VectorIDs = append(append([]uint64(nil), node.VectorIDs...), id)
		tt.nodes.Store(timestamp, &newNode)
	} else {
		tt.nodes.Store(timestamp, &TemporalNode{
			Timestamp: timestamp,
			VectorIDs: []uint64{id},
		})
		
		// Update sorted slice atomically (COW)
		currentSorted := tt.sorted.Load()
		newSorted := append(append([]int64(nil), *currentSorted...), timestamp)
		sort.Slice(newSorted, func(i, j int) bool {
			return newSorted[i] < newSorted[j]
		})
		tt.sorted.Store(&newSorted)
	}
}

// GetRange returns all vector IDs within the specified timestamp range.
func (tt *TemporalTree) GetRange(start, end int64) []uint64 {
	sortedPtr := tt.sorted.Load()
	if sortedPtr == nil {
		return nil
	}
	sorted := *sortedPtr
	n := len(sorted)
	if n == 0 {
		return nil
	}

	startIdx := sort.Search(n, func(i int) bool {
		return sorted[i] >= start
	})

	var results []uint64
	for i := startIdx; i < n && sorted[i] <= end; i++ {
		if nodeVal, ok := tt.nodes.Load(sorted[i]); ok {
			node := nodeVal.(*TemporalNode)
			results = append(results, node.VectorIDs...)
		}
	}
	return results
}

// GetRangeReversed returns all vector IDs within the specified timestamp range in descending order.
func (tt *TemporalTree) GetRangeReversed(start, end int64) []uint64 {
	sortedPtr := tt.sorted.Load()
	if sortedPtr == nil {
		return nil
	}
	sorted := *sortedPtr

	var results []uint64
	for i := len(sorted) - 1; i >= 0; i-- {
		ts := sorted[i]
		if ts >= start && ts <= end {
			if nodeVal, ok := tt.nodes.Load(ts); ok {
				node := nodeVal.(*TemporalNode)
				results = append(results, node.VectorIDs...)
			}
		}
	}
	return results
}

// GetBefore returns all vector IDs with timestamps before the specified value.
func (tt *TemporalTree) GetBefore(timestamp int64) []uint64 {
	sortedPtr := tt.sorted.Load()
	if sortedPtr == nil {
		return nil
	}
	sorted := *sortedPtr

	idx := sort.Search(len(sorted), func(i int) bool {
		return sorted[i] >= timestamp
	})

	var results []uint64
	for i := 0; i < idx; i++ {
		if nodeVal, ok := tt.nodes.Load(sorted[i]); ok {
			node := nodeVal.(*TemporalNode)
			results = append(results, node.VectorIDs...)
		}
	}
	return results
}

// GetAfter returns all vector IDs with timestamps after the specified value.
func (tt *TemporalTree) GetAfter(timestamp int64) []uint64 {
	sortedPtr := tt.sorted.Load()
	if sortedPtr == nil {
		return nil
	}
	sorted := *sortedPtr

	idx := sort.Search(len(sorted), func(i int) bool {
		return sorted[i] > timestamp
	})

	var results []uint64
	for i := idx; i < len(sorted); i++ {
		if nodeVal, ok := tt.nodes.Load(sorted[i]); ok {
			node := nodeVal.(*TemporalNode)
			results = append(results, node.VectorIDs...)
		}
	}
	return results
}

// GetLatest returns the vector IDs from the last n timestamps.
func (tt *TemporalTree) GetLatest(n int) []uint64 {
	sortedPtr := tt.sorted.Load()
	if sortedPtr == nil {
		return nil
	}
	sorted := *sortedPtr
	if n > len(sorted) {
		n = len(sorted)
	}

	var results []uint64
	for i := len(sorted) - n; i < len(sorted); i++ {
		if nodeVal, ok := tt.nodes.Load(sorted[i]); ok {
			node := nodeVal.(*TemporalNode)
			results = append(results, node.VectorIDs...)
		}
	}
	return results
}

// GetEarliest returns the vector IDs from the first n timestamps.
func (tt *TemporalTree) GetEarliest(n int) []uint64 {
	sortedPtr := tt.sorted.Load()
	if sortedPtr == nil {
		return nil
	}
	sorted := *sortedPtr
	if n > len(sorted) {
		n = len(sorted)
	}

	var results []uint64
	for i := 0; i < n; i++ {
		if nodeVal, ok := tt.nodes.Load(sorted[i]); ok {
			node := nodeVal.(*TemporalNode)
			results = append(results, node.VectorIDs...)
		}
	}
	return results
}

// Len returns the number of unique timestamps in the tree.
func (tt *TemporalTree) Len() int {
	return len(*tt.sorted.Load())
}

// NewTemporalIndex creates a new TemporalIndex instance.
func NewTemporalIndex(dimension int) *TemporalIndex {
	ti := &TemporalIndex{
		dimension: dimension,
		cache:     NewTemporalResultCache(1024),
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

	vec := &TemporalVector{
		ID:        id,
		Vector:    vector,
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

	val, _ := ti.byTimestamp.LoadOrStore(timestamp, &[]uint64{})
	ids := val.(*[]uint64)
	newIds := append(*ids, id)
	ti.byTimestamp.Store(timestamp, &newIds)

	return nil
}

// AddBatch inserts multiple vectors into the TemporalIndex.
func (ti *TemporalIndex) AddBatch(ids []uint64, vectors [][]float32, timestamps []int64, metadata [][]byte) error {
	ti.mu.Lock()
	defer ti.mu.Unlock()

	tree := ti.temporalTree.Load()
	for i := range ids {
		if len(vectors[i]) != ti.dimension {
			continue // Or return error? For batch, maybe skip or return first error.
		}

		var m []byte
		if i < len(metadata) {
			m = metadata[i]
		}
		vec := &TemporalVector{
			ID:        ids[i],
			Vector:    vectors[i],
			Timestamp: timestamps[i],
			Metadata:  m,
			Tombstone: false,
		}

		ti.vectors.Store(ids[i], vec)
		if tree != nil {
			tree.Insert(timestamps[i], ids[i])
		}

		val, _ := ti.byTimestamp.LoadOrStore(timestamps[i], &[]uint64{})
		tsIds := val.(*[]uint64)
		newIds := append(*tsIds, ids[i])
		ti.byTimestamp.Store(timestamps[i], &newIds)
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

	newVec := &TemporalVector{
		ID:        id,
		Vector:    vector,
		Timestamp: timestamp,
		Metadata:  metadata,
		Tombstone: false,
	}

	ti.vectors.Store(id, newVec)
	tree := ti.temporalTree.Load()
	if tree != nil {
		tree.Insert(timestamp, id)
	}
	// Note: id was already counted, so no pointCount.Add(1)

	val, _ = ti.byTimestamp.LoadOrStore(timestamp, &[]uint64{})
	ids := val.(*[]uint64)
	newIds := append(*ids, id)
	ti.byTimestamp.Store(timestamp, &newIds)

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

	validIDs := tree.GetBefore(timestamp + 1)

	type scoredResult struct {
		id       uint64
		distance float64
	}

	results := make([]scoredResult, 0, len(validIDs))
	pool := internalcore.GetSharedPool()

	// 1. Filter out tombstones and collect vectors
	type candidate struct {
		id     uint64
		vector []float32
	}
	candidates := make([]candidate, len(validIDs))
	pool.ParallelFor(len(validIDs), 2048, func(start, end int) {
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
			candidates[i] = candidate{id: id, vector: vec.Vector}
		}
	})

	// Compact candidates
	filteredCandidates := make([]candidate, 0, len(candidates))
	for _, c := range candidates {
		if c.vector != nil {
			filteredCandidates = append(filteredCandidates, c)
		}
	}
	candidates = filteredCandidates

	if len(candidates) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	// 2. Compute norms (distances)
	distances := make([]float32, len(candidates))
	
	var gpuIdx gputypes.Index
	if val := ti.gpuIndex.Load(); val != nil {
		gpuIdx = val.(gputypes.Index)
	}

	if gpuIdx != nil {
		// Batch all vectors into flat slice for GPU
		flatVectors := make([]float32, len(candidates)*ti.dimension)
		pool.ParallelFor(len(candidates), 512, func(start, end int) {
			for i := start; i < end; i++ {
				copy(flatVectors[i*ti.dimension:(i+1)*ti.dimension], candidates[i].vector)
			}
		})
		gpuRes, err := gpuIdx.NormBatch(flatVectors, ti.dimension)
		if err == nil {
			distances = gpuRes
		} else {
			// Fallback to CPU
			pool.ParallelFor(len(candidates), 1024, func(start, end int) {
				for i := start; i < end; i++ {
					distances[i] = ti.computeNorm(candidates[i].vector)
				}
			})
		}
	} else {
		// CPU Parallel
		pool.ParallelFor(len(candidates), 1024, func(start, end int) {
			for i := start; i < end; i++ {
				distances[i] = ti.computeNorm(candidates[i].vector)
			}
		})
	}

	// 3. Collect results
	for i, c := range candidates {
		results = append(results, scoredResult{id: c.id, distance: float64(distances[i])})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	searchResults := make([]lbtypes.SearchResult, 0, min(k, len(results)))
	for i := 0; i < min(k, len(results)); i++ {
		searchResults = append(searchResults, lbtypes.SearchResult{
			ID:       lbtypes.VectorID(results[i].id), // #nosec G115
			Distance: float32(results[i].distance),
			Score:    float32(1.0 / (1.0 + results[i].distance)),
		})
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

	sortedPtr := tree.sorted.Load()
	if sortedPtr == nil || len(*sortedPtr) == 0 {
		return nil
	}
	sorted := *sortedPtr
	latestTs := sorted[len(sorted)-1]

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

	validIDs := tree.GetRange(startTime, endTime)

	type scoredResult struct {
		id       uint64
		distance float64
	}

	pool := internalcore.GetSharedPool()
	// Collect vectors for batch norm computation
	type candidate struct {
		id     uint64
		vector []float32
	}
	candidates := make([]candidate, len(validIDs))
	pool.ParallelFor(len(validIDs), 2048, func(start, end int) {
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
			candidates[i] = candidate{id: id, vector: vec.Vector}
		}
	})

	filteredCandidates := make([]candidate, 0, len(candidates))
	for _, c := range candidates {
		if c.vector != nil {
			filteredCandidates = append(filteredCandidates, c)
		}
	}
	candidates = filteredCandidates

	if len(candidates) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	distances := make([]float32, len(candidates))
	var gpuIdx gputypes.Index
	if val := ti.gpuIndex.Load(); val != nil {
		gpuIdx = val.(gputypes.Index)
	}

	if gpuIdx != nil {
		flatVectors := make([]float32, len(candidates)*ti.dimension)
		pool.ParallelFor(len(candidates), 512, func(start, end int) {
			for i := start; i < end; i++ {
				copy(flatVectors[i*ti.dimension:(i+1)*ti.dimension], candidates[i].vector)
			}
		})
		gpuRes, err := gpuIdx.NormBatch(flatVectors, ti.dimension)
		if err == nil {
			distances = gpuRes
		} else {
			pool.ParallelFor(len(candidates), 1024, func(start, end int) {
				for i := start; i < end; i++ {
					distances[i] = ti.computeNorm(candidates[i].vector)
				}
			})
		}
	} else {
		pool.ParallelFor(len(candidates), 1024, func(start, end int) {
			for i := start; i < end; i++ {
				distances[i] = ti.computeNorm(candidates[i].vector)
			}
		})
	}

	results := make([]scoredResult, len(candidates))
	for i, c := range candidates {
		results[i] = scoredResult{id: c.id, distance: float64(distances[i])}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	searchResults := make([]lbtypes.SearchResult, 0, min(k, len(results)))
	for i := 0; i < min(k, len(results)); i++ {
		searchResults = append(searchResults, lbtypes.SearchResult{
			ID:       lbtypes.VectorID(results[i].id), // #nosec G115
			Distance: float32(results[i].distance),
			Score:    float32(1.0 / (1.0 + results[i].distance)),
		})
	}

	return searchResults, nil
}

// SearchSlidingWindow performs a search over the last n vector updates.
func (ti *TemporalIndex) SearchSlidingWindow(ctx context.Context, windowSize int, k int) ([]lbtypes.SearchResult, error) {
	tree := ti.temporalTree.Load()
	if tree == nil {
		return []lbtypes.SearchResult{}, nil
	}

	validIDs := tree.GetLatest(windowSize)
	type scoredResult struct {
		id       uint64
		distance float64
	}

	// Collect vectors for batch norm computation
	type candidate struct {
		id     uint64
		vector []float32
	}

	pool := internalcore.GetSharedPool()
	candidates := make([]candidate, len(validIDs))
	pool.ParallelFor(len(validIDs), 2048, func(start, end int) {
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
			candidates[i] = candidate{id: id, vector: vec.Vector}
		}
	})

	filteredCandidates := make([]candidate, 0, len(candidates))
	for _, c := range candidates {
		if c.vector != nil {
			filteredCandidates = append(filteredCandidates, c)
		}
	}
	candidates = filteredCandidates

	if len(candidates) == 0 {
		return []lbtypes.SearchResult{}, nil
	}

	distances := make([]float32, len(candidates))
	var gpuIdx gputypes.Index
	if val := ti.gpuIndex.Load(); val != nil {
		gpuIdx = val.(gputypes.Index)
	}

	if gpuIdx != nil {
		flatVectors := make([]float32, len(candidates)*ti.dimension)
		pool.ParallelFor(len(candidates), 512, func(start, end int) {
			for i := start; i < end; i++ {
				copy(flatVectors[i*ti.dimension:(i+1)*ti.dimension], candidates[i].vector)
			}
		})
		gpuRes, err := gpuIdx.NormBatch(flatVectors, ti.dimension)
		if err == nil {
			distances = gpuRes
		} else {
			pool.ParallelFor(len(candidates), 1024, func(start, end int) {
				for i := start; i < end; i++ {
					distances[i] = ti.computeNorm(candidates[i].vector)
				}
			})
		}
	} else {
		pool.ParallelFor(len(candidates), 1024, func(start, end int) {
			for i := start; i < end; i++ {
				distances[i] = ti.computeNorm(candidates[i].vector)
			}
		})
	}

	results := make([]scoredResult, len(candidates))
	for i, c := range candidates {
		results[i] = scoredResult{id: c.id, distance: float64(distances[i])}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	searchResults := make([]lbtypes.SearchResult, 0, min(k, len(results)))
	for i := 0; i < min(k, len(results)); i++ {
		searchResults = append(searchResults, lbtypes.SearchResult{
			ID:       lbtypes.VectorID(results[i].id), // #nosec G115
			Distance: float32(results[i].distance),
			Score:    float32(1.0 / (1.0 + results[i].distance)),
		})
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

	type scoredResult struct {
		id       uint64
		distance float64
	}

	results := make([]scoredResult, 0, len(validIDs))
	var resMu sync.Mutex
	pool := internalcore.GetSharedPool()

	pool.ParallelFor(len(validIDs), 1024, func(start, end int) {
		localResults := make([]scoredResult, 0, end-start)
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
			dist := float64(ti.computeNorm(vec.Vector))
			localResults = append(localResults, scoredResult{id: id, distance: dist})
		}
		if len(localResults) > 0 {
			resMu.Lock()
			results = append(results, localResults...)
			resMu.Unlock()
		}
	})

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	searchResults := make([]lbtypes.SearchResult, 0, min(k, len(results)))
	for i := 0; i < min(k, len(results)); i++ {
		searchResults = append(searchResults, lbtypes.SearchResult{
			ID:       lbtypes.VectorID(results[i].id), // #nosec G115
			Distance: float32(results[i].distance),
			Score:    float32(1.0 / (1.0 + results[i].distance)),
		})
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
