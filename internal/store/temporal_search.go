package store

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	gputypes "github.com/23skdu/longbow/internal/gpu/types"
	"github.com/23skdu/longbow/internal/mathutil"
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/simd"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
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
	Enabled bool
	// VersionHistory indicates whether to keep a full history of vector updates.
	VersionHistory bool
	// MaxVersions is the maximum number of versions to keep per vector.
	MaxVersions int
	// RetentionPeriod is the duration for which temporal data is kept.
	RetentionPeriod time.Duration
	// TTLEnabled indicates whether Time-To-Live (TTL) is active for vectors.
	TTLEnabled bool
	// DefaultTTL is the default duration a vector remains valid.
	DefaultTTL time.Duration
	// CleanupInterval is the frequency of background cleanup tasks.
	CleanupInterval time.Duration
	// AggregationEnabled indicates whether temporal aggregation features are active.
	AggregationEnabled bool
	// MaxBuckets is the maximum number of buckets for temporal aggregation.
	MaxBuckets int
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
	segmentTree  atomic.Pointer[SegmentTree]
	history      *VersionHistory
	cache        *TemporalResultCache
	pointCount   atomic.Int64
	gpuIndex     atomic.Value // holds gputypes.Index
	ds           *Dataset     // parent dataset back-pointer

	ingestCh    chan temporalIngestTask
	ingestWg    sync.WaitGroup
	asyncIngest atomic.Bool
}

type temporalIngestTask struct {
	timestamps []int64
	ids        []uint64
	norms      []float32
}

// TemporalPredicate implements types.HNSWPredicate for temporal filtering.

func (ti *TemporalIndex) GetVectorIndex() VectorIndex {
	if ti.ds == nil {
		return nil
	}
	return ti.ds.Index
}

// TemporalResultCache provides LRU caching for temporal search results.
//
// Expiries and capacity evictions are counted separately. They both surface as
// "not in cache" to the caller, but they mean opposite things: an expiry says the
// TTL is shorter than the reuse interval, and a capacity eviction says the cache
// is too small. Collapsing them into one counter makes "the cache is thrashing"
// no easier to diagnose than having no counter at all, which is what R11a was
// about.

func (ti *TemporalIndex) GetBounds() (int64, int64) {
	tt := ti.temporalTree.Load()
	if tt == nil {
		return math.MaxInt64, math.MinInt64
	}
	return tt.GetBounds()
}

// NewTemporalIndex creates a new TemporalIndex instance with async ingestion
// enabled by default. AddBatch offloads TemporalTree and SegmentTree updates
// to a background worker, returning immediately after storing vector data.
// Call SetAsyncIngestion(false) to restore synchronous behavior.
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
	ti.startIngestWorker()
	ti.asyncIngest.Store(true)
	return ti
}

// getShard returns the shard for a given ID.
func (ti *TemporalIndex) getShard(id uint64) *temporalShard {
	return &ti.shards[id%uint64(TemporalShards)]
}

// Close releases resources associated with the temporal index.
func (ti *TemporalIndex) Close() error {
	ti.stopIngestWorker()

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

// SetAsyncIngestion enables or disables asynchronous temporal ingestion.
// When enabled, AddBatch offloads TemporalTree and SegmentTree updates to
// a background worker, returning immediately after storing vector data in shards.
func (ti *TemporalIndex) SetAsyncIngestion(async bool) {
	if async {
		if !ti.asyncIngest.Swap(true) {
			ti.startIngestWorker()
		}
	} else {
		if ti.asyncIngest.Swap(false) {
			ti.stopIngestWorker()
		}
	}
}

func (ti *TemporalIndex) startIngestWorker() {
	ti.ingestCh = make(chan temporalIngestTask, 64)
	ti.ingestWg.Add(1)
	go func() {
		defer ti.ingestWg.Done()
		for task := range ti.ingestCh {
			tree := ti.temporalTree.Load()
			if tree != nil {
				tree.InsertBatch(task.timestamps, task.ids, task.norms)
			}
			segmentTree := ti.segmentTree.Load()
			if segmentTree != nil {
				uids := make([]uint32, len(task.ids))
				for i, id := range task.ids {
					uids[i] = uint32(id) // #nosec G115
				}
				segmentTree.InsertBatch(task.timestamps, task.timestamps, uids)
			}
		}
	}()
}

func (ti *TemporalIndex) stopIngestWorker() {
	if ti.ingestCh != nil {
		close(ti.ingestCh)
		ti.ingestWg.Wait()
		ti.ingestCh = nil
	}
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
	segmentTree := ti.segmentTree.Load()
	if segmentTree != nil {
		segmentTree.Insert(timestamp, timestamp, uint32(id)) // #nosec G115
	}
	ti.pointCount.Add(1)

	if ti.history != nil {
		ti.history.Add(id, vector, norm, timestamp, metadata)
	}

	return nil
}

// AddBatch inserts multiple vectors into the TemporalIndex.
func (ti *TemporalIndex) AddBatch(ids []uint64, vectors [][]float32, timestamps []int64, metadata [][]byte) error {
	n := len(ids)
	if n == 0 {
		return nil
	}

	norms := make([]float32, n)
	workers := runtime.GOMAXPROCS(0)
	if workers > n {
		workers = n
	}
	chunkSize := (n + workers - 1) / workers

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		start := w * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}
		go func(s, e int) {
			defer wg.Done()
			for i := s; i < e; i++ {
				if len(vectors[i]) == ti.dimension {
					norms[i] = ti.computeNorm(vectors[i])
				}
			}
		}(start, end)
	}
	wg.Wait()

	ti.mu.Lock()
	for i := range ids {
		if len(vectors[i]) != ti.dimension {
			continue
		}
		var m []byte
		if i < len(metadata) {
			m = metadata[i]
		}
		vec := &TemporalVector{
			ID:        ids[i],
			Vector:    vectors[i],
			Norm:      norms[i],
			Timestamp: timestamps[i],
			Metadata:  m,
			Tombstone: false,
		}
		shard := ti.getShard(ids[i])
		shard.mu.Lock()
		shard.data[ids[i]] = vec
		shard.mu.Unlock()
		if ti.history != nil {
			ti.history.Add(ids[i], vectors[i], norms[i], timestamps[i], m)
		}
	}

	if ti.asyncIngest.Load() {
		task := temporalIngestTask{
			timestamps: timestamps,
			ids:        ids,
			norms:      norms,
		}
		select {
		case ti.ingestCh <- task:
		default:
			// Channel full; fall back to synchronous insert
			tree := ti.temporalTree.Load()
			if tree != nil {
				tree.InsertBatch(timestamps, ids, norms)
			}
			segmentTree := ti.segmentTree.Load()
			if segmentTree != nil {
				uids := make([]uint32, n)
				for i, id := range ids {
					uids[i] = uint32(id) // #nosec G115
				}
				segmentTree.InsertBatch(timestamps, timestamps, uids)
			}
		}
	} else {
		tree := ti.temporalTree.Load()
		if tree != nil {
			tree.InsertBatch(timestamps, ids, norms)
		}
		segmentTree := ti.segmentTree.Load()
		if segmentTree != nil {
			uids := make([]uint32, n)
			for i, id := range ids {
				uids[i] = uint32(id) // #nosec G115
			}
			segmentTree.InsertBatch(timestamps, timestamps, uids)
		}
	}
	ti.pointCount.Add(int64(n))
	ti.mu.Unlock()

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

	segmentTree := ti.segmentTree.Load()
	if segmentTree != nil {
		segmentTree.Remove(vec.Timestamp, vec.Timestamp, uint32(id)) // #nosec G115
	}
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
	// Temporal path regresses -16-38% under emlgo (roadmap.md §2 temporal pin): pin standard math.
	restoreStd := mathutil.PushStandard()
	defer restoreStd()

	cacheKey := fmt.Sprintf("asof:%d:%d", timestamp, k)
	if results, ok := ti.cache.Get(cacheKey); ok {
		return results, nil
	}

	vIdx := ti.GetVectorIndex()
	if vIdx != nil {
		dim := ti.dimension
		if dim == 0 {
			dim = int(vIdx.GetDimension())
		}
		if dim > 0 {
			// R11b: this vector is deliberately left zeroed, and it reads like a
			// missing assignment. It is not. TemporalSearchRequest carries no query
			// vector - only a dataset, search type, k and a timestamp - so there is
			// nothing to populate. As-of search is an enumeration ("k records that
			// existed as of T"), not a similarity search.
			//
			// Routing an enumeration through the vector index therefore ranks the
			// visible records by distance to the origin, i.e. by norm, and the
			// distances returned are distances to the zero vector rather than any
			// measure of relevance. That is the pinned behaviour in
			// temporal_asof_semantics_test.go, and it is why temporal results must
			// not be compared against modes that take a real query vector.
			queryVec := make([]float32, dim)
			options := lbtypes.SearchOptions{}

			var results []lbtypes.SearchResult
			var err error

			segmentTree := ti.segmentTree.Load()
			if segmentTree != nil {
				bm := segmentTree.QueryRange(0, timestamp)
				results, err = vIdx.SearchVectorsWithBitmap(ctx, queryVec, k, bm, options)
			} else {
				pred := &TemporalPredicate{
					minTs:  0,
					maxTs:  timestamp,
					shards: &ti.shards,
				}
				options.Predicate = pred
				results, err = vIdx.SearchVectors(ctx, queryVec, k, nil, options)
			}
			if err == nil {
				ti.cache.Set(cacheKey, results, 5*time.Minute)
				return results, nil
			}
		}
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
	if len(tree.leafRefs) == 0 {
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
	// Temporal path regresses -16-38% under emlgo (roadmap.md §2 temporal pin): pin standard math.
	restoreStd := mathutil.PushStandard()
	defer restoreStd()

	vIdx := ti.GetVectorIndex()
	if vIdx != nil {
		dim := ti.dimension
		if dim == 0 {
			dim = int(vIdx.GetDimension())
		}
		if dim > 0 {
			queryVec := make([]float32, dim)
			options := lbtypes.SearchOptions{}

			var results []lbtypes.SearchResult
			var err error

			segmentTree := ti.segmentTree.Load()
			if segmentTree != nil {
				bm := segmentTree.QueryRange(startTime, endTime)
				results, err = vIdx.SearchVectorsWithBitmap(ctx, queryVec, k, bm, options)
			} else {
				pred := &TemporalPredicate{
					minTs:  startTime,
					maxTs:  endTime,
					shards: &ti.shards,
				}
				options.Predicate = pred
				results, err = vIdx.SearchVectors(ctx, queryVec, k, nil, options)
			}
			if err == nil {
				return results, nil
			}
		}
	}

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
	// Temporal path regresses -16-38% under emlgo (roadmap.md §2 temporal pin): pin standard math.
	restoreStd := mathutil.PushStandard()
	defer restoreStd()

	tree := ti.temporalTree.Load()
	if tree == nil {
		return []lbtypes.SearchResult{}, nil
	}

	vIdx := ti.GetVectorIndex()
	if vIdx != nil {
		dim := ti.dimension
		if dim == 0 {
			dim = int(vIdx.GetDimension())
		}
		if dim > 0 {
			validIDs := tree.GetUniqueLatest(windowSize)
			if len(validIDs) == 0 {
				return []lbtypes.SearchResult{}, nil
			}
			idMap := make(map[uint64]struct{})
			for _, id := range validIDs {
				idMap[id] = struct{}{}
			}
			pred := &SlidingWindowPredicate{
				validIDs: idMap,
			}
			queryVec := make([]float32, dim)
			options := lbtypes.SearchOptions{
				Predicate: pred,
			}
			results, err := vIdx.SearchVectors(ctx, queryVec, k, nil, options)
			if err == nil {
				return results, nil
			}
		}
	}

	// Fallback: brute-force heap scan over latest windowSize unique IDs.
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
	// Temporal path regresses -16-38% under emlgo (roadmap.md §2 temporal pin): pin standard math.
	restoreStd := mathutil.PushStandard()
	defer restoreStd()

	vIdx := ti.GetVectorIndex()
	now := time.Now().UnixNano()
	start := now - duration.Nanoseconds()

	if vIdx != nil {
		dim := ti.dimension
		if dim == 0 {
			dim = int(vIdx.GetDimension())
		}
		if dim > 0 {
			pred := &TemporalPredicate{
				minTs:  start,
				maxTs:  now,
				shards: &ti.shards,
			}
			queryVec := make([]float32, dim)
			options := lbtypes.SearchOptions{
				Predicate: pred,
			}
			results, err := vIdx.SearchVectors(ctx, queryVec, k, nil, options)
			if err == nil {
				return results, nil
			}
		}
	}

	tree := ti.temporalTree.Load()
	if tree == nil {
		return []lbtypes.SearchResult{}, nil
	}

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
	ID        uint64    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Vector    []float32 `json:"vector,omitempty"`
	Metadata  []byte    `json:"metadata,omitempty"`
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
