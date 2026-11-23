package temporal

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	gputypes "github.com/23skdu/longbow/internal/gpu/types"
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
	mu            sync.Mutex
	dimension     int
	shards        [TemporalShards]temporalShard
	temporalTree  atomic.Pointer[TemporalTree]
	segmentTree   atomic.Pointer[SegmentTree]
	history       *VersionHistory
	cache         *TemporalResultCache
	rangeFillMu   sync.Mutex
	pointCount    atomic.Int64
	mutationSeq   atomic.Uint64
	gpuIndex      atomic.Value // holds gputypes.Index
	vectorIndexFn func() lbtypes.VectorIndexer

	ingestMu            sync.Mutex
	ingestQueue         []temporalIngestTask
	ingestScratch       []temporalIngestTask
	ingestWorkerRunning bool
	ingestClosed        bool
	ingestWg            sync.WaitGroup
	asyncIngest         atomic.Bool
	closed              atomic.Bool
}

type temporalIngestTask struct {
	timestamps []int64
	ids        []uint64
	norms      []float32
}

// TemporalPredicate implements types.HNSWPredicate for temporal filtering.

// SetVectorIndexProvider registers a callback returning the parent dataset vector index.
func (ti *TemporalIndex) SetVectorIndexProvider(fn func() lbtypes.VectorIndexer) {
	ti.vectorIndexFn = fn
}

// SetDatasetLabel labels the underlying result cache metrics after the dataset name.
func (ti *TemporalIndex) SetDatasetLabel(name string) {
	if ti.cache != nil {
		ti.cache.SetDatasetLabel(name)
	}
}

// Dimension returns the configured vector dimension.
func (ti *TemporalIndex) Dimension() int {
	return ti.dimension
}

// GetVectorIndex returns the vector index.
func (ti *TemporalIndex) GetVectorIndex() lbtypes.VectorIndexer {
	if ti.vectorIndexFn == nil {
		return nil
	}
	return ti.vectorIndexFn()
}

// TemporalResultCache provides LRU caching for temporal search results.
//
// Expiries and capacity evictions are counted separately. They both surface as
// "not in cache" to the caller, but they mean opposite things: an expiry says the
// TTL is shorter than the reuse interval, and a capacity eviction says the cache
// is too small. Collapsing them into one counter makes "the cache is thrashing"
// no easier to diagnose than having no counter at all, which is what R11a was
// about.

// GetBounds returns the bounds.
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
	ti.asyncIngest.Store(true)
	return ti
}

// getShard returns the shard for a given ID.
func (ti *TemporalIndex) getShard(id uint64) *temporalShard {
	return &ti.shards[id%uint64(TemporalShards)]
}

var errTemporalIndexClosed = fmt.Errorf("temporal index is closed")

const temporalIngestQueueCap = 64

// Close releases resources associated with the temporal index.
func (ti *TemporalIndex) Close() error {
	ti.stopIngestWorker()

	ti.mu.Lock()
	defer ti.mu.Unlock()
	ti.closed.Store(true)

	// Clear the sharded maps
	for i := 0; i < TemporalShards; i++ {
		ti.shards[i].mu.Lock()
		ti.shards[i].data = nil
		ti.shards[i].mu.Unlock()
	}

	if tree := ti.temporalTree.Swap(nil); tree != nil {
		tree.Release()
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
// When enabled, AddBatch and AddBatchNorms offload TemporalTree and SegmentTree
// updates to an on-demand worker that exits once the queue is drained.
func (ti *TemporalIndex) SetAsyncIngestion(async bool) {
	ti.ingestMu.Lock()
	if ti.ingestClosed {
		ti.ingestMu.Unlock()
		return
	}
	ti.asyncIngest.Store(async)
	ti.ingestMu.Unlock()
	if !async {
		ti.ingestWg.Wait()
	}
}

func (ti *TemporalIndex) dispatchTreeTask(task temporalIngestTask) {
	if ti.asyncIngest.Load() && ti.enqueueAsyncTask(task) {
		return
	}
	ti.applyTreeTask(task)
}

func (ti *TemporalIndex) enqueueAsyncTask(task temporalIngestTask) bool {
	ti.ingestMu.Lock()
	if ti.ingestClosed || !ti.asyncIngest.Load() || len(ti.ingestQueue) >= temporalIngestQueueCap {
		ti.ingestMu.Unlock()
		return false
	}
	ti.ingestQueue = append(ti.ingestQueue, task)
	if !ti.ingestWorkerRunning {
		ti.ingestWorkerRunning = true
		ti.ingestWg.Add(1)
		go ti.runIngestWorker()
	}
	ti.ingestMu.Unlock()
	return true
}

func (ti *TemporalIndex) applyTreeTask(task temporalIngestTask) {
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

// runIngestWorker drains queued tree updates on demand and exits once empty so
// idle TemporalIndex instances keep zero background goroutines.
func (ti *TemporalIndex) runIngestWorker() {
	defer ti.ingestWg.Done()
	for {
		ti.ingestMu.Lock()
		if len(ti.ingestQueue) == 0 {
			ti.ingestWorkerRunning = false
			ti.ingestQueue = nil
			ti.ingestScratch = nil
			ti.ingestMu.Unlock()
			return
		}
		pending := ti.ingestQueue
		ti.ingestQueue = ti.ingestScratch[:0]
		ti.ingestScratch = nil
		ti.ingestMu.Unlock()

		for i, task := range pending {
			ti.applyTreeTask(task)
			pending[i] = temporalIngestTask{}
		}

		ti.ingestMu.Lock()
		if len(ti.ingestQueue) == 0 {
			ti.ingestWorkerRunning = false
			ti.ingestQueue = nil
			ti.ingestScratch = nil
			ti.ingestMu.Unlock()
			return
		}
		ti.ingestScratch = pending[:0]
		ti.ingestMu.Unlock()
	}
}

func (ti *TemporalIndex) stopIngestWorker() {
	ti.ingestMu.Lock()
	ti.ingestClosed = true
	ti.asyncIngest.Store(false)
	ti.ingestMu.Unlock()
	ti.ingestWg.Wait()
}

// Add inserts a new vector with a timestamp into the temporal index.
func (ti *TemporalIndex) Add(id uint64, vector []float32, timestamp int64, metadata []byte) error {
	ti.mu.Lock()
	if ti.closed.Load() {
		ti.mu.Unlock()
		return errTemporalIndexClosed
	}
	defer ti.mu.Unlock()

	if ti.dimension == 0 && len(vector) > 0 {
		ti.dimension = len(vector)
	}
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
	ti.mutationSeq.Add(1)

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

	ti.mu.Lock()
	if ti.closed.Load() {
		ti.mu.Unlock()
		return errTemporalIndexClosed
	}
	dim := ti.dimension
	if dim == 0 {
		for _, v := range vectors {
			if len(v) > 0 {
				dim = len(v)
				ti.dimension = dim
				break
			}
		}
	}
	ti.mu.Unlock()

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
				if i < len(vectors) && len(vectors[i]) == dim {
					norms[i] = ti.computeNorm(vectors[i])
				}
			}
		}(start, end)
	}
	wg.Wait()

	ti.mu.Lock()
	if ti.closed.Load() {
		ti.mu.Unlock()
		return errTemporalIndexClosed
	}
	for i := range ids {
		if i >= len(vectors) || len(vectors[i]) != dim {
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

	ti.dispatchTreeTask(temporalIngestTask{
		timestamps: timestamps,
		ids:        ids,
		norms:      norms,
	})
	ti.pointCount.Add(int64(n))
	ti.mutationSeq.Add(1)
	ti.mu.Unlock()

	return nil
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
