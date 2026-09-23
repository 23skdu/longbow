package index

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"golang.org/x/sync/errgroup"
)

// AddByLocation adds a vector to the sharded index using its storage location.
func (idx *ShardedHNSW) AddByLocation(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	if idx.dataset == nil {
		return 0, fmt.Errorf("no dataset")
	}
	idx.dataset.RLockData()
	defer idx.dataset.RUnlockData()

	return idx.AddByLocationUnsafe(ctx, batchIdx, rowIdx)
}

// AddByLocationUnsafe adds a vector without taking dataset locks.
func (idx *ShardedHNSW) AddByLocationUnsafe(ctx context.Context, batchIdx, rowIdx int) (uint32, error) {
	if batchIdx >= len(idx.dataset.GetRecords()) {
		return 0, fmt.Errorf("invalid batch idx")
	}
	rec := idx.dataset.GetRecords()[batchIdx]
	return idx.AddByRecord(ctx, rec, rowIdx, batchIdx)
}

// AddSafe adds a single record to the sharded index safely.
func (idx *ShardedHNSW) AddSafe(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (VectorID, error) {
	id, err := idx.AddByRecord(ctx, rec, rowIdx, batchIdx)
	if err != nil {
		return 0, err
	}
	return VectorID(id), nil
}

// AddBatch adds a batch of records to the sharded index in parallel.
func (idx *ShardedHNSW) AddBatch(ctx context.Context, recs []arrow.RecordBatch, rowIdxs, batchIdxs []int) ([]uint32, error) {
	if len(recs) == 0 || len(rowIdxs) == 0 {
		return nil, nil
	}

	n := len(rowIdxs)
	globalIDs := make([]uint32, n)

	// 1. Group indices by shard
	type shardJob struct {
		indices []int // Original indices in the batch
	}
	shardJobs := make(map[int]*shardJob)

	for i := 0; i < n; i++ {
		// Allocate Global ID
		rawID := idx.nextID.Add(1) - 1
		if rawID > math.MaxUint32 {
			return nil, fmt.Errorf("vector ID overflow: %d exceeds max uint32", rawID)
		}
		gid := VectorID(rawID) // #nosec G115 - bounds checked above
		globalIDs[i] = uint32(gid)

		// Set Global Location
		idx.locationStore.EnsureCapacity(gid)
		idx.locationStore.Set(gid, Location{BatchIdx: batchIdxs[i], RowIdx: rowIdxs[i]})
		idx.locationStore.UpdateSize(gid)

		// Route to Shard
		shardIdx := idx.sharder.GetShard(gid)
		job, ok := shardJobs[shardIdx]
		if !ok {
			job = &shardJob{indices: make([]int, 0, n/idx.config.NumShards)}
			shardJobs[shardIdx] = job
		}
		job.indices = append(job.indices, i)
	}

	// 2. Parallel Insert across shards
	g, ctx := errgroup.WithContext(ctx)

	// Time from here until the lock is fully acquired is the contention window.
	contentionStart := time.Now()
	idx.shardsMu.RLock()
	// Ensure shards exist (linear sharding growth)
	maxShardIdx := 0
	for sIdx := range shardJobs {
		if sIdx > maxShardIdx {
			maxShardIdx = sIdx
		}
	}
	if maxShardIdx >= len(idx.shards) {
		idx.shardsMu.RUnlock()
		idx.shardsMu.Lock()
		for i := len(idx.shards); i <= maxShardIdx; i++ {
			idx.shards = append(idx.shards, idx.newShard(i))
		}
		if maxShardIdx >= len(idx.shardLocks) {
			newLocks := make([]sync.Mutex, maxShardIdx+1)
			copy(newLocks, idx.shardLocks)
			idx.shardLocks = newLocks
		}
		idx.shardsMu.Unlock()
		idx.shardsMu.RLock()
	}

	// Record the lock-acquisition latency (write-contention proxy) once per AddBatch call.
	if idx.dataset != nil {
		metrics.HnswUpdateContentionSeconds.WithLabelValues(idx.dataset.GetName()).Observe(time.Since(contentionStart).Seconds())
	}

	for shardIdx, job := range shardJobs {
		sIdx := shardIdx
		j := job
		shard := idx.shards[sIdx]

		g.Go(func() error {
			idx.shardLocks[sIdx].Lock()

			shardRowIdxs := make([]int, len(j.indices))
			shardBatchIdxs := make([]int, len(j.indices))
			for k, idxInBatch := range j.indices {
				shardRowIdxs[k] = rowIdxs[idxInBatch]
				shardBatchIdxs[k] = batchIdxs[idxInBatch]
			}

			localIDs, err := shard.index.AddBatch(ctx, recs, shardRowIdxs, shardBatchIdxs)
			if err == nil {
				// Register Global->Local mappings
				for k, lid := range localIDs {
					idxInBatch := j.indices[k]
					gid := VectorID(globalIDs[idxInBatch])
					shard.registerID(lid, gid, idx.globalToLocal)
				}
			}
			idx.shardLocks[sIdx].Unlock()

			if err != nil {
				return err
			}

			// Each successful batch insert into a shard implies a CoW adjacency
			// list copy inside the underlying ArrowHNSW graph.  Count them to
			// surface write-contention pressure in dashboards.
			if idx.dataset != nil {
				metrics.HnswCowCopyCount.WithLabelValues(idx.dataset.GetName(), fmt.Sprintf("%d", sIdx)).Add(float64(len(localIDs)))
			}
			metrics.ShardedHnswShardSize.WithLabelValues(idx.dataset.GetName(), fmt.Sprintf("%d", sIdx)).Add(float64(len(localIDs)))
			return nil
		})
	}

	idx.shardsMu.RUnlock()

	if err := g.Wait(); err != nil {
		return nil, err
	}

	idx.updateShardBalanceMetrics()
	return globalIDs, nil
}

// DeleteBatch removes multiple vectors from the sharded index.
func (idx *ShardedHNSW) DeleteBatch(ctx context.Context, ids []uint32) error {
	// Group by shard
	shardIds := make(map[int][]uint32)
	for _, id := range ids {
		vid := VectorID(id)
		shardIdx := idx.sharder.GetShard(vid)
		shardIds[shardIdx] = append(shardIds[shardIdx], id)
	}

	idx.shardsMu.RLock()
	defer idx.shardsMu.RUnlock()

	for shardIdx, distinctIDs := range shardIds {
		if shardIdx >= len(idx.shards) || idx.shards[shardIdx] == nil {
			continue
		}
		shard := idx.shards[shardIdx]
		// Convert Global IDs to Local IDs
		var localIDs []uint32
		for _, gid := range distinctIDs {
			if loc, ok := idx.globalToLocal.Get(VectorID(gid)); ok {
				localIDs = append(localIDs, uint32(loc.BatchIdx)) // #nosec G115
			}
		}
		if len(localIDs) > 0 {
			idx.shardLocks[shardIdx].Lock()
			err := shard.index.DeleteBatch(ctx, localIDs)
			idx.shardLocks[shardIdx].Unlock()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// AddByRecord adds a single vector from an Arrow RecordBatch to the sharded index.
func (idx *ShardedHNSW) AddByRecord(ctx context.Context, rec arrow.RecordBatch, rowIdx, batchIdx int) (uint32, error) {
	// Allocate Global ID
	rawID := idx.nextID.Add(1) - 1
	if rawID > math.MaxUint32 {
		return 0, fmt.Errorf("vector ID overflow: %d exceeds max uint32", rawID)
	}
	id := VectorID(rawID) // #nosec G115 - bounds checked above

	// Update global locations (Lock-Free)
	idx.locationStore.EnsureCapacity(id)
	idx.locationStore.Set(id, Location{BatchIdx: batchIdx, RowIdx: rowIdx})
	idx.locationStore.UpdateSize(id)

	// Route to Shard
	shardIdx := idx.sharder.GetShard(id)

	idx.shardsMu.RLock()
	if shardIdx < len(idx.shards) {
		shard := idx.shards[shardIdx]
		idx.shardsMu.RUnlock()
		idx.shardLocks[shardIdx].Lock()
		localID, err := shard.index.AddByRecord(ctx, rec, rowIdx, batchIdx)
		idx.shardLocks[shardIdx].Unlock()
		if err != nil {
			return 0, fmt.Errorf("shard insert failed: %w", err)
		}
		shard.registerID(localID, id, idx.globalToLocal)
		metrics.ShardedHnswShardSize.WithLabelValues(idx.dataset.GetName(), fmt.Sprintf("%d", shardIdx)).Inc()
		if id%1000 == 0 {
			idx.updateShardBalanceMetrics()
		}
		return uint32(id), nil
	}
	idx.shardsMu.RUnlock()

	// If we are here, we might need to grow.
	// Only linear sharding supports growth.
	if idx.config.UseRingSharding {
		return 0, fmt.Errorf("shard index out of bounds (dynamic growth not supported in ring mode)")
	}

	// Dynamic Growth (Double-checked locking)
	idx.shardsMu.Lock()
	if shardIdx < len(idx.shards) {
		// Someone else created it
		shard := idx.shards[shardIdx]
		idx.shardsMu.Unlock()
		idx.shardLocks[shardIdx].Lock()
		localID, err := shard.index.AddByRecord(ctx, rec, rowIdx, batchIdx)
		idx.shardLocks[shardIdx].Unlock()
		if err != nil {
			return 0, fmt.Errorf("shard insert failed: %w", err)
		}
		shard.registerID(localID, id, idx.globalToLocal)
		metrics.ShardedHnswShardSize.WithLabelValues(idx.dataset.GetName(), fmt.Sprintf("%d", shardIdx)).Inc()
		if id%1000 == 0 {
			idx.updateShardBalanceMetrics()
		}
		return uint32(id), nil
	}

	// Grow
	// We fill potential gaps if shardIdx skips
	for i := len(idx.shards); i <= shardIdx; i++ {
		idx.shards = append(idx.shards, idx.newShard(i))
	}
	if shardIdx >= len(idx.shardLocks) {
		newLocks := make([]sync.Mutex, shardIdx+1)
		copy(newLocks, idx.shardLocks)
		idx.shardLocks = newLocks
	}
	shard := idx.shards[shardIdx]
	idx.shardsMu.Unlock()

	// Insert
	idx.shardLocks[shardIdx].Lock()
	localID, err := shard.index.AddByRecord(ctx, rec, rowIdx, batchIdx)
	if err == nil {
		shard.registerID(localID, id, idx.globalToLocal)
	}
	idx.shardLocks[shardIdx].Unlock()
	if err != nil {
		return 0, fmt.Errorf("shard insert failed: %w", err)
	}

	metrics.ShardedHnswShardSize.WithLabelValues(idx.dataset.GetName(), fmt.Sprintf("%d", shardIdx)).Inc()
	if id%1000 == 0 {
		idx.updateShardBalanceMetrics()
	}
	return uint32(id), nil
}
