package store

import (
	"context"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// migrateToSharded performs the migration from HNSWIndex to ShardedHNSW.
func (idx *AutoShardingIndex) migrateToSharded() {
	start := time.Now()

	// Use a context timeout to prevent the migration from leaving the dataset
	// stuck in BUSY state forever. If the migration doesn't complete within
	// this window, the deferred cleanup (migrating.Store(false), MigrationFinished)
	// will still run via the function return.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Robust recovery + cleanup: ensure the pause channel is always closed so that
	// waiting ingestion workers are not stuck forever, even on panic.
	defer func() {
		idx.migrationPauseMu.Lock()
		if idx.migrationPause != nil {
			close(idx.migrationPause)
			idx.migrationPause = nil
		}
		idx.migrationPauseMu.Unlock()

		if r := recover(); r != nil {
			idx.dataset.Logger.Error().
				Interface("panic", r).
				Msg("Index migration failed due to panic")
			idx.migrating.Store(false)
		}
	}()
	defer idx.migrating.Store(false)

	idx.mu.RLock()
	oldIndex := idx.current
	nTotal := oldIndex.Len()
	dims := 0
	dims = int(oldIndex.GetDimension())
	idx.mu.RUnlock()

	if dims == 0 {
		// Cannot migrate an index without known dimensionality.
		// Defer migration until the first batch sets it.
		idx.dataset.Logger.Warn().Msg("Deferring index migration: dimensionality is zero")
		return
	}

	idx.dataset.Logger.Info().
		Int("vectors", nTotal).
		Int("dims", dims).
		Msg("Index migration started")

	// LOCK ORDER: ds.dataMu MUST be locked before idx.mu to avoid deadlock with Search
	idx.dataset.dataMu.RLock()
	idx.mu.Lock()
	if idx.sharded {
		idx.mu.Unlock()
		idx.dataset.dataMu.RUnlock()
		return
	}

	nTotal = idx.current.Len()
	if nTotal < idx.config.ShardThreshold {
		idx.mu.Unlock()
		idx.dataset.dataMu.RUnlock()
		return
	}

	// Capture snapshots for migration
	oldIndex = idx.current
	var oldDataType types.VectorDataType
	if ah, ok := oldIndex.(*ArrowHNSW); ok {
		oldDataType = ah.GetConfig().DataType
	} else if sh, ok := oldIndex.(*ShardedHNSW); ok {
		oldDataType = sh.GetConfig().DataType
	}

	shardedConfig := DefaultShardedHNSWConfig()
	shardedConfig.Metric = idx.dataset.Metric
	shardedConfig.Dimension = oldIndex.GetDimension()
	shardedConfig.DataType = oldDataType
	if idx.config.ShardCount > 0 {
		shardedConfig.NumShards = idx.config.ShardCount
	}
	if idx.config.ShardSplitThreshold > 0 {
		shardedConfig.ShardSplitThreshold = idx.config.ShardSplitThreshold
	}
	shardedConfig.UseRingSharding = idx.config.UseRingSharding

	idx.mu.Unlock()
	idx.dataset.dataMu.RUnlock()

	newIndex := NewShardedHNSW(shardedConfig, idx.dataset)

	// Pre-warm the new index to the total expected size to reduce allocation churn
	newIndex.PreWarm(nTotal)

	if idx.dataset.Admission != nil {
		idx.dataset.Admission.MigrationStarted()
		defer idx.dataset.Admission.MigrationFinished()
	}

	// Promote newIndex to interimIndex so that AddBatch starts hitting it immediately
	idx.mu.Lock()
	idx.interimIndex = newIndex
	idx.mu.Unlock()

	// Signal ingestion workers to pause: from here until migration completes, AddBatch
	// calls from ingestion will block, avoiding lock contention with the migration loop
	// on the new sharded index's per-shard locks.
	idx.migrationPauseMu.Lock()
	idx.migrationPause = make(chan struct{})
	idx.migrationPauseMu.Unlock()

	// Transition monolithic index to off-heap shadow mode to free up heap for the new index
	idx.mu.Lock()
	if err := oldIndex.RelocateToOffHeap(); err != nil {
		idx.dataset.Logger.Error().Err(err).Msg("Failed to relocate monolithic index to off-heap")
	}
	idx.mu.Unlock()
	// Reclaim memory immediately
	runtime.GC()
	debug.FreeOSMemory()

	// Migration parameters
	baseBatchSize := 5000 // Increased for throughput
	lastMigrated := 0

	for {
		if ctx.Err() != nil {
			idx.dataset.Logger.Warn().
				Err(ctx.Err()).
				Int("migrated", lastMigrated).
				Msg("Migration context cancelled, aborting")
			break
		}

		// 1. Memory Check (Heap + Off-Heap)
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		offHeapMem := lbmem.GetGlobalOffHeapAllocated()

		maxMem := int64(0)
		if idx.dataset.Admission != nil {
			maxMem = idx.dataset.Admission.maxMemory.Load()
		}

		physicalMem := int64(m.HeapAlloc) + offHeapMem // #nosec G115
		usageRatio := 0.0
		if maxMem > 0 {
			usageRatio = float64(physicalMem) / float64(maxMem)
		}

		// Adaptive Batch Sizing & Pressure Management
		currentBatchSize := baseBatchSize

		// Dynamic Slab-Capacity Aware Migration Batch Size Calculator
		getBytesPerElement := func(dt types.VectorDataType) int {
			switch dt {
			case types.VectorTypeFloat32, types.VectorTypeInt32, types.VectorTypeUint32:
				return 4
			case types.VectorTypeFloat16, types.VectorTypeInt16, types.VectorTypeUint16:
				return 2
			case types.VectorTypeInt8, types.VectorTypeUint8, types.VectorTypeTQ:
				return 1
			case types.VectorTypeFloat64, types.VectorTypeInt64, types.VectorTypeUint64, types.VectorTypeComplex64:
				return 8
			case types.VectorTypeComplex128:
				return 16
			default:
				return 4
			}
		}

		safeSlabLimit := 512 * 1024 // 512 KB maximum batch size allocation window
		bytesPerElement := getBytesPerElement(oldDataType)
		vectorBytes := dims * bytesPerElement
		if vectorBytes > 0 {
			dynamicLimit := safeSlabLimit / vectorBytes
			if dynamicLimit > 0 {
				currentBatchSize = min(currentBatchSize, dynamicLimit)
			}
		}

		// Scale batch size dynamically based on CPU core counts to minimize thread thrashing.
		// Machines with more cores can process larger parallel batches without lock contention.
		numCPU := runtime.NumCPU()
		cpuScaleFactor := float64(numCPU) / 4.0
		if cpuScaleFactor < 1.0 {
			cpuScaleFactor = 1.0
		}
		if cpuScaleFactor > 8.0 {
			cpuScaleFactor = 8.0
		}
		currentBatchSize = int(float64(currentBatchSize) * cpuScaleFactor)

		// Enforce sensible bounds (minimum 50 to avoid absolute thrashing, max baseBatchSize)
		if currentBatchSize < 50 {
			currentBatchSize = 50
		}

		if usageRatio > 0.75 {
			currentBatchSize = min(currentBatchSize, 1000)
		}
		if usageRatio > 0.85 {
			currentBatchSize = min(currentBatchSize, 250)

			idx.dataset.Logger.Warn().
				Int64("usage_bytes", physicalMem).
				Int64("max_bytes", maxMem).
				Float64("ratio", usageRatio).
				Msg("Migration throttled: critical memory pressure")

			runtime.GC()
			if usageRatio > 0.97 {
				debug.FreeOSMemory()
			}
			time.Sleep(500 * time.Millisecond)
		}

		// 1b. Migration Lane Throttling
		if idx.dataset.Admission != nil {
			migrationDeadline := time.Now().Add(3 * time.Second)
			for {
				if err := idx.dataset.Admission.AdmitMigration(context.Background()); err == nil {
					break
				}
				if time.Now().After(migrationDeadline) {
					idx.dataset.Logger.Warn().Msg("Migration lane throttling timeout exceeded, continuing migration despite backpressure")
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}

		idx.mu.RLock()
		oldIdx := idx.current
		isSharded := idx.sharded
		nSnap := oldIdx.Len()
		idx.mu.RUnlock()

		if isSharded || lastMigrated >= nSnap {
			break
		}

		endIdx := lastMigrated + currentBatchSize
		if endIdx > nSnap {
			endIdx = nSnap
		}

		// 2. Process batch: collect locations and records
		type item struct {
			rec      arrow.RecordBatch
			rowIdx   int
			batchIdx int
		}
		items := make([]item, 0, endIdx-lastMigrated)

		idx.dataset.dataMu.RLock()
		records := idx.dataset.Records.Read()
		for id := lastMigrated; id < endIdx; id++ {
			locAny, ok := oldIdx.GetLocation(uint32(id))
			if ok {
				if loc, ok := locAny.(Location); ok && loc.BatchIdx >= 0 && loc.BatchIdx < len(records) {
					rec := records[loc.BatchIdx]
					rec.Retain()
					items = append(items, item{rec: rec, rowIdx: loc.RowIdx, batchIdx: loc.BatchIdx})
				}
			}
		}
		idx.dataset.dataMu.RUnlock()

		if len(items) > 0 {
			// Convert to batch parameters mapped to the true dataset BatchIdx
			maxBatchIdx := -1
			for _, it := range items {
				if it.batchIdx > maxBatchIdx {
					maxBatchIdx = it.batchIdx
				}
			}

			// Build recs slice: one entry per batchIdx. Track which items were
			// already retained (only the first item per batchIdx is used in recs;
			// release the surplus Retain()s on duplicate batchIdx items now).
			recs := make([]arrow.RecordBatch, maxBatchIdx+1)
			rowIdxs := make([]int, len(items))
			batchIdxs := make([]int, len(items))

			for i, it := range items {
				if recs[it.batchIdx] != nil && recs[it.batchIdx] != it.rec {
					// A different record was already placed at this slot —
					// release the surplus retain to avoid a memory leak.
					it.rec.Release()
				} else {
					recs[it.batchIdx] = it.rec
				}
				rowIdxs[i] = it.rowIdx
				batchIdxs[i] = it.batchIdx // Use true dataset index
			}

			// Add batch to new index (Parallelized inside ShardedHNSW)
			gIDs, err := newIndex.AddBatch(context.Background(), recs, rowIdxs, batchIdxs)

			// Release records (one Release per Retain; duplicates already released above)
			for _, r := range recs {
				if r != nil {
					r.Release()
				}
			}

			if err != nil {
				// Non-critical: log and continue rather than aborting the entire migration.
				// Vectors that can't be resolved are skipped; the majority will still be
				// migrated and the index will shard successfully.
				idx.dataset.Logger.Warn().
					Err(err).
					Int("lastMigrated", lastMigrated).
					Int("endIdx", endIdx).
					Msg("Migration batch had missing vectors - continuing")
			}

			// Correct locations in global locationStore of newIndex
			if sh, ok := newIndex.(*ShardedHNSW); ok {
				for i, gid := range gIDs {
					vid := VectorID(gid)
					sh.LocationStore().Set(vid, Location{BatchIdx: items[i].batchIdx, RowIdx: items[i].rowIdx})
				}
			}
		}

		lastMigrated = endIdx

		// Incremental Handover: To prevent "vector missing for row X" failures under high-load memory pressure,
		// we delay releasing monolithic storage chunks until the migration is fully completed.
		// Reclaim GC memory periodically based on migrated progress.
		currentChunk := (lastMigrated / types.ChunkSize) - 1
		if currentChunk >= 0 {
			if usageRatio > 0.85 {
				runtime.GC()
			} else if currentChunk%4 == 0 { // Don't GC on every chunk to avoid too much jitter
				runtime.GC()
			}
		}

		// Give other threads a window
		runtime.Gosched()
	}

	// Final swap
	idx.dataset.dataMu.RLock()
	idx.mu.Lock()

	if idx.sharded {
		idx.dataset.dataMu.RUnlock()
		idx.mu.Unlock()
		return
	}

	// Swap
	idx.current = newIndex
	idx.sharded = true
	idx.interimIndex = nil
	idx.dataset.dataMu.RUnlock()
	idx.mu.Unlock()

	// Wait for any remaining AddBatch operations on oldIndex to finish before closing.
	// Use a timeout so a hung AddBatch (e.g. blocked in ParallelFor) does not permanently
	// block migration. If the wait times out we close the old index anyway — the deferred
	// waitGroup.Done() will still run eventually and the old index's Close is safe to call
	// concurrently with a running AddBatch because ArrowHNSW.Close acquires h.closeMu.
	wgDone := make(chan struct{})
	go func() {
		idx.waitGroup.Wait()
		close(wgDone)
	}()
	select {
	case <-wgDone:
	case <-time.After(30 * time.Second):
		idx.dataset.Logger.Warn().Msg("Migration waitGroup.Wait timed out after 30s — closing old index without waiting")
	}

	// Close old index to release all remaining resources
	_ = oldIndex.Close()

	// Reclaim memory immediately back to OS
	released := lbmem.ReleaseGlobalSlabPoolsUnused()
	if released > 0 {
		idx.dataset.Logger.Info().Int("released_slabs", released).Msg("Released unused monolithic index slabs back to the OS")
	}
	runtime.GC()
	debug.FreeOSMemory()

	// Relocate the new ShardedHNSW index to off-heap memory.
	// The old monolithic index was already relocated during migration (line ~329), but the
	// new sharded index allocates slab chunks on the Go heap. Moving it off-heap here
	// breaks the O(N) heap growth curve and allows the GC to reclaim the freed pages.
	var rssBefore, rssAfter uint64
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	rssBefore = memStats.HeapInuse

	if err := newIndex.RelocateToOffHeap(); err != nil {
		idx.dataset.Logger.Warn().Err(err).Msg("Failed to relocate ShardedHNSW to off-heap; index will remain on Go heap")
	} else {
		runtime.GC()
		debug.FreeOSMemory()
		runtime.ReadMemStats(&memStats)
		rssAfter = memStats.HeapInuse
		var freedMB int64
		if rssBefore > rssAfter {
			freedMB = int64(rssBefore-rssAfter) / (1024 * 1024) // #nosec G115
		}
		idx.dataset.Logger.Info().
			Uint64("heap_before_mb", rssBefore/(1024*1024)).
			Uint64("heap_after_mb", rssAfter/(1024*1024)).
			Int64("freed_mb", freedMB).
			Msg("ShardedHNSW relocated to off-heap after migration")
	}

	duration := time.Since(start)
	metrics.IndexMigrationDuration.Observe(duration.Seconds())
	idx.dataset.Logger.Info().
		Dur("duration", duration).
		Int("vectors", nTotal).
		Msg("Index migration complete")
}

// ReleaseMonolithicChunk releases a monolithic chunk from memory.
func (idx *AutoShardingIndex) ReleaseMonolithicChunk(cID int) error {
	idx.mu.RLock()
	curr := idx.current
	idx.mu.RUnlock()

	if curr != nil {
		return curr.ReleaseMonolithicChunk(cID)
	}
	return nil
}
