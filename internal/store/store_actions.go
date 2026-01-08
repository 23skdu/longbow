package store

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"

	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
)

// DoAction handles custom actions like deletion and status
func (s *VectorStore) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch action.Type {
	case "cluster-status":
		if s.Mesh == nil {
			return status.Error(codes.Unavailable, "gossip mesh not enabled")
		}
		members := s.Mesh.GetMembers()
		// Sort by ID for consistent output
		sort.Slice(members, func(i, j int) bool {
			return members[i].ID < members[j].ID
		})

		resp := map[string]interface{}{
			"self":    s.Mesh.GetIdentity(),
			"members": members,
			"count":   len(members),
		}

		body, err := json.Marshal(resp)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to serialize status: %v", err)
		}

		if err := stream.Send(&flight.Result{Body: body}); err != nil {
			return err
		}
		return nil

	case "delete":
		var req struct {
			Dataset string `json:"dataset"`
			ID      string `json:"id"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}

		ds, err := s.getDataset(req.Dataset)
		if err != nil {
			return err
		}

		found := false
		ds.dataMu.RLock()
		for i, rec := range ds.Records {
			idColIdx := -1
			for j, field := range rec.Schema().Fields() {
				if field.Name == "id" {
					idColIdx = j
					break
				}
			}
			if idColIdx == -1 {
				continue
			}

			idCol, ok := rec.Column(idColIdx).(*array.String)
			if !ok {
				continue
			}

			for j := 0; j < idCol.Len(); j++ {
				if idCol.Value(j) != req.ID {
					continue
				}

				// Check if already deleted
				ts := ds.Tombstones[i]
				if ts != nil && ts.Contains(j) {
					continue
				}

				// Found it! Set tombstone
				ds.dataMu.RUnlock()
				ds.dataMu.Lock()
				if ds.Tombstones[i] == nil {
					ds.Tombstones[i] = qry.NewBitset()
				}
				ds.Tombstones[i].Set(j)
				ds.dataMu.Unlock()
				metrics.TombstonesTotal.WithLabelValues(req.Dataset).Inc()
				found = true
				ds.dataMu.RLock() // Re-lock for loop continuity or exit
				break
			}
			if found {
				break
			}
		}
		ds.dataMu.RUnlock()

		if !found {
			return status.Errorf(codes.NotFound, "id %s not found in dataset %s", req.ID, req.Dataset)
		}

		if err := stream.Send(&flight.Result{Body: []byte("deleted")}); err != nil {
			return err
		}
		return nil

	case "delete-dataset":
		var curr map[string]interface{}
		if err := json.Unmarshal(action.Body, &curr); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}

		dsName, ok := curr["dataset"].(string)
		if !ok {
			return status.Error(codes.InvalidArgument, "missing dataset name")
		}

		s.mu.Lock()
		ds, ok := s.datasets[dsName]
		if !ok {
			s.mu.Unlock()
			return status.Errorf(codes.NotFound, "dataset %s not found", dsName)
		}

		// Use existing eviction logic to free memory and close resources
		s.evictDataset(ds.Name)

		// Remove from map
		delete(s.datasets, dsName)
		s.mu.Unlock()

		s.logger.Info().Str("dataset", dsName).Msg("Dataset deleted")
		if err := stream.Send(&flight.Result{Body: []byte("deleted")}); err != nil {
			return err
		}
		return nil

	case "delete-vector":
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error().
					Interface("recover", r).
					Msg("PANIC in delete-vector action")
			}
		}()

		var curr map[string]interface{}
		if err := json.Unmarshal(action.Body, &curr); err != nil {
			// Try Unmarshal as []struct? No, map is safer for now
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}

		dsName, ok := curr["dataset"].(string)
		if !ok {
			return status.Error(codes.InvalidArgument, "missing dataset name")
		}

		var vid uint32
		if v, ok := curr["vector_id"].(float64); ok {
			vid = uint32(v)
		} else {
			return status.Error(codes.InvalidArgument, "missing or invalid vector_id")
		}

		ds, err := s.getDataset(dsName)
		if err != nil {
			return err
		}

		if ds.Index == nil {
			return status.Error(codes.FailedPrecondition, "index not initialized")
		}

		// Resolve location using interface method (works for all index types)
		loc, found := ds.Index.GetLocation(VectorID(vid))
		if !found {
			return status.Errorf(codes.NotFound, "vector id %d not found in dataset %s (index len=%d)", vid, dsName, ds.Index.Len())
		}

		// set tombstone
		ds.dataMu.Lock()
		if ds.Tombstones[loc.BatchIdx] == nil {
			ds.Tombstones[loc.BatchIdx] = qry.NewBitset()
		}
		ts := ds.Tombstones[loc.BatchIdx]
		ds.dataMu.Unlock()

		ts.Set(loc.RowIdx)
		metrics.TombstonesTotal.WithLabelValues(dsName).Inc()

		if err := stream.Send(&flight.Result{Body: []byte("deleted")}); err != nil {
			return err
		}
		return nil

	case "add-edge":
		return s.handleAddEdge(action.Body, stream)

	case "VectorSearchByID":
		return s.handleVectorSearchByIDAction(action, stream)

	case "traverse-graph":
		return s.handleTraverseGraph(action.Body, stream)

	case "GetGraphStats":
		return s.handleGetGraphStats(action.Body, stream)
	}
	return status.Error(codes.Unimplemented, "unknown action type "+action.Type)
}

// DoPut - Optimized implementation with batching
func (s *VectorStore) DoPut(stream flight.FlightService_DoPutServer) error {
	r, err := flight.NewRecordReader(stream)
	if err != nil {
		s.logger.Error().Err(err).Msg("DoPut failed to create reader")
		return err
	}
	defer r.Release()

	var name string
	var ds *Dataset

	// Check descriptor immediately (sent with Schema)
	fd := r.LatestFlightDescriptor()
	if fd != nil && len(fd.Path) > 0 {
		name = fd.Path[0]
	} else {
		// Fallback or error
		return fmt.Errorf("missing flight descriptor path")
	}

	s.logger.Info().Str("name", name).Msg("DoPut started (Batched)")

	s.mu.Lock()
	if _, ok := s.datasets[name]; !ok {
		// Create new dataset with schema from reader
		ds := NewDataset(name, r.Schema())

		// Note: hnsw2Index initialization happens via hook to avoid import cycle.
		// See cmd/longbow/main.go for initialization hook setup.

		ds.Topo = s.numaTopology
		s.datasets[name] = ds

		// Call initialization hook if registered (for hnsw2, etc.)
		if s.datasetInitHook != nil {
			s.datasetInitHook(ds)

			// Account for initial index memory (e.g., hnsw2 pre-allocations)
			if ds.Index != nil {
				initialIndexMem := ds.Index.EstimateMemory()
				ds.IndexMemoryBytes.Store(initialIndexMem)
				s.currentMemory.Add(initialIndexMem)
			} else if ds.GetHNSW2Index() != nil {
				// Special case for hnsw2 via interface
				if est, ok := ds.GetHNSW2Index().(interface{ EstimateMemory() int64 }); ok {
					initialIndexMem := est.EstimateMemory()
					ds.IndexMemoryBytes.Store(initialIndexMem)
					s.currentMemory.Add(initialIndexMem)
				}
			}
		}
	}
	ds = s.datasets[name]

	// Initialize GPU if enabled
	if hnswIdx, ok := ds.Index.(*HNSWIndex); ok {
		s.initGPUIfEnabled(hnswIdx)
	}
	s.mu.Unlock()

	// Batching configuration
	const maxBatchSize = 100
	batch := make([]arrow.RecordBatch, 0, maxBatchSize)

	// Helper to flush batch
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Optimization: Concatenate small batches into one large batch
		// to reduce WAL overhead and lock contention.
		var combined arrow.RecordBatch
		if len(batch) == 1 {
			combined = batch[0]
			combined.Retain()
		} else {
			var err error
			combined, err = s.concatenateBatches(batch)
			if err != nil {
				s.logger.Error().Err(err).Msg("Failed to concatenate batches")
				// Fallback to processing individually
				if err := s.flushPutBatch(ds, name, batch); err != nil {
					return err
				}
				batch = batch[:0]
				return nil
			}
		}
		// defer combined.Release() // This was commented out, no change needed.

		// Flush single combined batch
		if err := s.flushPutBatch(ds, name, []arrow.RecordBatch{combined}); err != nil {
			combined.Release()
			return err
		}

		// Clear batch slice
		for _, b := range batch {
			b.Release()
		}
		batch = batch[:0]
		return nil
	}

	for r.Next() {
		rec := r.RecordBatch()

		// Adaptive Batching (Option 1):
		// If the record is large enough and we don't have pending small records,
		// write it directly to avoid concatenation/slice overhead.
		if len(batch) == 0 && rec.NumRows() >= 100 {
			rec.Retain()
			if err := s.flushPutBatch(ds, name, []arrow.RecordBatch{rec}); err != nil {
				rec.Release()
				return err
			}
			continue
		}

		rec.Retain()
		batch = append(batch, rec)

		if len(batch) >= maxBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	if r.Err() != nil {
		s.logger.Error().Err(r.Err()).Msg("DoPut stream error")
		// Cleanup pending
		for _, b := range batch {
			b.Release()
		}
		return r.Err()
	}

	// Flush remaining
	if len(batch) > 0 {
		if err := flush(); err != nil {
			for _, b := range batch {
				b.Release()
			}
			return err
		}
	}

	s.logger.Info().Str("name", name).Msg("DoPut completed (Batched)")
	return nil
}

// flushPutBatch handles writing a batch of records to WAL and memory
func (s *VectorStore) flushPutBatch(ds *Dataset, name string, batch []arrow.RecordBatch) error {
	if len(batch) == 0 {
		return nil
	}

	// 1. Write to WAL (Async/Batched)
	// We use the helper which delegates to engine
	ts := time.Now().UnixNano()
	for _, rec := range batch {
		if err := s.writeToWAL(rec, name); err != nil {
			// Log the error but continue processing the rest of the batch
			s.logger.Error().Err(err).Str("dataset", name).Msg("Failed to write record to WAL during flushPutBatch")
			// The s.writeToWAL function already handles metrics for success/failure
		}
	}

	// 2. Append to Memory AND Initialize Index if needed (Batch Lock)
	totalSize := int64(0)
	totalRows := int64(0)

	// Calculate size and prep metrics first (outside lock)
	for _, rec := range batch {
		batchSize := estimateBatchSize(rec)
		totalSize += batchSize
		totalRows += int64(rec.NumRows())

		metrics.DoPutPayloadSizeBytes.Observe(float64(batchSize))
		// Advise Memory: Random access for HNSW
		AdviseRecord(rec, AdviceRandom)
	}

	// Check memory limit before accepting write
	if err := s.checkMemoryBeforeWrite(totalSize); err != nil {
		return err
	}

	s.currentMemory.Add(totalSize)
	ds.SizeBytes.Add(totalSize)
	metrics.FlightRowsProcessed.WithLabelValues("put", "ok").Add(float64(totalRows))

	ds.dataMu.Lock()
	dsLockStart := time.Now()

	// Lazy Index Initialization (moved from DoPut)
	if ds.Index == nil {
		// Use AutoShardingIndex by default, using global store config
		config := s.autoShardingConfig
		// Ensure sane defaults if zero-valued (though should be set by main)
		if config.ShardThreshold == 0 {
			config.ShardThreshold = 10000
			config.Enabled = true
			config.ShardCount = runtime.NumCPU()
		}

		aIdx := NewAutoShardingIndex(ds, config)

		// Set initial dimension from the first batch to avoid race on searches
		if len(batch) > 0 {
			if vecCol := findVectorColumn(batch[0]); vecCol != nil {
				if listArr, ok := vecCol.(*array.FixedSizeList); ok {
					dim := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
					aIdx.SetInitialDimension(dim)
				}
			}
		}

		ds.Index = aIdx
	}

	batchStartIdx := len(ds.Records)
	ds.Records = append(ds.Records, batch...)

	// Record NUMA node for these batches
	currCPU := GetCurrentCPU()
	currNode := -1
	if s.numaTopology != nil {
		currNode = s.numaTopology.GetNodeForCPU(currCPU)
	}
	for i, rec := range batch {
		ds.BatchNodes = append(ds.BatchNodes, currNode)
		ds.UpdatePrimaryIndex(batchStartIdx+i, rec)
	}

	ds.dataMu.Unlock()
	metrics.DatasetLockWaitDurationSeconds.WithLabelValues("put").Observe(time.Since(dsLockStart).Seconds())

	// 3. Update LWW/Merkle and Queue Indexing
	for i, rec := range batch {
		s.updateLWWAndMerkle(ds, rec, ts)

		batchIdx := batchStartIdx + i
		// Dispatch batch-level indexing job
		rec.Retain()
		job := IndexJob{
			DatasetName: name,
			Record:      rec,
			BatchIdx:    batchIdx,
			CreatedAt:   time.Now(),
		}

		// Try to send, backing off if full
		sent := false
		for attempt := 0; attempt < 50; attempt++ { // Reduced retry attempts slightly for batch jobs
			if s.indexQueue.Send(job) {
				sent = true
				break
			}
			// Queue full - wait a bit to apply backpressure
			time.Sleep(2 * time.Millisecond)
		}

		if !sent {
			s.logger.Warn().Str("dataset", name).Int("batch_idx", batchIdx).Msg("Dropped batch index job after retries (queue full)")
			rec.Release()
			metrics.IndexJobsDroppedTotal.Inc()
		}
	}

	return nil
}

// StoreRecordBatch stores a batch of records in a dataset
func (s *VectorStore) StoreRecordBatch(ctx context.Context, name string, rec arrow.RecordBatch) error {
	s.mu.Lock()
	ds, ok := s.datasets[name]
	if !ok {
		ds = NewDataset(name, rec.Schema())
		ds.Topo = s.numaTopology
		s.datasets[name] = ds
	}
	s.mu.Unlock()

	// WAL write
	if err := s.writeToWAL(rec, name); err != nil {
		return err
	}

	// Memory append
	ds.dataMu.Lock()

	// Lazy Index Init
	if ds.Index == nil {
		config := s.autoShardingConfig
		if config.ShardThreshold == 0 {
			config.ShardThreshold = 10000
			config.Enabled = true
			config.ShardCount = runtime.NumCPU()
		}
		aIdx := NewAutoShardingIndex(ds, config)
		if vecCol := findVectorColumn(rec); vecCol != nil {
			if listArr, ok := vecCol.(*array.FixedSizeList); ok {
				dim := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
				aIdx.SetInitialDimension(dim)
			}
		}
		ds.Index = aIdx
	}

	batchIdx := len(ds.Records)
	ds.Records = append(ds.Records, rec)
	rec.Retain()
	ds.UpdatePrimaryIndex(batchIdx, rec)
	ds.dataMu.Unlock()

	// Dispatch Index Job
	s.updateLWWAndMerkle(ds, rec, time.Now().UnixNano()) // timestamp approx
	rec.Retain()
	job := IndexJob{
		DatasetName: name,
		Record:      rec,
		BatchIdx:    batchIdx,
		CreatedAt:   time.Now(),
	}
	// Try to send, backing off if full
	sent := false
	for attempt := 0; attempt < 50; attempt++ {
		if s.indexQueue.Send(job) {
			sent = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sent {
		s.logger.Warn().Str("dataset", name).Msg("Index queue full, dropping (sync store)")
		metrics.IndexJobsDroppedTotal.Inc()
		rec.Release()
	}

	// Memory tracking
	size := estimateBatchSize(rec)
	ds.SizeBytes.Add(size)
	s.currentMemory.Add(size)

	// Inverted index update (Hybrid)
	s.indexTextColumnsForHybridSearch(rec, 0) // Simplified baseRowID for now

	// Compaction trigger
	if s.compactionWorker != nil {
		ds.dataMu.RLock()
		numBatches := len(ds.Records)
		ds.dataMu.RUnlock()
		if numBatches >= s.compactionConfig.MinBatchesToCompact {
			_ = s.compactionWorker.TriggerCompaction(name)
		}
	}

	return nil
}

// estimateBatchSize calculates appropriate size in bytes of a record batch
func estimateBatchSize(rec arrow.RecordBatch) int64 {
	if rec == nil {
		return 0
	}
	size := int64(0)
	for _, col := range rec.Columns() {
		// Approximate: sum of all buffer lengths
		for _, buf := range col.Data().Buffers() {
			if buf != nil {
				size += int64(buf.Len())
			}
		}
		// Recurse for children (e.g. List arrays)
		// Note: Children() returns []ArrowData, which is internal.
		// For correctness with Arrow Go, we might rely on Buffers() mostly.
		// Detailed recursion is complex without `array.Data` access if not exported.
		// However col.Data() gives ArrayData which has Children().
		for _, child := range col.Data().Children() {
			for _, buf := range child.Buffers() {
				if buf != nil {
					size += int64(buf.Len())
				}
			}
		}
	}
	return size
}

// concatenateBatches merges multiple record batches into one
func (s *VectorStore) concatenateBatches(batches []arrow.RecordBatch) (arrow.RecordBatch, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to concatenate")
	}
	schema := batches[0].Schema()
	numCols := int(schema.NumFields())
	columns := make([]arrow.Array, numCols)
	defer func() {
		// Clean up if we fail mid-way
		for _, col := range columns {
			if col != nil {
				col.Release()
			}
		}
	}()

	for i := 0; i < numCols; i++ {
		// Collect arrays for this column from all batches
		colArrays := make([]arrow.Array, len(batches))
		for j, batch := range batches {
			colArrays[j] = batch.Column(i)
		}

		// Use Arrow's array.Concatenate
		concatenated, err := array.Concatenate(colArrays, s.mem)
		if err != nil {
			return nil, fmt.Errorf("failed to concatenate column %d: %w", i, err)
		}
		columns[i] = concatenated
	}

	// Calculate total rows
	totalRows := int64(0)
	for _, b := range batches {
		totalRows += b.NumRows()
	}

	return array.NewRecordBatch(schema, columns, totalRows), nil
}
