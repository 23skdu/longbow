package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	lmem "github.com/23skdu/longbow/internal/memory"
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

		ds, ok := s.getDataset(req.Dataset)
		if !ok {
			// This was err return in old code, assuming err != nil check implies not found or error
			// The original code: ds, err := s.getDataset... if err != nil return err
			// Our helper returns (ds, bool). So if !ok return error.
			return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
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

		// Use RCU to delete
		var ds *Dataset
		var deleted bool
		s.updateDatasets(func(m map[string]*Dataset) {
			if d, ok := m[dsName]; ok {
				ds = d
				delete(m, dsName)
				deleted = true
			}
		})

		if !deleted {
			return status.Errorf(codes.NotFound, "dataset %s not found", dsName)
		}

		// Use existing eviction logic to free memory and close resources
		s.evictDataset(ds.Name)

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

		ds, ok := s.getDataset(dsName)
		if !ok {
			return status.Errorf(codes.NotFound, "dataset %s not found", dsName)
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
	// Use TrackingAllocator to monitor zero-copy behavior (expecting low allocations)
	// and track metadata overhead.
	trackAlloc := lmem.NewTrackingAllocator(memory.DefaultAllocator)
	r, err := flight.NewRecordReader(stream, ipc.WithAllocator(trackAlloc))
	if err != nil {
		s.logger.Error().Err(err).Msg("DoPut failed to create reader")
		return err
	}
	defer r.Release()

	var name string

	// Check descriptor immediately (sent with Schema)
	fd := r.LatestFlightDescriptor()
	if fd != nil && len(fd.Path) > 0 {
		name = fd.Path[0]
	} else {
		return fmt.Errorf("missing flight descriptor path")
	}

	s.logger.Info().Str("name", name).Msg("DoPut started (Batched)")

	// Use RCU helper for create
	ds, created := s.getOrCreateDataset(name, func() *Dataset {
		ds := NewDataset(name, r.Schema())
		ds.Topo = s.numaTopology

		// Disk Store Initialization (Phase 6)
		if strings.HasPrefix(name, "test_disk") || os.Getenv("LONGBOW_USE_DISK") == "1" {
			path := filepath.Join(s.dataPath, name+"_vectors.bin")
			dim := 0
			// Manual find vector column from schema
			for _, f := range r.Schema().Fields() {
				if f.Name == "vector" {
					if fst, ok := f.Type.(*arrow.FixedSizeListType); ok {
						dim = int(fst.Len())
						break
					}
				}
			}

			if dim > 0 {
				dvs, err := NewDiskVectorStore(path, dim)
				if err != nil {
					s.logger.Error().Err(err).Msg("Failed to create DiskVectorStore")
				} else {
					ds.DiskStore = dvs
					s.logger.Info().Str("path", path).Int("dim", dim).Msg("DiskVectorStore initialized (DoPut)")
				}
			}
		}

		// Call initialization hook if registered (for hnsw2, etc.)
		if s.datasetInitHook != nil {
			s.datasetInitHook(ds)
			// Account for initial index memory
			if ds.Index != nil {
				initialIndexMem := ds.Index.EstimateMemory()
				ds.IndexMemoryBytes.Store(initialIndexMem)
			} else if ds.GetHNSW2Index() != nil {
				if est, ok := ds.GetHNSW2Index().(interface{ EstimateMemory() int64 }); ok {
					initialIndexMem := est.EstimateMemory()
					ds.IndexMemoryBytes.Store(initialIndexMem)
				}
			}
		}
		return ds
	})

	if ds == nil {
		return status.Errorf(codes.Internal, "failed to retrieve or create dataset %s", name)
	}

	if created {
		mem := ds.IndexMemoryBytes.Load()
		if mem > 100*1024*1024 {
			s.logger.Warn().
				Str("dataset", name).
				Int64("mem", mem).
				Msg("Huge initial memory for dataset")
		}
		s.currentMemory.Add(mem)
	}

	// Initialize GPU if enabled
	ds.dataMu.RLock()
	idx := ds.Index
	ds.dataMu.RUnlock()
	if hnswIdx, ok := idx.(*HNSWIndex); ok {
		s.initGPUIfEnabled(hnswIdx)
	}

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
				if err := s.flushPutBatch(name, batch); err != nil {
					return err
				}
				batch = batch[:0]
				return nil
			}
		}

		// Flush single combined batch
		if err := s.flushPutBatch(name, []arrow.RecordBatch{combined}); err != nil {
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
			if err := s.flushPutBatch(name, []arrow.RecordBatch{rec}); err != nil {
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
func (s *VectorStore) flushPutBatch(name string, batch []arrow.RecordBatch) error {
	if len(batch) == 0 {
		return nil
	}

	// 1. Write to WAL (Sync)
	ts := time.Now().UnixNano()
	for _, rec := range batch {
		if err := s.writeToWAL(rec, name, ts); err != nil {
			s.logger.Error().Err(err).Str("dataset", name).Msg("Failed to write record to WAL during flushPutBatch")
			// Depending on strictness, we might return error here.
			// For now, we follow existing behavior: log and proceed (though skipping WAL is dangerous for durability).
			// Ideally we should return err if WAL is mandatory.
			// Let's return error to be safe.
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	// 2. Enqueue to Ingestion Pipeline (Async)
	for _, rec := range batch {
		rec.Retain() // Retain for the worker (which will Release)

		// Update lag metric
		metrics.IngestionLagCount.Add(float64(rec.NumRows()))

		select {
		case s.ingestionQueue <- ingestionJob{datasetName: name, batch: rec}:
			// Enqueued
		default:
			// Backpressure: if queue full, block or fail.
			// Ideally block to slow down client.
			s.ingestionQueue <- ingestionJob{datasetName: name, batch: rec}
		}
	}

	return nil
}

// StoreRecordBatch stores a batch of records in a dataset
func (s *VectorStore) StoreRecordBatch(ctx context.Context, name string, rec arrow.RecordBatch) error {
	// 1. Write to WAL (Sync)
	ts := time.Now().UnixNano()
	if err := s.writeToWAL(rec, name, ts); err != nil {
		return err
	}

	// 2. Enqueue to Ingestion Pipeline (Async)
	rec.Retain() // Retain for worker
	metrics.IngestionLagCount.Add(float64(rec.NumRows()))

	s.ingestionQueue <- ingestionJob{datasetName: name, batch: rec}

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

// applyBatchToMemory applies a batch to the in-memory dataset and dispatches indexing
func (s *VectorStore) applyBatchToMemory(name string, rec arrow.RecordBatch) error {
	ds, _ := s.getOrCreateDataset(name, func() *Dataset {
		ds := NewDataset(name, rec.Schema())
		ds.Topo = s.numaTopology

		// Disk Store Initialization (Phase 6)
		fmt.Printf("CRITICAL DEBUG: Checking DiskStore for %s\n", name)
		s.logger.Info().Str("name", name).Msg("Checking DiskStore init condition")
		if strings.HasPrefix(name, "test_disk") || os.Getenv("LONGBOW_USE_DISK") == "1" {
			path := filepath.Join(s.dataPath, name+"_vectors.bin")
			// Determine dim from schema
			dim := 0
			if vecCol := findVectorColumn(rec); vecCol != nil {
				if listArr, ok := vecCol.(*array.FixedSizeList); ok {
					dim = int(listArr.DataType().(*arrow.FixedSizeListType).Len())
				} else {
					s.logger.Warn().Msg("Vector column found but not FixedSizeList")
				}
			} else {
				s.logger.Warn().Msg("Vector column not found in schema")
			}
			s.logger.Info().Int("dim", dim).Str("path", path).Msg("DiskStore resolved dim")

			if dim > 0 {
				dvs, err := NewDiskVectorStore(path, dim)
				if err != nil {
					s.logger.Error().Err(err).Msg("Failed to create DiskVectorStore")
				} else {
					ds.DiskStore = dvs
					s.logger.Info().Str("path", path).Int("dim", dim).Msg("DiskVectorStore initialized")
				}
			}
		}
		return ds
	})

	// Memory tracking
	batchSize := estimateBatchSize(rec)
	// Check memory limit
	if err := s.checkMemoryBeforeWrite(batchSize, name); err != nil {
		return err
	}

	metrics.DoPutPayloadSizeBytes.Observe(float64(batchSize))
	AdviseRecord(rec, AdviceRandom)

	if batchSize > 100*1024*1024 {
		s.logger.Warn().Int64("size", batchSize).Msg("Large memory addition in DoPut")
	}
	s.currentMemory.Add(batchSize)
	ds.SizeBytes.Add(batchSize)
	metrics.FlightRowsProcessed.WithLabelValues("put", "ok").Add(float64(rec.NumRows()))

	ts := time.Now().UnixNano()

	ds.dataMu.Lock()
	dsLockStart := time.Now()

	// Lazy Index Initialization
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
	rec.Retain() // Dataset holds a reference

	// Record NUMA node
	currCPU := GetCurrentCPU()
	currNode := -1
	if s.numaTopology != nil {
		currNode = s.numaTopology.GetNodeForCPU(currCPU)
	}
	ds.BatchNodes = append(ds.BatchNodes, currNode)
	ds.UpdatePrimaryIndex(batchIdx, rec)

	// Append to DiskStore if enabled
	if ds.DiskStore != nil {
		vecColIdx := -1
		// Find vector col idx (cache this?)
		for i, f := range rec.Schema().Fields() {
			if f.Name == "vector" { // Convention
				vecColIdx = i
				break
			}
		}

		if vecColIdx != -1 {
			n := int(rec.NumRows())
			for i := 0; i < n; i++ {
				vec, err := ExtractVectorFromArrow(rec, i, vecColIdx)
				if err == nil {
					_, err := ds.DiskStore.Append(vec)
					if err != nil {
						s.logger.Error().Err(err).Msg("Failed to append to DiskStore")
					} else {
						metrics.DiskStoreWriteBytesTotal.WithLabelValues(name).Add(float64(len(vec) * 4))
					}
				}
			}
		}
	}

	ds.dataMu.Unlock()
	metrics.DatasetLockWaitDurationSeconds.WithLabelValues("put").Observe(time.Since(dsLockStart).Seconds())

	// Update LWW/Merkle and Queue Indexing
	s.updateLWWAndMerkle(ds, rec, ts)

	// Dispatch batch-level indexing job
	rec.Retain() // IndexJob holds ref
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
		s.logger.Warn().Str("dataset", name).Int("batch_idx", batchIdx).Msg("Dropped batch index job after retries (queue full)")
		rec.Release()
		metrics.IndexJobsDroppedTotal.Inc()
	}

	// Inverted index update (Hybrid)
	s.indexTextColumnsForHybridSearch(rec, 0)

	// Compaction trigger
	if s.compactionWorker != nil {
		shouldCompact := false
		ds.dataMu.RLock()
		if len(ds.Records) >= s.compactionConfig.MinBatchesToCompact {
			shouldCompact = true
		}
		ds.dataMu.RUnlock()
		if shouldCompact {
			_ = s.compactionWorker.TriggerCompaction(name)
		}
	}

	return nil
}
