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

	case "check_readiness":
		var req struct {
			Dataset string `json:"dataset"`
		}
		// Body is optional
		if len(action.Body) > 0 {
			if err := json.Unmarshal(action.Body, &req); err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
			}
		}

		resp := map[string]interface{}{
			"status": "READY",
		}

		// 1. Check Global Queue
		qLen := s.indexQueue.Len()
		if qLen > 0 {
			resp["status"] = "BUSY"
			resp["reason"] = fmt.Sprintf("global index queue has %d jobs", qLen)
		} else if req.Dataset != "" {
			// 2. Check Specific Dataset
			ds, ok := s.getDataset(req.Dataset)
			if !ok {
				resp["status"] = "NOT_FOUND"
				resp["reason"] = "dataset not found"
			} else {
				pending := ds.PendingIndexJobs.Load()
				pendingIngestion := ds.PendingIngestion.Load()
				if pending > 0 || pendingIngestion > 0 {
					resp["status"] = "BUSY"
					resp["reason"] = fmt.Sprintf("dataset has %d pending index jobs, %d pending ingestion jobs", pending, pendingIngestion)
				} else if ds.Index == nil {
					resp["status"] = "BUSY"
					resp["reason"] = "index not initialized"
				}
				resp["index_len"] = ds.IndexLen()
			}
		}

		body, err := json.Marshal(resp)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to serialize status: %v", err)
		}
		return stream.Send(&flight.Result{Body: body})

	case "delete", "Delete":
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

		// Use PrimaryIndex for O(1) lookup
		if ds.PrimaryIndex != nil {
			if loc, ok := ds.PrimaryIndex[req.ID]; ok {
				// We found the location!

				// Optimization: Check if already deleted inside the read lock first
				// to avoid Upgrade to Write Lock if not needed.
				if ts, ok := ds.Tombstones[loc.BatchIdx]; ok && ts != nil && ts.Contains(loc.RowIdx) {
					// Already deleted, treat as success
					found = true
				} else {
					// Need to set tombstone. Upgrade to write lock.
					// Note: Upgrading RLock to Lock is not atomic. We must RUnlock then Lock.
					ds.dataMu.RUnlock()
					ds.dataMu.Lock()

					// Re-verify location after re-lock (though PrimaryIndex is append-only for IDs usually)
					// Verify tombstone again
					if ds.Tombstones[loc.BatchIdx] == nil {
						ds.Tombstones[loc.BatchIdx] = qry.NewBitset()
					}
					ds.Tombstones[loc.BatchIdx].Set(loc.RowIdx)
					// Also update global 'deleted' set in HNSW if needed?
					// Currently HNSW relies on dataset Tombstones or its own bitset.
					// HNSW has 'deleted' bitset, synced via CleanupTombstones usually.
					// But we should probably mark it here too if HNSW is tightly coupled?
					// The architecture seems to be: Dataset Tombstones are source of truth.

					metrics.TombstonesTotal.WithLabelValues(req.Dataset).Inc()

					// Re-acquire read lock for remaining logic if needed (e.g., if we were in a loop)
					// But we are effectively done.
					// To match surrounding code flow:
					ds.dataMu.Unlock()
					ds.dataMu.RLock() // Re-lock to match defer RUnlock()
					found = true
				}
			}
		}

		// Fallback Linear Scan (only if PrimaryIndex failed or nil)
		if !found && ds.PrimaryIndex == nil {
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

				col := rec.Column(idColIdx)
				rowIdx := -1

				// Handle different ID types
				switch arr := col.(type) {
				case *array.String:
					for j := 0; j < arr.Len(); j++ {
						if arr.Value(j) == req.ID {
							rowIdx = j
							break
						}
					}
				case *array.Int64:
					var intID int64
					if n, _ := fmt.Sscanf(req.ID, "%d", &intID); n == 1 {
						for j := 0; j < arr.Len(); j++ {
							if arr.Value(j) == intID {
								rowIdx = j
								break
							}
						}
					}
				case *array.Uint64:
					var uintID uint64
					if n, _ := fmt.Sscanf(req.ID, "%d", &uintID); n == 1 {
						for j := 0; j < arr.Len(); j++ {
							if arr.Value(j) == uintID {
								rowIdx = j
								break
							}
						}
					}
				}

				if rowIdx != -1 {
					// Check if already deleted
					ts := ds.Tombstones[i]
					if ts != nil && ts.Contains(rowIdx) {
						found = true // Already deleted
						break
					}

					ds.dataMu.RUnlock()
					ds.dataMu.Lock()
					if ds.Tombstones[i] == nil {
						ds.Tombstones[i] = qry.NewBitset()
					}
					ds.Tombstones[i].Set(rowIdx)
					ds.dataMu.Unlock()
					metrics.TombstonesTotal.WithLabelValues(req.Dataset).Inc()
					found = true
					ds.dataMu.RLock()
					break
				}
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

	case "HybridSearch":
		var req struct {
			Dataset   string                 `json:"dataset"`
			Vector    []float32              `json:"vector"`
			K         int                    `json:"k"`
			TextQuery string                 `json:"text_query"`
			Alpha     float32                `json:"alpha"`
			Filters   map[string]interface{} `json:"filters"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}

		// Convert generic dictionary filters to string map if needed, or update HybridSearch sig
		// s.HybridSearch signature: (ctx, name, query []float32, k int, filters map[string]string)
		// We'll coerce filters to map[string]string for now
		strFilters := make(map[string]string)
		for k, v := range req.Filters {
			strFilters[k] = fmt.Sprintf("%v", v)
		}

		// Generate Cache Key
		cacheKey := HashHybridQuery(
			req.Dataset,
			req.Vector,
			req.TextQuery,
			req.K,
			req.Alpha,
			60,  // Default RRF k
			0.0, // Default Graph Alpha
			0,   // Default Graph Depth
		)

		// Check Cache
		if cached, ok := s.queryCache.Get(cacheKey); ok {
			// Hit!
			body, err := json.Marshal(cached)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to marshal cached results: %v", err)
			}
			return stream.Send(&flight.Result{Body: body})
		}

		// Use SearchHybrid for text+vector search
		// Signature: (ctx, name, query, textQuery, k, alpha, rrfK, graphAlpha, graphDepth)
		// Filters are currently not supported in this pipeline path
		results, err := s.SearchHybrid(
			stream.Context(),
			req.Dataset,
			req.Vector,
			req.TextQuery,
			req.K,
			req.Alpha,
			60,  // Default RRF k
			0.0, // Default Graph Alpha
			0,   // Default Graph Depth
		)
		if err == nil {
			// Cache the result
			s.queryCache.Put(cacheKey, results)
		}
		if err != nil {
			return ToGRPCStatus(err)
		}

		// Serialize results
		body, err := json.Marshal(results)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal hybrid results: %v", err)
		}
		return stream.Send(&flight.Result{Body: body})
	}
	return status.Error(codes.Unimplemented, "unknown action type "+action.Type)
}

// DoPut - Optimized implementation with batching
func (s *VectorStore) DoPut(stream flight.FlightService_DoPutServer) error {
	// Use TrackingAllocator to monitor zero-copy behavior (expecting low allocations)
	// and track metadata overhead.
	trackAlloc := lmem.NewTrackingAllocator(s.pooledMem)
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
		// Pre-warm dataset with schema to avoid lazy init overhead in first batch
		s.PrewarmDataset(name, r.Schema())
	} else {
		return fmt.Errorf("missing flight descriptor path")
	}

	s.logger.Info().Str("name", name).Msg("DoPut started (Batched)")
	s.logger.Info().Str("schema", r.Schema().String()).Msg("DoPut Schema")

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
				if err := s.flushPutBatch(ds, batch); err != nil {
					return err
				}
				batch = batch[:0]
				return nil
			}
		}

		// Flush single combined batch
		err := s.flushPutBatch(ds, []arrow.RecordBatch{combined})
		combined.Release()
		if err != nil {
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
		if len(batch) == 0 && rec.NumRows() >= 25000 {
			rec.Retain()
			if err := s.flushPutBatch(ds, []arrow.RecordBatch{rec}); err != nil {
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
func (s *VectorStore) flushPutBatch(ds *Dataset, batch []arrow.RecordBatch) error {
	if len(batch) == 0 {
		return nil
	}
	name := ds.Name

	// 1. Enqueue to Persistence Queue (Async WAL) & Ingestion Queue (Async Indexing)
	// We do this in parallel or sequentially.
	// Since both are async queues now, the latency is just channel send.

	ts := time.Now().UnixNano()

	// Backpressure: Check if we should throttle
	if s.CheckIngestionBackpressure() {
		// Log warning occasionally (every 5 seconds?) or use rate limiter
		s.logger.Warn().Msg("Applying ingestion backpressure (throttling)")
		// Loop with sleep until pressure relieves or context done
		ticker := time.NewTicker(100 * time.Millisecond) // Check every 100ms
		defer ticker.Stop()

		// Wait loop
		for s.CheckIngestionBackpressure() {
			select {
			case <-ticker.C:
				// Continue checking
			}
		}
	}

	for _, rec := range batch {
		rec.Retain() // Retain for Persistence Worker
		rec.Retain() // Retain for Ingestion Worker (applyBatchToMemory triggers release)

		// Note: We retain twice because two different workers will Release() it.

		// Update lag metric
		metrics.IngestionLagCount.Add(float64(rec.NumRows()))

		// 1. Send to Persistence (Backpressure if full to ensure durability logic isn't overrun)
		// If queue is full, we block. This throttles client if disk is slow.
		s.persistenceQueue <- persistenceJob{datasetName: name, batch: rec, ts: ts}

		// 2. Send to Ingestion
		// Increment pending ingestion count
		ds.PendingIngestion.Add(1)

		s.ingestionQueue.PushBlocking(ingestionJob{datasetName: name, batch: rec, ts: ts}, 5*time.Second)
	}

	return nil
}

// StoreRecordBatch stores a batch of records in a dataset
func (s *VectorStore) StoreRecordBatch(ctx context.Context, name string, rec arrow.RecordBatch) error {
	ts := time.Now().UnixNano()

	rec.Retain() // For Persistence
	rec.Retain() // For Ingestion
	metrics.IngestionLagCount.Add(float64(rec.NumRows()))

	s.persistenceQueue <- persistenceJob{datasetName: name, batch: rec, ts: ts}
	s.ingestionQueue.PushBlocking(ingestionJob{datasetName: name, batch: rec, ts: ts}, 5*time.Second)

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
func (s *VectorStore) applyBatchToMemory(name string, rec arrow.RecordBatch, ts int64) error {
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

	if batchSize > 100*1024*1024 {
		s.logger.Warn().Int64("size", batchSize).Msg("Large memory addition in DoPut")
	}
	s.currentMemory.Add(batchSize)
	ds.SizeBytes.Add(batchSize)
	metrics.FlightRowsProcessed.WithLabelValues("put", "ok").Add(float64(rec.NumRows()))

	// Extract IDs and Vectors outside lock for better concurrency
	idMap := ds.ExtractIDs(rec)

	// Prepare DiskStore data outside lock
	var diskVecs [][]float32
	if ds.DiskStore != nil {
		vecColIdx := -1
		for i, f := range rec.Schema().Fields() {
			if f.Name == "vector" {
				vecColIdx = i
				break
			}
		}
		if vecColIdx != -1 {
			n := int(rec.NumRows())
			diskVecs = make([][]float32, 0, n)
			for i := 0; i < n; i++ {
				if vec, err := ExtractVectorFromArrow(rec, i, vecColIdx); err == nil {
					diskVecs = append(diskVecs, vec)
				}
			}
		}
	}

	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()
	dsLockStart := time.Now()

	// Lazy Index Initialization
	if ds.Index == nil {
		config := s.autoShardingConfig
		if config.ShardThreshold == 0 {
			config.ShardThreshold = 10000
			config.Enabled = true
			config.ShardCount = runtime.NumCPU()
		}

		// Infer DataType from the FIRST record
		dataType := InferVectorDataType(rec.Schema(), "vector")
		if config.IndexConfig == nil {
			hnswCfg := DefaultArrowHNSWConfig()
			hnswCfg.Metric = ds.Metric
			hnswCfg.DataType = dataType
			config.IndexConfig = &hnswCfg
		} else {
			config.IndexConfig.DataType = dataType
		}

		aIdx := NewAutoShardingIndex(ds, config)
		if vecCol := findVectorColumn(rec); vecCol != nil {
			if listArr, ok := vecCol.(*array.FixedSizeList); ok {
				dim := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
				// Use logical dimension for ArrowHNSW
				if dataType == VectorTypeComplex64 || dataType == VectorTypeComplex128 {
					dim /= 2
				}
				aIdx.SetInitialDimension(dim)
			}
		}
		ds.Index = aIdx
	}

	batchIdx := len(ds.Records)
	ds.Records = append(ds.Records, rec)
	rec.Retain() // Dataset holds a reference

	// Increment pending jobs count while holding lock to ensure compaction sees it
	ds.PendingIndexJobs.Add(1)

	// Record NUMA node
	currCPU := GetCurrentCPU()
	currNode := -1
	if s.numaTopology != nil {
		currNode = s.numaTopology.GetNodeForCPU(currCPU)
	}
	ds.BatchNodes = append(ds.BatchNodes, currNode)

	// Bulk Update Primary Index under lock
	if idMap != nil {
		if ds.PrimaryIndex == nil {
			ds.PrimaryIndex = make(map[string]RowLocation)
		}
		for id, rowIdx := range idMap {
			ds.PrimaryIndex[id] = RowLocation{BatchIdx: batchIdx, RowIdx: rowIdx}
		}
	}

	metrics.DatasetLockWaitDurationSeconds.WithLabelValues("put").Observe(time.Since(dsLockStart).Seconds())

	// Batch append to DiskStore outside main dataset lock
	if len(diskVecs) > 0 {
		if _, err := ds.DiskStore.BatchAppend(diskVecs); err != nil {
			s.logger.Error().Err(err).Msg("Failed to batch append to DiskStore")
		} else {
			metrics.DiskStoreWriteBytesTotal.WithLabelValues(name).Add(float64(len(diskVecs) * ds.DiskStore.dim * 4))
		}
	}

	// Update LWW/Merkle and Queue Indexing
	s.updateLWWAndMerkle(ds, rec, ts)

	// Dispatch batch-level indexing job asynchronously to avoid blocking DoPut
	rec.Retain() // IndexJob holds ref
	job := IndexJob{
		DatasetName: name,
		Record:      rec,
		BatchIdx:    batchIdx,
		CreatedAt:   time.Now(),
	}

	// Try non-blocking send first
	if !s.indexQueue.Send(job) {
		// Queue full: dispatch in goroutine to unblock DoPut latency
		// Note: This relies on WAL for durability if the process crashes before this goroutine finishes.
		metrics.IndexJobsOverflowTotal.Inc()
		s.pendingOverflowJobs.Add(1)
		go func() {
			defer s.pendingOverflowJobs.Add(-1)
			// Block efficiently until space is available
			s.indexQueue.Block(job)
		}()
	}

	// Inverted index update removed from here - now handled by runIndexWorker asynchronously

	// Compaction trigger check (now integrated into the primary lock section or performed without re-locking)
	if s.compactionWorker != nil {
		if len(ds.Records) >= s.compactionConfig.MinBatchesToCompact {
			_ = s.compactionWorker.TriggerCompaction(name)
		}
	}

	return nil
}
