package store

import (
	"context"
	"encoding/json"
	"errors"
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

		resp := map[string]any{
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

		resp := map[string]any{
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
				resp["index_ready"] = ds.Index != nil
			}
		}

		body, err := json.Marshal(resp)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to serialize status: %v", err)
		}
		return stream.Send(&flight.Result{Body: body})

	case "wait-for-indexing":
		var req struct {
			Dataset string `json:"dataset"`
		}
		if len(action.Body) > 0 {
			if err := json.Unmarshal(action.Body, &req); err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
			}
		}
		if req.Dataset == "" {
			return status.Errorf(codes.InvalidArgument, "dataset name is required")
		}
		s.WaitForIndexing(req.Dataset)
		resp := map[string]any{"status": "complete", "dataset": req.Dataset}
		body, err := json.Marshal(resp)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to serialize response: %v", err)
		}
		if err := stream.Send(&flight.Result{Body: body}); err != nil {
			return err
		}
		return nil

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
					dsLockStart := time.Now()
					ds.dataMu.RUnlock()
					ds.dataMu.Lock()
					metrics.DatasetLockWaitDurationSeconds.WithLabelValues("delete_upgrade").Observe(time.Since(dsLockStart).Seconds())

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

	case "alter_schema", "alter-schema":
		var req struct {
			Dataset string `json:"dataset"`
			Action  string `json:"action"` // "add" or "drop"
			Column  string `json:"column"`
			Type    string `json:"type,omitempty"` // Data type string for add
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}

		ds, ok := s.getDataset(req.Dataset)
		if !ok {
			return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
		}

		switch strings.ToLower(req.Action) {
		case "add":
			var dtype arrow.DataType
			switch strings.ToLower(req.Type) {
			case "int64":
				dtype = arrow.PrimitiveTypes.Int64
			case "int32":
				dtype = arrow.PrimitiveTypes.Int32
			case "float32":
				dtype = arrow.PrimitiveTypes.Float32
			case "float64":
				dtype = arrow.PrimitiveTypes.Float64
			case "string":
				dtype = arrow.BinaryTypes.String
			case "bool":
				dtype = arrow.FixedWidthTypes.Boolean
			default:
				return status.Errorf(codes.InvalidArgument, "unsupported type: %s", req.Type)
			}
			if err := ds.SchemaManager.AddColumn(req.Column, dtype); err != nil {
				return status.Errorf(codes.Internal, "failed to add column: %v", err)
			}
		case "drop":
			if err := ds.SchemaManager.DropColumn(req.Column); err != nil {
				return status.Errorf(codes.Internal, "failed to drop column: %v", err)
			}
		default:
			return status.Errorf(codes.InvalidArgument, "invalid action: %s", req.Action)
		}

		ds.dataMu.Lock()
		ds.Schema = ds.SchemaManager.GetCurrentSchema()
		ds.dataMu.Unlock()

		return stream.Send(&flight.Result{Body: []byte("schema altered")})

	case "delete-dataset", "DeleteNamespace", "delete-namespace":
		var curr map[string]any
		if err := json.Unmarshal(action.Body, &curr); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}

		dsName, ok := curr["dataset"].(string)
		if !ok {
			dsName, ok = curr["name"].(string)
		}
		if !ok {
			return status.Error(codes.InvalidArgument, "missing dataset name (use 'dataset' or 'name')")
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

		var curr map[string]any
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
		locRaw, found := ds.Index.GetLocation(uint32(vid))
		if !found {
			return status.Errorf(codes.NotFound, "vector id %d not found in dataset %s (index len=%d)", vid, dsName, ds.Index.Len())
		}
		loc := locRaw.(Location)

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

	case "VectorSearch":
		return s.handleVectorSearchAction(action, stream)

	case "VectorSearchByID":
		return s.handleVectorSearchByIDAction(action, stream)

	case "search", "dense", "sparse", "filtered", "hybrid":
		// Handle generic search action types - map to VectorSearch handler
		// Client sends: search, dense, sparse, filtered, hybrid
		// Server expects: VectorSearch
		return s.handleVectorSearchAction(action, stream)

	case "traverse-graph":
		return s.handleTraverseGraph(action.Body, stream)

	case "GetGraphStats":
		return s.handleGetGraphStats(action.Body, stream)

	case "HybridSearch":
		var req struct {
			Dataset   string         `json:"dataset"`
			Vector    []float32      `json:"vector"`
			K         int            `json:"k"`
			TextQuery string         `json:"text_query"`
			Alpha     float32        `json:"alpha"`
			Filters   map[string]any `json:"filters"`
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
			return status.Errorf(codes.Internal, "failed to parse filters: %v", err)
		}

		// Serialize results
		body, err := json.Marshal(results)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to marshal hybrid results: %v", err)
		}
		return stream.Send(&flight.Result{Body: body})

	case "Compact":
		var req struct {
			Dataset string `json:"dataset"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}
		if s.compactionWorker != nil {
			s.compactionWorker.Trigger(req.Dataset)
		}
		return stream.Send(&flight.Result{Body: []byte("compaction_triggered")})

	case "TieredOffload":
		var req struct {
			Dataset string `json:"dataset"`
			MaxAge  string `json:"max_age"` // e.g., "1h"
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}
		ds, ok := s.getDataset(req.Dataset)
		if !ok {
			return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
		}
		if ds.DiskStore == nil {
			return status.Error(codes.FailedPrecondition, "dataset does not have a disk store")
		}

		maxAge, err := time.ParseDuration(req.MaxAge)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid max_age: %v", err)
		}

		offloaded, err := ds.DiskStore.EnforcePolicy(stream.Context(), maxAge)
		if err != nil {
			return status.Errorf(codes.Internal, "offload failed: %v", err)
		}

		resp := map[string]any{
			"offloaded_blocks": offloaded,
		}
		body, _ := json.Marshal(resp)
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

	s.logger.Info().Str("dataset", name).Msg("DoPut started (Batched)")
	s.logger.Info().Str("schema", r.Schema().String()).Msg("DoPut Schema")
	// Use RCU helper for create
	ds, created := s.getOrCreateDataset(name, func() *Dataset {
		ds := NewDataset(name, r.Schema())
		ds.Logger = s.logger
		ds.Topo = s.numaTopology

		// Disk Store Initialization (Phase 6)
		if strings.HasPrefix(name, "test_disk") || os.Getenv("LONGBOW_USE_DISK") == "1" {
			path := filepath.Join(s.dataPath, name+"_vectors.bin")
			dim := 0
			// Manual find vector column from schema
			for _, f := range r.Schema().Fields() {
				if f.Name == "vector" || f.Name == "embedding" {
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

		return ds
	})

	if ds == nil {
		return status.Errorf(codes.Internal, "failed to retrieve or create dataset %s", name)
	}

	// Schema Evolution & Validation
	// Validate compatibility and evolve if additive changes are present
	if err := ds.SchemaManager.Evolve(r.Schema()); err != nil {
		s.logger.Error().Err(err).Str("dataset", name).Msg("Schema evolution/validation failed")
		return status.Errorf(codes.InvalidArgument, "schema mismatch: %v", err)
	}

	// Update dataset's schema reference to ensure it uses the latest version
	// We need to lock to update the pointer safely
	ds.dataMu.Lock()
	ds.Schema = ds.SchemaManager.GetCurrentSchema()
	ds.dataMu.Unlock()

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
	if idx != nil {
		s.initGPUIfEnabled(idx)
	}

	// Batching configuration
	const maxBatchRows = 50000             // Aggressive batching for small vectors
	const maxBatchBytes = 32 * 1024 * 1024 // 32MB cap
	batch := make([]arrow.RecordBatch, 0, 100)

	// Helper to flush batch
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Check total size of batch
		totalBytes := int64(0)
		for _, b := range batch {
			totalBytes += estimateBatchSize(b)
		}

		metrics.DoPutBatchSizeBytes.Observe(float64(totalBytes))

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
				if err := s.flushPutBatch(stream.Context(), ds, batch); err != nil {
					return err
				}
				batch = batch[:0]
				return nil
			}
		}

		// Flush single combined batch
		err := s.flushPutBatch(stream.Context(), ds, []arrow.RecordBatch{combined})
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

		// Adaptive Batching (Byte-Aware Option 1):
		// If the record is large enough (>= 10MB) and we don't have pending small records,
		// write it directly to avoid concatenation/slice overhead.
		recSize := estimateBatchSize(rec)
		if len(batch) == 0 && recSize >= maxBatchBytes {
			rec.Retain()
			metrics.DoPutBatchSizeBytes.Observe(float64(recSize))
			if err := s.flushPutBatch(stream.Context(), ds, []arrow.RecordBatch{rec}); err != nil {
				rec.Release()
				return err
			}
			rec.Release()
			continue
		}

		rec.Retain()
		batch = append(batch, rec)

		// Check accumulator size
		totalBatchBytes := int64(0)
		totalBatchRows := int64(0)
		for _, b := range batch {
			totalBatchBytes += estimateBatchSize(b)
			totalBatchRows += b.NumRows()
		}

		if totalBatchRows >= maxBatchRows || totalBatchBytes >= maxBatchBytes {
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
func (s *VectorStore) flushPutBatch(ctx context.Context, ds *Dataset, batch []arrow.RecordBatch) error {
	s.broadcastCDC(ds.Name, batch)

	if len(batch) == 0 {
		return nil
	}
	name := ds.Name
	s.logger.Info().Str("dataset", name).Int("batch_size", len(batch)).Msg("Flushing put batch")

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
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
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
		select {
		case s.persistenceQueue <- persistenceJob{datasetName: name, batch: rec, ts: ts}:
		case <-ctx.Done():
			rec.Release() // Release both retains on cancellation
			rec.Release()
			return ctx.Err()
		}

		// 2. Send to Ingestion
		// Increment pending ingestion count
		ds.PendingIngestion.Add(1)

		if !s.ingestionQueue.PushBlocking(ingestionJob{ds: ds, batch: rec, ts: ts}, 5*time.Second) {
			// If PushBlocking fails (timeout or stop), we must adjust PendingIngestion
			ds.PendingIngestion.Add(-1)
			return errors.New("failed to enqueue ingestion job (timeout or queue closed)")
		}
	}

	return nil
}

func (s *VectorStore) StoreRecordBatch(ctx context.Context, name string, rec arrow.RecordBatch) error {
	if rec == nil {
		return errors.New("nil record batch")
	}
	ts := time.Now().UnixNano()

	ds, _ := s.getOrCreateDataset(name, func() *Dataset {
		d := NewDataset(name, rec.Schema())
		d.Logger = s.logger
		return d
	})

	rec.Retain() // For Persistence
	rec.Retain() // For Ingestion
	metrics.IngestionLagCount.Add(float64(rec.NumRows()))

	select {
	case s.persistenceQueue <- persistenceJob{datasetName: name, batch: rec, ts: ts}:
	case <-ctx.Done():
		rec.Release()
		rec.Release()
		return ctx.Err()
	}

	// Track pending ingestion BEFORE enqueuing to fix WaitForIndexing races
	ds.PendingIngestion.Add(1)

	// Dispatch for ingestion
	if !s.ingestionQueue.PushBlocking(ingestionJob{ds: ds, batch: rec, ts: ts}, 10*time.Second) {
		ds.PendingIngestion.Add(-1)
		return errors.New("failed to enqueue ingestion job")
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

// applyBatchToMemory applies a batch to the in-memory dataset and dispatches indexing
func (s *VectorStore) applyBatchToMemory(ds *Dataset, rec arrow.RecordBatch, ts int64) error {
	name := ds.Name

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
			if f.Name == "vector" || f.Name == "embedding" {
				vecColIdx = i
				break
			}
		}
		if vecColIdx != -1 {
			n := int(rec.NumRows())
			diskVecs = make([][]float32, 0, n)
			for i := 0; i < n; i++ {
				vec, err := ExtractVectorFromArrow(rec, i, vecColIdx)
				if err != nil {
					continue
				}
				diskVecs = append(diskVecs, vec)
			}
		}
	}

	dsLockStart := time.Now()
	ds.dataMu.Lock()

	// Lazy Index Initialization
	// Also check if existing index has wrong DataType (e.g. Pre-warmed with Float32 but data is Float64)
	var needsReindex bool
	if ds.Index != nil {
		if hnsw, ok := ds.Index.(*ArrowHNSW); ok {
			vecColName := "vector"
			for _, f := range rec.Schema().Fields() {
				if f.Name == "vector" || f.Name == "embedding" {
					vecColName = f.Name
					break
				}
			}
			wantType := InferVectorDataType(rec.Schema(), vecColName)
			if hnsw.config.DataType != wantType {
				s.logger.Info().Str("dataset", name).Str("have", hnsw.config.DataType.String()).Str("want", wantType.String()).Msg("Re-creating index for DataType mismatch")
				needsReindex = true
			}
		}
	}
	if ds.Index == nil || needsReindex {
		s.logger.Info().Str("dataset", name).Msg("Attempting lazy index initialization")
		config := s.autoShardingConfig
		if config.ShardThreshold == 0 {
			config.ShardThreshold = 10000
			config.Enabled = true
			config.ShardCount = runtime.NumCPU()
		}

		// Infer DataType from the FIRST record
		vecColName := "vector"
		for _, f := range rec.Schema().Fields() {
			if f.Name == "vector" || f.Name == "embedding" {
				vecColName = f.Name
				break
			}
		}
		dataType := InferVectorDataType(rec.Schema(), vecColName)
		s.logger.Info().Str("dataset", name).Str("dataType", dataType.String()).Str("column", vecColName).Msg("Inferred vector data type for new index")

		if config.IndexConfig == nil {
			hnswCfg := DefaultArrowHNSWConfig()
			hnswCfg.Metric = ds.Metric
			hnswCfg.DataType = dataType
			config.IndexConfig = &hnswCfg
		} else {
			// Clone the config to avoid polluting the shared autoShardingConfig
			clonedCfg := *config.IndexConfig
			clonedCfg.DataType = dataType
			config.IndexConfig = &clonedCfg
		}

		aIdx := NewAutoShardingIndex(ds, config)
		if vecCol := findVectorColumn(rec); vecCol != nil {
			if listArr, ok := vecCol.(*array.FixedSizeList); ok {
				dim := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
				s.logger.Info().Str("dataset", name).Int("dim", dim).Str("dataType", dataType.String()).Msg("findVectorColumn result")
				if dataType == VectorTypeFloat32 {
					if listType, ok := listArr.DataType().(*arrow.FixedSizeListType); ok {
						if listType.Elem().ID() == arrow.FLOAT32 && dim%2 == 0 {
							dataType = VectorTypeComplex64
							dim /= 2
							config.IndexConfig.DataType = dataType
							aIdx = NewAutoShardingIndex(ds, config)
							s.logger.Info().Str("dataset", name).Int("dim", dim).Str("dataType", dataType.String()).Msg("Detected complex64 from physical dimension")
						}
					}
				} else if dataType == VectorTypeComplex64 || dataType == VectorTypeComplex128 {
					dim /= 2
				}
				aIdx.SetInitialDimension(dim)
			}
		} else {
			s.logger.Error().Str("dataset", name).Msg("findVectorColumn returned nil")
		}
		ds.Index = aIdx
	}

	batchIdx := len(ds.Records)
	ds.Records = append(ds.Records, rec)
	rec.Retain()

	ds.PendingIndexJobs.Add(rec.NumRows())

	currCPU := GetCurrentCPU()
	currNode := -1
	if s.numaTopology != nil {
		currNode = s.numaTopology.GetNodeForCPU(currCPU)
	}
	ds.BatchNodes = append(ds.BatchNodes, currNode)

	metrics.DatasetLockWaitDurationSeconds.WithLabelValues("put").Observe(time.Since(dsLockStart).Seconds())

	// Update Primary Index and LWW/Merkle inside the main lock to prevent races
	// between concurrent ingestion workers.
	ds.UpdatePrimaryIndexAsync(batchIdx, idMap)
	s.updateLWWAndMerkle(ds, rec, ts)

	// Compaction trigger check inside lock
	if s.compactionWorker != nil && len(ds.Records) >= s.compactionConfig.MinBatchesToCompact {
		s.compactionWorker.TriggerCompaction()
	}

	ds.dataMu.Unlock()

	// Batch append to DiskStore outside main dataset lock to avoid blocking other workers
	if len(diskVecs) > 0 {
		if _, err := ds.DiskStore.BatchAppend(diskVecs); err != nil {
			s.logger.Error().Err(err).Msg("Failed to batch append to DiskStore")
		} else {
			metrics.DiskStoreWriteBytesTotal.WithLabelValues(name).Add(float64(len(diskVecs) * ds.DiskStore.dim * 4))
		}
	}

	// Dispatch batch-level indexing job asynchronously to avoid blocking DoPut
	rec.Retain() // IndexJob holds ref
	job := IndexJob{
		DatasetName: name,
		Record:      rec,
		BatchIdx:    batchIdx,
		CreatedAt:   time.Now(),
	}

	if !s.indexQueue.Send(job) {
		metrics.IndexJobsOverflowTotal.Inc()
		s.pendingOverflowJobs.Add(1)
		go func() {
			defer s.pendingOverflowJobs.Add(-1)
			s.indexQueue.Block(job, 1*time.Second)
		}()
	}

	return nil
}
