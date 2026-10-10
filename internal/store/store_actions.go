package store

import (
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/storage"
	"github.com/23skdu/longbow/internal/store/types"
)

// DoAction handles custom actions like deletion, status, and graph operations.
func (s *VectorStore) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch action.Type {
	case "ForceSnapshot":
		err := s.Snapshot(stream.Context())
		if err != nil {
			return status.Errorf(codes.Internal, "failed to trigger manual snapshot: %v", err)
		}
		if err := stream.Send(&flight.Result{Body: []byte("ACK")}); err != nil {
			return err
		}
		return nil

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

	case "ResetDataset":
		var req struct {
			Name string `json:"name"`
		}
		if len(action.Body) > 0 {
			if err := json.Unmarshal(action.Body, &req); err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
			}
		}

		if req.Name != "" && req.Name != "all" {
			s.logger.Info().Str("dataset", req.Name).Msg("In-place ResetDataset called for specific dataset")
			if err := s.DropDataset(stream.Context(), req.Name); err != nil {
				return types.ToGRPCStatus(err)
			}
			debug.FreeOSMemory()
			if err := stream.Send(&flight.Result{Body: []byte(`{"status": "reset_success"}`)}); err != nil {
				return err
			}
			return nil
		}

		// Reset ALL datasets!
		s.logger.Info().Msg("In-place ResetDataset called for ALL datasets")
		datasetsPtr := s.datasets.Load()
		if datasetsPtr != nil {
			datasets := *datasetsPtr
			for name := range datasets {
				s.logger.Info().Str("dataset", name).Msg("Dropping dataset during global in-place reset")
				if err := s.DropDataset(stream.Context(), name); err != nil {
					s.logger.Error().Err(err).Str("dataset", name).Msg("Failed to drop dataset during global reset")
				}
			}
		}

		debug.FreeOSMemory()
		if err := stream.Send(&flight.Result{Body: []byte(`{"status": "reset_all_success"}`)}); err != nil {
			return err
		}
		return nil

	case "ReplicateWAL":
		if len(action.Body) == 0 {
			return status.Error(codes.InvalidArgument, "empty WAL payload")
		}

		// Decode and apply in memory
		engine := s.engine.Load()
		if engine != nil {
			err := engine.AppendReplicatedWAL(action.Body)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to append replicated WAL: %v", err)
			}

			entries, err := storage.DecodeWALBlock(action.Body, engine.GetAllocator())
			if err != nil {
				return status.Errorf(codes.Internal, "failed to decode replicated WAL: %v", err)
			}
			for _, entry := range entries {
				// Apply to in-memory datasets
				_ = s.applyReplayBatch(entry.Name, entry.Record, entry.Seq, entry.Ts)
				entry.Record.Release()
			}
		}

		if err := stream.Send(&flight.Result{Body: []byte("ACK")}); err != nil {
			return err
		}
		return nil

	case "check_readiness":
		var req struct {
			Dataset string `json:"dataset"`
		}
		// Body is optional
		if len(action.Body) > 0 {
			if err := query.ParseDatasetRequest(action.Body, &req.Dataset); err != nil {
				return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
			}
		}

		resp := map[string]any{
			"status": "READY",
		}

		// 1. Check Memory Pressure FIRST (prevents admission deadlock).
		//    When physical memory exceeds the hard limit, ingestion is throttled to 0
		//    which keeps pending > 0 forever — if we checked queue length first,
		//    we'd report BUSY instead of RESOURCE_EXHAUSTED and clients would block
		//    indefinitely up to the 4-hour timeout.
		if s.admission != nil {
			if err := s.admission.CanAdmitSearch(); err != nil {
				st, ok := status.FromError(err)
				if (ok && st.Code() == codes.ResourceExhausted) || strings.Contains(err.Error(), "ResourceExhausted") || strings.Contains(err.Error(), "exceeds limit") {
					resp["status"] = "RESOURCE_EXHAUSTED"
					resp["exhausted"] = true
					resp["reason"] = fmt.Sprintf("memory pressure: %v", err)
					metrics.ReadinessExhaustedTotal.Inc()
					body, marshalErr := json.Marshal(resp)
					if marshalErr != nil {
						return status.Errorf(codes.Internal, "failed to serialize status: %v", marshalErr)
					}
					return stream.Send(&flight.Result{Body: body})
				}
			}
		}

		// 2. Check Global Queue
		qLen := s.indexQueue.Len()
		if qLen > 0 {
			resp["status"] = "BUSY"
			resp["reason"] = fmt.Sprintf("global index queue has %d jobs", qLen)
		} else if req.Dataset != "" {
			// 3. Check Specific Dataset
			ds, ok := s.getDataset(req.Dataset)
			if !ok {
				resp["status"] = "NOT_FOUND"
				resp["reason"] = "dataset not found"
			} else {
				pending := ds.PendingIndexJobs.Load()
				pendingIngestion := ds.PendingIngestion.Load()
				activeStreams := ds.ActiveIngestStreams.Load()
				isMigrating := ds.Admission != nil && ds.Admission.migratingCount.Load() > 0

				// Stuck PendingIngestion watchdog: if > 0 for > 60s with no completion or active streams,
				// exclude it from the BUSY check (don't modify the counter so eventual worker completion
				// still produces correct value).
				if pendingIngestion > 0 && activeStreams == 0 && !isMigrating {
					lastCompletion := ds.LastIngestionCompletion.Load()
					elapsed := time.Now().Unix() - lastCompletion
					if lastCompletion > 0 && elapsed > 60 {
						s.logger.Warn().
							Str("dataset", req.Dataset).
							Int64("pending_ingestion", pendingIngestion).
							Int64("pending_index_jobs", pending).
							Int64("elapsed_sec", elapsed).
							Msg("PendingIngestion stuck >60s - excluding from BUSY check")
						pendingIngestion = 0
					} else if lastCompletion == 0 && elapsed > 60 {
						s.logger.Warn().
							Str("dataset", req.Dataset).
							Int64("pending_ingestion", pendingIngestion).
							Int64("elapsed_sec", elapsed).
							Msg("PendingIngestion stuck >60s with no completion time recorded")
						pendingIngestion = 0
					}
				}

				if pending > 0 || pendingIngestion > 0 || activeStreams > 0 || isMigrating {
					resp["status"] = "BUSY"
					resp["reason"] = fmt.Sprintf("dataset has %d pending index jobs, %d pending ingestion jobs, %d active streams, migrating=%t", pending, pendingIngestion, activeStreams, isMigrating)
				} else if ds.Index == nil {
					resp["status"] = "BUSY"
					resp["reason"] = "index not initialized"
				} else if !ds.IsReady.Load() {
					resp["status"] = "BUSY"
					resp["reason"] = "metadata registration in progress"
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
			if err := query.ParseDatasetRequest(action.Body, &req.Dataset); err != nil {
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

	case "drop", "Drop":
		var req struct {
			Dataset string `json:"dataset"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			// Fallback to simple string if not JSON object
			var name string
			if err := json.Unmarshal(action.Body, &name); err == nil {
				req.Dataset = name
			} else {
				return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
			}
		}
		if err := s.DropDataset(stream.Context(), req.Dataset); err != nil {
			return status.Errorf(codes.Internal, "failed to drop dataset: %v", err)
		}
		s.logger.Info().Str("dataset", req.Dataset).Msg("Dataset dropped via action")
		return stream.Send(&flight.Result{Body: []byte(`{"status": "dropped"}`)})

	case "delete", "Delete":
		var req core.VectorSearchByIDRequest
		if err := query.ParseSearchByIDRequest(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}
		s.WaitForIndexing(req.Dataset)

		ds, ok := s.getDataset(req.Dataset)
		if !ok {
			// This was err return in old code, assuming err != nil check implies not found or error
			// The original code: ds, err := s.getDataset... if err != nil return err
			// Our helper returns (ds, bool). So if !ok return error.
			return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
		}

		ds.dataMu.RLock()
		found := false
		ds.metadataMu.Lock()

		// Use PrimaryIndex for O(1) lookup
		if ds.PrimaryIndex != nil {
			if loc, ok := ds.PrimaryIndex[req.ID]; ok {
				// We found the location!

				// Check if already deleted
				if ts, ok := ds.Tombstones[loc.BatchIdx]; ok && ts != nil && ts.Contains(loc.RowIdx) {
					// Already deleted, treat as success
					found = true
				} else {
					// Set tombstone.
					if ds.Tombstones[loc.BatchIdx] == nil {
						ds.Tombstones[loc.BatchIdx] = types.NewBitset()
					}
					ds.Tombstones[loc.BatchIdx].Set(loc.RowIdx)
					ds.RecordBatchDeletion(loc.BatchIdx)
					metrics.TombstonesTotal.WithLabelValues(ds.Name).Inc()
					found = true
				}
			}
		}
		ds.metadataMu.Unlock()

		// Fallback Linear Scan (if not found in PrimaryIndex)
		if !found {
			for i, rec := range ds.Records.Read() {
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

					ds.metadataMu.Lock()
					if ds.Tombstones[i] == nil {
						ds.Tombstones[i] = types.NewBitset()
					}
					ds.Tombstones[i].Set(rowIdx)
					ds.RecordBatchDeletion(i)
					ds.metadataMu.Unlock()
					metrics.TombstonesTotal.WithLabelValues(req.Dataset).Inc()
					found = true
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

	case "DeleteNamespace", "delete-namespace", "delete_namespace":
		var nsName string
		if err := query.ParseDatasetRequest(action.Body, &nsName); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}

		if nsName == "" {
			return status.Error(codes.InvalidArgument, "missing namespace name")
		}

		if err := s.DeleteNamespace(nsName); err != nil {
			return status.Errorf(codes.Internal, "failed to delete namespace: %v", err)
		}
		if err := stream.Send(&flight.Result{Body: []byte("deleted")}); err != nil {
			return err
		}
		return nil

	case "delete-dataset":
		var dsName string
		if err := query.ParseDatasetRequest(action.Body, &dsName); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}

		if dsName == "" {
			return status.Error(codes.InvalidArgument, "missing dataset name")
		}

		if err := s.DropDataset(stream.Context(), dsName); err != nil {
			return status.Errorf(codes.NotFound, "failed to drop dataset: %v", err)
		}
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
			ds.Tombstones[loc.BatchIdx] = types.NewBitset()
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
		return s.HandleVectorSearchAction(action, stream)

	case "VectorSearchByID":
		return s.handleVectorSearchByIDAction(action, stream)

	case "search", "dense", "sparse", "filtered", "hybrid":
		// Handle generic search action types - map to VectorSearch handler
		// Client sends: search, dense, sparse, filtered, hybrid
		// Server expects: VectorSearch
		return s.HandleVectorSearchAction(action, stream)

	case "traverse-graph":
		return s.handleTraverseGraph(action.Body, stream)

	case "GetGraphStats":
		return s.handleGetGraphStats(action.Body, stream)

	case "calculate-pagerank":
		return s.handleCalculatePageRank(action.Body, stream)

	case "detect-communities":
		return s.handleDetectCommunities(action.Body, stream)

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
		// Use SearchHybrid for text+vector search with Circuit Breaker
		cb := s.Breakers.GetOrCreate(req.Dataset)
		resultsAny, err := cb.Execute(func() (any, error) {
			return s.SearchHybrid(
				stream.Context(),
				req.Dataset,
				req.Vector,
				req.TextQuery,
				req.K,
				req.Alpha,
				60,    // Default RRF k
				0.0,   // Default Graph Alpha
				0,     // Default Graph Depth
				false, // RawHybrid
			)
		})

		var results []types.SearchResult
		if err == nil {
			results = resultsAny.([]types.SearchResult)
			// Cache the result
			s.queryCache.PutUint64(cacheKey, results)
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

	case "create_dataset", "CreateDataset":
		var req struct {
			Name           string `json:"name"`
			Dimension      int    `json:"dimension"`
			VectorType     string `json:"vector_type,omitempty"`
			TurboQuantBits int    `json:"turboquant_bits,omitempty"`
			Metric         string `json:"metric,omitempty"`
			GeoEnabled     bool   `json:"geo_enabled,omitempty"`
			DiskEnabled    bool   `json:"disk_enabled,omitempty"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}
		if req.Name == "" {
			return status.Errorf(codes.InvalidArgument, "dataset name is required")
		}

		// Create metadata with longbow prefix
		metaMap := make(map[string]string)
		if req.VectorType != "" {
			metaMap["longbow.vector_type"] = req.VectorType
		}
		if req.TurboQuantBits > 0 {
			metaMap["longbow.turboquant_bits"] = fmt.Sprintf("%d", req.TurboQuantBits)
		}
		if req.Metric != "" {
			metaMap["longbow.metric"] = req.Metric
		}
		meta := arrow.MetadataFrom(metaMap)

		var vectorType arrow.DataType = arrow.PrimitiveTypes.Float32
		if req.Dimension > math.MaxInt32 || req.Dimension < 0 {
			return fmt.Errorf("vector dimension %d out of range (max %d)", req.Dimension, math.MaxInt32)
		}
		vecDim := int32(req.Dimension) // #nosec G115 (checked above)
		switch strings.ToLower(req.VectorType) {
		case "float16":
			vectorType = arrow.FixedWidthTypes.Float16
		case "float32":
			vectorType = arrow.PrimitiveTypes.Float32
		case "float64":
			vectorType = arrow.PrimitiveTypes.Float64
		case "int8":
			vectorType = arrow.PrimitiveTypes.Int8
		case "int16":
			vectorType = arrow.PrimitiveTypes.Int16
		case "int32":
			vectorType = arrow.PrimitiveTypes.Int32
		case "int64":
			vectorType = arrow.PrimitiveTypes.Int64
		case "uint8":
			vectorType = arrow.PrimitiveTypes.Uint8
		case "uint16":
			vectorType = arrow.PrimitiveTypes.Uint16
		case "uint32":
			vectorType = arrow.PrimitiveTypes.Uint32
		case "uint64":
			vectorType = arrow.PrimitiveTypes.Uint64
		case "complex64":
			vectorType = arrow.PrimitiveTypes.Float32
			vecDim *= 2
		case "complex128":
			vectorType = arrow.PrimitiveTypes.Float64
			vecDim *= 2
		case "turboquant", "tq":
			vectorType = arrow.PrimitiveTypes.Float32
		}

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.FixedSizeListOf(vecDim, vectorType)}, // #nosec G115
			{Name: "timestamp", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}},
		}, &meta)

		_, created := s.getOrCreateDataset(req.Name, func() *Dataset {
			ds := NewDataset(req.Name, schema)
			ds.Logger = s.logger
			ds.Topo = s.numaTopology
			if req.GeoEnabled {
				geoCfg := &GeoSearchConfig{
					DistanceType: GeoDistanceHaversine,
					EarthRadius:  6371.0,
				}
				ds.GeoIndex = NewGeoIndex(ds.Name, req.Dimension, geoCfg)
				ds.GeoIndex.ds = ds
				if gIdx, err := s.getGPUIndex(req.Dimension); err == nil {
					ds.GeoIndex.SetGPUIndex(gIdx)
				}
			}
			if req.DiskEnabled {
				ds.DiskStore, _ = NewDiskVectorStore(filepath.Join(s.dataPath, "disk", ds.Name), req.Dimension)
			}
			return ds
		})

		resp := map[string]any{"status": "created", "dataset": req.Name, "created": created}
		body, _ := json.Marshal(resp)
		return stream.Send(&flight.Result{Body: body})

	case "CreateNamespace":
		var req struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}
		if err := s.CreateNamespace(req.Name); err != nil {
			return status.Errorf(codes.AlreadyExists, "failed to create namespace: %v", err)
		}
		return stream.Send(&flight.Result{Body: []byte("namespace created")})

	case "ListNamespaces":
		names := s.ListNamespaces()
		body, _ := json.Marshal(map[string]any{"namespaces": names})
		return stream.Send(&flight.Result{Body: body})

	case "ListDatasetsInNamespace":
		var req struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}
		datasets := s.ListDatasetsInNamespace(req.Name)
		body, _ := json.Marshal(map[string]any{"datasets": datasets})
		return stream.Send(&flight.Result{Body: body})

	case "GeoSearch":
		var req types.GeoSearchRequest
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
		}
		ds, ok := s.getDataset(req.Dataset)
		if !ok {
			return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
		}
		if ds.GeoIndex == nil {
			return status.Error(codes.FailedPrecondition, "dataset has no geo index")
		}
		ds.dataMu.RLock()
		defer ds.dataMu.RUnlock()

		// Wrap with Circuit Breaker
		cb := s.Breakers.GetOrCreate(req.Dataset)
		resultsAny, err := cb.Execute(func() (any, error) {
			switch req.SearchType {
			case "radius":
				return ds.GeoIndex.SearchRadius(stream.Context(), req.Center, req.RadiusKm, req.K)
			case "box":
				if req.Box == nil {
					return nil, status.Error(codes.InvalidArgument, "box required")
				}
				return ds.GeoIndex.SearchBox(stream.Context(), *req.Box, req.K)
			case "hybrid":
				return ds.GeoIndex.HybridSearch(stream.Context(), req.QueryVector, req.Center, req.RadiusKm, req.K)
			default:
				return nil, status.Error(codes.InvalidArgument, "invalid search type")
			}
		})

		var results []types.SearchResult
		if err == nil {
			results = resultsAny.([]types.SearchResult)
		} else {
			return status.Errorf(codes.Internal, "geo search failed: %v", err)
		}
		results = s.mapInternalToUserIDsLocked(ds, results)
		body, _ := json.Marshal(results)
		return stream.Send(&flight.Result{Body: body})
	}
	return status.Error(codes.Unimplemented, "unknown action type "+action.Type)
}

// DoPut handles streaming ingestion of Arrow RecordBatches into the store.
