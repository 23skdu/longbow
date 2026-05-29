package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"

	lmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/storage"
	internalcore "github.com/23skdu/longbow/internal/store/index"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/23skdu/longbow/internal/tracing"
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
				activeStreams := ds.ActiveIngestStreams.Load()
				isMigrating := ds.Admission != nil && ds.Admission.migratingCount.Load() > 0
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
				return ds.GeoIndex.HybridSearch(stream.Context(), nil, req.Center, req.RadiusKm, req.K)
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
func (s *VectorStore) DoPut(stream flight.FlightService_DoPutServer) error {
	_, span := tracing.CreateSpan(stream.Context(), "DoPut")
	if span != nil {
		defer span.End()
	}

	// Fallback to standard reader for reliability during benchmarks
	r, err := flight.NewRecordReader(stream)
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

	// 0. Admission Control (Backpressure)
	if s.admission != nil {
		if err := s.admission.Admit(stream.Context(), "ingest"); err != nil {
			// If throttled, try one aggressive GC and retry
			if status.Code(err) == codes.ResourceExhausted {
				s.logger.Warn().Msg("Ingestion throttled, triggering emergency GC and retrying...")
				runtime.GC()
				debug.FreeOSMemory()
				time.Sleep(100 * time.Millisecond)

				if err2 := s.admission.Admit(stream.Context(), "ingest"); err2 != nil {
					return err2
				}
				s.logger.Info().Msg("Emergency GC successful, ingestion resumed")
			} else {
				return err
			}
		}
	}

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

	ds.ActiveIngestStreams.Add(1)
	defer ds.ActiveIngestStreams.Add(-1)

	// Namespace quota check (will be done per-flush in the loop below)
	nsName, _ := ParseNamespacedPath(name)
	var ns *Namespace
	if ns = s.GetNamespace(nsName); ns != nil {
		// Check initial quota on first batch
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
	const maxBatchRows = 10000             // Aggressive batching for small vectors
	const maxBatchBytes = 32 * 1024 * 1024 // 32MB cap
	batch := make([]arrow.RecordBatch, 0, 100)

	// Helper to flush batch
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Namespace quota check on flush
		if ns != nil {
			numVectors := int64(0)
			totalBytes := int64(0)
			for _, b := range batch {
				numVectors += b.NumRows()
				totalBytes += estimateBatchSize(b)
			}
			if err := ns.CheckQuota(numVectors, 0, totalBytes); err != nil {
				return status.Errorf(codes.ResourceExhausted, "namespace quota exceeded: %v", err)
			}
		}

		// Check total size of batch
		totalBytes := int64(0)
		totalRows := int64(0)
		for _, b := range batch {
			totalBytes += estimateBatchSize(b)
			totalRows += b.NumRows()
		}

		startFlush := time.Now()
		metrics.DoPutBatchSizeBytes.Observe(float64(totalBytes))
		metrics.DoPutBatchSizeVectors.Observe(float64(totalRows))

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

		// Flush single combined batch with Circuit Breaker.
		// pprof labels allow "go tool pprof" to filter by dataset/op so profiles
		// show per-dataset ingestion cost rather than a flat flushPutBatch stack.
		cb := s.Breakers.GetOrCreate(ds.Name)
		var flushErr error
		pprof.Do(stream.Context(), pprof.Labels("dataset", ds.Name, "op", "flush"), func(ctx context.Context) {
			_, flushErr = cb.Execute(func() (any, error) {
				return nil, s.flushPutBatch(ctx, ds, []arrow.RecordBatch{combined})
			})
		})
		combined.Release()
		if flushErr != nil {
			return flushErr
		}

		metrics.DoPutBatchLatencySeconds.Observe(time.Since(startFlush).Seconds())

		// Clear batch slice
		for _, b := range batch {
			b.Release()
		}
		batch = batch[:0]
		return nil
	}

	for r.Next() {
		rec := r.RecordBatch()

		// If the record itself is larger than maxBatchRows, slice it into manageable chunks.
		// This prevents O(N^2) bottlenecks in the indexing worker's AddBatchBulk phase.
		if rec.NumRows() > maxBatchRows {
			for i := int64(0); i < rec.NumRows(); i += maxBatchRows {
				end := i + maxBatchRows
				if end > rec.NumRows() {
					end = rec.NumRows()
				}
				subRec := rec.NewSlice(i, end)

				// Process sub-record
				subRecSize := estimateBatchSize(subRec)
				if len(batch) == 0 && subRecSize >= maxBatchBytes {
					subRec.Retain()
					metrics.DoPutBatchSizeBytes.Observe(float64(subRecSize))
					if err := s.flushPutBatch(stream.Context(), ds, []arrow.RecordBatch{subRec}); err != nil {
						subRec.Release()
						return err
					}
					subRec.Release()
					continue
				}

				subRec.Retain()
				batch = append(batch, subRec)

				// Check accumulator size
				totalBatchRows := int64(0)
				for _, b := range batch {
					totalBatchRows += b.NumRows()
				}

				if totalBatchRows >= maxBatchRows {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			continue
		}

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
		totalBatchRows := int64(0)
		for _, b := range batch {
			totalBatchRows += b.NumRows()
		}

		if totalBatchRows >= maxBatchRows {
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

	// Record to auto-scaler (Part 1.1)
	if s.scaler != nil {
		totalRows := 0
		for _, rec := range batch {
			totalRows += int(rec.NumRows())
		}
		s.scaler.RecordIngest(totalRows)
	}

	// Soft Backpressure: Apply linear delay if system is starting to get stressed
	if delay := s.IngestionBackpressureDelay(); delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	// Hard Backpressure: Block if system is at capacity
	if s.CheckIngestionBackpressure() {
		// Log warning occasionally (every 5 seconds?) or use rate limiter
		s.logger.Warn().Msg("Applying ingestion backpressure (HARD block)")
		// Loop with sleep until pressure relieves or context done
		ticker := time.NewTicker(200 * time.Millisecond) // Check every 200ms
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

		if !s.ingestionQueue.PushBlocking(IngestionJob{DS: ds, Batch: rec, TS: ts}, 5*time.Second) {
			// If PushBlocking fails (timeout or stop), we must adjust PendingIngestion
			ds.PendingIngestion.Add(-1)
			return errors.New("failed to enqueue ingestion job (timeout or queue closed)")
		}
	}

	return nil
}

// StoreRecordBatch ingests a record batch into the specified dataset.
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
	if !s.ingestionQueue.PushBlocking(IngestionJob{DS: ds, Batch: rec, TS: ts}, 10*time.Second) {
		ds.PendingIngestion.Add(-1)
		return errors.New("failed to enqueue ingestion job")
	}

	return nil
}

// concatenateBatches merges multiple record batches into one
func (s *VectorStore) concatenateBatches(batches []arrow.RecordBatch) (arrow.RecordBatch, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to concatenate")
	}
	schema := batches[0].Schema()
	numCols := int(schema.NumFields())
	columns := make([]arrow.Array, numCols)
	success := false
	defer func() {
		if !success {
			// Clean up if we fail mid-way
			for _, col := range columns {
				if col != nil {
					col.Release()
				}
			}
		}
	}()

	for i := 0; i < numCols; i++ {
		// Collect arrays for this column from all batches
		colArrays := make([]arrow.Array, len(batches))
		for j, batch := range batches {
			colArrays[j] = batch.Column(i)
		}

		// Use Arrow's array.Concatenate with pooled allocator for transient ingestion buffers
		alloc := s.pooledMem
		if alloc == nil {
			alloc = s.mem
		}
		if alloc == nil {
			alloc = memory.DefaultAllocator
		}
		concatenated, err := array.Concatenate(colArrays, alloc)
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

	success = true
	batch := array.NewRecordBatch(schema, columns, totalRows)
	for _, col := range columns {
		col.Release()
	}
	return batch, nil
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

	// Auto-quantization under memory pressure (>60% capacity).
	// Threshold intentionally lowered from 70% to 60% to give the compression path
	// a wider window before the heap is exhausted — preventing ResourceExhausted at ~425K vectors.
	maxMem := s.maxMemory.Load()
	if maxMem > 0 && s.currentMemory.Load() > int64(float64(maxMem)*0.60) {
		if ds.PreferredVectorType == types.VectorTypeFloat32 || ds.PreferredVectorType == types.VectorTypeUnknown {
			s.logger.Warn().Str("dataset", ds.Name).Msg("Memory pressure >60%. Dynamically promoting dataset to TurboQuant8.")
			ds.PreferredVectorType = types.VectorTypeTQ

			if h, ok := ds.Index.(*ArrowHNSW); ok {
				h.EnableTurboQuant(8)
			} else if asi, ok := ds.Index.(*AutoShardingIndex); ok {
				asi.mu.Lock()
				if h, ok := asi.current.(*ArrowHNSW); ok {
					h.EnableTurboQuant(8)
				} else if sh, ok := asi.current.(*ShardedHNSW); ok {
					for _, shardIdx := range sh.Shards() {
						if ah, ok := shardIdx.(*ArrowHNSW); ok {
							ah.EnableTurboQuant(8)
						}
					}
				}
				asi.mu.Unlock()
			} else if sh, ok := ds.Index.(*ShardedHNSW); ok {
				for _, shardIdx := range sh.Shards() {
					if ah, ok := shardIdx.(*ArrowHNSW); ok {
						ah.EnableTurboQuant(8)
					}
				}
			}
		}
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

	// Determine vector column for DiskStore (Zero-Copy Persistence)
	diskVecColIdx := -1
	if ds.DiskStore != nil {
		for i, f := range rec.Schema().Fields() {
			if f.Name == "vector" || f.Name == "embedding" {
				diskVecColIdx = i
				break
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
			for _, f := range ds.Schema.Fields() {
				if f.Name == "vector" {
					break
				}
			}
			wantType := InferVectorDataType(ds.Schema, "vector")
			if hnsw.GetConfig().DataType != wantType {
				s.logger.Info().Str("dataset", name).Str("have", hnsw.GetConfig().DataType.String()).Str("want", wantType.String()).Msg("Re-creating index for DataType mismatch")
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

		// Unspecified default logic: promote to turboquant8
		hasMetadataType := false
		if rec.Schema() != nil {
			md := rec.Schema().Metadata()
			if _, ok := md.GetValue("longbow.vector_type"); ok {
				hasMetadataType = true
			} else {
				idx := rec.Schema().FieldIndices(vecColName)
				if len(idx) > 0 {
					f := rec.Schema().Field(idx[0])
					if _, ok := f.Metadata.GetValue("longbow.vector_type"); ok {
						hasMetadataType = true
					}
				}
			}
		}

		if ds.PreferredVectorType != types.VectorTypeUnknown {
			dataType = ds.PreferredVectorType
		} else if !hasMetadataType && dataType == types.VectorTypeFloat32 {
			dataType = types.VectorTypeTQ
			ds.PreferredVectorType = types.VectorTypeTQ
		}

		s.logger.Info().Str("dataset", name).Str("dataType", dataType.String()).Str("column", vecColName).Msg("Inferred vector data type for new index")

		if config.IndexConfig == nil {
			hnswCfg := DefaultArrowHNSWConfig()
			hnswCfg.Metric = ds.Metric
			hnswCfg.DataType = dataType

			if dataType == VectorTypeTQ {
				hnswCfg.TurboQuantEnabled = true
				if ds.TurboQuantBits() > 0 {
					hnswCfg.TurboQuantBits = ds.TurboQuantBits()
				} else if hnswCfg.TurboQuantBits == 0 {
					hnswCfg.TurboQuantBits = 8
				}
			}
			config.IndexConfig = &hnswCfg
		} else {
			// Clone the config to avoid polluting the shared autoShardingConfig
			clonedCfg := *config.IndexConfig

			// Use preferred type if specified
			if ds.PreferredVectorType != types.VectorTypeUnknown {
				dataType = ds.PreferredVectorType
			}
			clonedCfg.DataType = dataType

			if dataType == VectorTypeTQ {
				clonedCfg.TurboQuantEnabled = true
				if ds.TurboQuantBits() > 0 {
					clonedCfg.TurboQuantBits = ds.TurboQuantBits()
				} else if clonedCfg.TurboQuantBits == 0 {
					clonedCfg.TurboQuantBits = 8
				}
			}
			config.IndexConfig = &clonedCfg
		}

		aIdx := NewAutoShardingIndex(ds, config)
		if vecCol := findVectorColumn(rec); vecCol != nil {
			if listArr, ok := vecCol.(*array.FixedSizeList); ok {
				dim := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
				s.logger.Info().Str("dataset", name).Int("dim", dim).Str("dataType", dataType.String()).Msg("findVectorColumn result")
				switch dataType {
				case VectorTypeFloat32:
					if listType, ok := listArr.DataType().(*arrow.FixedSizeListType); ok {
						if listType.Elem().ID() == arrow.FLOAT32 && dim%2 == 0 {
							// Only detect complex if field name suggests it
							if strings.Contains(strings.ToLower(vecCol.Data().DataType().Name()), "complex") {
								dataType = VectorTypeComplex64
								dim /= 2
								config.IndexConfig.DataType = dataType
								aIdx = NewAutoShardingIndex(ds, config)
								s.logger.Info().Str("dataset", name).Int("dim", dim).Str("dataType", dataType.String()).Msg("Detected complex64 from physical dimension")
							}
						}
					}
				case VectorTypeComplex64, VectorTypeComplex128:
					dim /= 2
				}
				if setter, ok := aIdx.(interface{ SetInitialDimension(int) }); ok {
					setter.SetInitialDimension(dim)
				}
			}
		} else {
			s.logger.Error().Str("dataset", name).Msg("findVectorColumn returned nil")
		}
		ds.Index = aIdx

		// Pre-warm the index metadata cache with the current schema so the first
		// AddBatch call doesn't pay lazy-cache population latency.
		if hnsw, ok := aIdx.(*ArrowHNSW); ok {
			hnsw.PreWarmMetadata(rec.Schema())
		} else if asi, ok := aIdx.(*AutoShardingIndex); ok {
			asi.mu.RLock()
			if current, ok := asi.current.(*ArrowHNSW); ok {
				current.PreWarmMetadata(rec.Schema())
			}
			asi.mu.RUnlock()
		}
	}

	currentRecords := ds.Records.Read()
	batchIdx := len(currentRecords)
	newRecords := make([]arrow.RecordBatch, len(currentRecords)+1)
	copy(newRecords, currentRecords)
	newRecords[len(currentRecords)] = rec
	ds.Records.UpdateInPlace(newRecords)
	rec.Retain()

	// Index text columns for hybrid BM25 search
	baseRowID := uint32(0)
	for _, r := range currentRecords {
		// #nosec G115
		baseRowID += uint32(r.NumRows())
	}
	s.indexTextColumnsForHybridSearch(ds, rec, baseRowID)

	// Mark dataset as ready after first successful ingestion
	if !ds.IsReady.Load() {
		ds.IsReady.Store(true)
		s.logger.Info().Str("dataset", name).Msg("Dataset metadata registration complete (Ready for queries)")
	}

	currCPU := lmem.GetCurrentCPU()
	currNode := -1
	if s.numaTopology != nil {
		currNode = s.numaTopology.GetNodeForCPU(currCPU)
	}
	currentNodes := ds.BatchNodes.Read()
	newNodes := make([]int, len(currentNodes)+1)
	copy(newNodes, currentNodes)
	newNodes[len(currentNodes)] = currNode
	ds.BatchNodes.UpdateInPlace(newNodes)

	metrics.DatasetLockWaitDurationSeconds.WithLabelValues("put").Observe(time.Since(dsLockStart).Seconds())

	ds.dataMu.Unlock()

	// Update Primary Index and LWW/Merkle outside the main lock to prevent search-blocking
	// contention while processing O(N) metadata updates.
	// metadataMu inside these methods ensures consistency.
	ds.UpdatePrimaryIndexAsync(batchIdx, idMap)
	idMap.Release()
	s.updateLWWAndMerkle(ds, rec, ts)

	// Batch append to DiskStore outside main dataset lock to avoid blocking other workers
	if diskVecColIdx != -1 {
		if _, err := ds.DiskStore.BatchAppendArrow(rec, diskVecColIdx); err != nil {
			s.logger.Error().Err(err).Msg("Failed to batch append to DiskStore (Zero-Copy)")
		} else {
			metrics.DiskStoreWriteBytesTotal.WithLabelValues(name).Add(float64(rec.NumRows() * int64(ds.DiskStore.dim) * 4))
		}
	}

	// Temporal Index Hook
	if s.temporalConfig.Enabled && ds.TemporalIndex != nil {
		idColIdx := -1
		vecColIdx := -1
		tsColIdx := -1
		for i, f := range rec.Schema().Fields() {
			switch f.Name {
			case "id":
				idColIdx = i
			case "vector", "embedding":
				vecColIdx = i
			case "timestamp":
				tsColIdx = i
			}
		}

		if idColIdx != -1 && vecColIdx != -1 {
			numRows := int(rec.NumRows())
			ids := make([]uint64, numRows)
			vectors := make([][]float32, numRows)
			timestamps := make([]int64, numRows)

			idArr := rec.Column(idColIdx).(*array.String)
			vecCol := rec.Column(vecColIdx)

			var tsArr arrow.Array
			if tsColIdx != -1 {
				tsArr = rec.Column(tsColIdx)
			}

			// Handle both FixedSizeList and variable-length List
			var listLen int
			if fs, ok := vecCol.DataType().(*arrow.FixedSizeListType); ok {
				listLen = int(fs.Len())
			}

			// Parallel Extraction
			pool := internalcore.GetSharedPool()
			pool.ParallelFor(numRows, 1024, func(start, end int) {
				for i := start; i < end; i++ {
					if idArr.IsValid(i) && vecCol.IsValid(i) {
						idStr := idArr.Value(i)
						id, _ := strconv.ParseUint(idStr, 10, 64)
						ids[i] = id

						if tsArr != nil && tsArr.IsValid(i) {
							switch arr := tsArr.(type) {
							case *array.Int64:
								timestamps[i] = arr.Value(i)
							case *array.Timestamp:
								timestamps[i] = int64(arr.Value(i))
							default:
								timestamps[i] = ts
							}
						} else {
							timestamps[i] = ts
						}

						var vStart, vEnd int
						var values arrow.Array
						if fs, ok := vecCol.(*array.FixedSizeList); ok {
							vStart = i * listLen
							vEnd = (i + 1) * listLen
							values = fs.ListValues()
						} else if l, ok := vecCol.(*array.List); ok {
							offsets := l.Offsets()
							vStart = int(offsets[i])
							vEnd = int(offsets[i+1])
							values = l.ListValues()
						}

						if values != nil {
							switch valArr := values.(type) {
							case *array.Float32:
								if vStart < valArr.Len() && vEnd <= valArr.Len() {
									src := valArr.Float32Values()[vStart:vEnd]
									sub := make([]float32, len(src))
									copy(sub, src)
									vectors[i] = sub
								}
							case *array.Float64:
								if vStart < valArr.Len() && vEnd <= valArr.Len() {
									f64Values := valArr.Float64Values()[vStart:vEnd]
									sub := make([]float32, len(f64Values))
									for j, v := range f64Values {
										sub[j] = float32(v)
									}
									vectors[i] = sub
								}
							}
						}
					}
				}
			})

			// Batch Add
			_ = ds.TemporalIndex.AddBatch(ids, vectors, timestamps, nil)
		}
	}

	// Geospatial Index Hook
	geoPointIdx := -1
	for i, f := range rec.Schema().Fields() {
		if f.Name == "geo_point" {
			geoPointIdx = i
			break
		}
	}

	if geoPointIdx != -1 {
		ds.dataMu.Lock()
		if ds.GeoIndex == nil {
			// Initialize with default config if missing
			geoCfg := &GeoSearchConfig{
				DistanceType: GeoDistanceHaversine,
				EarthRadius:  6371.0,
			}
			// Use dimension from vector column if possible, otherwise default to 128
			dim := 128
			if vecCol := findVectorColumn(rec); vecCol != nil {
				if listArr, ok := vecCol.(*array.FixedSizeList); ok {
					dim = int(listArr.DataType().(*arrow.FixedSizeListType).Len())
				}
			}
			ds.GeoIndex = NewGeoIndex(ds.Name, dim, geoCfg)
			s.logger.Info().Str("dataset", ds.Name).Int("dim", dim).Msg("Lazily initialized GeoIndex")
		}
		ds.dataMu.Unlock()

		idColIdx := -1
		vecColIdx := -1
		for i, f := range rec.Schema().Fields() {
			switch f.Name {
			case "id":
				idColIdx = i
			case "vector", "embedding":
				vecColIdx = i
			}
		}

		if idColIdx != -1 && vecColIdx != -1 {
			numRows := int(rec.NumRows())
			ids := make([]uint64, numRows)
			vectors := make([][]float32, numRows)
			points := make([]types.GeoPoint, numRows)
			valid := make([]bool, numRows)

			idArr := rec.Column(idColIdx).(*array.String)
			vecArr := rec.Column(vecColIdx).(*array.FixedSizeList)
			geoArr := rec.Column(geoPointIdx).(*array.FixedSizeList)
			geoValues := geoArr.ListValues().(*array.Float64).Float64Values()
			listLen := int(vecArr.DataType().(*arrow.FixedSizeListType).Len())

			// Parallel Extraction
			pool := internalcore.GetSharedPool()
			pool.ParallelFor(numRows, 1024, func(start, end int) {
				for i := start; i < end; i++ {
					if idArr.IsValid(i) && vecArr.IsValid(i) && geoArr.IsValid(i) {
						idStr := idArr.Value(i)
						id, _ := strconv.ParseUint(idStr, 10, 64)
						ids[i] = id

						vStart := i * listLen
						vEnd := (i + 1) * listLen
						listValues := vecArr.ListValues()

						switch values := listValues.(type) {
						case *array.Float32:
							src := values.Float32Values()[vStart:vEnd]
							sub := make([]float32, len(src))
							copy(sub, src)
							vectors[i] = sub
						case *array.Float64:
							f64Values := values.Float64Values()[vStart:vEnd]
							sub := make([]float32, len(f64Values))
							for j, v := range f64Values {
								sub[j] = float32(v)
							}
							vectors[i] = sub
						case *array.Int8:
							src := values.Int8Values()[vStart:vEnd]
							sub := make([]float32, len(src))
							for j, v := range src {
								sub[j] = float32(v)
							}
							vectors[i] = sub
						case *array.Int16:
							src := values.Int16Values()[vStart:vEnd]
							sub := make([]float32, len(src))
							for j, v := range src {
								sub[j] = float32(v)
							}
							vectors[i] = sub
						case *array.Int32:
							src := values.Int32Values()[vStart:vEnd]
							sub := make([]float32, len(src))
							for j, v := range src {
								sub[j] = float32(v)
							}
							vectors[i] = sub
						case *array.Int64:
							src := values.Int64Values()[vStart:vEnd]
							sub := make([]float32, len(src))
							for j, v := range src {
								sub[j] = float32(v)
							}
							vectors[i] = sub
						case *array.Uint8:
							src := values.Uint8Values()[vStart:vEnd]
							sub := make([]float32, len(src))
							for j, v := range src {
								sub[j] = float32(v)
							}
							vectors[i] = sub
						case *array.Uint16:
							src := values.Uint16Values()[vStart:vEnd]
							sub := make([]float32, len(src))
							for j, v := range src {
								sub[j] = float32(v)
							}
							vectors[i] = sub
						case *array.Uint32:
							src := values.Uint32Values()[vStart:vEnd]
							sub := make([]float32, len(src))
							for j, v := range src {
								sub[j] = float32(v)
							}
							vectors[i] = sub
						case *array.Uint64:
							src := values.Uint64Values()[vStart:vEnd]
							sub := make([]float32, len(src))
							for j, v := range src {
								sub[j] = float32(v)
							}
							vectors[i] = sub
						case *array.Float16:
							src := values.Values()[vStart:vEnd]
							sub := make([]float32, len(src))
							for j, v := range src {
								sub[j] = v.Float32()
							}
							vectors[i] = sub
						}

						if vectors[i] != nil {
							points[i] = types.GeoPoint{Lat: geoValues[i*2], Lon: geoValues[i*2+1]}
							valid[i] = true
						}
					}
				}
			})

			// Filter valid and Batch Add
			validIds := make([]uint64, 0, numRows)
			validVectors := make([][]float32, 0, numRows)
			validPoints := make([]types.GeoPoint, 0, numRows)
			for i := 0; i < numRows; i++ {
				if valid[i] {
					validIds = append(validIds, ids[i])
					validVectors = append(validVectors, vectors[i])
					validPoints = append(validPoints, points[i])
				}
			}

			_ = ds.GeoIndex.AddBatch(validIds, validVectors, validPoints, nil)
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

	ds.PendingIndexJobs.Add(rec.NumRows())
	if !s.indexQueue.Send(job) {
		metrics.IndexJobsOverflowTotal.Inc()
		s.pendingOverflowJobs.Add(1)
		go func() {
			defer s.pendingOverflowJobs.Add(-1)
			if !s.indexQueue.Block(job, 5*time.Second) {
				ds.PendingIndexJobs.Add(-rec.NumRows())
				rec.Release()
				s.logger.Warn().Str("dataset", name).Msg("Index job dropped after blocking timeout")
			}
		}()
	}

	return nil
}
