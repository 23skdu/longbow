package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/23skdu/longbow/internal/core"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// No longer using global pool here, moved to VectorStore

// handleVectorSearchAction handles the VectorSearch DoAction request
func (s *VectorStore) handleVectorSearchAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	start := time.Now()

	// Use ArenaAllocator for Arrow records
	mem := lbmem.NewArenaAllocator()
	defer mem.Release()

	var req query.VectorSearchRequest
	var parseErr error

	parser := s.vectorSearchParserPool.Get().(*query.ZeroAllocVectorSearchParser)
	defer s.vectorSearchParserPool.Put(parser)

	req, parseErr = parser.Parse(action.Body)
	if parseErr != nil {
		metrics.VectorSearchParseFallbackTotal.Inc()
		req = query.VectorSearchRequest{}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			metrics.VectorSearchActionErrors.Inc()
			s.logger.Warn().Err(err).Msg("VectorSearch JSON parse failed")
			return status.Errorf(codes.InvalidArgument, "invalid JSON request: %v", err)
		}
	} else {
		metrics.ZeroAllocVectorSearchParseTotal.Inc()
	}
	if req.K < 1 {
		metrics.VectorSearchActionErrors.Inc()
		return status.Error(codes.InvalidArgument, "k must be at least 1")
	}

	if req.EfSearch != 0 && (req.EfSearch < 16 || req.EfSearch > 4096) {
		metrics.VectorSearchActionErrors.Inc()
		return status.Errorf(codes.InvalidArgument, "ef_search must be between 16 and 4096, got %d", req.EfSearch)
	}

	// Parse consistency level for distributed queries
	var _ ConsistencyLevel
	if req.Consistency != "" {
		var err error
		_, err = ParseConsistencyLevel(req.Consistency)
		if err != nil {
			metrics.VectorSearchActionErrors.Inc()
			return status.Errorf(codes.InvalidArgument, "invalid consistency: %v", err)
		}
		s.logger.Debug().Str("consistency", req.Consistency).Msg("consistency level requested")
	}

	// Determine if Hybrid Search
	isHybrid := req.TextQuery != "" || (req.Alpha > 0 && req.Alpha < 1.0)

	// Combine Vector and Vectors into a single slice for processing
	var queryVectors [][]float32
	if len(req.Vector) > 0 {
		queryVectors = append(queryVectors, req.Vector)
	}
	if len(req.Vectors) > 0 {
		queryVectors = append(queryVectors, req.Vectors...)
	}

	// Allow pure sparse/keyword search (alpha=0.0 with text_query, no vector required)
	if len(queryVectors) == 0 && req.TextQuery == "" {
		metrics.VectorSearchActionErrors.Inc()
		return status.Error(codes.InvalidArgument, "no query vector(s) provided")
	}
	// Process each query vector
	for _, queryVec := range queryVectors {
		var searchResults []SearchResult
		var err error

		if isHybrid {
			// Perform Hybrid Search
			searchResults, err = s.SearchHybrid(stream.Context(), req.Dataset, queryVec, req.TextQuery, req.K, req.Alpha, 60, req.GraphAlpha, 2)
			if err != nil {
				metrics.VectorSearchActionErrors.Inc()
				continue
			}
		} else {
			// Standard Vector Search
			ds, ok := s.getDataset(req.Dataset)
			if !ok {
				metrics.VectorSearchActionErrors.Inc()
				return status.Errorf(codes.NotFound, "dataset not found: %s", req.Dataset)
			}

			// Acquire read lock (SearchHybrid manages its own locks, but here we do it manually)
			ds.dataMu.RLock()
			if ds.evicting.Load() {
				ds.dataMu.RUnlock()
				metrics.EvictionRejectedQueries.Inc()
				return status.Errorf(codes.Unavailable, "dataset %s is being evicted", req.Dataset)
			}

			if ds.Index == nil {
				ds.dataMu.RUnlock()
				metrics.VectorSearchActionErrors.Inc()
				return status.Error(codes.FailedPrecondition, "dataset has no HNSW index")
			}

			// Validate dimension
			expectedDim := ds.Index.GetDimension()
			actualDim := uint32(len(queryVec)) // #nosec G115
			dsType := InferVectorDataType(ds.Schema, "vector")
			
			// Check if dataset is logically complex (interleaved floats)
			isComplex := dsType == VectorTypeComplex64 || dsType == VectorTypeComplex128
			if !isComplex && ds.Schema != nil {
				if val, ok := ds.Schema.Metadata().GetValue("longbow.complex"); ok && val == "true" {
					isComplex = true
				}
			}

			if isComplex {
				// Interleaved float32s -> logical complex dimension is half
				actualDim /= 2
			}

			if actualDim != expectedDim {
				ds.dataMu.RUnlock()
				metrics.VectorSearchActionErrors.Inc()
				return status.Errorf(codes.InvalidArgument, "dimension mismatch: expected %d, got %d", expectedDim, actualDim)
			}

			// Perform search
			var errSearch error
			searchResults, errSearch = ds.Index.SearchVectors(stream.Context(), queryVec, req.K, req.Filters, SearchOptions{
				IncludeVectors: req.IncludeVectors,
				VectorFormat:   types.MapStringToVectorDataType(req.VectorFormat),
				FilterExpr:     ParseFilter(req.FilterExpr),
				Ef:             req.EfSearch,
				VectorType:     types.MapStringToVectorDataType(req.VectorType),
				TurboQuantBits: req.TurboQuantBits,
			})
			if errSearch != nil {
				ds.dataMu.RUnlock()
				metrics.VectorSearchActionErrors.Inc()
				return status.Errorf(codes.Internal, "vector search failed: %v", errSearch)
			}

			if req.EfSearch > 0 {
				metrics.VectorSearchEfSearch.Observe(float64(req.EfSearch))
			}

			// Graph Re-ranking (Standard Path)
			if req.GraphAlpha > 0 && ds.Graph != nil {
				// RankWithGraph expects VectorID (internal) which we have here.
				// It returns expanded set, we might keep it or trim?
				// SearchHybrid trims at the end. Here we just return what RankWithGraph gives?
				// But we should probably clamp to K?
				// RankWithGraph already dedupes.
				// Note: ds.dataMu is RLocked here, which RankWithGraph expects?
				// RankWithGraph calls TraverseParallel -> Traverse -> gs.dataMu.RLock().
				// gs (GraphStore) has its OWN lock. ds.Graph is just a pointer.
				// So calling it while holding ds.dataMu RLock is safe (assuming no lock inversion).
				graphDepth := 2
				ranked := ds.Graph.RankWithGraph(searchResults, req.GraphAlpha, graphDepth)
				if len(ranked) > 0 {
					searchResults = ranked
				}
			}

			// Map internal IDs to User IDs
			searchResults = s.mapInternalToUserIDsLocked(ds, searchResults)
			ds.dataMu.RUnlock()
		}

		// Perform Global Search (Scatter-Gather) if not local-only
		if !req.LocalOnly && s.Mesh != nil {
			peers := s.Mesh.GetMembers()
			var remotePeers []mesh.Member
			selfID := s.Mesh.GetIdentity().ID
			for i := range peers {
				p := &peers[i]
				if p.ID != selfID {
					remotePeers = append(remotePeers, *p)
				}
			}

			// Override Vector in request for remote peers
			batchReq := req
			batchReq.Vector = queryVec
			batchReq.Vectors = nil // Don't forward the whole batch to each peer for each call?
			// Wait, the aggregator/coordinator might need adjustment too.
			// For now, let's just forward the single vector.
			searchResults, err = s.coordinator.GlobalSearch(stream.Context(), searchResults, &batchReq, remotePeers)
			if err != nil {
				s.logger.Warn().Err(err).Msg("Global search partial failure")
			}
		}

		// Execute Window Functions
		if len(req.WindowFunctions) > 0 {
			windowOp := query.NewWindowOperator()
			searchResults = windowOp.Execute(searchResults, req.WindowFunctions)
		}

		// Build Arrow RecordBatch
		pool := mem
		fields := []arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
			{Name: "score", Type: arrow.PrimitiveTypes.Float32},
		}

		// Add dynamic Window Function columns
		for _, wf := range req.WindowFunctions {
			var colType arrow.DataType
			switch wf.Name {
			case "row_number", "rank", "dense_rank":
				colType = arrow.PrimitiveTypes.Int64
			case "sum", "avg", "min", "max":
				colType = arrow.PrimitiveTypes.Float64
			default:
				colType = arrow.PrimitiveTypes.Float64
			}
			fields = append(fields, arrow.Field{Name: wf.As, Type: colType})
		}

		schema := arrow.NewSchema(fields, nil)

		builder := array.NewRecordBuilder(pool, schema)
		// deferred release removed to avoid accumulation in loop

		idBuilder := builder.Field(0).(*array.Uint64Builder)
		scoreBuilder := builder.Field(1).(*array.Float32Builder)

		idBuilder.Reserve(len(searchResults))
		scoreBuilder.Reserve(len(searchResults))

		for _, res := range searchResults {
			idBuilder.Append(uint64(res.ID))
			scoreBuilder.Append(res.Score)

			colOffset := 2
			// Append Window Function results
			if len(req.WindowFunctions) > 0 {
				metaMap, _ := core.DecodeMetadata(res.Metadata)
				for wfIdx, wf := range req.WindowFunctions {
					val, ok := metaMap[wf.As]
					if !ok {
						builder.Field(colOffset + wfIdx).AppendNull()
						continue
					}

					switch wf.Name {
					case "row_number", "rank", "dense_rank":
						var intVal int64
						switch v := val.(type) {
						case int: intVal = int64(v)
						case int64: intVal = v
						case float64: intVal = int64(v)
						}
						builder.Field(colOffset + wfIdx).(*array.Int64Builder).Append(intVal)
					case "sum", "avg", "min", "max":
						var floatVal float64
						switch v := val.(type) {
						case float64: floatVal = v
						case float32: floatVal = float64(v)
						case int: floatVal = float64(v)
						case int64: floatVal = float64(v)
						}
						builder.Field(colOffset + wfIdx).(*array.Float64Builder).Append(floatVal)
					default:
						builder.Field(colOffset + wfIdx).(*array.Float64Builder).Append(0.0)
					}
				}
			}
		}

		rec := builder.NewRecordBatch()
		// deferred release removed to avoid accumulation in loop

		// Serialize to IPC buffer
		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
		if err := writer.Write(rec); err != nil {
			metrics.VectorSearchActionErrors.Inc()
			// Release resources before returning
			rec.Release()
			builder.Release()
			return status.Errorf(codes.Internal, "failed to write arrow batch: %v", err)
		}
		if err := writer.Close(); err != nil {
			metrics.VectorSearchActionErrors.Inc()
			// Release resources before returning
			rec.Release()
			builder.Release()
			return status.Errorf(codes.Internal, "failed to close arrow writer: %v", err)
		}

		if err := stream.Send(&flight.Result{Body: buf.Bytes()}); err != nil {
			metrics.VectorSearchActionErrors.Inc()
			// Release resources before returning
			rec.Release()
			builder.Release()
			return err
		}
	}

	metrics.VectorSearchActionTotal.Inc()
	metrics.VectorSearchActionDuration.Observe(time.Since(start).Seconds())
	return nil
}

// handleVectorSearchByIDAction handles the VectorSearchByID DoAction request
func (s *VectorStore) handleVectorSearchByIDAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req query.VectorSearchByIDRequest
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset not found: %s", req.Dataset)
	}

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	if ds.Index == nil {
		return status.Error(codes.FailedPrecondition, "dataset has no index")
	}

	// 1. Find the vector by User ID
	var targetVec []float32
	found := false

	// Try Primary Index first (O(1))
	if ds.PrimaryIndex != nil {
		if loc, ok := ds.PrimaryIndex[req.ID]; ok {
			// Check if deleted
			isDeleted := false
			if ts, ok := ds.Tombstones[loc.BatchIdx]; ok && ts != nil && ts.Contains(loc.RowIdx) {
				isDeleted = true
			}

			if !isDeleted {
				if loc.BatchIdx < len(ds.Records) {
					rec := ds.Records[loc.BatchIdx]
					vec, err := ExtractVectorFromArrow(rec, loc.RowIdx, -1)
					if err != nil {
						return status.Errorf(codes.Internal, "failed to extract vector: %v", err)
					}
					targetVec = vec
					found = true
				}
			}
		}
	}

	// Fallback to linear scan if index miss (shouldn't happen if index is fully populated)
	// or if PrimaryIndex wasn't initialized.
	if !found && ds.PrimaryIndex == nil {
		for i, rec := range ds.Records {
			// Check for "id" column
			idColIdx := -1
			for j, field := range rec.Schema().Fields() {
				if field.Name == "id" {
					idColIdx = j
					break
				}
			}
			if idColIdx == -1 {
				continue // No ID column in this batch
			}

			// Support String and Int64 IDs
			// Comparison logic to match req.ID (which is string)
			// If column is Int64, we parse req.ID
			col := rec.Column(idColIdx)
			rowIdx := -1

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

			// Fallback: Check using String() formatted representation if specific type match failed or wasn't covered
			if rowIdx == -1 {
				switch arr := col.(type) {
				case *array.Int64:
					var val int64
					if n, _ := fmt.Sscanf(req.ID, "%d", &val); n == 1 {
						for j := 0; j < arr.Len(); j++ {
							if arr.Value(j) == val {
								rowIdx = j
								break
							}
						}
					}
				case *array.Uint64:
					var val uint64
					if n, _ := fmt.Sscanf(req.ID, "%d", &val); n == 1 {
						for j := 0; j < arr.Len(); j++ {
							if arr.Value(j) == val {
								rowIdx = j
								break
							}
						}
					}
				case *array.String:
					for j := 0; j < arr.Len(); j++ {
						if arr.Value(j) == req.ID {
							rowIdx = j
							break
						}
					}
				}
			}

			if rowIdx != -1 {
				// Found it! Check if deleted.
				ts := ds.Tombstones[i]
				if ts != nil && ts.Contains(rowIdx) {
					continue // Deleted
				}

				// Extract Vector
				vec, err := ExtractVectorFromArrow(rec, rowIdx, -1) // -1 auto-detects vector column
				if err != nil {
					return status.Errorf(codes.Internal, "failed to extract vector: %v", err)
				}
				targetVec = vec
				found = true
				break
			}
		}
	}

	if !found {
		return status.Errorf(codes.NotFound, "id '%s' not found in dataset '%s'", req.ID, req.Dataset)
	}

	// 2. Perform Search
	results, err := ds.Index.SearchVectors(stream.Context(), targetVec, req.K, nil, SearchOptions{
		IncludeVectors: req.IncludeVectors,
		VectorFormat:   types.MapStringToVectorDataType(req.VectorFormat),
	})
	if err != nil {
		return status.Errorf(codes.Internal, "search failed: %v", err)
	}

	// 3. Map Results
	results = s.mapInternalToUserIDsLocked(ds, results)

	resp := query.VectorSearchResponse{
		IDs:    make([]uint64, len(results)),
		Scores: make([]float32, len(results)),
	}
	for i, res := range results {
		resp.IDs[i] = uint64(res.ID)
		resp.Scores[i] = res.Score
	}

	respBytes, _ := json.Marshal(resp)
	if err := stream.Send(&flight.Result{Body: respBytes}); err != nil {
		return err
	}

	return nil
}
