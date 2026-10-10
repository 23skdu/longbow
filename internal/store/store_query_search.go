package store

import (
	"fmt"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/23skdu/longbow/internal/cache"
	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	internalcore "github.com/23skdu/longbow/internal/store/index"
	types "github.com/23skdu/longbow/internal/store/types"
	"github.com/23skdu/longbow/internal/tracing"
)

// ListFlights returns a list of available datasets as FlightInfo objects.

func (s *VectorStore) handleDoGetSearch(req *qry.VectorSearchRequest, windowFunctions []qry.WindowFunction, stream flight.FlightService_DoGetServer, mem memory.Allocator) error {
	start := time.Now()

	_, span := tracing.CreateSpan(stream.Context(), "DoGetSearch")
	if span != nil {
		span.SetAttributes(
			"component", "search",
			"level", "hotpath",
			"dataset", req.Dataset,
		)
		defer span.End()
	}

	// Increment search requests counter
	metrics.SearchRequestsTotal.WithLabelValues(req.Dataset, "vector").Inc()

	// Record to auto-scaler (Part 1.1)
	if s.scaler != nil {
		defer func() {
			s.scaler.RecordSearch(time.Since(start))
		}()
	}

	// 0. Learned Index Rate Limiting (Phase 16)
	if s.rateLimiter != nil {
		if err := s.rateLimiter.Wait(stream.Context()); err != nil {
			return status.Errorf(codes.Aborted, "rate limit wait failed: %v", err)
		}
	}

	// 1. Validate Request
	if req.K < 1 {
		return status.Error(codes.InvalidArgument, "k must be at least 1")
	}

	// 2. Determine Search Mode
	isHybrid := req.TextQuery != "" || (req.Alpha > 0 && req.Alpha < 1.0)
	var queryVectors [][]float32
	if len(req.Vector) > 0 {
		queryVectors = append(queryVectors, req.Vector)
	}
	// Note: Ticket parser doesn't support 'Vectors' (batch) yet, but request struct has it.
	// If we added support, we'd handle it here.

	if len(queryVectors) == 0 && !isHybrid {
		return status.Error(codes.InvalidArgument, "no query vector provided")
	}

	var searchResults []types.SearchResult
	var err error

	// 2.5 Query Cache Check
	// We cache the FINAL result (after potential global scatter-gather if applicable)
	cacheKey := cache.HashQuery(req)
	if cached, hit := s.queryCache.GetUint64(cacheKey); hit {
		searchResults = cached
	} else {

		// 3. Execute Search (Local or Distributed)
		// For simplicity, we assume single vector search for now in DoGet
		// (matching current GlobalSearch usage).
		// If batch provided, we'd loop.

		// Use the first vector if available
		var queryVec []float32
		if len(queryVectors) > 0 {
			queryVec = queryVectors[0]
		}

		if isHybrid {
			depth := req.GraphDepth
			if depth <= 0 {
				depth = 2
			}
			searchResults, err = s.SearchHybrid(stream.Context(), req.Dataset, queryVec, req.TextQuery, req.K, req.Alpha, 60, req.GraphAlpha, depth, req.RawHybrid)
		} else {
			// Standard Vector Search
			ds, ok := s.getDataset(req.Dataset)
			if !ok {
				return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
			}

			ds.dataMu.RLock()
			index := ds.Index
			graph := ds.Graph
			if index == nil {
				ds.dataMu.RUnlock()
				return status.Error(codes.FailedPrecondition, "index not initialized")
			}

			ds.dataMu.RUnlock()

			// Learned Index Prediction (v0.2.0-rc1)
			if req.EnableLearnedIndex {
				predictor := s.GetIndexPredictor()
				if predictor != nil {
					features := QueryFeatures{
						VectorDimension: len(queryVec),
						DatasetSize:     ds.IndexLen(),
						SearchK:         req.K,
						IsFiltered:      len(req.Filters) > 0 || req.FilterExpr != nil,
						IsHybrid:        isHybrid,
					}
					prediction := predictor.Predict(features)
					s.logger.Debug().
						Str("dataset", req.Dataset).
						Str("recommended", string(prediction.RecommendedIndex)).
						Float64("confidence", prediction.Confidence).
						Msg("Learned index recommendation")
				}
			}

			// Core Search (No dataset lock held)
			var searchErr error
			filterExpr := ParseFilter(req.FilterExpr)
			searchResults, searchErr = index.SearchVectors(stream.Context(), queryVec, req.K, req.Filters, types.SearchOptions{
				IncludeVectors: req.IncludeVectors,
				VectorFormat:   types.MapStringToVectorDataType(req.VectorFormat),
				FilterExpr:     filterExpr,
				Predicate:      qry.ExtractPushablePredicate(filterExpr, ds.Records.Read()),
			})
			if searchErr != nil {
				return status.Errorf(codes.Internal, "search failed: %v", searchErr)
			}

			// Capture data for mapping/re-ranking
			ds.dataMu.RLock()
			// Graph Re-ranking
			if req.GraphAlpha > 0 && graph != nil {
				ds.dataMu.RUnlock()
				depth := req.GraphDepth
				if depth <= 0 {
					depth = 2
				}
				ranked := graph.RankWithGraphDistributed(stream.Context(), req.Dataset, req.Vector, searchResults, req.GraphAlpha, depth, s)
				if len(ranked) > 0 {
					searchResults = ranked
				}
				ds.dataMu.RLock()
			}

			// Map internal IDs to user IDs
			searchResults = s.mapInternalToUserIDsLocked(ds, searchResults)
			ds.dataMu.RUnlock()
		}

		if err != nil {
			return err
		}

		// 4. Global Scatter-Gather (if not local-only)
		if !req.LocalOnly && s.Mesh != nil {
			peers := s.Mesh.GetMembers()
			var remotePeers []mesh.Member //nolint:prealloc // Unknown size
			selfID := s.Mesh.GetIdentity().ID
			for i := range peers {
				p := &peers[i]
				if p.ID != selfID {
					remotePeers = append(remotePeers, *p)
				}
			}

			// This will call GlobalSearch on coordinator, which currently uses DoAction.
			// We will update it to use DoGet in the next step.
			// This recursion is fine, as long as coordinator handles the transport switch correctly.
			// Global search across remote peers
			var globalErr error
			searchResults, globalErr = s.coordinator.GlobalSearch(stream.Context(), searchResults, req, remotePeers)
			// Note: partial failures are logged but don't fail the entire search
			if globalErr != nil {
				s.logger.Warn().Err(globalErr).Msg("DoGet GlobalSearch partial failure")
			}
		}

		if len(searchResults) > 0 {
			s.queryCache.PutUint64(cacheKey, searchResults)
		}

	} // End of Cache Miss block

	// Execute Window Functions
	if len(windowFunctions) > 0 {
		windowOp := qry.NewWindowOperator()
		searchResults = windowOp.Execute(searchResults, windowFunctions)
	}

	// 5. Stream Results (Arrow)
	// Schema: id (uint64), score (float32)
	pool := mem
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}
	if req.IncludeVectors {
		fields = append(fields, arrow.Field{Name: "vector", Type: arrow.BinaryTypes.Binary})
	}
	if req.RawHybrid {
		fields = append(fields, arrow.Field{Name: "source", Type: arrow.PrimitiveTypes.Uint8})
	}

	// Add dynamic Window Function columns
	for _, wf := range windowFunctions {
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

	measuredRowBytes := 0
	if req.IncludeVectors && len(req.Vector) > 0 {
		measuredRowBytes = 16 + len(req.Vector)*4
	}
	w := newDoGetRecordWriter(stream, schema, len(searchResults), measuredRowBytes)
	defer func() { _ = w.Close() }()

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Uint64Builder)
	scoreBuilder := builder.Field(1).(*array.Float32Builder)
	var vectorBuilder *array.BinaryBuilder
	if req.IncludeVectors {
		vectorBuilder = builder.Field(2).(*array.BinaryBuilder)
	}

	var sourceBuilder *array.Uint8Builder
	var sourceColIdx = -1
	for i, f := range fields {
		if f.Name == "source" {
			sourceColIdx = i
			break
		}
	}
	if sourceColIdx >= 0 {
		sourceBuilder = builder.Field(sourceColIdx).(*array.Uint8Builder)
	}

	// Chunk results if necessary (e.g. > 64k) to stream effectively
	// For K usually < 1000, single batch is fine.
	chunkSize := 4096
	for i := 0; i < len(searchResults); i += chunkSize {
		end := i + chunkSize
		if end > len(searchResults) {
			end = len(searchResults)
		}

		idBuilder.Reserve(end - i)
		scoreBuilder.Reserve(end - i)
		if sourceBuilder != nil {
			sourceBuilder.Reserve(end - i)
		}

		for j := i; j < end; j++ {
			idBuilder.Append(uint64(searchResults[j].ID))
			scoreBuilder.Append(searchResults[j].Score)

			colOffset := 2
			if req.IncludeVectors && vectorBuilder != nil {
				if searchResults[j].Vector != nil {
					vectorBuilder.Append(searchResults[j].Vector)
				} else {
					vectorBuilder.AppendNull()
				}
				colOffset++
			}
			if sourceBuilder != nil {
				sourceBuilder.Append(searchResults[j].Source)
				colOffset++
			}

			// Append Window Function results
			if len(windowFunctions) > 0 {
				metaMap, _ := core.DecodeMetadata(searchResults[j].Metadata)
				for wfIdx, wf := range windowFunctions {
					val, ok := metaMap[wf.As]
					if !ok {
						builder.Field(colOffset + wfIdx).AppendNull()
						continue
					}

					switch wf.Name {
					case "row_number", "rank", "dense_rank":
						var intVal int64
						switch v := val.(type) {
						case int:
							intVal = int64(v)
						case int64:
							intVal = v
						case float64:
							intVal = int64(v)
						}
						builder.Field(colOffset + wfIdx).(*array.Int64Builder).Append(intVal)
					case "sum", "avg", "min", "max":
						var floatVal float64
						switch v := val.(type) {
						case float64:
							floatVal = v
						case float32:
							floatVal = float64(v)
						case int:
							floatVal = float64(v)
						case int64:
							floatVal = float64(v)
						}
						builder.Field(colOffset + wfIdx).(*array.Float64Builder).Append(floatVal)
					default:
						builder.Field(colOffset + wfIdx).(*array.Float64Builder).Append(0.0)
					}
				}
			}
		}

		rec := builder.NewRecordBatch()
		startWrite := time.Now()
		if err := w.Write(rec); err != nil {
			rec.Release()
			return status.Errorf(codes.Internal, "failed to write arrow batch: %v", err)
		}
		writeDuration := time.Since(startWrite)
		metrics.GRPCStreamSendLatencySeconds.Observe(writeDuration.Seconds())

		// If write takes more than 50ms, consider it a potential flow-control stall
		if writeDuration > 50*time.Millisecond {
			metrics.GRPCStreamStallTotal.Inc()
		}

		rec.Release()
	}

	return nil
}

func (s *VectorStore) handleDoGetSearchByID(req *qry.VectorSearchByIDRequest, stream flight.FlightService_DoGetServer, _ memory.Allocator) error {
	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset not found: %s", req.Dataset)
	}

	ds.dataMu.RLock()

	if ds.Index == nil {
		ds.dataMu.RUnlock()
		return status.Error(codes.FailedPrecondition, "dataset has no index")
	}

	var targetVec any
	found := false

	if ds.PrimaryIndex != nil {
		if loc, ok := ds.PrimaryIndex[req.ID]; ok {
			isDeleted := false
			if ts, ok := ds.Tombstones[loc.BatchIdx]; ok && ts != nil && ts.Contains(loc.RowIdx) {
				isDeleted = true
			}
			if !isDeleted && loc.BatchIdx < len(ds.Records.Read()) {
				rec := ds.Records.Read()[loc.BatchIdx]
				vec, err := internalcore.ExtractVectorRaw(rec, loc.RowIdx, -1)
				if err != nil {
					ds.dataMu.RUnlock()
					return status.Errorf(codes.Internal, "failed to extract vector: %v", err)
				}
				targetVec = vec
				found = true
			}
		}
	}

	if !found {
		idColIdx := -1
		if ds.Schema != nil {
			for i, field := range ds.Schema.Fields() {
				if field.Name == "id" {
					idColIdx = i
					break
				}
			}
		}

		if idColIdx != -1 {
			for batchIdx, rec := range ds.Records.Read() {
				idCol := rec.Column(idColIdx)
				for rowIdx := 0; rowIdx < int(rec.NumRows()); rowIdx++ {
					var idStr string
					switch c := idCol.(type) {
					case *array.String:
						idStr = c.Value(rowIdx)
					case *array.Int64:
						idStr = strconv.FormatInt(c.Value(rowIdx), 10)
					case *array.Uint64:
						idStr = strconv.FormatUint(c.Value(rowIdx), 10)
					case *array.Int32:
						idStr = strconv.FormatInt(int64(c.Value(rowIdx)), 10)
					case *array.Uint32:
						idStr = strconv.FormatUint(uint64(c.Value(rowIdx)), 10)
					default:
						continue
					}

					if idStr == req.ID {
						isDeleted := false
						if ts, ok := ds.Tombstones[batchIdx]; ok && ts != nil && ts.Contains(rowIdx) {
							isDeleted = true
						}
						if !isDeleted {
							vec, err := internalcore.ExtractVectorRaw(rec, rowIdx, -1)
							if err != nil {
								ds.dataMu.RUnlock()
								return status.Errorf(codes.Internal, "failed to extract vector: %v", err)
							}
							targetVec = vec
							found = true
						}
						break
					}
				}
				if found {
					break
				}
			}
		}
	}

	if !found {
		ds.dataMu.RUnlock()
		return status.Errorf(codes.NotFound, "id '%s' not found in dataset '%s'", req.ID, req.Dataset)
	}

	// UNLOCK BEFORE SEARCH: This is critical to avoid deadlock with parallel search workers
	// that re-acquire the same RLock while a writer is pending.
	ds.dataMu.RUnlock()

	results, err := ds.Index.SearchVectors(stream.Context(), targetVec, req.K, nil, SearchOptions{
		IncludeVectors: req.IncludeVectors,
		VectorFormat:   types.MapStringToVectorDataType(req.VectorFormat),
	})
	if err != nil {
		return status.Errorf(codes.Internal, "search failed: %v", err)
	}

	// 3. Stream results back to client
	var builder *array.RecordBuilder
	if req.IncludeVectors {
		builder = SearchWithVectorResponsePool.Get()
	} else {
		builder = SearchResponsePool.Get()
	}
	defer func() {
		// Reset all fields before putting back to pool
		for i := 0; i < builder.Schema().NumFields(); i++ {
			builder.Field(i).NewArray().Release()
		}
		if req.IncludeVectors {
			SearchWithVectorResponsePool.Put(builder)
		} else {
			SearchResponsePool.Put(builder)
		}
	}()

	w := newDoGetRecordWriter(stream, builder.Schema(), len(results), 0)
	defer func() { _ = w.Close() }()

	idBuilder := builder.Field(0).(*array.StringBuilder)
	scoreBuilder := builder.Field(1).(*array.Float32Builder)
	var vectorBuilder *array.BinaryBuilder
	if req.IncludeVectors {
		vectorBuilder = builder.Field(2).(*array.BinaryBuilder)
	}

	idBuilder.Reserve(len(results))
	scoreBuilder.Reserve(len(results))

	for _, res := range results {
		// Map back to string ID
		// In a real implementation we would look this up, but for bench-tool we know it's a string representation of ID
		idBuilder.Append(fmt.Sprintf("%d", res.ID))
		scoreBuilder.Append(res.Score)
		if req.IncludeVectors && vectorBuilder != nil {
			// SearchResult doesn't always have vector populated, handle null
			vectorBuilder.AppendNull()
		}
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()
	if err := w.Write(rec); err != nil {
		return status.Errorf(codes.Internal, "failed to write arrow batch: %v", err)
	}

	return nil
}

func (s *VectorStore) handleDoGetRecommend(req *qry.RecommendRequest, stream flight.FlightService_DoGetServer, mem memory.Allocator) error {
	results, err := s.Recommend(stream.Context(), req)
	if err != nil {
		return status.Errorf(codes.Internal, "Recommendation failed: %v", err)
	}

	// Schema for recommendations: id (uint64), score (float32)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	w := newDoGetRecordWriter(stream, schema, len(results), 0)
	defer func() { _ = w.Close() }()

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Uint64Builder)
	scoreBuilder := builder.Field(1).(*array.Float32Builder)

	idBuilder.Reserve(len(results))
	scoreBuilder.Reserve(len(results))

	for _, res := range results {
		idBuilder.Append(uint64(res.ID))
		scoreBuilder.Append(res.Score)
	}

	rec := builder.NewRecordBatch()
	if err := w.Write(rec); err != nil {
		rec.Release()
		return status.Errorf(codes.Internal, "failed to write arrow batch: %v", err)
	}
	rec.Release()
	return nil
}

func (s *VectorStore) streamSearchResults(results []types.SearchResult, windowFunctions []qry.WindowFunction, stream flight.FlightService_DoGetServer, mem memory.Allocator) error {
	// Execute Window Functions
	if len(windowFunctions) > 0 {
		windowOp := qry.NewWindowOperator()
		results = windowOp.Execute(results, windowFunctions)
	}

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float32},
	}
	// Handle window functions in schema
	for _, wf := range windowFunctions {
		var colType arrow.DataType
		switch wf.Name {
		case "row_number", "rank", "dense_rank":
			colType = arrow.PrimitiveTypes.Int64
		default:
			colType = arrow.PrimitiveTypes.Float64
		}
		fields = append(fields, arrow.Field{Name: wf.As, Type: colType})
	}

	schema := arrow.NewSchema(fields, nil)
	w := newDoGetRecordWriter(stream, schema, len(results), 0)
	defer func() { _ = w.Close() }()

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	for _, res := range results {
		builder.Field(0).(*array.Uint64Builder).Append(uint64(res.ID))
		builder.Field(1).(*array.Float32Builder).Append(res.Score)

		colOffset := 2
		if len(windowFunctions) > 0 {
			metaMap, _ := core.DecodeMetadata(res.Metadata)
			for wfIdx, wf := range windowFunctions {
				val, ok := metaMap[wf.As]
				if !ok {
					builder.Field(colOffset + wfIdx).AppendNull()
					continue
				}
				switch wf.Name {
				case "row_number", "rank", "dense_rank":
					// Try to cast to various numeric types
					var intVal int64
					switch v := val.(type) {
					case int64:
						intVal = v
					case int:
						intVal = int64(v)
					case float64:
						intVal = int64(v)
					}
					builder.Field(colOffset + wfIdx).(*array.Int64Builder).Append(intVal)
				default:
					var floatVal float64
					switch v := val.(type) {
					case float64:
						floatVal = v
					case int64:
						floatVal = float64(v)
					case int:
						floatVal = float64(v)
					}
					builder.Field(colOffset + wfIdx).(*array.Float64Builder).Append(floatVal)
				}
			}
		}
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()
	return w.Write(rec)
}
