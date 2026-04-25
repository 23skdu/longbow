package store

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/23skdu/longbow/internal/cache"
	"github.com/23skdu/longbow/internal/core"
	lbflight "github.com/23skdu/longbow/internal/flight"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	internalcore "github.com/23skdu/longbow/internal/store/internal/core"
	types "github.com/23skdu/longbow/internal/store/types"
	"github.com/23skdu/longbow/internal/tracing"
)

func (s *VectorStore) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	var ticketQuery qry.TicketQuery
	var err error
	if c != nil && len(c.Expression) > 0 {
		ticketQuery, err = qry.ParseTicketQuerySafe(c.Expression)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "Invalid criteria: %v", err)
		}
	}

	var datasets []*Dataset
	s.IterateDatasets(func(name string, ds *Dataset) {
		if ds != nil {
			datasets = append(datasets, ds)
		}
	})

	for _, ds := range datasets {
		// Apply filters
		match := true
		for _, f := range ticketQuery.Filters {
			switch f.Field {
			case "name":
				if f.Operator == "contains" {
					if !strings.Contains(ds.Name, f.Value) {
						match = false
					}
				}
			case "rows":
				var numRows int64
				ds.dataMu.RLock()
				for _, rec := range ds.Records {
					numRows += rec.NumRows()
				}
				ds.dataMu.RUnlock()

				val, err := strconv.ParseInt(f.Value, 10, 64)
				if err != nil {
					match = false
					break
				}
				switch f.Operator {
				case ">":
					if numRows <= val {
						match = false
					}
				case "<=":
					if numRows > val {
						match = false
					}
				case "==":
					if numRows != val {
						match = false
					}
				}
			}
			if !match {
				break
			}
		}

		if match {
			info := &flight.FlightInfo{
				FlightDescriptor: &flight.FlightDescriptor{
					Type: flight.DescriptorPATH,
					Path: []string{ds.Name},
				},
			}
			if err := stream.Send(info); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *VectorStore) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(desc.Path) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Empty path")
	}
	name := desc.Path[0]
	ds, ok := s.getDataset(name)
	if !ok {
		return nil, status.Error(codes.NotFound, "dataset not found")
	}

	return &flight.FlightInfo{
		FlightDescriptor: desc,
		TotalRecords:     int64(len(ds.Records)),
		TotalBytes:       ds.SizeBytes.Load(),
	}, nil
}
func (s *VectorStore) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	return nil, nil
}

// DoGet - Minimal implementation
func (s *VectorStore) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	startDoGet := time.Now()
	// Parse ticket
	query, err := qry.ParseTicketQuerySafe(tkt.Ticket)
	if err != nil {
		// Fallback: treat as plain string name if parse fails
		sStr := string(tkt.Ticket)
		if sStr != "" && sStr[0] != '{' {
			query.Name = sStr
		} else {
			s.logger.Error().Err(err).Str("ticket_preview", string(tkt.Ticket)).Msg("Failed to parse ticket")
			return status.Error(codes.InvalidArgument, "invalid ticket format")
		}
		err = nil // Clear error after fallback
	}
	// s.logger.Info().Interface("parsed_query", query).Msg("DEBUG: DoGet parsed query")

	// Resolve CTEs if present
	cteResults := make(map[string][]types.SearchResult)
	if len(query.CTEs) > 0 {
		if err := s.resolveCTEs(stream.Context(), query.CTEs, cteResults); err != nil {
			return status.Errorf(codes.Internal, "failed to resolve CTEs: %v", err)
		}
	}

	// Resolve Subqueries in filters
	if len(query.Filters) > 0 {
		if err := s.resolveSubqueries(stream.Context(), query.Filters); err != nil {
			return status.Errorf(codes.Internal, "failed to resolve subqueries: %v", err)
		}
	}

	// Create Request-Scoped Arena Allocator
	// This reduces GC pressure for transient buffers (masks, filtered batches, serialized records)
	mem := lbmem.NewArenaAllocator()
	defer mem.Release()

	// Handle Search Request via DoGet (Native Arrow Streaming)
	md, _ := metadata.FromIncomingContext(stream.Context())
	isGlobal := false
	if vals := md.Get("x-longbow-global"); len(vals) > 0 && vals[0] == "true" {
		isGlobal = true
	}

	switch {
	case query.GeoSearch != nil:
		return s.handleDoGetGeoSearch(query.GeoSearch, query.WindowFunctions, stream, mem)
	case query.TemporalSearch != nil:
		return s.handleDoGetTemporalSearch(query.TemporalSearch, query.WindowFunctions, stream, mem)
	case query.Search != nil:
		if isGlobal {
			query.Search.LocalOnly = false
		}
		return s.handleDoGetSearch(query.Search, query.WindowFunctions, stream, mem)
	case query.SearchByID != nil:
		return s.handleDoGetSearchByID(query.SearchByID, stream, mem)
	case query.Recommend != nil:
		return s.handleDoGetRecommend(query.Recommend, stream, mem)
	case len(query.Vector) > 0:
		searchReq := &types.VectorSearchRequest{
			Dataset: query.Name,
			Vector:  query.Vector,
			K:       query.K,
		}
		return s.handleDoGetSearch(searchReq, query.WindowFunctions, stream, mem)
	}

	// Existing Dataset Fetch Logic
	name := query.Name
	s.logger.Info().
		Str("name", name).
		Int("filters", len(query.Filters)).
		Interface("parsed_filters", query.Filters).
		Msg("DoGet called")

	// Check CTE first
	if cteRes, exists := cteResults[name]; exists {
		return s.streamSearchResults(cteRes, query.WindowFunctions, stream, mem)
	}

	ds, ok := s.getDataset(name)
	if !ok {
		var keys []string
		s.IterateDatasets(func(k string, _ *Dataset) {
			keys = append(keys, k)
		})
		s.logger.Warn().Str("wanted", name).Strs("available", keys).Msg("DoGet dataset not found")
		return status.Errorf(codes.NotFound, "dataset %s not found (available: %s)", name, strings.Join(keys, ", "))
	}

	ds.dataMu.RLock()
	// Check if dataset is already empty or if we have records
	if len(ds.Records) == 0 {
		ds.dataMu.RUnlock()
		s.logger.Warn().Msg("Dataset empty")
		return nil
	}

	// Use first record's schema (all records in a dataset must share schema)
	schema := ds.Records[0].Schema()

	// Adaptive Chunking (Byte-Aware Optimization)
	// We estimate row size to ensure chunks are at least ~2MB to saturate bandwidth
	// while keeping overhead low.
	avgRowSize := int64(256) // Default fallback
	if ds.Records[0].NumRows() > 0 {
		batchSize := estimateBatchSize(ds.Records[0])
		avgRowSize = batchSize / ds.Records[0].NumRows()
		if avgRowSize == 0 {
			avgRowSize = 1
		}
	}

	targetChunkBytes := int64(2 * 1024 * 1024) // 2MB Target
	minChunkRows := int(targetChunkBytes / avgRowSize)
	if minChunkRows < 4096 {
		minChunkRows = 4096 // Keep minimum floor of 4096
	} else if minChunkRows > 65536 {
		minChunkRows = 65536 // Cap max start to reasonable level
	}

	// Max chunk can be larger
	maxChunkRows := minChunkRows * 4
	if maxChunkRows > 131072 {
		maxChunkRows = 131072
	}

	chunkStrategy := lbflight.NewAdaptiveChunkStrategy(minChunkRows, maxChunkRows, 2.0)
	recordsToProcess, tombstonesToProcess := AdaptivelySliceBatches(ds.Records, ds.Tombstones, chunkStrategy)
	ds.dataMu.RUnlock() // RELEASE LOCK IMMEDIATELY AFTER CLONING REFERENCES

	s.logger.Info().Str("name", name).Int("batches", len(recordsToProcess)).Msg("DoGet streaming started")

	defer func() {
		for _, r := range recordsToProcess {
			r.Release()
		}
	}()

	ctx := stream.Context()
	rowsSent := int64(0)

	// Parallel Processing with Pipeline Support (Phase 5)
	// Recalculate workers based on chunked records
	numWorkers := runtime.NumCPU()
	if numWorkers > len(recordsToProcess) {
		numWorkers = len(recordsToProcess)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	resultsChan := make(chan arrow.RecordBatch, numWorkers*2)
	// Buffer 1 to prevent blocking on first error check
	errChan := make(chan error, 1)
	var wg sync.WaitGroup

	// Determine execution strategy
	var stageChan <-chan PipelineStage
	usePipeline := s.shouldUsePipeline(len(recordsToProcess))
	var pipeline *DoGetPipeline

	if usePipeline {
		// Use prefetching pipeline
		if s.doGetPipelinePool != nil {
			pipeline = s.doGetPipelinePool.Get()
		} else {
			pipeline = NewDoGetPipeline(8, 16) // Fallback defaults
		}

		// ProcessRecords handles feeding safely
		stageChan = pipeline.ProcessRecords(ctx, recordsToProcess, tombstonesToProcess, query.Filters, nil)
		metrics.DoGetPipelineStepsTotal.WithLabelValues("scan", "pipeline").Add(float64(len(recordsToProcess)))

	} else {
		// Simple feeder for small datasets
		metrics.DoGetPipelineStepsTotal.WithLabelValues("scan", "simple").Add(float64(len(recordsToProcess)))
		c := make(chan PipelineStage, len(recordsToProcess))
		stageChan = c
		go func() {
			defer close(c)
			for i, rec := range recordsToProcess {
				var ts *types.Bitset
				// Map access is safe under RLock
				if t, ok := tombstonesToProcess[i]; ok {
					ts = t
				}
				select {
				case c <- PipelineStage{
					Record:    rec,
					BatchIdx:  i,
					Tombstone: ts,
				}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Start Workers
	workerArenas := make([]*lbmem.ArenaAllocator, numWorkers)
	for i := range workerArenas {
		workerArenas[i] = lbmem.NewArenaAllocator()
	}
	defer func() {
		for _, a := range workerArenas {
			a.Release()
		}
	}()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			var evaluator *qry.FilterEvaluator
			workerMem := workerArenas[workerIdx]

			for stage := range stageChan {
				rec := stage.Record
				deleted := stage.Tombstone

				var processed arrow.RecordBatch
				var err error

				if len(query.Filters) > 0 {
					filterStart := time.Now()

					// Reusing evaluator
					if evaluator == nil {
						evaluator, err = qry.NewFilterEvaluator(rec, query.Filters)
					} else {
						err = evaluator.Reset(rec)
					}

					var mask *array.Boolean
					if err == nil {
						mask, err = evaluator.EvaluateToArrowBoolean(workerMem, int(rec.NumRows()))
					}

					var filtered arrow.RecordBatch
					if err == nil {
						filtered, err = filterRecordWithMask(ctx, workerMem, rec, mask)
					}
					if mask != nil {
						mask.Release()
					}
					metrics.FilterExecutionDurationSeconds.WithLabelValues(name).Observe(time.Since(filterStart).Seconds())
					if err != nil {
						select {
						case errChan <- err:
						default:
						} // Try send error
						return
					}
					if rec.NumRows() > 0 && filtered != nil {
						ratio := float64(filtered.NumRows()) / float64(rec.NumRows())
						metrics.FilterSelectivityRatio.WithLabelValues(name).Observe(ratio)
					}

					if filtered != nil && filtered.NumRows() > 0 {
						processed = filtered
					} else {
						if filtered != nil {
							filtered.Release()
						}
						continue
					}
				} else {
					// Use zero-copy with tombstone filtering (Phase 5)
					if deleted != nil && deleted.Count() > 0 {
						processed, err = ZeroCopyRecordBatch(workerMem, rec, deleted)
						metrics.DoGetZeroCopyTotal.WithLabelValues("zero_copy_mask").Inc()
					} else {
						// No tombstones - just retain (zero-copy!)
						rec.Retain()
						processed = rec
						metrics.DoGetZeroCopyTotal.WithLabelValues("zero_copy_retain").Inc()
					}
					if err != nil {
						select {
						case errChan <- err:
						default:
						}
						return
					}
				}

				// Send to results
				select {
				case resultsChan <- processed:
				case <-ctx.Done():
					return
				}
			}
		}(w)
	}

	// Monitor to close results channel
	go func() {
		wg.Wait()
		close(resultsChan)
		close(errChan)
	}()

	// Use standard Flight RecordWriter to stream results
	// This efficiently handles schema (first message) and subsequent batches
	// without intermediate copying or manual chunk management.
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	defer func() { _ = writer.Close() }()

	// Consume Results (Sequential Write)
	for {
		rec, ok := <-resultsChan
		if !ok {
			resultsChan = nil // Channel closed
		} else {
			// Guard against nil/empty records that can cause IPC writer panics
			if rec == nil || rec.NumRows() == 0 || rec.NumCols() == 0 {
				s.logger.Warn().Int64("rows", rec.NumRows()).Int64("cols", rec.NumCols()).Msg("Skipping invalid record in DoGet")
				if rec != nil {
					rec.Release()
				}
				continue
			}

			startWrite := time.Now()

			// Write batch directly to stream
			// Ensure schema strictly matches writer (e.g. metadata from compute kernels)
			if !rec.Schema().Equal(schema) {
				// Use helper to safely cast/align (avoiding panics if types mismatch)
				aligned, err := castRecordToSchema(mem, rec, schema)
				if err != nil {
					s.logger.Error().Err(err).Msg("Failed to align record batch schema")
					rec.Release()
					return err
				}
				rec.Release() // Release old wrapper
				rec = aligned
			}

			if err := writer.Write(rec); err != nil {
				s.logger.Error().Err(err).Msg("DoGet Send failed")
				rec.Release()
				return err
			}

			if rowsSent == 0 {
				metrics.DoGetTimeToFirstChunk.Observe(time.Since(startDoGet).Seconds())
			}

			rowsSent += rec.NumRows()
			rec.Release()

			writeDuration := time.Since(startWrite)
			metrics.GRPCStreamSendLatencySeconds.Observe(writeDuration.Seconds())

			if writeDuration > 50*time.Millisecond {
				metrics.GRPCStreamStallTotal.Inc()

			}

			// Track stats for test verification
			if usePipeline {
				s.incrementPipelineBatches(1)
			}
		}
		if ok && err != nil {

			return err
		}
		if resultsChan == nil {
			break
		}
	}

	if pipeline != nil && s.doGetPipelinePool != nil {
		s.doGetPipelinePool.Put(pipeline)
	}

	// Normal exit
	s.logger.Info().Int64("rows_sent", rowsSent).Msg("DoGet completed")
	metrics.FlightRowsProcessed.WithLabelValues("get", "ok").Add(float64(rowsSent))
	return nil
}

// MapInternalToUserIDs maps internal HNSW IDs to user-provided IDs
// MapInternalToUserIDs maps internal HNSW IDs to user-provided IDs
// This is the public wrapper that acquires a read lock.
func (s *VectorStore) MapInternalToUserIDs(ds *Dataset, results []types.SearchResult) []types.SearchResult {
	start := time.Now()
	defer func() {
		metrics.IDResolutionDuration.Observe(time.Since(start).Seconds())
	}()

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()
	return s.mapInternalToUserIDsLocked(ds, results)
}

// mapInternalToUserIDsLocked maps internal HNSW IDs to user-provided IDs.
// Caller MUST hold ds.dataMu.RLock (or Lock).
func (s *VectorStore) mapInternalToUserIDsLocked(ds *Dataset, results []types.SearchResult) []types.SearchResult {
	// Use the VectorIndex interface directly to look up locations.
	// This supports HNSWIndex, ArrowHNSW, AutoShardingIndex, etc.
	if ds.Index == nil {
		return results
	}

	mappedResults := make([]types.SearchResult, 0, len(results))

	for _, res := range results {
		// 1. Get location (Batch, Row) from VectorIndex
		locAny, found := ds.Index.GetLocation(uint32(res.ID))
		if !found {
			continue
		}
		loc, ok := locAny.(Location)
		if !ok {
			continue
		}

		// 2. Access RecordBatch
		if loc.BatchIdx >= len(ds.Records) {

			continue
		}
		rec := ds.Records[loc.BatchIdx]

		// 3. Find 'id' column
		// Optimization: could cache column index if schema is consistent
		idColIdx := -1
		for i, f := range rec.Schema().Fields() {
			if f.Name == "id" {
				idColIdx = i
				break
			}
		}

		if idColIdx == -1 {
			// No ID column, treat internal ID as valid
			mappedResults = append(mappedResults, res)
			continue
		}

		col := rec.Column(idColIdx)

		// 4. Extract User ID
		// ID column can be uint32 or uint64 (or others).
		// VectorID is uint32. If user ID is uint64 > 2^32, we have a truncation issue.
		// For now, cast to VectorID (uint32).
		var resolvedID types.VectorID

		switch c := col.(type) {
		case *array.Uint32:
			if loc.RowIdx < c.Len() {
				resolvedID = types.VectorID(c.Value(loc.RowIdx))
			} else {
				resolvedID = types.VectorID(res.ID) // Fallback
			}
		case *array.Uint64:
			if loc.RowIdx < c.Len() {
				resolvedID = types.VectorID(c.Value(loc.RowIdx)) // #nosec G115
			} else {
				resolvedID = res.ID
			}
		case *array.Int64:
			if loc.RowIdx < c.Len() {
				resolvedID = types.VectorID(c.Value(loc.RowIdx)) // #nosec G115
			} else {
				resolvedID = res.ID
			}
		case *array.Int32:
			if loc.RowIdx < c.Len() {
				resolvedID = types.VectorID(c.Value(loc.RowIdx)) // #nosec G115
			} else {
				resolvedID = res.ID
			}
		case *array.String:
			if loc.RowIdx < c.Len() {
				val := c.Value(loc.RowIdx)
				u, err := strconv.ParseUint(val, 10, 64)
				if err == nil {
					resolvedID = types.VectorID(u) // #nosec G115
				} else {
					// If not numeric, we're stuck with internal ID for the uint64 field.
					// A better fix would be to return StringIDs in the response.
					resolvedID = types.VectorID(res.ID)
				}
			} else {
				resolvedID = res.ID
			}
		default:
			// Unsupported ID type
			resolvedID = res.ID
		}

		// 5. Extract Metadata
		metadataColIdx := -1
		for i, f := range rec.Schema().Fields() {
			if f.Name == "metadata" {
				metadataColIdx = i
				break
			}
		}

		var metadata []byte
		if metadataColIdx != -1 {
			metaCol := rec.Column(metadataColIdx)
			if binCol, ok := metaCol.(*array.Binary); ok {
				if loc.RowIdx < binCol.Len() && binCol.IsValid(loc.RowIdx) {
					metadata = binCol.Value(loc.RowIdx)
				}
			} else if strCol, ok := metaCol.(*array.String); ok {
				if loc.RowIdx < strCol.Len() && strCol.IsValid(loc.RowIdx) {
					// Legacy string/JSON column - we'll keep as raw bytes for now
					metadata = []byte(strCol.Value(loc.RowIdx))
				}
			}
		}

		mappedResults = append(mappedResults, types.SearchResult{
			ID:       resolvedID,
			Score:    res.Score,
			Distance: res.Distance,
			Metadata: metadata,
			Vector:   res.Vector,
		})
	}

	return mappedResults
}

// GetDataset retrieves a dataset by name.
func (s *VectorStore) GetDataset(name string) (*Dataset, error) {
	ds, ok := s.getDataset(name)
	if !ok {
		return nil, NewNotFoundError("dataset", name)
	}
	return ds, nil
}

// HybridSearch is a wrapper for the HybridSearch function
func (s *VectorStore) HybridSearch(ctx context.Context, name string, query []float32, k int, filters map[string]string) ([]types.SearchResult, error) {
	return HybridSearch(ctx, s, name, query, k, filters)
}

// SearchHybrid is a wrapper for the SearchHybrid function (RRF version)
func (s *VectorStore) SearchHybrid(ctx context.Context, name string, query []float32, textQuery string, k int, alpha float32, rrfK int, graphAlpha float32, graphDepth int) ([]types.SearchResult, error) {
	// Expose graph params in future? For now default to 0 (disabled)
	return SearchHybrid(ctx, s, name, query, textQuery, k, alpha, rrfK, graphAlpha, graphDepth)
}

func findVectorColumn(rec arrow.RecordBatch) arrow.Array {
	if rec == nil || rec.Schema() == nil {
		return nil
	}
	for i, field := range rec.Schema().Fields() {
		if field.Name == "vector" || field.Name == "embedding" {
			return rec.Column(i)
		}
	}
	return nil
}

// handleDoGetSearch executes a search request and streams results as Arrow Records
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
	if cached, hit := s.queryCache.Get(cacheKey); hit {
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
			searchResults, err = s.SearchHybrid(stream.Context(), req.Dataset, queryVec, req.TextQuery, req.K, req.Alpha, 60, req.GraphAlpha, depth)
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

			// Core Search (No dataset lock held)
			var searchErr error
			filterExpr := ParseFilter(req.FilterExpr)
			searchResults, searchErr = index.SearchVectors(stream.Context(), queryVec, req.K, req.Filters, types.SearchOptions{
				IncludeVectors: req.IncludeVectors,
				VectorFormat:   types.MapStringToVectorDataType(req.VectorFormat),
				FilterExpr:     filterExpr,
				Predicate:      qry.ExtractPushablePredicate(filterExpr, ds.Records),
			})
			if searchErr != nil {
				return status.Errorf(codes.Internal, "search failed: %v", searchErr)
			}

			// Capture data for mapping/re-ranking
			ds.dataMu.RLock()
			// Graph Re-ranking
			if req.GraphAlpha > 0 && graph != nil {
				depth := req.GraphDepth
				if depth <= 0 {
					depth = 2
				}
				ranked := graph.RankWithGraph(searchResults, req.GraphAlpha, depth)
				if len(ranked) > 0 {
					searchResults = ranked
				}
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
			s.queryCache.Put(cacheKey, searchResults)
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

	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	defer func() { _ = w.Close() }()

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Uint64Builder)
	scoreBuilder := builder.Field(1).(*array.Float32Builder)
	var vectorBuilder *array.BinaryBuilder
	if req.IncludeVectors {
		vectorBuilder = builder.Field(2).(*array.BinaryBuilder)
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
			if !isDeleted && loc.BatchIdx < len(ds.Records) {
				rec := ds.Records[loc.BatchIdx]
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
		for batchIdx, rec := range ds.Records {
			idColIdx := -1
			for i, field := range rec.Schema().Fields() {
				if field.Name == "id" {
					idColIdx = i
					break
				}
			}

			if idColIdx == -1 {
				continue
			}

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
 
	w := flight.NewRecordWriter(stream, ipc.WithSchema(builder.Schema()))
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

	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
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

func (s *VectorStore) resolveCTEs(ctx context.Context, ctes []qry.CTE, results map[string][]types.SearchResult) error {
	for _, cte := range ctes {
		if cte.Search == nil {
			continue
		}
		// Execute the search for this CTE
		// Wrap Search in a TicketQuery to use executeInternalTicket with fallback support
		tkt := &qry.TicketQuery{
			Name:   cte.Search.Dataset,
			Search: cte.Search,
		}
		res, err := s.executeInternalTicket(ctx, tkt)
		if err != nil {
			return err
		}
		s.logger.Debug().Str("cte", cte.Name).Int("results", len(res)).Msg("CTE resolved")
		results[cte.Name] = res
	}
	return nil
}

func (s *VectorStore) resolveSubqueries(ctx context.Context, filters []qry.Filter) error {
	for i := range filters {
		f := &filters[i]
		// Recursive check
		if len(f.Filters) > 0 {
			if err := s.resolveSubqueries(ctx, f.Filters); err != nil {
				return err
			}
		}

		if f.Subquery != nil {
			// Execute subquery
			res, err := s.executeInternalTicket(ctx, f.Subquery)
			if err != nil {
				return err
			}
			s.logger.Debug().Str("field", f.Field).Int("results", len(res)).Msg("Subquery resolved")
			// Extract IDs (or first column) into ResolvedValues
			resolved := make([]any, len(res))
			for j, r := range res {
				resolved[j] = uint64(r.ID)
			}
			f.ResolvedValues = resolved
		}
	}
	return nil
}

func (s *VectorStore) executeInternalSearch(ctx context.Context, req *qry.VectorSearchRequest) ([]types.SearchResult, error) {
	isHybrid := req.TextQuery != "" || (req.Alpha > 0 && req.Alpha < 1.0)
	var queryVec []float32
	if len(req.Vector) > 0 {
		queryVec = req.Vector
	}

	if isHybrid {
		return s.SearchHybrid(ctx, req.Dataset, queryVec, req.TextQuery, req.K, req.Alpha, 60, req.GraphAlpha, 2)
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return nil, fmt.Errorf("dataset %s not found", req.Dataset)
	}

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	if ds.Index == nil {
		return nil, fmt.Errorf("index not initialized for %s", req.Dataset)
	}

	res, err := ds.Index.SearchVectors(ctx, queryVec, req.K, req.Filters, SearchOptions{
		IncludeVectors: req.IncludeVectors,
		VectorFormat:   types.MapStringToVectorDataType(req.VectorFormat),
		FilterExpr:     ParseFilter(req.FilterExpr),
	})
	if err != nil {
		return nil, err
	}

	return s.mapInternalToUserIDsLocked(ds, res), nil
}

func (s *VectorStore) executeInternalTicket(ctx context.Context, query *qry.TicketQuery) ([]types.SearchResult, error) {
	// If search is present AND has a vector/text, use vector search path
	if query.Search != nil && (len(query.Search.Vector) > 0 || query.Search.TextQuery != "") {
		return s.executeInternalSearch(ctx, query.Search)
	}

	// If search is present but has NO vector (metadata only), 
	// copy its parameters to the main query for table scan.
	if query.Search != nil {
		if query.Name == "" {
			query.Name = query.Search.Dataset
		}
		if query.Limit == 0 {
			query.Limit = int64(query.Search.K)
		}
		if len(query.Filters) == 0 && len(query.Search.Filters) > 0 {
			query.Filters = query.Search.Filters
		}
	}

	// Table scan fallback for metadata-only internal queries
	return s.executeInternalTable(query)
}

func (s *VectorStore) executeInternalTable(query *qry.TicketQuery) ([]types.SearchResult, error) {
	ds, ok := s.getDataset(query.Name)
	if !ok {
		return nil, fmt.Errorf("dataset %s not found", query.Name)
	}

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	var results []types.SearchResult
	limit := int(query.Limit)
	if limit <= 0 {
		limit = 1000 // Default internal limit
	}

	for i, rec := range ds.Records {
		if len(results) >= limit {
			break
		}

		// Apply filters
		var eval *qry.FilterEvaluator
		if len(query.Filters) > 0 {
			var err error
			eval, err = s.evaluateFilters(ds, i, query.Filters)
			if err != nil {
				return nil, err
			}
		}

		// Apply tombstones
		ts := ds.Tombstones[i]

		// Find ID column
		idColIdx := -1
		for j, field := range rec.Schema().Fields() {
			if field.Name == "id" {
				idColIdx = j
				break
			}
		}

		numRows := int(rec.NumRows())
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if len(results) >= limit {
				break
			}

			// Check filters
			if eval != nil && !eval.Matches(rowIdx) {
				continue
			}

			// Check tombstones
			if ts != nil && ts.Contains(rowIdx) {
				continue
			}

			var res types.SearchResult
			// We need a numeric ID for SearchResult.
			// If 'id' column exists, try to extract it.
			if idColIdx != -1 {
				col := rec.Column(idColIdx)
				switch c := col.(type) {
				case *array.Uint32:
					res.ID = types.VectorID(c.Value(rowIdx))
				case *array.Uint64:
					res.ID = types.VectorID(c.Value(rowIdx)) // #nosec G115
				case *array.Int64:
					res.ID = types.VectorID(c.Value(rowIdx)) // #nosec G115
				case *array.Int32:
					res.ID = types.VectorID(c.Value(rowIdx)) // #nosec G115
				default:
					// Fallback to internal ID (constructed from batch/row)
					res.ID = types.VectorID(uint32(i)<<16 | uint32(rowIdx))
				}
			} else {
				res.ID = types.VectorID(uint32(i)<<16 | uint32(rowIdx))
			}

			results = append(results, res)
		}
	}

	return results, nil
}

func (s *VectorStore) evaluateFilters(ds *Dataset, batchIdx int, filters []core.Filter) (*qry.FilterEvaluator, error) {
	rec := ds.Records[batchIdx]
	eval, err := qry.NewFilterEvaluator(rec, filters)
	if err != nil {
		return nil, err
	}
	return eval, nil
}

// streamSearchResults is a helper to stream a list of search results as RecordBatches
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
	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
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
					case int64: intVal = v
					case int: intVal = int64(v)
					case float64: intVal = int64(v)
					}
					builder.Field(colOffset + wfIdx).(*array.Int64Builder).Append(intVal)
				default:
					var floatVal float64
					switch v := val.(type) {
					case float64: floatVal = v
					case int64: floatVal = float64(v)
					case int: floatVal = float64(v)
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
func (s *VectorStore) handleDoGetGeoSearch(req *types.GeoSearchRequest, wfs []qry.WindowFunction, stream flight.FlightService_DoGetServer, mem *lbmem.ArenaAllocator) error {
	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
	}

	if ds.GeoIndex == nil {
		return status.Error(codes.FailedPrecondition, "dataset has no geospatial index")
	}

	// Lock dataset for search
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	var results []types.SearchResult
	var err error

	switch req.SearchType {
	case "radius":
		results, err = ds.GeoIndex.SearchRadius(stream.Context(), req.Center, req.RadiusKm, req.K)
	case "box":
		if req.Box == nil {
			return status.Error(codes.InvalidArgument, "bounding box required for 'box' search")
		}
		results, err = ds.GeoIndex.SearchBox(stream.Context(), *req.Box, req.K)
	case "hybrid":
		results, err = ds.GeoIndex.HybridSearch(stream.Context(), nil, req.Center, req.RadiusKm, req.K)
	default:
		return status.Errorf(codes.InvalidArgument, "invalid search_type: %s", req.SearchType)
	}

	if err != nil {
		return status.Errorf(codes.Internal, "geospatial search failed: %v", err)
	}

	// Map internal IDs to user IDs if primary index exists
	results = s.mapInternalToUserIDsLocked(ds, results)

	return s.streamSearchResults(results, wfs, stream, mem)
}

func (s *VectorStore) handleDoGetTemporalSearch(req *types.TemporalSearchRequest, wfs []qry.WindowFunction, stream flight.FlightService_DoGetServer, mem *lbmem.ArenaAllocator) error {
	if s.temporalIndex == nil {
		return status.Error(codes.FailedPrecondition, "temporal index not enabled")
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
	}

	var results []types.SearchResult
	var err error

	switch req.SearchType {
	case "as_of":
		results, err = s.temporalIndex.SearchAsOf(stream.Context(), req.Timestamp, req.K)
	case "range":
		results, err = s.temporalIndex.SearchRange(stream.Context(), req.StartTime, req.EndTime, req.K)
	case "sliding_window":
		results, err = s.temporalIndex.SearchSlidingWindow(stream.Context(), req.WindowSize, req.K)
	case "sliding_window_time":
		results, err = s.temporalIndex.SearchSlidingWindowByTime(stream.Context(), req.Duration, req.K)
	default:
		return status.Errorf(codes.InvalidArgument, "invalid temporal search_type: %s", req.SearchType)
	}

	if err != nil {
		return status.Errorf(codes.Internal, "temporal search failed: %v", err)
	}

	// Lock dataset for ID mapping (requires dataMu)
	ds.dataMu.RLock()
	results = s.mapInternalToUserIDsLocked(ds, results)
	ds.dataMu.RUnlock()

	return s.streamSearchResults(results, wfs, stream, mem)
}
