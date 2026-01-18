package store

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/23skdu/longbow/internal/cache"
	lbflight "github.com/23skdu/longbow/internal/flight"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
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
	// log.Printf("[DEBUG] DoGet received ticket (len=%d): %q", len(tkt.Ticket), string(tkt.Ticket))
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

	// Create Request-Scoped Arena Allocator
	// This reduces GC pressure for transient buffers (masks, filtered batches, serialized records)
	mem := lbmem.NewArenaAllocator()
	defer mem.Release()

	// Handle Search Request via DoGet (Native Arrow Streaming)
	if query.Search != nil {
		return s.handleDoGetSearch(query.Search, stream, mem)
	}

	// Existing Dataset Fetch Logic
	name := query.Name
	s.logger.Info().
		Str("name", name).
		Int("filters", len(query.Filters)).
		Interface("parsed_filters", query.Filters).
		Msg("DoGet called")

	ds, ok := s.getDataset(name)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset %s not found", name)
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
				var ts *qry.Bitset
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
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var evaluator *qry.FilterEvaluator

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
						mask, err = evaluator.EvaluateToArrowBoolean(mem, int(rec.NumRows()))
					}

					var filtered arrow.RecordBatch
					if err == nil {
						filtered, err = filterRecordWithMask(ctx, mem, rec, mask)
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
						processed, err = ZeroCopyRecordBatch(mem, rec, deleted)
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
		}()
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
func (s *VectorStore) MapInternalToUserIDs(ds *Dataset, results []SearchResult) []SearchResult {
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
func (s *VectorStore) mapInternalToUserIDsLocked(ds *Dataset, results []SearchResult) []SearchResult {
	// Use the VectorIndex interface directly to look up locations.
	// This supports HNSWIndex, ArrowHNSW, AutoShardingIndex, etc.
	if ds.Index == nil {
		return results
	}

	mappedResults := make([]SearchResult, 0, len(results))

	for _, res := range results {
		// 1. Get location (Batch, Row) from VectorIndex
		loc, found := ds.Index.GetLocation(res.ID)
		if !found {
			// If not found in index (race condition?), skip or keep
			// If we return raw result, it contains internal ID, which might confuse client.
			// But skipping might lose data.
			// Let's assume invalid and skip?
			// log.Printf("[DEBUG] MapInternalToUserIDs: ID %d not found in Index location store", res.ID)
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
		var resolvedID VectorID

		switch c := col.(type) {
		case *array.Uint32:
			if loc.RowIdx < c.Len() {
				resolvedID = VectorID(c.Value(loc.RowIdx))
			} else {
				resolvedID = res.ID // Fallback
			}
		case *array.Uint64:
			if loc.RowIdx < c.Len() {
				resolvedID = VectorID(c.Value(loc.RowIdx)) // Truncate if needed
			} else {
				resolvedID = res.ID
			}
		case *array.Int64:
			if loc.RowIdx < c.Len() {
				resolvedID = VectorID(c.Value(loc.RowIdx))
			} else {
				resolvedID = res.ID
			}
		case *array.Int32:
			if loc.RowIdx < c.Len() {
				resolvedID = VectorID(c.Value(loc.RowIdx))
			} else {
				resolvedID = res.ID
			}
		case *array.String:
			if loc.RowIdx < c.Len() {
				// We can't return string IDs in the uint64 field.
				// But we can try to parse it if it's a numeric string,
				// or hash it if we really need a uint64.
				// However, for archer integration, we often use numeric strings for testing,
				// or we need a way to return the actual string.
				val := c.Value(loc.RowIdx)
				u, err := strconv.ParseUint(val, 10, 64)
				if err == nil {
					resolvedID = VectorID(u)
				} else {
					// If not numeric, we're stuck with internal ID for the uint64 field.
					// A better fix would be to return StringIDs in the response.
					resolvedID = res.ID
				}
			} else {
				resolvedID = res.ID
			}
		default:
			// Unsupported ID type
			resolvedID = res.ID
		}

		mappedResults = append(mappedResults, SearchResult{
			ID:    resolvedID,
			Score: res.Score,
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
func (s *VectorStore) HybridSearch(ctx context.Context, name string, query []float32, k int, filters map[string]string) ([]SearchResult, error) {
	return HybridSearch(ctx, s, name, query, k, filters)
}

// SearchHybrid is a wrapper for the SearchHybrid function (RRF version)
func (s *VectorStore) SearchHybrid(ctx context.Context, name string, query []float32, textQuery string, k int, alpha float32, rrfK int, graphAlpha float32, graphDepth int) ([]SearchResult, error) {
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
func (s *VectorStore) handleDoGetSearch(req *qry.VectorSearchRequest, stream flight.FlightService_DoGetServer, mem memory.Allocator) error {
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

	var searchResults []SearchResult
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
			searchResults, err = s.SearchHybrid(stream.Context(), req.Dataset, queryVec, req.TextQuery, req.K, req.Alpha, 60, req.GraphAlpha, 2)
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

			// Validate dimension under lock
			if len(queryVec) > 0 && uint32(len(queryVec)) != index.GetDimension() {
				expected := index.GetDimension()
				ds.dataMu.RUnlock()
				return status.Errorf(codes.InvalidArgument, "dimension mismatch: expected %d, got %d", expected, len(queryVec))
			}
			ds.dataMu.RUnlock() // RELEASE BEFORE SEARCH

			// Core Search (No dataset lock held)
			searchResults, err = index.SearchVectors(stream.Context(), queryVec, req.K, req.Filters, SearchOptions{
				IncludeVectors: req.IncludeVectors,
				VectorFormat:   req.VectorFormat,
			})
			if err != nil {
				return status.Errorf(codes.Internal, "search failed: %v", err)
			}

			// Capture data for mapping/re-ranking
			ds.dataMu.RLock()
			// Graph Re-ranking
			if req.GraphAlpha > 0 && graph != nil {
				ranked := graph.RankWithGraph(searchResults, req.GraphAlpha, 2)
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
			if req.IncludeVectors && vectorBuilder != nil {
				if searchResults[j].Vector != nil {
					vectorBuilder.Append(searchResults[j].Vector)
				} else {
					vectorBuilder.AppendNull()
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
