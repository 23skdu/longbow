package store

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/pkg/loadbalancing"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"

	lbflight "github.com/23skdu/longbow/internal/flight"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	internalcore "github.com/23skdu/longbow/internal/store/index"
	types "github.com/23skdu/longbow/internal/store/types"
)

// ListFlights returns a list of available datasets as FlightInfo objects.
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
				for _, rec := range ds.Records.Read() {
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
			metadata := s.getPooledMetadataBuffer(loadbalancing.LoadHintsSize)
			hints := s.GetLoadHints()
			hints.Serialize(metadata)

			info := &flight.FlightInfo{
				FlightDescriptor: &flight.FlightDescriptor{
					Type: flight.DescriptorPATH,
					Path: []string{ds.Name},
				},
				AppMetadata: metadata,
			}
			err := stream.Send(info)
			s.putPooledMetadataBuffer(metadata)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// GetFlightInfo returns metadata about a specific dataset.
func (s *VectorStore) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if len(desc.Path) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Empty path")
	}
	name := desc.Path[0]
	if name == "_health" {
		metadata := s.getPooledMetadataBuffer(loadbalancing.LoadHintsSize)
		hints := s.nodeMonitor.GetLoadHints()
		hints.Serialize(metadata)
		return &flight.FlightInfo{
			FlightDescriptor: desc,
			AppMetadata:      metadata,
		}, nil
	}

	ds, ok := s.getDataset(name)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "dataset %s not found", name)
	}

	if !ds.IsReady.Load() {
		return nil, status.Errorf(codes.Unavailable, "dataset %s is being initialized", name)
	}

	// Include load balancing hints in AppMetadata (Apache Arrow Zero-Alloc approach)
	metadata := s.getPooledMetadataBuffer(loadbalancing.LoadHintsSize)
	hints := s.nodeMonitor.GetLoadHints()
	hints.Serialize(metadata)

	return &flight.FlightInfo{
		FlightDescriptor: desc,
		TotalRecords:     int64(len(ds.Records.Read())),
		TotalBytes:       ds.SizeBytes.Load(),
		AppMetadata:      metadata,
	}, nil
}

// GetSchema returns the Arrow schema for a specific dataset.
func (s *VectorStore) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if len(desc.Path) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty path")
	}
	name := desc.Path[0]
	ds, ok := s.getDataset(name)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "dataset %s not found", name)
	}
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()
	if ds.Schema == nil {
		return nil, status.Errorf(codes.Internal, "dataset %s has no schema", name)
	}
	b := flight.SerializeSchema(ds.Schema, memory.DefaultAllocator)
	return &flight.SchemaResult{Schema: b}, nil
}

// DoGet handles data retrieval and vector search queries via Arrow Tickets.
func (s *VectorStore) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	startDoGet := time.Now()
	// Parse ticket
	query, err := qry.ParseTicketQuerySafe(tkt.Ticket)
	if err != nil {
		// Fallback: treat as plain string name if parse fails
		sStr := string(tkt.Ticket)
		if sStr != "" && sStr[0] != '{' {
			query.Name = sStr
			err = nil // Reset error since fallback succeeded
		} else {
			s.logger.Error().Err(err).Str("ticket_preview", string(tkt.Ticket)).Msg("Failed to parse ticket")
			return status.Error(codes.InvalidArgument, "invalid ticket format")
		}
	}
	// 0. Admission Control (Backpressure)
	if s.admission != nil {
		if err := s.admission.Admit(stream.Context(), "search"); err != nil {
			return err
		}
	}

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

		// Wrap with Circuit Breaker
		cb := s.Breakers.GetOrCreate(query.Search.Dataset)
		_, err := cb.Execute(func() (any, error) {
			return nil, s.handleDoGetSearch(query.Search, query.WindowFunctions, stream, mem)
		})
		return err
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
	s.logger.Debug().
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
	if len(ds.Records.Read()) == 0 {
		ds.dataMu.RUnlock()
		s.logger.Warn().Msg("Dataset empty")
		return nil
	}

	// Use first record's schema (all records in a dataset must share schema)
	schema := ds.Records.Read()[0].Schema()

	// Adaptive Chunking (Byte-Aware Optimization)
	// We estimate row size to ensure chunks are at least ~2MB to saturate bandwidth
	// while keeping overhead low.
	avgRowSize := int64(256) // Default fallback
	firstBatch := ds.Records.Read()[0]
	if firstBatch.NumRows() > 0 {
		batchSize := estimateBatchSize(firstBatch)
		avgRowSize = batchSize / firstBatch.NumRows()
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
	recordsToProcess, tombstonesToProcess := AdaptivelySliceBatches(ds.Records.Read(), ds.Tombstones, chunkStrategy)
	ds.dataMu.RUnlock() // RELEASE LOCK IMMEDIATELY AFTER CLONING REFERENCES

	s.logger.Debug().Str("name", name).Int("batches", len(recordsToProcess)).Msg("DoGet streaming started")

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

	pool := internalcore.GetSharedPool()
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerIdx := w
		pool.Submit(func() {
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
		})
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
	writer := newDoGetRecordWriter(stream, schema, minChunkRows, int(avgRowSize))
	defer func() { _ = writer.Close() }()

	// Consume Results (Sequential Write)
	for {
		rec, ok := <-resultsChan
		if !ok {
			resultsChan = nil // Channel closed
		} else {
			// Guard against nil/empty records that can cause IPC writer panics.
			//
			// The empty-record case (rows=0, cols=0) is a benign stub-record
			// path — the producer emits a zero-sized record to signal batch
			// boundaries, and the DoGet path correctly skips it. The warning
			// used to fire 5× per DoGet on int8 50k+ runs (roadmap.md §4,
			// "Query Hotpath Logging Mutex Contention"), which polluted the
			// logs without indicating a real fault. Demoted to Debug.
			//
			// The nil case is a real producer bug (a nil record from the
			// channel means the producer panicked or returned early). Kept
			// at Warn so on-call sees it.
			if rec == nil {
				s.logger.Warn().Msg("Skipping nil record in DoGet (producer bug)")
				continue
			}
			if rec.NumRows() == 0 || rec.NumCols() == 0 {
				s.logger.Debug().Int64("rows", rec.NumRows()).Int64("cols", rec.NumCols()).Msg("Skipping empty stub record in DoGet")
				rec.Release()
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
	s.logger.Debug().Int64("rows_sent", rowsSent).Msg("DoGet completed")
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

	// Resolve column indices once from the dataset schema (shared by all record batches)
	idColIdx := -1
	metadataColIdx := -1
	if ds.Schema != nil {
		for i, f := range ds.Schema.Fields() {
			if f.Name == "id" {
				idColIdx = i
			}
			if f.Name == "metadata" {
				metadataColIdx = i
			}
		}
	}

	currentRecords := ds.Records.Read()
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
		if loc.BatchIdx >= len(currentRecords) {
			continue
		}
		rec := currentRecords[loc.BatchIdx]

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

		// Deep copy metadata and vector to release Arrow buffers.
		// This is critical because these results may be cached in the QueryCache,
		// and keeping a slice into a 2MB+ RecordBatch buffer prevents the entire
		// buffer from being garbage collected.
		var metaCopy []byte
		if len(metadata) > 0 {
			metaCopy = make([]byte, len(metadata))
			copy(metaCopy, metadata)
		}

		var vecCopy []byte
		if len(res.Vector) > 0 {
			vecCopy = make([]byte, len(res.Vector))
			copy(vecCopy, res.Vector)
		}

		mappedResults = append(mappedResults, types.SearchResult{
			ID:       resolvedID,
			Score:    res.Score,
			Distance: res.Distance,
			Metadata: metaCopy,
			Vector:   vecCopy,
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
