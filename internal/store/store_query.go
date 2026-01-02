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

	"github.com/23skdu/longbow/internal/metrics"
)

func (s *VectorStore) ListFlights(c *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	var query TicketQuery
	var err error
	if c != nil && len(c.Expression) > 0 {
		query, err = ParseTicketQuerySafe(c.Expression)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "Invalid criteria: %v", err)
		}
	}

	s.mu.RLock()
	datasets := make([]*Dataset, 0, len(s.datasets))
	for _, ds := range s.datasets {
		datasets = append(datasets, ds)
	}
	s.mu.RUnlock()

	for _, ds := range datasets {
		// Apply filters
		match := true
		for _, f := range query.Filters {
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
	ds, err := s.getDataset(name)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
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
	// Parse ticket
	query, err := ParseTicketQuerySafe(tkt.Ticket)
	if err != nil {
		// Fallback: treat as plain string name if parse fails
		sStr := string(tkt.Ticket)
		if len(sStr) > 0 && sStr[0] != '{' {
			query.Name = sStr
		} else {
			s.logger.Error().Err(err).Msg("Failed to parse ticket")
			return status.Error(codes.InvalidArgument, "invalid ticket format")
		}
	}

	name := query.Name
	s.logger.Info().
		Str("name", name).
		Int("filters", len(query.Filters)).
		Msg("DoGet called")

	ds, err := s.getDataset(name)
	if err != nil {
		return err
	}

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	if len(ds.Records) == 0 {
		s.logger.Warn().Msg("Dataset empty")
		return nil
	}

	// Use first record's schema
	schema := ds.Records[0].Schema()

	// Create Writer WITHOUT options first to be safe
	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	defer w.Close()

	ctx := stream.Context()
	rowsSent := int64(0)

	// Parallel Processing with Pipeline Support (Phase 5)
	numWorkers := runtime.NumCPU()
	if numWorkers > len(ds.Records) {
		numWorkers = len(ds.Records)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	resultsChan := make(chan arrow.RecordBatch, numWorkers*2)
	// Buffer 1 to prevent blocking on first error check
	errChan := make(chan error, 1)
	var wg sync.WaitGroup

	// Determine execution strategy
	// Determine execution strategy
	var stageChan <-chan PipelineStage
	usePipeline := s.shouldUsePipeline(len(ds.Records))
	var pipeline *DoGetPipeline

	if usePipeline {
		// Use prefetching pipeline
		if s.doGetPipelinePool != nil {
			pipeline = s.doGetPipelinePool.Get()
		} else {
			pipeline = NewDoGetPipeline(8, 16) // Fallback defaults
		}

		// ProcessRecords handles feeding safely
		stageChan = pipeline.ProcessRecords(ctx, ds.Records, ds.Tombstones, query.Filters, nil)
		metrics.DoGetPipelineStepsTotal.WithLabelValues("scan", "pipeline").Add(float64(len(ds.Records)))
		s.logger.Debug().Int("workers", pipeline.NumWorkers()).Msg("Using DoGetPipeline")
	} else {
		// Simple feeder for small datasets
		metrics.DoGetPipelineStepsTotal.WithLabelValues("scan", "simple").Add(float64(len(ds.Records)))
		c := make(chan PipelineStage, len(ds.Records))
		stageChan = c
		go func() {
			defer close(c)
			for i, rec := range ds.Records {
				var ts *Bitset
				// Map access is safe under RLock
				if t, ok := ds.Tombstones[i]; ok {
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
			for stage := range stageChan {
				rec := stage.Record
				deleted := stage.Tombstone

				var processed arrow.RecordBatch
				var err error

				if len(query.Filters) > 0 {
					filterStart := time.Now()
					filtered, err := filterRecord(ctx, s.mem, rec, query.Filters)
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
						processed, err = ZeroCopyRecordBatch(s.mem, rec, deleted)
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
					processed.Release()
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

	// Consume Results (Sequential Write)
	for {
		select {
		case batch, ok := <-resultsChan:
			if !ok {
				resultsChan = nil // Channel closed
			} else {
				// Verify Columns
				for i := 0; i < int(batch.NumCols()); i++ {
					col := batch.Column(i)
					if col.Len() != int(batch.NumRows()) {
						s.logger.Error().
							Int("col_idx", i).
							Int("col_len", col.Len()).
							Int64("batch_rows", batch.NumRows()).
							Msg("DoGet Batch Column Length Mismatch")
					}
					// Check data length for specific types if known, or just ensure not nil
					if col.Data() == nil {
						s.logger.Error().Int("col_idx", i).Msg("DoGet Batch Column Data is NIL")
					}
				}

				if err := w.Write(batch); err != nil {
					s.logger.Error().Err(err).Msg("DoGet Write failed")
					batch.Release()
					return err
				}
				rowsSent += batch.NumRows()
				batch.Release()

				// Track stats for test verification
				if usePipeline {
					s.incrementPipelineBatches(1)
				}

				if query.Limit > 0 && rowsSent >= query.Limit {
					// Stop workers? Context cancel?
					// Ideally we cancel context for workers logic but here we just break read loop.
					// Workers might still produce some.
					// Since we don't have cancelable context for workers explicitly here (using stream.Context),
					// we can't easily stop them. They will finish or block on closed resultsChan?
					// No, we read until closed. If we break early, we must drain or assume cleanup.
					// For MVP: Break loop. Workers finish.
					// But if we break, `go func` feeding `workChan` might block on `resultsChan` sends if buffer full?
					// Yes.
					// FIX: Drain channel if breaking early.
					goto DRAIN
				}
			}
		case err, ok := <-errChan:
			if ok && err != nil {
				return err
			}
		}
		if resultsChan == nil {
			break
		}
	}

	if pipeline != nil && s.doGetPipelinePool != nil {
		s.doGetPipelinePool.Put(pipeline)
	}

	// Normal exit
	return nil

DRAIN:
	if pipeline != nil && s.doGetPipelinePool != nil {
		s.doGetPipelinePool.Put(pipeline)
	}
	// Drain remaining results to prevent worker deadlock
	go func() {
		for range resultsChan {
			// discard
		}
	}()

	s.logger.Info().Int64("rows_sent", rowsSent).Msg("DoGet completed")
	metrics.FlightRowsProcessed.WithLabelValues("get", "ok").Add(float64(rowsSent))
	return nil
}

// MapInternalToUserIDs maps internal HNSW IDs to user-provided IDs
func (s *VectorStore) MapInternalToUserIDs(ds *Dataset, results []SearchResult) []SearchResult {
	start := time.Now()
	defer func() {
		metrics.IDResolutionDuration.Observe(time.Since(start).Seconds())
	}()

	s.mu.RLock()
	hnswIndex, ok := ds.Index.(*HNSWIndex)
	s.mu.RUnlock()

	if ok && hnswIndex != nil {
		// Optimization: if it's a plain HNSW index, use its built-in mapping which is faster (Phase 14)
		// but wait, its built-in mapping might be what we are implementing here!
		// Let's stick to the store-side mapping for now as it's more flexible with Arrow types.
	} else {
		// Fallback for AutoShardingIndex
		s.mu.RLock()
		autoIndex, isAuto := ds.Index.(*AutoShardingIndex)
		s.mu.RUnlock()

		if isAuto {
			autoIndex.mu.RLock()
			current := autoIndex.current
			autoIndex.mu.RUnlock()

			if h, ok := current.(*HNSWIndex); ok {
				hnswIndex = h
			}
		}
	}

	if hnswIndex == nil {
		return results
	}

	mappedResults := make([]SearchResult, 0, len(results))

	// We need to access dataset records. The HNSW index locations point to Batch/Row.
	// We'll use those to look up the ID from the "id" column of the record batch.
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	for _, res := range results {
		// 1. Get location (Batch, Row) from HNSW internal ID
		loc, found := hnswIndex.GetLocation(res.ID)
		if !found {
			// If not found in index (race condition?), skip or keep
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
	return s.getDataset(name)
}

func (s *VectorStore) getDataset(name string) (*Dataset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ds, ok := s.datasets[name]
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
func (s *VectorStore) SearchHybrid(ctx context.Context, name string, query []float32, textQuery string, k int, alpha float32, rrfK int) ([]SearchResult, error) {
	return SearchHybrid(ctx, s, name, query, textQuery, k, alpha, rrfK)
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
