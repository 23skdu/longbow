package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/tracing"
)

// DoPut runs the do put operation.
func (s *VectorStore) DoPut(stream flight.FlightService_DoPutServer) error {
	_, span := tracing.CreateSpan(stream.Context(), "DoPut")
	if span != nil {
		defer span.End()
	}

	r, err := flight.NewRecordReader(stream, ipc.WithAllocator(s.pooledMem))
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

	if _, err := s.AuthorizeDatasetAccess(stream.Context(), PermIngest, name); err != nil {
		return err
	}

	s.logger.Info().Str("dataset", name).Msg("DoPut started (Batched)")

	// 0. Admission Control (Backpressure)
	if s.admission != nil {
		if err := s.admission.Admit(stream.Context(), "ingest"); err != nil {
			return err
		}
	}

	s.logger.Info().Str("schema", r.Schema().String()).Msg("DoPut Schema")
	// Use RCU helper for create
	ds, created := s.getOrCreateDataset(name, func() *Dataset {
		ds := NewDataset(name, r.Schema())
		ds.Logger = s.logger
		ds.Topo = s.numaTopology
		s.initDiskStore(ds, name, r.Schema())
		return ds
	})

	if ds == nil {
		return status.Errorf(codes.Internal, "failed to retrieve or create dataset %s", name)
	}

	ds.ActiveIngestStreams.Add(1)
	defer ds.ActiveIngestStreams.Add(-1)

	// Namespace quota check (will be done per-flush in the loop below)
	nsName, _ := ParseNamespacedPath(name)
	ns := s.GetNamespace(nsName)

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
		defer func() {
			for _, b := range batch {
				b.Release()
			}
			batch = batch[:0]
		}()

		// Namespace and tenant usage quota check on flush
		totalRows := int64(0)
		totalBytes := int64(0)
		for _, b := range batch {
			totalRows += b.NumRows()
			totalBytes += estimateBatchSize(b)
		}
		if ns != nil {
			if err := ns.CheckQuota(totalRows, 0, totalBytes); err != nil {
				return status.Errorf(codes.ResourceExhausted, "namespace quota exceeded: %v", err)
			}
		}
		if err := s.enforceTenantIngestQuota(stream.Context(), name, totalRows, totalBytes); err != nil {
			return err
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
					metrics.DoPutBatchSizeBytes.Observe(float64(subRecSize))
					err := s.flushPutBatch(stream.Context(), ds, []arrow.RecordBatch{subRec})
					subRec.Release()
					if err != nil {
						return err
					}
					continue
				}

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
			return err
		}
	}

	s.logger.Info().Str("name", name).Msg("DoPut completed (Batched)")
	s.PublishIndexBoundaries()
	return nil
}

// flushPutBatch handles writing a batch of records to WAL and memory
func (s *VectorStore) flushPutBatch(ctx context.Context, ds *Dataset, batch []arrow.RecordBatch) error {
	reg := s.GetSchemaRegistry()
	for _, rec := range batch {
		if err := reg.ValidateBatch(ctx, ds.Name, rec); err != nil {
			return status.Errorf(codes.InvalidArgument, "%v", err)
		}
	}
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
