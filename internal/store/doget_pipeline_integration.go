package store

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"
)

// DoGetPipelineConfig holds configuration for DoGet pipeline processing
type DoGetPipelineConfig struct {
	Workers    int // Number of parallel workers
	BufferSize int // Channel buffer size
	Threshold  int // Minimum batches to trigger pipeline (vs serial)
}

// DefaultDoGetPipelineConfig returns sensible defaults
func DefaultDoGetPipelineConfig() DoGetPipelineConfig {
	return DoGetPipelineConfig{
		Workers:    4,
		BufferSize: 16,
		Threshold:  2, // Use pipeline for 2+ batches
	}
}

// pipelineStats tracks aggregate pipeline statistics
type pipelineStats struct {
	batchesProcessed atomic.Int64
	errorsTotal      atomic.Int64
}

// Global pipeline stats (thread-safe)
var globalPipelineStats = &pipelineStats{}

// NewVectorStoreWithPipeline creates a VectorStore with DoGet pipeline enabled
func NewVectorStoreWithPipeline(mem memory.Allocator, logger *zap.Logger, workers, bufferSize int) *VectorStore {
	return NewVectorStoreWithPipelineThreshold(mem, logger, workers, bufferSize, 2)
}

// NewVectorStoreWithPipelineThreshold creates a VectorStore with custom pipeline threshold
func NewVectorStoreWithPipelineThreshold(mem memory.Allocator, logger *zap.Logger, workers, bufferSize, threshold int) *VectorStore {
	// Create base store using existing constructor with defaults
	store := NewVectorStore(mem, logger, 1<<30, 100<<20, 24*time.Hour) // 1GB mem, 100MB WAL, 24h TTL
	if store == nil {
		return nil
	}

	// Initialize pipeline pool
	store.doGetPipelinePool = NewDoGetPipelinePool(workers, bufferSize)
	store.pipelineThreshold = threshold

	logger.Info("DoGet pipeline enabled",
		zap.Int("workers", workers),
		zap.Int("buffer_size", bufferSize),
		zap.Int("threshold", threshold),
	)

	return store
}

// GetDoGetPipelinePool returns the pipeline pool (nil if not configured)
func (s *VectorStore) GetDoGetPipelinePool() *DoGetPipelinePool {
	return s.doGetPipelinePool
}

// GetPipelineThreshold returns the batch count threshold for pipeline use
func (s *VectorStore) GetPipelineThreshold() int {
	return s.pipelineThreshold
}

// GetPipelineStats returns current pipeline statistics
func (s *VectorStore) GetPipelineStats() PipelineStats {
	return PipelineStats{
		BatchesProcessed: globalPipelineStats.batchesProcessed.Load(),
		BatchesFiltered:  0,
		ErrorCount:       globalPipelineStats.errorsTotal.Load(),
	}
}

// StoreRecords stores arrow records in the dataset (helper for tests)
func (s *VectorStore) StoreRecords(name string, records []arrow.Record) error { //nolint:staticcheck
	ds := &Dataset{
		Records: make([]arrow.Record, 0, len(records)), //nolint:staticcheck
	}
	for _, rec := range records {
		rec.Retain()
		ds.Records = append(ds.Records, rec)
	}
	s.vectors.Set(name, ds)
	return nil
}

// incrementPipelineBatches safely increments processed batch count
func (s *VectorStore) incrementPipelineBatches(count int64) {
	globalPipelineStats.batchesProcessed.Add(count)
}

// incrementPipelineErrors safely increments error count
func (s *VectorStore) incrementPipelineErrors() {
	globalPipelineStats.errorsTotal.Add(1)
}

// doGetWithPipeline processes multiple batches using the pipeline for parallelism
func (s *VectorStore) doGetWithPipeline(
	ctx context.Context,
	name string,
	recs []arrow.Record, //nolint:staticcheck
	query *TicketQuery,
	w *flight.Writer,
	limit int64,
) (int64, error) {
	if s.doGetPipelinePool == nil {
		return 0, fmt.Errorf("pipeline pool not initialized")
	}

	pipeline := s.doGetPipelinePool.Get()
	defer s.doGetPipelinePool.Put(pipeline)

	// Convert []arrow.Record to []arrow.RecordBatch for pipeline
	batches := make([]arrow.RecordBatch, len(recs))
	for i, rec := range recs { //nolint:staticcheck // interface conversion requires loop
		batches[i] = rec
	}

	// Create filter function that wraps filterRecordOptimized
	filterFn := func(filterCtx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
		arrowRec, ok := rec.(arrow.Record) //nolint:staticcheck
		if !ok {
			return nil, fmt.Errorf("invalid record type")
		}
		return s.filterRecordOptimized(filterCtx, name, arrowRec, 0, query.Filters)
	}

	// Process batches through pipeline
	resultCh, errCh := pipeline.Process(ctx, batches, filterFn)

	rowsSent := int64(0)

	// Consume results and write to stream
	for result := range resultCh {
		if limit > 0 && rowsSent >= limit {
			break
		}

		if result.Record == nil {
			continue
		}

		filteredRec, ok := result.Record.(arrow.Record) //nolint:staticcheck
		if !ok {
			continue
		}

		if filteredRec.NumRows() == 0 {
			filteredRec.Release()
			continue
		}

		toWrite := filteredRec
		sliced := false
		if limit > 0 && rowsSent+filteredRec.NumRows() > limit {
			remaining := limit - rowsSent
			toWrite = filteredRec.NewSlice(0, remaining)
			sliced = true
		}

		if err := w.Write(toWrite); err != nil {
			if sliced {
				toWrite.Release()
			}
			filteredRec.Release()
			return rowsSent, err
		}
		rowsSent += toWrite.NumRows()
		s.incrementPipelineBatches(1)

		if sliced {
			toWrite.Release()
		}
		filteredRec.Release()
	}

	// Check for pipeline errors
	select {
	case err := <-errCh:
		if err != nil {
			s.incrementPipelineErrors()
			return rowsSent, err
		}
	default:
	}

	return rowsSent, nil
}

// shouldUsePipeline decides whether to use pipeline based on batch count
func (s *VectorStore) shouldUsePipeline(batchCount int) bool {
	if s.doGetPipelinePool == nil {
		return false
	}
	threshold := s.pipelineThreshold
	if threshold <= 0 {
		threshold = 2 // default: use pipeline for 2+ batches
	}
	return batchCount >= threshold
}
