package store

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
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
	recs []arrow.RecordBatch,
	query *TicketQuery,
	w *flight.Writer,
	targetSchema *arrow.Schema,
	limit int64,
) (int64, error) {
	if s.doGetPipelinePool == nil {
		return 0, fmt.Errorf("pipeline pool not initialized")
	}

	pipeline := s.doGetPipelinePool.Get()
	defer s.doGetPipelinePool.Put(pipeline)

	// Create filter function that wraps filterRecordOptimized
	filterFn := func(filterCtx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
		return s.filterRecordOptimized(filterCtx, name, rec, 0, query.Filters)
	}

	// Process batches through pipeline
	resultCh, errCh := pipeline.Process(ctx, recs, filterFn)

	rowsSent := int64(0)

	// Consume results and write to stream
	for result := range resultCh {
		if limit > 0 && rowsSent >= limit {
			break
		}

		if result.Record == nil {
			continue
		}

		filteredRec := result.Record
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

		if err := validateRecordBatch(toWrite); err != nil {
			metrics.ValidationFailuresTotal.WithLabelValues("pipeline", "invalid_batch").Inc()
			if sliced {
				toWrite.Release()
			}
			filteredRec.Release()
			s.logger.Error("Invalid record batch in pipeline during DoGet",
				zap.Error(err),
				zap.Int64("numCols", toWrite.NumCols()),
				zap.Int("numFields", toWrite.Schema().NumFields()),
				zap.Int64("numRows", toWrite.NumRows()))
			s.incrementPipelineErrors()
			return rowsSent, fmt.Errorf("invalid record batch in pipeline: %w", err)
		}

		// Cast to target schema to handle evolution
		castedRecRaw, err := s.castRecordToSchema(toWrite, targetSchema)
		if err != nil {
			if sliced {
				toWrite.Release()
			}
			filteredRec.Release()
			s.logger.Error("Failed to cast record in pipeline", zap.Error(err))
			continue
		}

		// Deep copy for IPC safety
		castedRec, err := s.deepCopyRecordBatch(castedRecRaw)
		castedRecRaw.Release()
		if err != nil {
			if sliced {
				toWrite.Release()
			}
			filteredRec.Release()
			s.logger.Error("Failed to deep copy record in pipeline", zap.Error(err))
			continue
		}

		if castedRec.NumRows() == 0 {
			castedRec.Release()
			if sliced {
				toWrite.Release()
			}
			filteredRec.Release()
			continue
		}

		if err := w.Write(castedRec); err != nil {
			castedRec.Release()
			if sliced {
				toWrite.Release()
			}
			filteredRec.Release()
			return rowsSent, err
		}
		rowsSent += toWrite.NumRows()
		s.incrementPipelineBatches(1)
		castedRec.Release()

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

// filterRecordOptimized uses column index for equality filters when available
// Falls back to Arrow compute for non-indexed columns or non-equality operators
func (s *VectorStore) filterRecordOptimized(ctx context.Context, datasetName string, rec arrow.RecordBatch, batchIdx int, filters []Filter) (arrow.RecordBatch, error) {
	if len(filters) == 0 {
		rec.Retain()
		return rec, nil
	}

	// Check if we can use index for any equality filters
	var indexableFilters []Filter
	var remainingFilters []Filter

	for _, f := range filters {
		if f.Operator == "=" && s.columnIndex != nil && s.columnIndex.HasIndex(datasetName, f.Field) {
			indexableFilters = append(indexableFilters, f)
		} else {
			remainingFilters = append(remainingFilters, f)
		}
	}

	// If no indexable filters, use standard filterRecord
	if len(indexableFilters) == 0 {
		return s.filterRecord(ctx, rec, filters)
	}

	// Use index to get matching row indices for each filter
	var finalMask *array.Boolean
	for _, f := range indexableFilters {
		// Build mask for this filter using the index
		mask := s.columnIndex.BuildFilterMask(datasetName, batchIdx, f.Field, f.Value, int(rec.NumRows()), s.mem)
		if mask == nil {
			// No index data, fall back to full filter
			if finalMask != nil {
				finalMask.Release()
			}
			return s.filterRecord(ctx, rec, filters)
		}

		if finalMask == nil {
			finalMask = mask
		} else {
			// AND the masks together
			andRes, err := compute.CallFunction(ctx, "and", nil, compute.NewDatum(finalMask.Data()), compute.NewDatum(mask.Data()))
			finalMask.Release()
			mask.Release()
			if err != nil {
				return nil, fmt.Errorf("and masks: %w", err)
			}
			finalMask = andRes.(*compute.ArrayDatum).MakeArray().(*array.Boolean)
		}
	}

	// Apply indexed filter
	filterRes, err := compute.CallFunction(ctx, "filter", nil, compute.NewDatum(rec), compute.NewDatum(finalMask.Data()))
	finalMask.Release()
	if err != nil {
		return nil, fmt.Errorf("apply index filter: %w", err)
	}
	filteredRec := filterRes.(*compute.RecordDatum).Value

	// Apply remaining filters using standard method
	if len(remainingFilters) > 0 {
		finalRec, err := s.filterRecord(ctx, filteredRec, remainingFilters)
		filteredRec.Release()
		if err != nil {
			return nil, err
		}
		return finalRec, nil
	}

	return filteredRec, nil
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
