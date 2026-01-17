package store

import (
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// runIngestionWorker consumes batches from the ingestion pipeline and applies them to memory/index.
func (s *VectorStore) runIngestionWorker() {
	defer s.workerWg.Done()

	for {
		select {
		case <-s.stopChan:
			return
		default:
		}

		job, ok := s.ingestionQueue.Pop()
		if !ok {
			// Backoff if empty
			// Check stop channel while sleeping
			select {
			case <-s.stopChan:
				return
			case <-time.After(50 * time.Microsecond):
			}
			continue
		}

		// Found job
		s.logger.Debug().Str("dataset", job.datasetName).Int64("rows", job.batch.NumRows()).Msg("IngestionWorker picked up job")
		start := time.Now()

		// Update metrics
		metrics.IngestionQueueDepth.Set(float64(s.ingestionQueue.Len()))
		metrics.IngestionQueueLatency.Observe(time.Since(start).Seconds())

		// Decrement Lag
		metrics.IngestionLagCount.Sub(float64(job.batch.NumRows()))

		// Apply to memory
		if err := s.applyBatchToMemory(job.datasetName, job.batch, job.ts); err != nil {
			s.logger.Error().Err(err).Str("dataset", job.datasetName).Msg("Failed to apply batch from ingestion queue")
		}

		// Decrement PendingIngestion counter
		if ds, ok := s.getDataset(job.datasetName); ok {
			ds.PendingIngestion.Add(-1)
		}

		// Release the retained batch from DoPut
		job.batch.Release()
	}
}
