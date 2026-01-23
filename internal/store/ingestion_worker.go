package store

import (
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// runIngestionWorker consumes batches from the ingestion pipeline and applies them to memory/index.
func (s *VectorStore) runIngestionWorker() {
	defer s.workerWg.Done()

	// Create reusable timer for backoff (stopped initially)
	timer := time.NewTimer(0)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	defer timer.Stop()

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
			// Use reusable timer to avoid allocation churn
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(50 * time.Microsecond)

			select {
			case <-s.stopChan:
				return
			case <-timer.C:
			}
			continue
		}

		// Found job
		start := time.Now()

		// Update metrics
		metrics.IngestionQueueDepth.Set(float64(s.ingestionQueue.Len()))
		metrics.IngestionQueueLatency.Observe(time.Since(start).Seconds())

		// Decrement Lag
		metrics.IngestionLagCount.Sub(float64(job.batch.NumRows()))

		// Apply to memory
		if err := s.applyBatchToMemory(job.ds, job.batch, job.ts); err != nil {
			s.logger.Error().Err(err).Str("dataset", job.ds.Name).Msg("Failed to apply batch from ingestion queue")
		}

		// Decrement PendingIngestion counter
		job.ds.PendingIngestion.Add(-1)

		// Release the retained batch from DoPut
		job.batch.Release()
	}
}
