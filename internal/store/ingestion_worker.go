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
		case job := <-s.ingestionQueue:
			start := time.Now()

			// Update metrics
			metrics.IngestionQueueDepth.Set(float64(len(s.ingestionQueue)))
			metrics.IngestionQueueLatency.Observe(time.Since(start).Seconds()) // Ideally this tracks time since enqueue, but we don't store enqueue time yet.
			// For simplified MVP, this measures processing time.
			// To check latency in queue, we need EnqueueTime in ingestionJob.

			// Decrement Lag
			metrics.IngestionLagCount.Sub(float64(job.batch.NumRows()))

			// Apply to memory
			if err := s.applyBatchToMemory(job.datasetName, job.batch); err != nil {
				s.logger.Error().Err(err).Str("dataset", job.datasetName).Msg("Failed to apply batch from ingestion queue")
			}

			// Release the retained batch from DoPut
			job.batch.Release()
		}
	}
}
