package store

import (
	"context"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// runIngestionWorkerWithCtx consumes batches from the ingestion pipeline and applies them to memory/index.
func (s *VectorStore) runIngestionWorkerWithCtx(ctx context.Context) {
	// workerWg is handled by the caller (StartIngestionWorkers)

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
		case <-ctx.Done():
			return
		default:
			// Granular Backpressure: if memory pressure is extreme, throttle ingestion
			tuner := s.tuner.Load()
			if tuner != nil && tuner.IsHighPressure() {
				now := time.Now().Unix()
				lastLog := s.lastThrottlingLogTime.Load()
				if now-lastLog >= 5 {
					if s.lastThrottlingLogTime.CompareAndSwap(lastLog, now) {
						s.logger.Warn().Msg("High memory pressure detected, throttling ingestion worker")
					}
				}
				timer.Reset(200 * time.Millisecond)
				select {
				case <-s.stopChan:
					return
				case <-ctx.Done():
					return
				case <-timer.C:
					// Wait and continue
				}
			}
		}

		job, ok := s.ingestionQueue.Pop()
		if !ok {
			// Backoff if empty
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(1 * time.Millisecond)

			select {
			case <-s.stopChan:
				return
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			continue
		}

		// Found job
		start := time.Now()

		// Update metrics
		metrics.IngestionQueueDepth.Set(float64(s.ingestionQueue.Len()))

		// Apply to memory
		if err := s.applyBatchToMemory(job.DS, job.Batch, job.TS); err != nil {
			s.logger.Error().Err(err).Str("dataset", job.DS.Name).Msg("Failed to apply batch from ingestion queue")
		}

		// Update metrics (time since enqueued)
		metrics.IngestionQueueLatency.Observe(time.Since(start).Seconds())

		// Decrement Lag
		metrics.IngestionLagCount.Sub(float64(job.Batch.NumRows()))

		// Decrement PendingIngestion counter
		job.DS.PendingIngestion.Add(-1)

		// Release the retained batch from DoPut
		job.Batch.Release()
	}
}
