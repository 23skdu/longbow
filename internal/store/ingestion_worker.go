package store

import (
	"context"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// safeProcessBatch applies a batch to memory with panic recovery, ensuring PendingIngestion
// is always decremented and the batch is always released.
func safeProcessBatch(s *VectorStore, job IngestionJob, start time.Time) {
	var processed bool
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error().
				Interface("panic", r).
				Str("dataset", job.DS.Name).
				Msg("Panic in applyBatchToMemory, recovering and decrementing PendingIngestion")
		}
		// Always decrement PendingIngestion, even on panic
		job.DS.PendingIngestion.Add(-1)
		// Record last ingestion completion time for watchdog
		job.DS.LastIngestionCompletion.Store(time.Now().Unix())
		// Publish updated boundaries to the mesh
		s.PublishIndexBoundaries()
		// Release the retained batch from DoPut
		job.Batch.Release()
		_ = processed // suppress unused warning if needed
	}()

	if err := s.applyBatchToMemory(job.DS, job.Batch, job.TS); err != nil {
		s.logger.Error().Err(err).Str("dataset", job.DS.Name).Msg("Failed to apply batch from ingestion queue")
	}

	// Update metrics (time since enqueued)
	metrics.IngestionQueueLatency.Observe(time.Since(start).Seconds())

	// Decrement Lag
	metrics.IngestionLagCount.Sub(float64(job.Batch.NumRows()))

	processed = true
}

// runIngestionWorkerWithCtx consumes batches from the ingestion pipeline and applies them to memory/index.
func (s *VectorStore) runIngestionWorkerWithCtx(ctx context.Context) {
	// workerWg is handled by the caller (StartIngestionWorkers)

	// idleBackoff is only armed while the queue is empty. Its period is a
	// safety net for a missed notification, not the primary wakeup: the
	// queue signals consumers directly (see IngestionRingBuffer.Notify).
	idleBackoff := time.NewTimer(time.Hour)
	if !idleBackoff.Stop() {
		select {
		case <-idleBackoff.C:
		default:
		}
	}
	defer idleBackoff.Stop()

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
				if !idleBackoff.Stop() {
					select {
					case <-idleBackoff.C:
					default:
					}
				}
				idleBackoff.Reset(200 * time.Millisecond)
				select {
				case <-s.stopChan:
					return
				case <-ctx.Done():
					return
				case <-idleBackoff.C:
					// Wait and continue
				}
			}
		}

		job, ok := s.ingestionQueue.Pop()
		if !ok {
			// Queue is empty: sleep until a producer publishes something.
			// The long backoff only fires if the notification was lost, which
			// keeps the worker from parking forever at the cost of one timer
			// per idle period instead of one per 1 ms poll.
			if !idleBackoff.Stop() {
				select {
				case <-idleBackoff.C:
				default:
				}
			}
			idleBackoff.Reset(time.Hour)

			select {
			case <-s.stopChan:
				return
			case <-ctx.Done():
				return
			case <-s.ingestionQueue.Notify():
				continue
			case <-idleBackoff.C:
				continue
			}
		}

		// Found job
		start := time.Now()

		// Update metrics
		metrics.IngestionQueueDepth.Set(float64(s.ingestionQueue.Len()))

		// Process batch with panic recovery to ensure PendingIngestion is always decremented
		safeProcessBatch(s, job, start)
	}
}
