package store

import (
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// runPersistenceWorker runs in the background to handle WAL writes asynchronously.
// It drains the persistenceQueue and writes batches to the WAL.
func (s *VectorStore) runPersistenceWorker() {
	defer s.workerWg.Done()

	s.logger.Info().Msg("Persistence worker started")

	// Batching loop
	for {
		select {
		case <-s.stopChan:
			// Drain remaining jobs before exiting?
			// For strictly durability, we should try to drain.
			s.drainPersistenceQueue()
			return

		case job := <-s.persistenceQueue:
			s.handlePersistenceJob(job)
		}
	}
}

// drainPersistenceQueue attempts to flush remaining jobs on shutdown
func (s *VectorStore) drainPersistenceQueue() {
	s.logger.Info().Msg("Draining persistence queue...")
	count := 0
	for {
		select {
		case job := <-s.persistenceQueue:
			s.handlePersistenceJob(job)
			count++
		default:
			s.logger.Info().Int("count", count).Msg("Persistence queue drained")
			return
		}
	}
}

func (s *VectorStore) handlePersistenceJob(job persistenceJob) {
	// Ensure we release the retained record eventually
	defer job.batch.Release()

	if s.engine == nil {
		return
	}

	start := time.Now()

	// Assign Sequence Number here (Linearization point)
	seq := s.sequence.Add(1)

	// Write to WAL
	if err := s.engine.WriteToWAL(job.datasetName, job.batch, seq, job.ts); err != nil {
		s.logger.Error().Err(err).
			Str("dataset", job.datasetName).
			Uint64("seq", seq).
			Msg("Async persistence failed")
		// In a real system, we might retry or halt.
		// For now, metric and log.
		metrics.WALWriteErrors.Inc()
	} else {
		metrics.WALWriteDuration.Observe(time.Since(start).Seconds())
	}
}
