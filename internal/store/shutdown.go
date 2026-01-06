package store

import (
	"context"
	"sync/atomic"
	"time"
)

// shutdown state constants
const (
	stateRunning  int32 = 0
	stateShutdown int32 = 1
)

// Close performs a graceful shutdown with a default timeout.
// It is an alias for Shutdown(context.Background()) but handy for defer.
func (s *VectorStore) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.Shutdown(ctx)
}

// Shutdown performs a graceful shutdown of the VectorStore.
// It stops accepting new requests, drains the index queue, flushes the WAL,
// and closes all file handles. The context controls the shutdown timeout.
func (s *VectorStore) Shutdown(ctx context.Context) error {
	// Idempotent shutdown - only first call does work
	if !atomic.CompareAndSwapInt32(&s.shutdownState, stateRunning, stateShutdown) {
		s.logger.Info().Msg("Shutdown already in progress or completed")
		return nil
	}

	s.logger.Info().Msg("Starting graceful shutdown...")
	start := time.Now()
	var shutdownErr error

	// Step 1: Signal background workers to stop
	s.logger.Info().Msg("Signaling background workers to stop")
	close(s.stopChan)

	s.stopCompaction()
	// Step 2: Drain the index queue
	s.logger.Info().Msg("Draining index queue...")
	if err := s.drainIndexQueue(ctx); err != nil {
		s.logger.Error().Err(err).Msg("Failed to drain index queue")
		shutdownErr = NewShutdownError("drain", "index_queue", err)
	} else {
		s.logger.Info().Msg("Index queue drained successfully")
	}

	// 2. Stop WAL Batcher and Persistence
	if err := s.ClosePersistence(); err != nil {
		shutdownErr = NewShutdownError("close", "persistence", err)
		s.logger.Error().Err(err).Msg("Failed to close persistence")
	}

	// Step 4: Create final snapshot before closing
	select {
	case <-ctx.Done():
		s.logger.Warn().Msg("Shutdown timeout, skipping final snapshot")
	default:
		s.logger.Info().Msg("Creating final snapshot...")
		if err := s.Snapshot(); err != nil {
			s.logger.Error().Err(err).Msg("Failed to create final snapshot")
			// Don't fail shutdown for snapshot errors
		} else {
			s.logger.Info().Msg("Final snapshot created successfully")
			// Truncate logic is handled by Snapshot/Engine
		}
	}

	// Step 5: Close WAL file handle
	// Handled by ClosePersistence
	// Step 5: Close WAL file handle
	// Handled by engine

	// Step 6: Wait for workers to finish
	s.logger.Info().Msg("Waiting for workers to finish...")
	done := make(chan struct{})
	go func() {
		s.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info().Msg("All workers finished")
	case <-ctx.Done():
		s.logger.Warn().Err(ctx.Err()).Msg("Shutdown timeout waiting for workers")
		if shutdownErr == nil {
			shutdownErr = ctx.Err()
		}
	}

	elapsed := time.Since(start)
	if shutdownErr != nil {
		s.logger.Error().
			Dur("elapsed", elapsed).
			Err(shutdownErr).
			Msg("Graceful shutdown completed with errors")
	} else {
		s.logger.Info().Dur("elapsed", elapsed).Msg("Graceful shutdown completed successfully")
	}

	return shutdownErr
}

// drainIndexQueue closes the index channel and waits for pending jobs to complete
func (s *VectorStore) drainIndexQueue(ctx context.Context) error {
	// Close channel to signal workers to stop accepting new jobs
	s.indexQueue.Stop()

	// Wait for index workers to finish with timeout
	done := make(chan struct{})
	go func() {
		s.indexWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// isShutdown returns true if Shutdown has been called
func (s *VectorStore) isShutdown() bool {
	return atomic.LoadInt32(&s.shutdownState) == stateShutdown
}
