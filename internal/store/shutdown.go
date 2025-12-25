package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

// shutdown state constants
const (
	stateRunning  int32 = 0
	stateShutdown int32 = 1
)

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

	// Step 3: Flush and stop WAL batcher
	s.logger.Info().Msg("Flushing WAL batcher...")
	if s.walBatcher != nil {
		if err := s.walBatcher.Stop(); err != nil {
			s.logger.Error().Err(err).Msg("Failed to stop WAL batcher")
			if shutdownErr == nil {
				shutdownErr = NewShutdownError("stop", "WAL_batcher", err)
			}
		} else {
			s.logger.Info().Msg("WAL batcher stopped successfully")
		}
		s.walBatcher = nil
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
			// Truncate WAL after successful snapshot
			if err := s.TruncateWAL(); err != nil {
				s.logger.Error().Err(err).Msg("Failed to truncate WAL")
			} else {
				s.logger.Info().Msg("WAL truncated successfully")
			}
		}
	}

	// Step 5: Close WAL file handle
	s.walMu.Lock()
	if s.walFile != nil {
		s.logger.Info().Msg("Closing WAL file...")
		if err := s.walFile.Sync(); err != nil {
			s.logger.Error().Err(err).Msg("Failed to sync WAL")
		}
		if err := s.walFile.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Failed to close WAL")
			if shutdownErr == nil {
				shutdownErr = NewShutdownError("close", "WAL", err)
			}
		} else {
			s.logger.Info().Msg("WAL file closed successfully")
		}
		s.walFile = nil
	}
	s.walMu.Unlock()

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

// TruncateWAL truncates the WAL file after a successful snapshot.
// This should only be called after SaveSnapshot completes successfully.
func (s *VectorStore) TruncateWAL() error {
	s.walMu.Lock()
	defer s.walMu.Unlock()

	walPath := filepath.Join(s.dataPath, "wal.bin")

	// If WAL file is open, close it first
	if s.walFile != nil {
		if err := s.walFile.Sync(); err != nil {
			return NewShutdownError("truncate", "WAL", fmt.Errorf("sync: %w", err))
		}
		if err := s.walFile.Close(); err != nil {
			return NewShutdownError("truncate", "WAL", fmt.Errorf("close: %w", err))
		}
		s.walFile = nil
	}

	// Truncate by creating empty file
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return NewShutdownError("truncate", "WAL", err)
	}
	if err := f.Close(); err != nil {
		return NewShutdownError("truncate", "WAL", fmt.Errorf("close after truncate: %w", err))
	}

	s.logger.Info().Str("path", walPath).Msg("WAL truncated")
	return nil
}

// isShutdown returns true if Shutdown has been called
func (s *VectorStore) isShutdown() bool {
	return atomic.LoadInt32(&s.shutdownState) == stateShutdown
}
