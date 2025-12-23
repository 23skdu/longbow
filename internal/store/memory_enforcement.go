package store

import (
	"fmt"

	"github.com/23skdu/longbow/internal/metrics"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// checkMemoryBeforeWrite verifies memory is available before accepting a write.
// Returns error if memory limit would be exceeded.
func (s *VectorStore) checkMemoryBeforeWrite(estimatedSize int64) error {
	current := s.currentMemory.Load()
	limit := s.maxMemory.Load()

	// Check if write would exceed limit
	if current+estimatedSize > limit {
		// Attempt eviction if configured
		if !s.memoryConfig.RejectWrites {
			targetMemory := limit - estimatedSize
			if targetMemory < 0 {
				targetMemory = 0
			}

			err := s.evictToTarget(targetMemory)
			if err != nil {
				metrics.MemoryLimitRejects.Inc()
				return status.Errorf(codes.ResourceExhausted,
					"memory limit exceeded and eviction failed: %d/%d bytes used (write size: %d)",
					current, limit, estimatedSize)
			}
			metrics.MemoryEvictionsTriggered.Inc()
			return nil
		}

		// Reject write
		metrics.MemoryLimitRejects.Inc()
		return status.Errorf(codes.ResourceExhausted,
			"memory limit exceeded: %d/%d bytes used (write size: %d)",
			current, limit, estimatedSize)
	}

	// Check if we're approaching limit (within headroom)
	threshold := s.memoryConfig.EvictionThreshold()
	if current+estimatedSize > threshold {
		// Proactive eviction
		s.logger.Debug("Approaching memory limit, triggering proactive eviction",
			zap.Int64("current", current),
			zap.Int64("threshold", threshold),
			zap.Int64("limit", limit))

		targetMemory := threshold - estimatedSize
		if targetMemory < 0 {
			targetMemory = 0
		}
		_ = s.evictToTarget(targetMemory) // Best effort
	}

	return nil
}

// evictToTarget evicts datasets until currentMemory <= targetBytes.
// Returns error if unable to free enough space.
func (s *VectorStore) evictToTarget(targetBytes int64) error {
	s.mu.RLock()
	candidates := make([]*Dataset, 0, len(s.datasets))
	for _, ds := range s.datasets {
		candidates = append(candidates, ds)
	}
	s.mu.RUnlock()

	if len(candidates) == 0 {
		return fmt.Errorf("no datasets available for eviction")
	}

	// Sort by eviction policy
	switch s.memoryConfig.EvictionPolicy {
	case "largest":
		// Sort by size descending
		for i := 0; i < len(candidates); i++ {
			for j := i + 1; j < len(candidates); j++ {
				if candidates[i].SizeBytes.Load() < candidates[j].SizeBytes.Load() {
					candidates[i], candidates[j] = candidates[j], candidates[i]
				}
			}
		}
	case "oldest":
		// Sort by creation time ascending (oldest first)
		// Note: Dataset doesn't track creation time, using LastAccess as proxy
		for i := 0; i < len(candidates); i++ {
			for j := i + 1; j < len(candidates); j++ {
				if candidates[i].LastAccess().After(candidates[j].LastAccess()) {
					candidates[i], candidates[j] = candidates[j], candidates[i]
				}
			}
		}
	default: // "lru"
		// Sort by LastAccess ascending (LRU first)
		for i := 0; i < len(candidates); i++ {
			for j := i + 1; j < len(candidates); j++ {
				if candidates[i].LastAccess().After(candidates[j].LastAccess()) {
					candidates[i], candidates[j] = candidates[j], candidates[i]
				}
			}
		}
	}

	// Evict until target reached
	evicted := 0
	for _, ds := range candidates {
		if s.currentMemory.Load() <= targetBytes {
			break
		}
		s.evictDataset(ds)
		evicted++
	}

	// Check if we reached target
	if s.currentMemory.Load() > targetBytes {
		return fmt.Errorf("unable to evict enough memory: %d/%d bytes (target: %d, evicted %d datasets)",
			s.currentMemory.Load(), s.maxMemory.Load(), targetBytes, evicted)
	}

	s.logger.Info("Evicted datasets to meet memory target",
		zap.Int("count", evicted),
		zap.Int64("current_memory", s.currentMemory.Load()),
		zap.Int64("target", targetBytes))

	return nil
}
