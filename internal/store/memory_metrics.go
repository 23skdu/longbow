package store

import (
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// startMemoryMetricsUpdater starts a goroutine that periodically updates memory metrics
func (s *VectorStore) startMemoryMetricsUpdater() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-s.stopChan:
				return
			case <-ticker.C:
				current := s.currentMemory.Load()
				limit := s.maxMemory.Load()

				metrics.MemoryCurrentBytes.Set(float64(current))
				metrics.MemoryLimitBytes.Set(float64(limit))

				if limit > 0 {
					utilization := float64(current) / float64(limit)
					metrics.MemoryUtilization.Set(utilization)
				}
			}
		}
	}()
}
