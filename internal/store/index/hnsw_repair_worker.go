package core

import (
	"context"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
)

// StartRepairWorker starts a background worker that periodically repairs tombstones.
func (h *ArrowHNSW) StartRepairWorker(ctx context.Context, interval time.Duration, batchSize int) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Run repair
				count := h.RepairTombstones(ctx, batchSize)
				if count > 0 {
					metrics.HnswRepairSuccessTotal.Inc()
					metrics.HnswRepairNodesVisitedTotal.Add(float64(count))
				} else if count < 0 {
					metrics.HnswRepairFailureTotal.Inc()
				}
			}
		}
	}()
}
