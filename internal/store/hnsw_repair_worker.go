package store

import (
	"context"
	"time"
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
					// Log or metric already handled in RepairTombstones (placeholder for future use)
					_ = count
				}
			}
		}
	}()
}
