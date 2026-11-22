package store

// CanScaleDownIngestion reports whether the ingestion worker pool may shed one
// worker. Scaling down is blocked while pending work exists or while the pool
// is already at/below the configured minimum (hysteresis), preventing the
// auto-scaler from draining workers under sustained load and stalling forward
// progress.
func CanScaleDownIngestion(pendingJobs int64, currentWorkers, minWorkers int) bool {
	if pendingJobs > 0 {
		return false
	}
	if currentWorkers <= minWorkers {
		return false
	}
	return true
}

// CanScaleDownIndexing reports whether the indexing worker pool may shed one
// worker, applying the same hysteresis as ingestion.
func CanScaleDownIndexing(pendingJobs int64, currentWorkers, minWorkers int) bool {
	if pendingJobs > 0 {
		return false
	}
	if currentWorkers <= minWorkers {
		return false
	}
	return true
}
