package store

// CanScaleDownIngestion reports whether the ingestion worker pool may shed one
// worker. Scaling down is blocked while pending work exists, while waiters are
// queued on a dataset lock, or while the pool is already at/below the
// configured minimum (hysteresis).
func CanScaleDownIngestion(pendingJobs, lockWaiters int64, currentWorkers, minWorkers int) bool {
	return canScaleDown(pendingJobs, lockWaiters, currentWorkers, minWorkers)
}

// CanScaleDownIndexing reports whether the indexing worker pool may shed one
// worker, applying the same hysteresis as ingestion.
func CanScaleDownIndexing(pendingJobs, lockWaiters int64, currentWorkers, minWorkers int) bool {
	return canScaleDown(pendingJobs, lockWaiters, currentWorkers, minWorkers)
}

// canScaleDown implements the shared hysteresis policy. A lock waiter is
// treated as pending work because a convoy on a shared dataset can stall while
// the conventional job counters are momentarily empty.
func canScaleDown(pendingJobs, lockWaiters int64, currentWorkers, minWorkers int) bool {
	if pendingJobs > 0 || lockWaiters > 0 {
		return false
	}
	if currentWorkers <= minWorkers {
		return false
	}
	return true
}
