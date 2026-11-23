package engine

// lockDatasetForWrite waits for a dataset write lock while publishing the
// contended waiter to the autoscaler busy signal. The counter is decremented
// as soon as the lock is acquired, so it only reflects threads still queued.
func (s *VectorStore) lockDatasetForWrite(ds *Dataset) {
	s.datasetLockWaiters.Add(1)
	ds.dataMu.Lock()
	s.datasetLockWaiters.Add(-1)
}

// unlockDatasetForWrite releases a dataset write lock acquired with
// lockDatasetForWrite.
func (s *VectorStore) unlockDatasetForWrite(ds *Dataset) {
	ds.dataMu.Unlock()
}

// lockDatasetForRead waits for a dataset read lock while publishing the
// contended waiter to the autoscaler busy signal.
func (s *VectorStore) lockDatasetForRead(ds *Dataset) {
	s.datasetLockWaiters.Add(1)
	ds.dataMu.RLock()
	s.datasetLockWaiters.Add(-1)
}

// unlockDatasetForRead releases a dataset read lock acquired with
// lockDatasetForRead.
func (s *VectorStore) unlockDatasetForRead(ds *Dataset) {
	ds.dataMu.RUnlock()
}
