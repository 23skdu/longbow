package engine

import (
	"testing"
	"time"
)

// ShardedDataset coverage extension tests

// TestShardedDataset_LastAccess runs the test sharded dataset_last access operation.
func TestShardedDataset_LastAccess(t *testing.T) {
	ds := NewShardedDatasetDefault("test")

	before := time.Now()
	ds.SetLastAccess(before)

	got := ds.LastAccess()
	if !got.Equal(before) {
		t.Errorf("LastAccess() = %v, want %v", got, before)
	}
}

// TestShardedDataset_Version runs the test sharded dataset_version operation.
func TestShardedDataset_Version(t *testing.T) {
	ds := NewShardedDatasetDefault("test")

	if ds.Version() != 0 {
		t.Errorf("initial Version() = %d, want 0", ds.Version())
	}

	ds.IncrementVersion()
	if ds.Version() != 1 {
		t.Errorf("Version() after increment = %d, want 1", ds.Version())
	}

	ds.IncrementVersion()
	ds.IncrementVersion()
	if ds.Version() != 3 {
		t.Errorf("Version() after 3 increments = %d, want 3", ds.Version())
	}
}

// TestShardedDataset_IndexGetSet runs the test sharded dataset_index get set operation.
func TestShardedDataset_IndexGetSet(t *testing.T) {
	ds := NewShardedDatasetDefault("test")

	if ds.Index() != nil {
		t.Error("initial Index() should be nil")
	}

	idx := NewTestHNSWIndex(&Dataset{Name: "dummy"})
	ds.SetIndex(idx)

	got := ds.Index()
	if got != idx {
		t.Error("Index() should return the set index")
	}
}

// TestShardedDataset_Clear runs the test sharded dataset_clear operation.
func TestShardedDataset_Clear(t *testing.T) {
	ds := NewShardedDatasetDefault("test")
	ds.Clear()
	if ds.TotalRecords() != 0 {
		t.Errorf("TotalRecords() after Clear = %d, want 0", ds.TotalRecords())
	}
}

// ShardedRWMutex coverage extension tests

// TestShardedRWMutex_LockShard runs the test sharded rw mutex_lock shard operation.
func TestShardedRWMutex_LockShard(t *testing.T) {
	m := NewShardedRWMutexDefault()
	m.LockShard(0)
	m.UnlockShard(0)
	m.LockShard(1)
	m.UnlockShard(1)
}

// TestShardedRWMutex_RLockShard runs the test sharded rw mutex_r lock shard operation.
func TestShardedRWMutex_RLockShard(t *testing.T) {
	m := NewShardedRWMutexDefault()
	m.RLockShard(0)
	m.RLockShard(0)
	m.RUnlockShard(0)
	m.RUnlockShard(0)
}

// TestShardedRWMutex_LockAll runs the test sharded rw mutex_lock all operation.
func TestShardedRWMutex_LockAll(t *testing.T) {
	m := NewShardedRWMutexDefault()
	m.LockAll()
	m.UnlockAll()
	m.Lock(123)
	m.Unlock(123)
}

// TestShardedRWMutex_RLockAll runs the test sharded rw mutex_r lock all operation.
func TestShardedRWMutex_RLockAll(t *testing.T) {
	m := NewShardedRWMutexDefault()
	m.RLockAll()
	m.RUnlockAll()
	m.RLock(456)
}
