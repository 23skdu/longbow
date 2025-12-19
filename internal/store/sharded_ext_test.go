package store

import (
"testing"
"time"
)

// ShardedDataset coverage extension tests

func TestShardedDataset_LastAccess(t *testing.T) {
ds := NewShardedDatasetDefault("test")

before := time.Now()
ds.SetLastAccess(before)

got := ds.LastAccess()
if !got.Equal(before) {
t.Errorf("LastAccess() = %v, want %v", got, before)
}
}

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

func TestShardedDataset_IndexGetSet(t *testing.T) {
ds := NewShardedDatasetDefault("test")

if ds.Index() != nil {
t.Error("initial Index() should be nil")
}

idx := &HNSWIndex{dims: 128}
ds.SetIndex(idx)

got := ds.Index()
if got != idx {
t.Error("Index() should return the set index")
}
}

func TestShardedDataset_Clear(t *testing.T) {
ds := NewShardedDatasetDefault("test")
ds.Clear()
if ds.TotalRecords() != 0 {
t.Errorf("TotalRecords() after Clear = %d, want 0", ds.TotalRecords())
}
}

// ShardedRWMutex coverage extension tests

func TestShardedRWMutex_LockShard(t *testing.T) {
m := NewShardedRWMutexDefault()
m.LockShard(0)
m.UnlockShard(0)
m.LockShard(1)
m.UnlockShard(1)
}

func TestShardedRWMutex_RLockShard(t *testing.T) {
m := NewShardedRWMutexDefault()
m.RLockShard(0)
m.RLockShard(0)
m.RUnlockShard(0)
m.RUnlockShard(0)
}

func TestShardedRWMutex_LockAll(t *testing.T) {
m := NewShardedRWMutexDefault()
m.LockAll()
m.UnlockAll()
m.Lock(123)
m.Unlock(123)
}

func TestShardedRWMutex_RLockAll(t *testing.T) {
m := NewShardedRWMutexDefault()
m.RLockAll()
m.RUnlockAll()
m.RLock(456)
}

