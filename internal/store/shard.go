package store

import (
"hash/fnv"
"sync"
)

const numShards = 16

// shard represents a single segment of the sharded map
type shard struct {
mu   sync.RWMutex
data map[string]*Dataset
}

// ShardedMap provides concurrent access to datasets via sharding
type ShardedMap struct {
shards [numShards]*shard
}

// NewShardedMap creates a new sharded map
func NewShardedMap() *ShardedMap {
sm := &ShardedMap{}
for i := 0; i < numShards; i++ {
sm.shards[i] = &shard{
data: make(map[string]*Dataset),
}
}
return sm
}

// getShard returns the shard for a given key
func (sm *ShardedMap) getShard(key string) *shard {
h := fnv.New32a()
_, _ = h.Write([]byte(key))
return sm.shards[h.Sum32()%numShards]
}

// Get retrieves a dataset by name (read lock on single shard)
func (sm *ShardedMap) Get(name string) (*Dataset, bool) {
s := sm.getShard(name)
s.mu.RLock()
defer s.mu.RUnlock()
ds, ok := s.data[name]
return ds, ok
}

// Set stores a dataset (write lock on single shard)
func (sm *ShardedMap) Set(name string, ds *Dataset) {
s := sm.getShard(name)
s.mu.Lock()
defer s.mu.Unlock()
s.data[name] = ds
}

// Delete removes a dataset (write lock on single shard)
func (sm *ShardedMap) Delete(name string) {
s := sm.getShard(name)
s.mu.Lock()
defer s.mu.Unlock()
delete(s.data, name)
}

// GetOrCreate atomically gets or creates a dataset
func (sm *ShardedMap) GetOrCreate(name string, create func() *Dataset) *Dataset {
s := sm.getShard(name)

// Try read first
s.mu.RLock()
if ds, ok := s.data[name]; ok {
s.mu.RUnlock()
return ds
}
s.mu.RUnlock()

// Upgrade to write lock
s.mu.Lock()
defer s.mu.Unlock()

// Double-check after acquiring write lock
if ds, ok := s.data[name]; ok {
return ds
}

ds := create()
s.data[name] = ds
return ds
}

// Len returns total count across all shards (acquires all read locks)
func (sm *ShardedMap) Len() int {
count := 0
for i := 0; i < numShards; i++ {
sm.shards[i].mu.RLock()
count += len(sm.shards[i].data)
sm.shards[i].mu.RUnlock()
}
return count
}

// Range iterates over all datasets (acquires read locks per shard)
// The callback should not hold references to the dataset after returning
func (sm *ShardedMap) Range(fn func(name string, ds *Dataset) bool) {
for i := 0; i < numShards; i++ {
sm.shards[i].mu.RLock()
for name, ds := range sm.shards[i].data {
if !fn(name, ds) {
sm.shards[i].mu.RUnlock()
return
}
}
sm.shards[i].mu.RUnlock()
}
}

// RangeWithLock iterates with write lock (for modifications)
func (sm *ShardedMap) RangeWithLock(fn func(name string, ds *Dataset, deleteFn func())) {
for i := 0; i < numShards; i++ {
sm.shards[i].mu.Lock()
for name, ds := range sm.shards[i].data {
deleteFn := func() {
delete(sm.shards[i].data, name)
}
fn(name, ds, deleteFn)
}
sm.shards[i].mu.Unlock()
}
}

// Keys returns all dataset names
func (sm *ShardedMap) Keys() []string {
var keys []string
for i := 0; i < numShards; i++ {
sm.shards[i].mu.RLock()
for name := range sm.shards[i].data {
keys = append(keys, name)
}
sm.shards[i].mu.RUnlock()
}
return keys
}

// WithLock executes fn with write lock on the shard containing name
func (sm *ShardedMap) WithLock(name string, fn func(data map[string]*Dataset)) {
s := sm.getShard(name)
s.mu.Lock()
defer s.mu.Unlock()
fn(s.data)
}

// WithRLock executes fn with read lock on the shard containing name  
func (sm *ShardedMap) WithRLock(name string, fn func(data map[string]*Dataset)) {
s := sm.getShard(name)
s.mu.RLock()
defer s.mu.RUnlock()
fn(s.data)
}
