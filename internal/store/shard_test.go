package store

import (
"fmt"
"sync"
"testing"
"time"

"github.com/stretchr/testify/assert"
)

func TestShardedMap_BasicOperations(t *testing.T) {
sm := NewShardedMap()

// Test Set and Get
ds := &Dataset{lastAccess: time.Now().UnixNano()}
sm.Set("test1", ds)

got, ok := sm.Get("test1")
assert.True(t, ok)
assert.Equal(t, ds, got)

// Test Get non-existent
_, ok = sm.Get("nonexistent")
assert.False(t, ok)

// Test Delete
sm.Delete("test1")
_, ok = sm.Get("test1")
assert.False(t, ok)
}

func TestShardedMap_GetOrCreate(t *testing.T) {
sm := NewShardedMap()
createCount := 0

creator := func() *Dataset {
createCount++
return &Dataset{lastAccess: time.Now().UnixNano()}
}

// First call should create
ds1 := sm.GetOrCreate("test", creator)
assert.Equal(t, 1, createCount)
assert.NotNil(t, ds1)

// Second call should return existing
ds2 := sm.GetOrCreate("test", creator)
assert.Equal(t, 1, createCount) // Should not increment
assert.Equal(t, ds1, ds2)
}

func TestShardedMap_Len(t *testing.T) {
sm := NewShardedMap()
assert.Equal(t, 0, sm.Len())

for i := 0; i < 100; i++ {
sm.Set(fmt.Sprintf("key%d", i), &Dataset{})
}
assert.Equal(t, 100, sm.Len())
}

func TestShardedMap_Keys(t *testing.T) {
sm := NewShardedMap()
expected := map[string]bool{}

for i := 0; i < 50; i++ {
key := fmt.Sprintf("key%d", i)
sm.Set(key, &Dataset{})
expected[key] = true
}

keys := sm.Keys()
assert.Equal(t, 50, len(keys))

for _, k := range keys {
assert.True(t, expected[k], "Unexpected key: %s", k)
}
}

func TestShardedMap_Range(t *testing.T) {
sm := NewShardedMap()
for i := 0; i < 10; i++ {
sm.Set(fmt.Sprintf("key%d", i), &Dataset{})
}

count := 0
sm.Range(func(name string, ds *Dataset) bool {
count++
return true
})
assert.Equal(t, 10, count)

// Test early termination
count = 0
sm.Range(func(name string, ds *Dataset) bool {
count++
return count < 5
})
assert.Equal(t, 5, count)
}

func TestShardedMap_ConcurrentAccess(t *testing.T) {
sm := NewShardedMap()
var wg sync.WaitGroup
numGoroutines := 100
iterations := 1000

// Concurrent writes to different keys
for i := 0; i < numGoroutines; i++ {
wg.Add(1)
go func(id int) {
defer wg.Done()
for j := 0; j < iterations; j++ {
key := fmt.Sprintf("key%d_%d", id, j)
sm.Set(key, &Dataset{})
_, _ = sm.Get(key)
}
}(i)
}

wg.Wait()
assert.Equal(t, numGoroutines*iterations, sm.Len())
}

func TestShardedMap_ConcurrentReadWrite(t *testing.T) {
sm := NewShardedMap()
var wg sync.WaitGroup

// Pre-populate
for i := 0; i < 100; i++ {
sm.Set(fmt.Sprintf("key%d", i), &Dataset{})
}

// Concurrent reads and writes
for i := 0; i < 50; i++ {
wg.Add(2)
// Reader
go func(id int) {
defer wg.Done()
for j := 0; j < 1000; j++ {
key := fmt.Sprintf("key%d", id%100)
_, _ = sm.Get(key)
}
}(i)
// Writer
go func(id int) {
defer wg.Done()
for j := 0; j < 100; j++ {
key := fmt.Sprintf("new_key%d_%d", id, j)
sm.Set(key, &Dataset{})
}
}(i)
}

wg.Wait()
// Should complete without race conditions
assert.True(t, sm.Len() >= 100)
}

func TestShardedMap_Distribution(t *testing.T) {
sm := NewShardedMap()

// Add many keys to check distribution across shards
for i := 0; i < 1600; i++ {
sm.Set(fmt.Sprintf("dataset_%d", i), &Dataset{})
}

// Check that keys are distributed (not all in one shard)
shardCounts := make([]int, numShards)
for i := 0; i < numShards; i++ {
sm.shards[i].mu.RLock()
shardCounts[i] = len(sm.shards[i].data)
sm.shards[i].mu.RUnlock()
}

// Verify reasonable distribution (no shard should have all or none)
for i, count := range shardCounts {
assert.True(t, count > 0, "Shard %d is empty", i)
assert.True(t, count < 1600, "Shard %d has all keys", i)
}
}
