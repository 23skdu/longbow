package store

import (
"runtime"
"sync"
"testing"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/memory"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
)

// Test 1: NewHNSWIndexWithCapacity should pre-allocate locations slice
func TestNewHNSWIndexWithCapacity(t *testing.T) {
mem := memory.NewGoAllocator()
vectors := generateTestVectors(10, 4)
rec := makeHNSWTestRecord(mem, 4, vectors)
defer rec.Release()

ds := &Dataset{
Records: []arrow.RecordBatch{rec},
mu:      sync.RWMutex{},
}

// Create index with expected capacity of 1000 vectors
idx := NewHNSWIndexWithCapacity(ds, 1000)
require.NotNil(t, idx)

// Locations slice should have capacity >= 1000
assert.GreaterOrEqual(t, cap(idx.locations), 1000, "locations should be pre-allocated")
assert.Equal(t, 0, len(idx.locations), "locations length should start at 0")
}

// Test 2: AddBatchParallel should pre-grow locations before append
func TestAddBatchParallel_PreGrowsLocations(t *testing.T) {
mem := memory.NewGoAllocator()
vectors := generateTestVectors(100, 4)
rec := makeHNSWTestRecord(mem, 4, vectors)
defer rec.Release()

ds := &Dataset{
Records: []arrow.RecordBatch{rec},
mu:      sync.RWMutex{},
}

// Start with small capacity
idx := NewHNSWIndexWithCapacity(ds, 10)
initialCap := cap(idx.locations)

// Add 100 locations - should pre-grow, not multiple re-allocations
locations := make([]Location, 100)
for i := 0; i < 100; i++ {
locations[i] = Location{BatchIdx: 0, RowIdx: i}
}

err := idx.AddBatchParallel(locations, 4)
require.NoError(t, err)

// After adding 100, capacity should be at least 100
assert.GreaterOrEqual(t, cap(idx.locations), 100)
assert.Equal(t, 100, len(idx.locations))

// Verify it grew efficiently (not too many small re-allocations)
t.Logf("Initial cap: %d, Final cap: %d", initialCap, cap(idx.locations))
}

// Test 3: Float32 slice pool for distance calculations
func TestFloat32SlicePool(t *testing.T) {
pool := NewFloat32SlicePool(128) // 128-dim vectors

// Get slice from pool
slice1 := pool.Get()
require.NotNil(t, slice1)
assert.Equal(t, 128, len(slice1))
assert.GreaterOrEqual(t, cap(slice1), 128)

// Modify and return
slice1[0] = 1.0
slice1[127] = 2.0
pool.Put(slice1)

// Get again - should reuse (slice is zeroed or reused)
slice2 := pool.Get()
require.NotNil(t, slice2)
assert.Equal(t, 128, len(slice2))
}

// Test 4: VectorID slice pool for search results
func TestVectorIDSlicePool(t *testing.T) {
pool := NewVectorIDSlicePool(100) // k=100

slice1 := pool.Get()
require.NotNil(t, slice1)
assert.Equal(t, 100, cap(slice1))

// Use slice
slice1 = append(slice1, VectorID(1), VectorID(2), VectorID(3))
pool.Put(slice1)

// Get again
slice2 := pool.Get()
require.NotNil(t, slice2)
assert.Equal(t, 0, len(slice2), "returned slice should be reset to length 0")
assert.GreaterOrEqual(t, cap(slice2), 100)
}

// Test 5: Location slice pool for batch operations
func TestLocationSlicePool(t *testing.T) {
pool := NewLocationSlicePool(1000)

slice1 := pool.Get()
require.NotNil(t, slice1)
assert.GreaterOrEqual(t, cap(slice1), 1000)

// Fill it
for i := 0; i < 500; i++ {
slice1 = append(slice1, Location{BatchIdx: 0, RowIdx: i})
}
pool.Put(slice1)

// Get again - should be reset
slice2 := pool.Get()
assert.Equal(t, 0, len(slice2))
assert.GreaterOrEqual(t, cap(slice2), 1000)
}

// Test 6: HybridResult slice pool
func TestHybridResultSlicePool(t *testing.T) {
pool := NewHybridResultSlicePool(256)

slice1 := pool.Get()
require.NotNil(t, slice1)
assert.GreaterOrEqual(t, cap(slice1), 256)
assert.Equal(t, 0, len(slice1))

pool.Put(slice1)

slice2 := pool.Get()
assert.GreaterOrEqual(t, cap(slice2), 256)
}

// Test 7: Pool metrics should be tracked
func TestSlicePoolMetrics(t *testing.T) {
pool := NewFloat32SlicePoolWithMetrics(64, "test_float32")

// Multiple get/put cycles
for i := 0; i < 10; i++ {
s := pool.Get()
pool.Put(s)
}

// Verify metrics exist (implementation will expose counters)
stats := pool.Stats()
assert.GreaterOrEqual(t, stats.Gets, int64(10))
assert.GreaterOrEqual(t, stats.Puts, int64(10))
}

// Test 8: Concurrent pool access safety
func TestSlicePoolConcurrentAccess(t *testing.T) {
pool := NewFloat32SlicePool(64)
var wg sync.WaitGroup
numGoroutines := runtime.NumCPU() * 2
opsPerGoroutine := 1000

for i := 0; i < numGoroutines; i++ {
wg.Add(1)
go func() {
defer wg.Done()
for j := 0; j < opsPerGoroutine; j++ {
s := pool.Get()
// Simulate work
s[0] = float32(j)
pool.Put(s)
}
}()
}

wg.Wait()
// If we get here without race/panic, test passes
}

// Test 9: Pre-allocated hybrid pipeline results
func TestHybridPipelinePreAllocation(t *testing.T) {
// Test that filterByThreshold pre-allocates when possible
results := make([]HybridResult, 100)
for i := range results {
results[i] = HybridResult{ID: uint32(i), Score: float64(i) / 100.0}
}

// Filter with threshold - should use pooled slice internally
filtered := filterByThresholdPooled(results, 0.5)
require.NotNil(t, filtered)
assert.LessOrEqual(t, len(filtered), 100)
}

// Test 10: RankedResult slice pool for batch distance
func TestRankedResultSlicePool(t *testing.T) {
pool := NewRankedResultSlicePool(100)

slice := pool.Get()
require.NotNil(t, slice)
assert.GreaterOrEqual(t, cap(slice), 100)

pool.Put(slice)
}
