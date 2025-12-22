package store

import (
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Subtask 4: TDD Red Phase - TTL Per-Record Tests
// ============================================================================

// Test 1: RecordMetadata tracks creation time and TTL
func TestRecordMetadata_TTL(t *testing.T) {
	meta := NewRecordMetadata(100 * time.Millisecond)
	require.NotNil(t, meta)

	// Should not be expired immediately
	assert.False(t, meta.IsExpired(), "should not be expired immediately")

	// Wait for TTL to expire
	time.Sleep(150 * time.Millisecond)
	assert.True(t, meta.IsExpired(), "should be expired after TTL")
}

// Test 2: Zero TTL means no expiration
func TestRecordMetadata_ZeroTTL_NoExpiration(t *testing.T) {
	meta := NewRecordMetadata(0) // No TTL
	require.NotNil(t, meta)

	// Should never expire
	assert.False(t, meta.IsExpired())
	time.Sleep(10 * time.Millisecond)
	assert.False(t, meta.IsExpired(), "zero TTL should never expire")
}

// Test 3: RecordEvictionManager tracks records by pointer identity
func TestRecordEvictionManager_PointerIdentity(t *testing.T) {
	mgr := NewRecordEvictionManager()
	require.NotNil(t, mgr)

	// Create test record
	mem := memory.NewGoAllocator()
	builder := array.NewFloat32Builder(mem)
	builder.AppendValues([]float32{1.0, 2.0}, nil)
	arr := builder.NewArray()
	defer arr.Release() //nolint:gocritic // cleanup needed per iteration

	schema := arrow.NewSchema([]arrow.Field{{Name: "vec", Type: arrow.PrimitiveTypes.Float32}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, 2) //nolint:staticcheck // arrow.Record is internal
	defer rec.Release()                                   //nolint:gocritic // cleanup needed per iteration

	// Register with TTL
	mgr.Register(rec, 1*time.Hour)

	// Get by pointer identity
	meta := mgr.Get(rec)
	require.NotNil(t, meta, "should find metadata by pointer identity")
	assert.Equal(t, 1*time.Hour, meta.TTL)
}

// Test 4: Evict expired records from manager
func TestRecordEvictionManager_EvictExpired(t *testing.T) {
	mgr := NewRecordEvictionManager()

	mem := memory.NewGoAllocator()
	builder := array.NewFloat32Builder(mem)
	builder.AppendValues([]float32{1.0}, nil)
	arr := builder.NewArray()
	defer arr.Release() //nolint:gocritic // cleanup needed per iteration

	schema := arrow.NewSchema([]arrow.Field{{Name: "vec", Type: arrow.PrimitiveTypes.Float32}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, 1) //nolint:staticcheck
	defer rec.Release()                                   //nolint:gocritic // cleanup needed per iteration

	// Register with short TTL
	mgr.Register(rec, 50*time.Millisecond)

	// Should exist initially
	assert.NotNil(t, mgr.Get(rec))

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Evict expired
	expired := mgr.EvictExpired()
	assert.Len(t, expired, 1, "should evict 1 expired record")

	// Should be removed from manager
	assert.Nil(t, mgr.Get(rec), "should be removed after eviction")
}

// ============================================================================
// Subtask 5: TDD Red Phase - LRU Per-Record Tests
// ============================================================================

// Test 5: RecordMetadata tracks last access atomically
func TestRecordMetadata_LRU_LastAccess(t *testing.T) {
	meta := NewRecordMetadata(0)
	initialAccess := meta.GetLastAccess()

	// Record access
	time.Sleep(10 * time.Millisecond)
	meta.RecordAccess()

	assert.True(t, meta.GetLastAccess().After(initialAccess), "access time should be updated")
}

// Test 6: LRU eviction selects oldest accessed records
func TestRecordEvictionManager_LRU_SelectVictims(t *testing.T) {
	mgr := NewRecordEvictionManager()

	mem := memory.NewGoAllocator()
	recs := make([]arrow.Record, 0, 5) //nolint:prealloc,staticcheck

	// Create 5 records with staggered access times
	for i := 0; i < 5; i++ {
		builder := array.NewFloat32Builder(mem)
		builder.AppendValues([]float32{float32(i)}, nil)
		arr := builder.NewArray()
		defer arr.Release() //nolint:gocritic // cleanup needed per iteration

		schema := arrow.NewSchema([]arrow.Field{{Name: "vec", Type: arrow.PrimitiveTypes.Float32}}, nil)
		rec := array.NewRecord(schema, []arrow.Array{arr}, 1) //nolint:staticcheck
		defer rec.Release()                                   //nolint:gocritic // cleanup needed per iteration

		mgr.Register(rec, 0) // No TTL
		recs = append(recs, rec)
		time.Sleep(5 * time.Millisecond)
	}

	// Access recs[3] and recs[4] to make them "newer"
	mgr.Get(recs[3]).RecordAccess()
	mgr.Get(recs[4]).RecordAccess()

	// LRU eviction should select recs[0,1,2] as victims (oldest accessed)
	victims := mgr.SelectLRUVictims(3)
	assert.Len(t, victims, 3, "should select 3 victims")

	// Verify victims are not the recently accessed records
	// The returned pointers should be for recs[0,1,2] which had no recent access
	// We just verify we got 3 unique victims
	victimSet := make(map[uintptr]bool)
	for _, v := range victims {
		victimSet[v] = true
	}
	assert.Len(t, victimSet, 3, "should have 3 unique victims")
}

// ============================================================================
// Subtask 6: TDD Red Phase - LFU Per-Record Tests
// ============================================================================

// Test 7: RecordMetadata tracks access frequency atomically
func TestRecordMetadata_LFU_AccessCount(t *testing.T) {
	meta := NewRecordMetadata(0)
	assert.Equal(t, int64(0), meta.GetAccessCount())

	// Record multiple accesses
	for i := 0; i < 10; i++ {
		meta.RecordAccess()
	}

	assert.Equal(t, int64(10), meta.GetAccessCount(), "access count should be 10")
}

// Test 8: LFU eviction selects least frequently accessed records
func TestRecordEvictionManager_LFU_SelectVictims(t *testing.T) {
	mgr := NewRecordEvictionManager()

	mem := memory.NewGoAllocator()
	recs := make([]arrow.Record, 0, 5) //nolint:prealloc,staticcheck

	// Create 5 records with different access frequencies
	for i := 0; i < 5; i++ {
		builder := array.NewFloat32Builder(mem)
		builder.AppendValues([]float32{float32(i)}, nil)
		arr := builder.NewArray()
		defer arr.Release() //nolint:gocritic // cleanup needed per iteration

		schema := arrow.NewSchema([]arrow.Field{{Name: "vec", Type: arrow.PrimitiveTypes.Float32}}, nil)
		rec := array.NewRecord(schema, []arrow.Array{arr}, 1) //nolint:staticcheck
		defer rec.Release()                                   //nolint:gocritic // cleanup needed per iteration

		mgr.Register(rec, 0)
		recs = append(recs, rec)
	}

	// Access records with different frequencies: recs[3] and recs[4] most frequent
	for i := 0; i < 100; i++ {
		mgr.Get(recs[3]).RecordAccess()
		mgr.Get(recs[4]).RecordAccess()
	}
	for i := 0; i < 10; i++ {
		mgr.Get(recs[2]).RecordAccess()
	}
	// recs[0] and recs[1] have 0 accesses

	// LFU eviction should select recs[0,1] as victims (least frequent)
	victims := mgr.SelectLFUVictims(2)
	assert.Len(t, victims, 2, "should select 2 victims")
}

// Test 9: Concurrent access is safe (zero-lock via atomics)
func TestRecordMetadata_ConcurrentAccess(t *testing.T) {
	meta := NewRecordMetadata(0)

	var wg sync.WaitGroup
	numGoroutines := 100
	accessesPerGoroutine := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < accessesPerGoroutine; j++ {
				meta.RecordAccess()
				_ = meta.GetLastAccess()
				_ = meta.GetAccessCount()
				_ = meta.IsExpired()
			}
		}()
	}

	wg.Wait()

	// All accesses should be counted
	expected := int64(numGoroutines * accessesPerGoroutine)
	assert.Equal(t, expected, meta.GetAccessCount(), "concurrent access count should be accurate")
}

// Test 10: Prometheus metrics for evictions
func TestRecordEvictionManager_Metrics(t *testing.T) {
	mgr := NewRecordEvictionManager()

	mem := memory.NewGoAllocator()
	builder := array.NewFloat32Builder(mem)
	builder.AppendValues([]float32{1.0}, nil)
	arr := builder.NewArray()
	defer arr.Release() //nolint:gocritic // cleanup needed per iteration

	schema := arrow.NewSchema([]arrow.Field{{Name: "vec", Type: arrow.PrimitiveTypes.Float32}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, 1) //nolint:staticcheck
	defer rec.Release()                                   //nolint:gocritic // cleanup needed per iteration

	mgr.Register(rec, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)

	// Evict should increment metrics
	mgr.EvictExpired()

	// Metrics should show eviction (verified by checking counter)
	count := mgr.GetEvictionCount()
	assert.GreaterOrEqual(t, count, int64(1), "eviction count should be >= 1")
}

// Test 11: Integration - per-record eviction within Dataset
func TestDataset_PerRecordEviction(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create dataset with eviction manager
	ds := &Dataset{
		Records:        make([]arrow.RecordBatch, 0),
		Name:           "test",
		recordEviction: NewRecordEvictionManager(),
	}

	// Add records with different TTLs
	for i := 0; i < 3; i++ {
		builder := array.NewFloat32Builder(mem)
		builder.AppendValues([]float32{float32(i)}, nil)
		arr := builder.NewArray()
		defer arr.Release() //nolint:gocritic // cleanup needed per iteration

		schema := arrow.NewSchema([]arrow.Field{{Name: "vec", Type: arrow.PrimitiveTypes.Float32}}, nil)
		rec := array.NewRecord(schema, []arrow.Array{arr}, 1) //nolint:staticcheck
		defer rec.Release()                                   //nolint:gocritic // cleanup needed per iteration

		// First record: short TTL, others: long TTL
		ttl := 1 * time.Hour
		if i == 0 {
			ttl = 50 * time.Millisecond
		}
		ds.recordEviction.Register(rec, ttl)
		ds.Records = append(ds.Records, rec)
	}

	assert.Len(t, ds.Records, 3)

	// Wait for first record to expire
	time.Sleep(100 * time.Millisecond)

	// Evict expired records from dataset
	expired := ds.EvictExpiredRecords()
	assert.Len(t, expired, 1, "should evict 1 expired record")
	assert.Len(t, ds.Records, 2, "should have 2 remaining records")
}
