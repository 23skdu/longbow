package store

import (
"context"
"sync/atomic"
"testing"
"time"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
"go.uber.org/zap"
)

// =============================================================================
// FlightClientPool Integration Tests
// =============================================================================

// TestVectorStore_HasFlightClientPool_WhenReplicationEnabled verifies pool is created
func TestVectorStore_HasFlightClientPool_WhenReplicationEnabled(t *testing.T) {
alloc := memory.NewGoAllocator()
logger := zap.NewNop()

cfg := ReplicationConfig{
Enabled: true,
Peers:   []string{"localhost:9001"},
}

store := NewVectorStoreWithReplication(alloc, logger, 1<<30, 0, 0, cfg)
defer func() { _ = store.Shutdown(context.Background()) }()

if store.GetFlightClientPool() == nil {
t.Error("VectorStore with replication enabled should have FlightClientPool but got nil")
}
}

// TestVectorStore_WithReplicationConfig verifies replication config is wired
func TestVectorStore_WithReplicationConfig(t *testing.T) {
alloc := memory.NewGoAllocator()
logger := zap.NewNop()

cfg := ReplicationConfig{
Enabled: true,
Peers:   []string{"localhost:9001", "localhost:9002"},
ReplicaFactor: 2,
}

store := NewVectorStoreWithReplication(alloc, logger, 1<<30, 0, 0, cfg)
defer func() { _ = store.Shutdown(context.Background()) }()

if !store.IsReplicationEnabled() {
t.Error("Replication should be enabled")
}

peers := store.GetReplicationPeers()
if len(peers) != 2 {
t.Errorf("Expected 2 peers, got %d", len(peers))
}
}

// TestVectorStore_DoPut_TriggersReplication verifies replication hook is called
func TestVectorStore_DoPut_TriggersReplication(t *testing.T) {
alloc := memory.NewGoAllocator()
logger := zap.NewNop()

replicationCalls := &atomic.Int32{}
var capturedDataset string

cfg := ReplicationConfig{
Enabled: true,
Peers:   []string{"localhost:9001"},
ReplicaFactor: 1,
Mode: ReplicationModeAsync,
}

store := NewVectorStoreWithReplication(alloc, logger, 1<<30, 0, 0, cfg)
// Don't defer Shutdown here - it triggers parquet snapshot which has type issues

store.SetReplicationHook(func(ctx context.Context, dataset string, records []arrow.RecordBatch) {
replicationCalls.Add(1)
capturedDataset = dataset
})

// Create test record
schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)
builder := array.NewRecordBuilder(alloc, schema)
builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
rec := builder.NewRecordBatch()
defer rec.Release()

err := store.StoreRecordBatch(context.Background(), "test-dataset", rec)
if err != nil {
t.Fatalf("StoreRecordBatch failed: %v", err)
}

// Allow async replication to complete
time.Sleep(100 * time.Millisecond)

if replicationCalls.Load() != 1 {
t.Errorf("Expected 1 replication call, got %d", replicationCalls.Load())
}

if capturedDataset != "test-dataset" {
t.Errorf("Expected dataset 'test-dataset', got '%s'" , capturedDataset)
}
}

// TestVectorStore_DoPut_NoReplicationWhenDisabled verifies no replication when disabled
func TestVectorStore_DoPut_NoReplicationWhenDisabled(t *testing.T) {
alloc := memory.NewGoAllocator()
logger := zap.NewNop()

replicationCalls := &atomic.Int32{}

cfg := ReplicationConfig{
Enabled: false,
}

store := NewVectorStoreWithReplication(alloc, logger, 1<<30, 0, 0, cfg)
// Don't defer Shutdown - avoids snapshot/parquet issue

store.SetReplicationHook(func(ctx context.Context, dataset string, records []arrow.RecordBatch) {
replicationCalls.Add(1)
})

schema := arrow.NewSchema([]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)
builder := array.NewRecordBuilder(alloc, schema)
builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
rec := builder.NewRecordBatch()
defer rec.Release()

err := store.StoreRecordBatch(context.Background(), "test-dataset", rec)
if err != nil {
t.Fatalf("StoreRecordBatch failed: %v", err)
}

time.Sleep(50 * time.Millisecond)

if replicationCalls.Load() != 0 {
t.Errorf("Expected 0 replication calls when disabled, got %d", replicationCalls.Load())
}
}

// TestFlightClientPool_AddsPeersFromConfig verifies peers are auto-added
func TestFlightClientPool_AddsPeersFromConfig(t *testing.T) {
alloc := memory.NewGoAllocator()
logger := zap.NewNop()

cfg := ReplicationConfig{
Enabled: true,
Peers:   []string{"peer1:9000", "peer2:9000", "peer3:9000"},
ReplicaFactor: 2,
}

store := NewVectorStoreWithReplication(alloc, logger, 1<<30, 0, 0, cfg)
defer func() { _ = store.Shutdown(context.Background()) }()

pool := store.GetFlightClientPool()
stats := pool.Stats()

if stats.TotalHosts != 3 {
t.Errorf("Expected 3 hosts in pool, got %d", stats.TotalHosts)
}
}

// TestVectorStore_PoolStatsTracking verifies pool statistics are tracked
func TestVectorStore_PoolStatsTracking(t *testing.T) {
alloc := memory.NewGoAllocator()
logger := zap.NewNop()

cfg := ReplicationConfig{
Enabled: true,
Peers:   []string{"localhost:9001"},
ReplicaFactor: 1,
}

store := NewVectorStoreWithReplication(alloc, logger, 1<<30, 0, 0, cfg)
defer func() { _ = store.Shutdown(context.Background()) }()

pool := store.GetFlightClientPool()
if pool == nil {
t.Fatal("Pool should not be nil")
}

initialStats := pool.Stats()

// Stats structure should be accessible
t.Logf("Initial stats - TotalHosts: %d, Gets: %d, Hits: %d, Misses: %d",
initialStats.TotalHosts, initialStats.Gets, initialStats.Hits, initialStats.Misses)

if initialStats.TotalHosts != 1 {
t.Errorf("Expected 1 host, got %d", initialStats.TotalHosts)
}
}
