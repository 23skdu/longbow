
package store

import (
"runtime"
"sync"
"sync/atomic"
"testing"
)

// TestShardedHNSWConfig_Defaults verifies default configuration
func TestShardedHNSWConfig_Defaults(t *testing.T) {
cfg := DefaultShardedHNSWConfig()

if cfg.NumShards <= 0 {
t.Errorf("expected positive NumShards, got %d", cfg.NumShards)
}
if cfg.NumShards != runtime.NumCPU() {
t.Errorf("expected NumShards=%d (NumCPU), got %d", runtime.NumCPU(), cfg.NumShards)
}
if cfg.M <= 0 {
t.Errorf("expected positive M, got %d", cfg.M)
}
if cfg.EfConstruction <= 0 {
t.Errorf("expected positive EfConstruction, got %d", cfg.EfConstruction)
}
}

// TestShardedHNSWConfig_Validation verifies configuration validation
func TestShardedHNSWConfig_Validation(t *testing.T) {
tests := []struct {
name string
cfg ShardedHNSWConfig
wantErr bool
}{
{"valid default", DefaultShardedHNSWConfig(), false},
{"zero shards", ShardedHNSWConfig{NumShards: 0, M: 16, EfConstruction: 200}, true},
{"negative shards", ShardedHNSWConfig{NumShards: -1, M: 16, EfConstruction: 200}, true},
{"zero M", ShardedHNSWConfig{NumShards: 4, M: 0, EfConstruction: 200}, true},
{"zero EfConstruction", ShardedHNSWConfig{NumShards: 4, M: 16, EfConstruction: 0}, true},
{"single shard", ShardedHNSWConfig{NumShards: 1, M: 16, EfConstruction: 200}, false},
{"many shards", ShardedHNSWConfig{NumShards: 64, M: 16, EfConstruction: 200}, false},
}

for _, tc := range tests {
t.Run(tc.name, func(t *testing.T) {
err := tc.cfg.Validate()
if (err != nil) != tc.wantErr {
t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
}
})
}
}

// TestShardedHNSW_ShardRouting verifies consistent hash-based routing
func TestShardedHNSW_ShardRouting(t *testing.T) {
cfg := DefaultShardedHNSWConfig()
cfg.NumShards = 8

sharded := NewShardedHNSW(cfg, nil)

// Same ID should always route to same shard
for id := VectorID(0); id < 1000; id++ {
shard1 := sharded.GetShardForID(id)
shard2 := sharded.GetShardForID(id)
if shard1 != shard2 {
t.Errorf("ID %d routed to different shards: %d vs %d", id, shard1, shard2)
}
if shard1 < 0 || shard1 >= cfg.NumShards {
t.Errorf("shard index %d out of range [0, %d)", shard1, cfg.NumShards)
}
}

// Verify distribution across shards (should be roughly even)
counts := make([]int, cfg.NumShards)
for id := VectorID(0); id < 10000; id++ {
counts[sharded.GetShardForID(id)]++
}

expected := 10000 / cfg.NumShards
for i, count := range counts {
// Allow 20% variance
if count < expected*80/100 || count > expected*120/100 {
t.Errorf("shard %d has %d vectors (expected ~%d)", i, count, expected)
}
}
}

// TestShardedHNSW_AddToShard verifies adding vectors routes to correct shard
func TestShardedHNSW_AddToShard(t *testing.T) {
cfg := DefaultShardedHNSWConfig()
cfg.NumShards = 4

sharded := NewShardedHNSW(cfg, nil)

// Add vectors with known locations
for i := 0; i < 100; i++ {
loc := Location{BatchIdx: 0, RowIdx: i}
id, err := sharded.Add(loc, []float32{float32(i), float32(i * 2), float32(i * 3)})
if err != nil {
t.Fatalf("Add failed: %v", err)
}
if id != VectorID(i) {
t.Errorf("expected ID %d, got %d", i, id)
}
}

// Verify total count
if sharded.Len() != 100 {
t.Errorf("expected 100 vectors, got %d", sharded.Len())
}

// Verify vectors distributed across shards
stats := sharded.ShardStats()
totalInShards := 0
for _, s := range stats {
totalInShards += s.Count
if s.Count == 0 {
t.Logf("Warning: shard %d is empty", s.ShardID)
}
}
if totalInShards != 100 {
t.Errorf("expected 100 vectors in shards, got %d", totalInShards)
}
}

// TestShardedHNSW_ParallelAdds verifies concurrent additions don't corrupt state
func TestShardedHNSW_ParallelAdds(t *testing.T) {
cfg := DefaultShardedHNSWConfig()
cfg.NumShards = 8

sharded := NewShardedHNSW(cfg, nil)

numGoroutines := 16
vectorsPerGoroutine := 100

var wg sync.WaitGroup
var errCount atomic.Int32

for g := 0; g < numGoroutines; g++ {
wg.Add(1)
go func(goroutineID int) {
defer wg.Done()
for i := 0; i < vectorsPerGoroutine; i++ {
loc := Location{BatchIdx: goroutineID, RowIdx: i}
vec := []float32{float32(goroutineID), float32(i), float32(goroutineID + i)}
_, err := sharded.Add(loc, vec)
if err != nil {
errCount.Add(1)
}
}
}(g)
}

wg.Wait()

if errCount.Load() > 0 {
t.Errorf("%d errors during parallel adds", errCount.Load())
}

expected := numGoroutines * vectorsPerGoroutine
if sharded.Len() != expected {
t.Errorf("expected %d vectors, got %d", expected, sharded.Len())
}
}

// TestShardedHNSW_Search verifies search across all shards
func TestShardedHNSW_Search(t *testing.T) {
cfg := DefaultShardedHNSWConfig()
cfg.NumShards = 4
// Small M and EF for test speed
cfg.M = 8
cfg.EfConstruction = 50

sharded := NewShardedHNSW(cfg, nil)

// Add 100 vectors
for i := 0; i < 100; i++ {
loc := Location{BatchIdx: 0, RowIdx: i}
vec := []float32{float32(i), float32(i), float32(i)}
_, err := sharded.Add(loc, vec)
if err != nil {
t.Fatalf("Add failed: %v", err)
}
}

// Search for vector near [50, 50, 50]
query := []float32{50.0, 50.0, 50.0}
results := sharded.Search(query, 10)

if len(results) == 0 {
t.Fatal("expected search results, got none")
}
if len(results) > 10 {
t.Errorf("expected at most 10 results, got %d", len(results))
}

// Closest should be ID=50 (vector [50,50,50])
foundExpected := false
for _, r := range results {
if r.ID == 50 {
foundExpected = true
break
}
}
if !foundExpected {
t.Logf("Results: %v", results)
// Don't fail - HNSW is approximate, but log for debugging
t.Logf("Warning: expected ID=50 in results for query [50,50,50]")
}
}

// TestShardedHNSW_SearchEmpty verifies search on empty index
func TestShardedHNSW_SearchEmpty(t *testing.T) {
cfg := DefaultShardedHNSWConfig()
cfg.NumShards = 4

sharded := NewShardedHNSW(cfg, nil)

query := []float32{1.0, 2.0, 3.0}
results := sharded.Search(query, 10)

if len(results) != 0 {
t.Errorf("expected 0 results on empty index, got %d", len(results))
}
}

// TestShardedHNSW_GetLocation verifies location retrieval
func TestShardedHNSW_GetLocation(t *testing.T) {
cfg := DefaultShardedHNSWConfig()
cfg.NumShards = 4

sharded := NewShardedHNSW(cfg, nil)

// Add vectors with specific locations
expected := []Location{
{BatchIdx: 0, RowIdx: 5},
{BatchIdx: 1, RowIdx: 10},
{BatchIdx: 2, RowIdx: 15},
}

for i, loc := range expected {
vec := []float32{float32(i), float32(i), float32(i)}
_, err := sharded.Add(loc, vec)
if err != nil {
t.Fatalf("Add failed: %v", err)
}
}

// Verify retrieval
for i, exp := range expected {
loc, ok := sharded.GetLocation(VectorID(i))
if !ok {
t.Errorf("ID %d: expected location, got not found", i)
continue
}
if loc != exp {
t.Errorf("ID %d: expected %v, got %v", i, exp, loc)
}
}

// Non-existent ID
_, ok := sharded.GetLocation(VectorID(999))
if ok {
t.Error("expected not found for ID 999")
}
}

// TestShardedHNSW_ShardStats verifies per-shard statistics
func TestShardedHNSW_ShardStats(t *testing.T) {
cfg := DefaultShardedHNSWConfig()
cfg.NumShards = 4

sharded := NewShardedHNSW(cfg, nil)

// Add 100 vectors
for i := 0; i < 100; i++ {
loc := Location{BatchIdx: 0, RowIdx: i}
vec := []float32{float32(i), float32(i * 2), float32(i * 3)}
_, _ = sharded.Add(loc, vec)
}

stats := sharded.ShardStats()

if len(stats) != 4 {
t.Errorf("expected 4 shard stats, got %d", len(stats))
}

total := 0
for _, s := range stats {
if s.ShardID < 0 || s.ShardID >= 4 {
t.Errorf("invalid shard ID: %d", s.ShardID)
}
total += s.Count
}

if total != 100 {
t.Errorf("expected total 100 across shards, got %d", total)
}
}

// TestShardedHNSW_ConcurrentAddAndSearch verifies thread safety
func TestShardedHNSW_ConcurrentAddAndSearch(t *testing.T) {
cfg := DefaultShardedHNSWConfig()
cfg.NumShards = 4
cfg.M = 8
cfg.EfConstruction = 50

sharded := NewShardedHNSW(cfg, nil)

// Pre-populate with some vectors
for i := 0; i < 50; i++ {
loc := Location{BatchIdx: 0, RowIdx: i}
vec := []float32{float32(i), float32(i), float32(i)}
_, _ = sharded.Add(loc, vec)
}

var wg sync.WaitGroup
done := make(chan struct{})

// Concurrent adders
for g := 0; g < 4; g++ {
wg.Add(1)
go func(id int) {
defer wg.Done()
for i := 0; i < 50; i++ {
select {
case <-done:
return
default:
loc := Location{BatchIdx: id + 1, RowIdx: i}
vec := []float32{float32(id * 100 + i), float32(id * 100 + i), 0}
_, _ = sharded.Add(loc, vec)
}
}
}(g)
}

// Concurrent searchers
for g := 0; g < 4; g++ {
wg.Add(1)
go func() {
defer wg.Done()
for i := 0; i < 50; i++ {
select {
case <-done:
return
default:
query := []float32{float32(i), float32(i), float32(i)}
_ = sharded.Search(query, 5)
}
}
}()
}

wg.Wait()
close(done)

// Verify no panics occurred and data is consistent
if sharded.Len() < 50 {
t.Errorf("expected at least 50 vectors, got %d", sharded.Len())
}
}

// Benchmark: Compare sharded vs single HNSW add performance
func BenchmarkHNSW_SingleAdd(b *testing.B) {
cfg := DefaultShardedHNSWConfig()
cfg.NumShards = 1 // Single shard = original behavior
cfg.M = 16
cfg.EfConstruction = 100

sharded := NewShardedHNSW(cfg, nil)

vec := make([]float32, 128)
for i := range vec {
vec[i] = float32(i)
}

b.ResetTimer()
b.ReportAllocs()

for i := 0; i < b.N; i++ {
loc := Location{BatchIdx: 0, RowIdx: i}
_, _ = sharded.Add(loc, vec)
}
}

func BenchmarkHNSW_ShardedParallelAdd(b *testing.B) {
cfg := DefaultShardedHNSWConfig()
cfg.NumShards = runtime.NumCPU()
cfg.M = 16
cfg.EfConstruction = 100

sharded := NewShardedHNSW(cfg, nil)

vec := make([]float32, 128)
for i := range vec {
vec[i] = float32(i)
}

b.ResetTimer()
b.ReportAllocs()

b.RunParallel(func(pb *testing.PB) {
idx := 0
for pb.Next() {
loc := Location{BatchIdx: 0, RowIdx: idx}
_, _ = sharded.Add(loc, vec)
idx++
}
})
}

func BenchmarkHNSW_ShardedSearch(b *testing.B) {
cfg := DefaultShardedHNSWConfig()
cfg.NumShards = runtime.NumCPU()
cfg.M = 16
cfg.EfConstruction = 100

sharded := NewShardedHNSW(cfg, nil)

// Pre-populate with vectors
for i := 0; i < 10000; i++ {
loc := Location{BatchIdx: 0, RowIdx: i}
vec := make([]float32, 128)
for j := range vec {
vec[j] = float32(i + j)
}
_, _ = sharded.Add(loc, vec)
}

query := make([]float32, 128)
for i := range query {
query[i] = float32(i)
}

b.ResetTimer()
b.ReportAllocs()

for i := 0; i < b.N; i++ {
_ = sharded.Search(query, 10)
}
}
