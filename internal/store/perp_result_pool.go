package store

import (
"errors"
"runtime"
"sync"
"sync/atomic"
)

// =============================================================================
// PerPResultPool: Per-Processor Local Pools for Search Results
// Eliminates sync.Pool contention at high core counts by sharding pools
// =============================================================================

// PerPResultPoolConfig configures the per-processor result pool.
type PerPResultPoolConfig struct {
NumShards   int  // Number of shards (default: GOMAXPROCS)
EnableStats bool // Enable statistics tracking
}

// DefaultPerPResultPoolConfig returns sensible defaults.
func DefaultPerPResultPoolConfig() PerPResultPoolConfig {
return PerPResultPoolConfig{
NumShards:   runtime.GOMAXPROCS(0),
EnableStats: true,
}
}

// Validate checks configuration validity.
func (c PerPResultPoolConfig) Validate() error {
if c.NumShards <= 0 {
return errors.New("NumShards must be > 0")
}
return nil
}

// -----------------------------------------------------------------------------
// Statistics Types
// -----------------------------------------------------------------------------

// PerPResultPoolStats holds aggregate statistics.
type PerPResultPoolStats struct {
TotalGets uint64
TotalPuts uint64
Hits      uint64
Misses    uint64
}

// PerPShardStats holds per-shard statistics.
type PerPShardStats struct {
ShardID int
Gets    uint64
Puts    uint64
Hits    uint64
Misses  uint64
}

// -----------------------------------------------------------------------------
// perpShard: Individual pool shard with its own sync.Pools
// -----------------------------------------------------------------------------

type perpShard struct {
pool10   sync.Pool
pool20   sync.Pool
pool50   sync.Pool
pool100  sync.Pool
pool256  sync.Pool
pool1000 sync.Pool

// Statistics (only used if EnableStats)
gets   atomic.Uint64
puts   atomic.Uint64
hits   atomic.Uint64
misses atomic.Uint64
}

func newPerpShard() *perpShard {
return &perpShard{
pool10:   sync.Pool{},
pool20:   sync.Pool{},
pool50:   sync.Pool{},
pool100:  sync.Pool{},
pool256:  sync.Pool{},
pool1000: sync.Pool{},
}
}

// get retrieves a slice from this shard's pool.
func (s *perpShard) get(k int, trackStats bool) ([]VectorID, bool) {
var ptr any
var targetCap int

switch {
case k <= 10:
ptr = s.pool10.Get()
targetCap = 10
case k <= 20:
ptr = s.pool20.Get()
targetCap = 20
case k <= 50:
ptr = s.pool50.Get()
targetCap = 50
case k <= 100:
ptr = s.pool100.Get()
targetCap = 100
case k <= 256:
ptr = s.pool256.Get()
targetCap = 256
case k <= 1000:
ptr = s.pool1000.Get()
targetCap = 1000
default:
// Oversized - allocate directly
if trackStats {
s.gets.Add(1)
s.misses.Add(1)
}
return make([]VectorID, k), false
}

if trackStats {
s.gets.Add(1)
}

if ptr == nil {
// Pool miss - allocate new
if trackStats {
s.misses.Add(1)
}
slice := make([]VectorID, k, targetCap)
return slice, false
}

// Pool hit
if trackStats {
s.hits.Add(1)
}
slice := *ptr.(*[]VectorID)
// Reslice to requested length
slice = slice[:k]
// Zero the slice to prevent data leaks
for i := range slice {
slice[i] = 0
}
return slice, true
}

// put returns a slice to this shard's pool.
func (s *perpShard) put(slice []VectorID, trackStats bool) {
if slice == nil {
return
}

// Restore full capacity
capacity := cap(slice)
slice = slice[:capacity]

if trackStats {
s.puts.Add(1)
}

switch capacity {
case 10:
s.pool10.Put(&slice)
case 20:
s.pool20.Put(&slice)
case 50:
s.pool50.Put(&slice)
case 100:
s.pool100.Put(&slice)
case 256:
s.pool256.Put(&slice)
case 1000:
s.pool1000.Put(&slice)
// Non-standard capacities are not pooled
}
}

func (s *perpShard) stats() PerPShardStats {
return PerPShardStats{
Gets:   s.gets.Load(),
Puts:   s.puts.Load(),
Hits:   s.hits.Load(),
Misses: s.misses.Load(),
}
}

func (s *perpShard) resetStats() {
s.gets.Store(0)
s.puts.Store(0)
s.hits.Store(0)
s.misses.Store(0)
}

// -----------------------------------------------------------------------------
// PerPResultPool: Main per-processor pool
// -----------------------------------------------------------------------------

// PerPResultPool provides per-processor local pools for search result slices.
// It eliminates sync.Pool contention by sharding pools across processors.
type PerPResultPool struct {
shards      []*perpShard
numShards   int
enableStats bool
}

// NewPerPResultPool creates a new per-processor result pool.
// If config is nil, default configuration is used.
func NewPerPResultPool(config *PerPResultPoolConfig) *PerPResultPool {
cfg := DefaultPerPResultPoolConfig()
if config != nil {
cfg = *config
}

if err := cfg.Validate(); err != nil {
// Fall back to defaults on invalid config
cfg = DefaultPerPResultPoolConfig()
}

shards := make([]*perpShard, cfg.NumShards)
for i := range shards {
shards[i] = newPerpShard()
}

return &PerPResultPool{
shards:      shards,
numShards:   cfg.NumShards,
enableStats: cfg.EnableStats,
}
}

// NumShards returns the number of shards.
func (p *PerPResultPool) NumShards() int {
return p.numShards
}

// selectShard returns the shard index for the current operation.
// Uses round-robin for fair distribution across shards.
// selectShardByK returns deterministic shard based on k value
// This ensures Get(k) and Put of same-capacity slice hit the same shard
func (p *PerPResultPool) selectShardByK(k int) int {
var bucket int
switch {
case k <= 10:
bucket = 0
case k <= 50:
bucket = 1
case k <= 100:
bucket = 2
case k <= 256:
bucket = 3
case k <= 1000:
bucket = 4
default:
bucket = 5
}
return bucket % p.numShards
}

// Get retrieves a []VectorID slice of the specified length.
// For common k values, slices are pooled per-shard.
func (p *PerPResultPool) Get(k int) []VectorID {
shardIdx := p.selectShardByK(k)
slice, _ := p.shards[shardIdx].get(k, p.enableStats)
return slice
}

// Put returns a []VectorID slice to the appropriate pool shard.
func (p *PerPResultPool) Put(slice []VectorID) {
if slice == nil {
return
}
shardIdx := p.selectShardByK(cap(slice))
p.shards[shardIdx].put(slice, p.enableStats)
}

// Stats returns aggregate statistics across all shards.
func (p *PerPResultPool) Stats() PerPResultPoolStats {
var stats PerPResultPoolStats
for _, s := range p.shards {
ss := s.stats()
stats.TotalGets += ss.Gets
stats.TotalPuts += ss.Puts
stats.Hits += ss.Hits
stats.Misses += ss.Misses
}
return stats
}

// PerShardStats returns statistics for each shard.
func (p *PerPResultPool) PerShardStats() []PerPShardStats {
stats := make([]PerPShardStats, p.numShards)
for i, s := range p.shards {
stats[i] = s.stats()
stats[i].ShardID = i
}
return stats
}

// ResetStats resets all statistics counters.
func (p *PerPResultPool) ResetStats() {
for _, s := range p.shards {
s.resetStats()
}
}
