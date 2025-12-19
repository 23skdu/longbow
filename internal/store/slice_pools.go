package store

import (
"sync"
"sync/atomic"

"github.com/23skdu/longbow/internal/metrics"
)

// PoolStats tracks pool usage statistics
type PoolStats struct {
Gets   int64
Puts   int64
Hits   int64
Misses int64
}

// Float32SlicePool pools []float32 slices of fixed dimension
type Float32SlicePool struct {
pool sync.Pool
dim  int
// Statistics (no New func for accurate tracking)
gets   atomic.Int64
puts   atomic.Int64
hits   atomic.Int64
misses atomic.Int64
name   string
}

// NewFloat32SlicePool creates a pool for float32 slices of given dimension
func NewFloat32SlicePool(dim int) *Float32SlicePool {
return &Float32SlicePool{
pool: sync.Pool{}, // No New func - enables hit/miss tracking
dim:  dim,
}
}

// NewFloat32SlicePoolWithMetrics creates a pool with Prometheus metrics
func NewFloat32SlicePoolWithMetrics(dim int, name string) *Float32SlicePool {
return &Float32SlicePool{
pool: sync.Pool{},
dim:  dim,
name: name,
}
}

// Get retrieves a slice from pool or allocates new
func (p *Float32SlicePool) Get() []float32 {
p.gets.Add(1)
if ptr := p.pool.Get(); ptr != nil {
p.hits.Add(1)
if p.name != "" {
metrics.SlicePoolHitsTotal.WithLabelValues(p.name).Inc()
}
return *(ptr.(*[]float32))
}
p.misses.Add(1)
if p.name != "" {
metrics.SlicePoolMissesTotal.WithLabelValues(p.name).Inc()
}
return make([]float32, p.dim)
}

// Put returns a slice to the pool
func (p *Float32SlicePool) Put(s []float32) {
if s == nil || len(s) != p.dim {
return
}
p.puts.Add(1)
p.pool.Put(&s)
}

// Stats returns current pool statistics
func (p *Float32SlicePool) Stats() PoolStats {
return PoolStats{
Gets:   p.gets.Load(),
Puts:   p.puts.Load(),
Hits:   p.hits.Load(),
Misses: p.misses.Load(),
}
}

// VectorIDSlicePool pools []VectorID slices
type VectorIDSlicePool struct {
pool sync.Pool
cap  int
}

// NewVectorIDSlicePool creates a pool for VectorID slices with given capacity
func NewVectorIDSlicePool(capacity int) *VectorIDSlicePool {
return &VectorIDSlicePool{
pool: sync.Pool{},
cap:  capacity,
}
}

// Get retrieves a slice from pool or allocates new
func (p *VectorIDSlicePool) Get() []VectorID {
if ptr := p.pool.Get(); ptr != nil {
s := ptr.(*[]VectorID)
return (*s)[:0] // Reset length, keep capacity
}
return make([]VectorID, 0, p.cap)
}

// Put returns a slice to the pool
func (p *VectorIDSlicePool) Put(s []VectorID) {
if s == nil || cap(s) < p.cap {
return
}
p.pool.Put(&s)
}

// LocationSlicePool pools []Location slices
type LocationSlicePool struct {
pool sync.Pool
cap  int
}

// NewLocationSlicePool creates a pool for Location slices with given capacity
func NewLocationSlicePool(capacity int) *LocationSlicePool {
return &LocationSlicePool{
pool: sync.Pool{},
cap:  capacity,
}
}

// Get retrieves a slice from pool or allocates new
func (p *LocationSlicePool) Get() []Location {
if ptr := p.pool.Get(); ptr != nil {
s := ptr.(*[]Location)
return (*s)[:0]
}
return make([]Location, 0, p.cap)
}

// Put returns a slice to the pool
func (p *LocationSlicePool) Put(s []Location) {
if s == nil || cap(s) < p.cap {
return
}
p.pool.Put(&s)
}

// HybridResultSlicePool pools []HybridResult slices
type HybridResultSlicePool struct {
pool sync.Pool
cap  int
}

// NewHybridResultSlicePool creates a pool for HybridResult slices
func NewHybridResultSlicePool(capacity int) *HybridResultSlicePool {
return &HybridResultSlicePool{
pool: sync.Pool{},
cap:  capacity,
}
}

// Get retrieves a slice from pool or allocates new
func (p *HybridResultSlicePool) Get() []HybridResult {
if ptr := p.pool.Get(); ptr != nil {
s := ptr.(*[]HybridResult)
return (*s)[:0]
}
return make([]HybridResult, 0, p.cap)
}

// Put returns a slice to the pool
func (p *HybridResultSlicePool) Put(s []HybridResult) {
if s == nil || cap(s) < p.cap {
return
}
p.pool.Put(&s)
}

// RankedResultSlicePool pools []RankedResult slices
type RankedResultSlicePool struct {
pool sync.Pool
cap  int
}

// NewRankedResultSlicePool creates a pool for RankedResult slices
func NewRankedResultSlicePool(capacity int) *RankedResultSlicePool {
return &RankedResultSlicePool{
pool: sync.Pool{},
cap:  capacity,
}
}

// Get retrieves a slice from pool or allocates new
func (p *RankedResultSlicePool) Get() []RankedResult {
if ptr := p.pool.Get(); ptr != nil {
s := ptr.(*[]RankedResult)
return (*s)[:0]
}
return make([]RankedResult, 0, p.cap)
}

// Put returns a slice to the pool
func (p *RankedResultSlicePool) Put(s []RankedResult) {
if s == nil || cap(s) < p.cap {
return
}
p.pool.Put(&s)
}

// filterByThresholdPooled filters HybridResults using pooled slice (for internal use)
func filterByThresholdPooled(results []HybridResult, threshold float64) []HybridResult {
// Use pre-allocated capacity based on input size
filtered := make([]HybridResult, 0, len(results))
for _, r := range results {
if r.Score >= threshold {
filtered = append(filtered, r)
}
}
return filtered
}
