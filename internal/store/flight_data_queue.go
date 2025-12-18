package store

import (
"context"
"runtime"
"sync"
"sync/atomic"
"time"

"github.com/apache/arrow-go/v18/arrow"
)

// ============================================================================
// FlightDataChunk - data passed through the queue
// ============================================================================

// FlightDataChunk represents a received Arrow RecordBatch with metadata.
type FlightDataChunk struct {
DatasetName string
Record      arrow.RecordBatch
ReceivedAt  time.Time
}

// ============================================================================
// FlightDataQueueConfig
// ============================================================================

// FlightDataQueueConfig configures the flight data queue.
type FlightDataQueueConfig struct {
QueueSize      int           // buffered channel capacity
EnqueueTimeout time.Duration // timeout for TryEnqueue
DrainTimeout   time.Duration // timeout for draining on close
}

// DefaultFlightDataQueueConfig returns sensible defaults.
func DefaultFlightDataQueueConfig() FlightDataQueueConfig {
return FlightDataQueueConfig{
QueueSize:      1024,
EnqueueTimeout: 100 * time.Millisecond,
DrainTimeout:   5 * time.Second,
}
}

// Validate checks config validity.
func (c FlightDataQueueConfig) Validate() error {
if c.QueueSize <= 0 {
return NewConfigError("FlightDataQueueConfig", "QueueSize", "", "must be > 0")
}
if c.EnqueueTimeout <= 0 {
return NewConfigError("FlightDataQueueConfig", "EnqueueTimeout", "", "must be > 0")
}
if c.DrainTimeout <= 0 {
return NewConfigError("FlightDataQueueConfig", "DrainTimeout", "", "must be > 0")
}
return nil
}

// ============================================================================
// FlightDataQueueStats
// ============================================================================

// FlightDataQueueStats tracks queue operations.
type FlightDataQueueStats struct {
Enqueued int64
Dequeued int64
Dropped  int64
}

// ============================================================================
// FlightDataQueue
// ============================================================================

// FlightDataQueue is a buffered channel wrapper for decoupling receive from processing.
type FlightDataQueue struct {
config  FlightDataQueueConfig
ch      chan *FlightDataChunk
closed  atomic.Bool
closeMu sync.Mutex

// stats
enqueued atomic.Int64
dequeued atomic.Int64
dropped  atomic.Int64
}

// NewFlightDataQueue creates a new queue.
func NewFlightDataQueue(config FlightDataQueueConfig) *FlightDataQueue {
return &FlightDataQueue{
config: config,
ch:     make(chan *FlightDataChunk, config.QueueSize),
}
}

// TryEnqueue attempts to enqueue a chunk without blocking indefinitely.
// Returns false if queue is full or closed.
func (q *FlightDataQueue) TryEnqueue(chunk *FlightDataChunk) bool {
if q.closed.Load() {
q.dropped.Add(1)
return false
}

select {
case q.ch <- chunk:
q.enqueued.Add(1)
return true
case <-time.After(q.config.EnqueueTimeout):
q.dropped.Add(1)
return false
}
}

// Dequeue retrieves the next chunk, respecting context cancellation.
// Returns (nil, false) if queue is closed and empty or context canceled.
func (q *FlightDataQueue) Dequeue(ctx context.Context) (*FlightDataChunk, bool) {
select {
case chunk, ok := <-q.ch:
if ok {
q.dequeued.Add(1)
}
return chunk, ok
case <-ctx.Done():
return nil, false
}
}

// Len returns current queue length.
func (q *FlightDataQueue) Len() int {
return len(q.ch)
}

// IsClosed returns whether queue is closed.
func (q *FlightDataQueue) IsClosed() bool {
return q.closed.Load()
}

// Close closes the queue, preventing new enqueues.
func (q *FlightDataQueue) Close() {
q.closeMu.Lock()
defer q.closeMu.Unlock()

if q.closed.Swap(true) {
return // already closed
}
close(q.ch)
}

// Stats returns current statistics.
func (q *FlightDataQueue) Stats() FlightDataQueueStats {
return FlightDataQueueStats{
Enqueued: q.enqueued.Load(),
Dequeued: q.dequeued.Load(),
Dropped:  q.dropped.Load(),
}
}

// ============================================================================
// ChunkWorkerPoolConfig
// ============================================================================

// ChunkWorkerPoolConfig configures the worker pool.
type ChunkWorkerPoolConfig struct {
NumWorkers     int           // number of worker goroutines
ProcessTimeout time.Duration // timeout for processing each chunk
}

// DefaultChunkWorkerPoolConfig returns sensible defaults.
func DefaultChunkWorkerPoolConfig() ChunkWorkerPoolConfig {
numCPU := runtime.GOMAXPROCS(0)
if numCPU < 2 {
numCPU = 2
}
return ChunkWorkerPoolConfig{
NumWorkers:     numCPU,
ProcessTimeout: 30 * time.Second,
}
}

// Validate checks config validity.
func (c ChunkWorkerPoolConfig) Validate() error {
if c.NumWorkers <= 0 {
return NewConfigError("ChunkWorkerPoolConfig", "NumWorkers", "", "must be > 0")
}
if c.ProcessTimeout <= 0 {
return NewConfigError("ChunkWorkerPoolConfig", "ProcessTimeout", "", "must be > 0")
}
return nil
}

// ============================================================================
// ChunkWorkerPoolStats
// ============================================================================

// ChunkWorkerPoolStats tracks worker pool operations.
type ChunkWorkerPoolStats struct {
Processed int64
Errors    int64
}

// ============================================================================
// ChunkHandler - function signature for processing chunks
// ============================================================================

// ChunkHandler is the callback for processing a chunk.
type ChunkHandler func(ctx context.Context, chunk *FlightDataChunk) error

// ============================================================================
// ChunkWorkerPool
// ============================================================================

// ChunkWorkerPool processes chunks from a queue using multiple workers.
type ChunkWorkerPool struct {
config  ChunkWorkerPoolConfig
queue   *FlightDataQueue
handler ChunkHandler

running atomic.Bool
wg      sync.WaitGroup
ctx     context.Context
cancel  context.CancelFunc

// stats
processed atomic.Int64
errors    atomic.Int64
}

// NewChunkWorkerPool creates a new worker pool.
func NewChunkWorkerPool(config ChunkWorkerPoolConfig, queue *FlightDataQueue, handler ChunkHandler) *ChunkWorkerPool {
return &ChunkWorkerPool{
config:  config,
queue:   queue,
handler: handler,
}
}

// Start launches worker goroutines.
func (p *ChunkWorkerPool) Start() {
if p.running.Swap(true) {
return // already running
}

p.ctx, p.cancel = context.WithCancel(context.Background())

for i := 0; i < p.config.NumWorkers; i++ {
p.wg.Add(1)
go p.worker()
}
}

// worker is the main loop for each worker goroutine.
func (p *ChunkWorkerPool) worker() {
defer p.wg.Done()

for {
chunk, ok := p.queue.Dequeue(p.ctx)
if !ok {
return // queue closed or context canceled
}

// Process with timeout
processCtx, processCancel := context.WithTimeout(p.ctx, p.config.ProcessTimeout)
err := p.handler(processCtx, chunk)
processCancel()

if err != nil {
p.errors.Add(1)
} else {
p.processed.Add(1)
}
}
}

// Stop signals workers to stop and waits for completion.
func (p *ChunkWorkerPool) Stop() {
if !p.running.Swap(false) {
return // not running
}

if p.cancel != nil {
p.cancel()
}
p.wg.Wait()
}

// IsRunning returns whether pool is running.
func (p *ChunkWorkerPool) IsRunning() bool {
return p.running.Load()
}

// Stats returns current statistics.
func (p *ChunkWorkerPool) Stats() ChunkWorkerPoolStats {
return ChunkWorkerPoolStats{
Processed: p.processed.Load(),
Errors:    p.errors.Load(),
}
}
