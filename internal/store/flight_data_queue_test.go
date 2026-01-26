package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ============================================================================
// FlightDataQueue Tests
// ============================================================================

func TestFlightDataQueueConfig_Defaults(t *testing.T) {
	cfg := DefaultFlightDataQueueConfig()

	if cfg.QueueSize != 1024 {
		t.Errorf("expected QueueSize=1024, got %d", cfg.QueueSize)
	}
	if cfg.EnqueueTimeout != 100*time.Millisecond {
		t.Errorf("expected EnqueueTimeout=100ms, got %v", cfg.EnqueueTimeout)
	}
	if cfg.DrainTimeout != 5*time.Second {
		t.Errorf("expected DrainTimeout=5s, got %v", cfg.DrainTimeout)
	}
}

func TestFlightDataQueueConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     FlightDataQueueConfig
		wantErr bool
	}{
		{"valid", DefaultFlightDataQueueConfig(), false},
		{"zero queue size", FlightDataQueueConfig{QueueSize: 0, EnqueueTimeout: time.Second, DrainTimeout: time.Second}, true},
		{"negative queue size", FlightDataQueueConfig{QueueSize: -1, EnqueueTimeout: time.Second, DrainTimeout: time.Second}, true},
		{"zero enqueue timeout", FlightDataQueueConfig{QueueSize: 100, EnqueueTimeout: 0, DrainTimeout: time.Second}, true},
		{"zero drain timeout", FlightDataQueueConfig{QueueSize: 100, EnqueueTimeout: time.Second, DrainTimeout: 0}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFlightDataQueue_NewAndClose(t *testing.T) {
	cfg := DefaultFlightDataQueueConfig()
	q := NewFlightDataQueue(cfg)

	if q == nil {
		t.Fatal("NewFlightDataQueue returned nil")
	}

	if q.Len() != 0 {
		t.Errorf("expected empty queue, got len=%d", q.Len())
	}

	if q.IsClosed() {
		t.Error("queue should not be closed initially")
	}

	q.Close()

	if !q.IsClosed() {
		t.Error("queue should be closed after Close()")
	}
}

func TestFlightDataQueue_EnqueueDequeue(t *testing.T) {
	cfg := DefaultFlightDataQueueConfig()
	q := NewFlightDataQueue(cfg)
	defer q.Close()

	// Create test chunk
	chunk := createTestChunk("test-dataset")

	// Enqueue
	ok := q.TryEnqueue(chunk)
	if !ok {
		t.Fatal("TryEnqueue failed")
	}

	if q.Len() != 1 {
		t.Errorf("expected len=1, got %d", q.Len())
	}

	// Dequeue
	ctx := context.Background()
	result, ok := q.Dequeue(ctx)
	if !ok {
		t.Fatal("Dequeue failed")
	}

	if result.DatasetName != "test-dataset" {
		t.Errorf("expected dataset=test-dataset, got %s", result.DatasetName)
	}

	if q.Len() != 0 {
		t.Errorf("expected len=0 after dequeue, got %d", q.Len())
	}
}

func TestFlightDataQueue_TryEnqueue_NonBlocking(t *testing.T) {
	// Create queue with size 2
	cfg := FlightDataQueueConfig{
		QueueSize:      2,
		EnqueueTimeout: 10 * time.Millisecond,
		DrainTimeout:   time.Second,
	}
	q := NewFlightDataQueue(cfg)
	defer q.Close()

	// Fill queue
	chunk1 := createTestChunk("ds1")
	chunk2 := createTestChunk("ds2")
	chunk3 := createTestChunk("ds3")

	if !q.TryEnqueue(chunk1) {
		t.Fatal("first enqueue failed")
	}
	if !q.TryEnqueue(chunk2) {
		t.Fatal("second enqueue failed")
	}

	// Third enqueue should fail (non-blocking)
	start := time.Now()
	ok := q.TryEnqueue(chunk3)
	elapsed := time.Since(start)

	// Should return quickly without blocking for long
	if elapsed > 50*time.Millisecond {
		t.Errorf("TryEnqueue blocked too long: %v", elapsed)
	}

	if ok {
		t.Error("expected TryEnqueue to fail on full queue")
	}
}

func TestFlightDataQueue_Dequeue_ContextCancel(t *testing.T) {
	cfg := DefaultFlightDataQueueConfig()
	q := NewFlightDataQueue(cfg)
	defer q.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, ok := q.Dequeue(ctx)
	elapsed := time.Since(start)

	if ok {
		t.Error("expected Dequeue to fail on empty queue with canceled context")
	}

	if elapsed > 100*time.Millisecond {
		t.Errorf("Dequeue should respect context timeout, took %v", elapsed)
	}
}

func TestFlightDataQueue_Dequeue_AfterClose(t *testing.T) {
	cfg := DefaultFlightDataQueueConfig()
	q := NewFlightDataQueue(cfg)

	// Enqueue one item
	chunk := createTestChunk("test")
	q.TryEnqueue(chunk)

	// Close queue
	q.Close()

	// Should still be able to drain existing items
	ctx := context.Background()
	_, ok := q.Dequeue(ctx)
	if !ok {
		t.Error("should be able to dequeue item after close")
	}

	// Next dequeue should fail
	_, ok = q.Dequeue(ctx)
	if ok {
		t.Error("dequeue should fail on empty closed queue")
	}
}

func TestFlightDataQueue_Stats(t *testing.T) {
	cfg := DefaultFlightDataQueueConfig()
	q := NewFlightDataQueue(cfg)
	defer q.Close()

	chunk := createTestChunk("test")
	q.TryEnqueue(chunk)

	stats := q.Stats()
	if stats.Enqueued != 1 {
		t.Errorf("expected Enqueued=1, got %d", stats.Enqueued)
	}

	ctx := context.Background()
	q.Dequeue(ctx)

	stats = q.Stats()
	if stats.Dequeued != 1 {
		t.Errorf("expected Dequeued=1, got %d", stats.Dequeued)
	}
}

func TestFlightDataQueue_Concurrent(t *testing.T) {
	cfg := FlightDataQueueConfig{
		QueueSize:      100,
		EnqueueTimeout: 100 * time.Millisecond,
		DrainTimeout:   5 * time.Second,
	}
	q := NewFlightDataQueue(cfg)

	var enqueued, dequeued atomic.Int64
	var wg sync.WaitGroup

	// 4 producers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(_ int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				chunk := createTestChunk("concurrent")
				if q.TryEnqueue(chunk) {
					enqueued.Add(1)
				}
			}
		}(i)
	}

	// 2 consumers
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, ok := q.Dequeue(ctx)
				if !ok {
					return
				}
				dequeued.Add(1)
			}
		}()
	}

	// Wait for producers
	time.Sleep(500 * time.Millisecond)
	q.Close()
	cancel()
	wg.Wait()

	t.Logf("Enqueued: %d, Dequeued: %d", enqueued.Load(), dequeued.Load())
}

// ============================================================================
// ChunkWorkerPool Tests
// ============================================================================

func TestChunkWorkerPoolConfig_Defaults(t *testing.T) {
	cfg := DefaultChunkWorkerPoolConfig()

	if cfg.NumWorkers <= 0 {
		t.Errorf("expected NumWorkers > 0, got %d", cfg.NumWorkers)
	}
	if cfg.ProcessTimeout != 30*time.Second {
		t.Errorf("expected ProcessTimeout=30s, got %v", cfg.ProcessTimeout)
	}
}

func TestChunkWorkerPoolConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ChunkWorkerPoolConfig
		wantErr bool
	}{
		{"valid", DefaultChunkWorkerPoolConfig(), false},
		{"zero workers", ChunkWorkerPoolConfig{NumWorkers: 0, ProcessTimeout: time.Second}, true},
		{"negative workers", ChunkWorkerPoolConfig{NumWorkers: -1, ProcessTimeout: time.Second}, true},
		{"zero timeout", ChunkWorkerPoolConfig{NumWorkers: 4, ProcessTimeout: 0}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestChunkWorkerPool_StartStop(t *testing.T) {
	cfg := ChunkWorkerPoolConfig{
		NumWorkers:     2,
		ProcessTimeout: time.Second,
	}
	qCfg := DefaultFlightDataQueueConfig()
	queue := NewFlightDataQueue(qCfg)

	var processed atomic.Int64
	handler := func(ctx context.Context, chunk *FlightDataChunk) error {
		processed.Add(1)
		return nil
	}

	pool := NewChunkWorkerPool(cfg, queue, handler)
	pool.Start()

	if !pool.IsRunning() {
		t.Error("pool should be running after Start()")
	}

	// Enqueue some work
	for i := 0; i < 5; i++ {
		chunk := createTestChunk("test")
		queue.TryEnqueue(chunk)
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	pool.Stop()
	queue.Close()

	if pool.IsRunning() {
		t.Error("pool should not be running after Stop()")
	}

	if processed.Load() != 5 {
		t.Errorf("expected 5 processed, got %d", processed.Load())
	}
}

func TestChunkWorkerPool_Stats(t *testing.T) {
	cfg := ChunkWorkerPoolConfig{
		NumWorkers:     2,
		ProcessTimeout: time.Second,
	}
	qCfg := DefaultFlightDataQueueConfig()
	queue := NewFlightDataQueue(qCfg)

	handler := func(ctx context.Context, chunk *FlightDataChunk) error {
		return nil
	}

	pool := NewChunkWorkerPool(cfg, queue, handler)
	pool.Start()

	// Enqueue work
	for i := 0; i < 10; i++ {
		chunk := createTestChunk("stats-test")
		queue.TryEnqueue(chunk)
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	stats := pool.Stats()
	if stats.Processed != 10 {
		t.Errorf("expected Processed=10, got %d", stats.Processed)
	}
	if stats.Errors != 0 {
		t.Errorf("expected Errors=0, got %d", stats.Errors)
	}

	pool.Stop()
	queue.Close()
}

func TestChunkWorkerPool_ErrorHandling(t *testing.T) {
	cfg := ChunkWorkerPoolConfig{
		NumWorkers:     1,
		ProcessTimeout: time.Second,
	}
	qCfg := DefaultFlightDataQueueConfig()
	queue := NewFlightDataQueue(qCfg)

	var callCount atomic.Int64
	handler := func(ctx context.Context, chunk *FlightDataChunk) error {
		callCount.Add(1)
		return fmt.Errorf("queue check failed")
	}

	pool := NewChunkWorkerPool(cfg, queue, handler)
	pool.Start()

	// Enqueue work that will fail
	chunk := createTestChunk("error-test")
	queue.TryEnqueue(chunk)

	time.Sleep(100 * time.Millisecond)

	stats := pool.Stats()
	if stats.Errors == 0 {
		t.Error("expected error count > 0")
	}

	pool.Stop()
	queue.Close()
}

// ============================================================================
// Benchmark
// ============================================================================

func BenchmarkFlightDataQueue_EnqueueDequeue(b *testing.B) {
	cfg := FlightDataQueueConfig{
		QueueSize:      10000,
		EnqueueTimeout: time.Second,
		DrainTimeout:   time.Second,
	}
	queue := NewFlightDataQueue(cfg)
	defer queue.Close()

	chunk := &FlightDataChunk{
		DatasetName: "bench",
		Record:      nil,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		queue.TryEnqueue(chunk)
		queue.Dequeue(context.Background())
	}
}

// ============================================================================
// Helpers
// ============================================================================

func createTestChunk(datasetName string) *FlightDataChunk {
	alloc := memory.NewGoAllocator()
	builder := array.NewFloat32Builder(alloc)
	defer builder.Release()

	builder.AppendValues([]float32{1.0, 2.0, 3.0}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 3)

	return &FlightDataChunk{
		DatasetName: datasetName,
		Record:      rec,
		ReceivedAt:  time.Now(),
	}
}
