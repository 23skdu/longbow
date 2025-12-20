package store

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// =============================================================================
// IndexJobQueue Tests - TDD for Non-Blocking DoPut
// =============================================================================

// TestIndexJobQueueConfig_Defaults tests default configuration values
func TestIndexJobQueueConfig_Defaults(t *testing.T) {
	cfg := DefaultIndexJobQueueConfig()

	assert.Equal(t, 20000, cfg.MainChannelSize, "MainChannelSize")
	assert.Equal(t, 200000, cfg.OverflowBufferSize, "OverflowBufferSize")
	assert.Equal(t, 1*time.Millisecond, cfg.DrainInterval, "DrainInterval")
}

// TestIndexJobQueue_NonBlockingSend tests that Send never blocks
func TestIndexJobQueue_NonBlockingSend(t *testing.T) {
	cfg := IndexJobQueueConfig{
		MainChannelSize:    10,
		OverflowBufferSize: 100,
		DropOnOverflow:     false,
		DrainInterval:      time.Millisecond,
	}
	q := NewIndexJobQueue(cfg)
	defer q.Stop()

	// Send more than channel capacity - should not block
	done := make(chan bool)
	go func() {
		for i := 0; i < 50; i++ {
			q.Send(IndexJob{DatasetName: "test", BatchIdx: 0, RowIdx: i})
		}
		done <- true
	}()

	select {
	case <-done:
	// Success - did not block
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Send blocked when it should be non-blocking")
	}
}

// TestIndexJobQueue_OverflowBuffer tests overflow to secondary buffer
func TestIndexJobQueue_OverflowBuffer(t *testing.T) {
	cfg := IndexJobQueueConfig{
		MainChannelSize:    5,
		OverflowBufferSize: 100,
		DropOnOverflow:     false,
		DrainInterval:      time.Hour, // Don't drain automatically
	}
	q := NewIndexJobQueue(cfg)
	defer q.Stop()

	// Don't consume from main channel - fill it up
	for i := 0; i < 20; i++ {
		q.Send(IndexJob{DatasetName: "test", BatchIdx: 0, RowIdx: i})
	}

	stats := q.Stats()
	if stats.OverflowCount == 0 {
		t.Error("Expected some jobs to overflow to secondary buffer")
	}
	if stats.DroppedCount != 0 {
		t.Errorf("Expected no drops, got %d", stats.DroppedCount)
	}
}

// TestIndexJobQueue_DropOnOverflow tests drop strategy when both buffers full
func TestIndexJobQueue_DropOnOverflow(t *testing.T) {
	cfg := IndexJobQueueConfig{
		MainChannelSize:    5,
		OverflowBufferSize: 10,
		DropOnOverflow:     true,
		DrainInterval:      time.Hour, // Don't drain
	}
	q := NewIndexJobQueue(cfg)
	defer q.Stop()

	// Fill both main channel and overflow buffer
	for i := 0; i < 50; i++ {
		q.Send(IndexJob{DatasetName: "test", BatchIdx: 0, RowIdx: i})
	}

	stats := q.Stats()
	if stats.DroppedCount == 0 {
		t.Error("Expected some jobs to be dropped when overflow full")
	}
}

// TestIndexJobQueue_DrainToConsumer tests that overflow drains to main channel
func TestIndexJobQueue_DrainToConsumer(t *testing.T) {
	cfg := IndexJobQueueConfig{
		MainChannelSize:    5,
		OverflowBufferSize: 100,
		DropOnOverflow:     false,
		DrainInterval:      time.Millisecond,
	}
	q := NewIndexJobQueue(cfg)
	defer q.Stop()

	// Send jobs
	for i := 0; i < 20; i++ {
		q.Send(IndexJob{DatasetName: "test", BatchIdx: 0, RowIdx: i})
	}

	// Consume from main channel
	consumed := 0
	timeout := time.After(500 * time.Millisecond)
Consume:
	for {
		select {
		case <-q.Jobs():
			consumed++
			if consumed >= 20 {
				break Consume
			}
		case <-timeout:
			break Consume
		}
	}

	if consumed != 20 {
		t.Errorf("Expected to consume 20 jobs, got %d", consumed)
	}
}

// TestIndexJobQueue_Stats tests statistics tracking
func TestIndexJobQueue_Stats(t *testing.T) {
	cfg := IndexJobQueueConfig{
		MainChannelSize:    10,
		OverflowBufferSize: 100,
		DropOnOverflow:     false,
		DrainInterval:      time.Millisecond,
	}
	q := NewIndexJobQueue(cfg)
	defer q.Stop()

	for i := 0; i < 25; i++ {
		q.Send(IndexJob{DatasetName: "test", BatchIdx: 0, RowIdx: i})
	}

	stats := q.Stats()
	if stats.TotalSent != 25 {
		t.Errorf("TotalSent: got %d, want 25", stats.TotalSent)
	}
	if stats.DirectSent+stats.OverflowCount != 25 {
		t.Errorf("DirectSent(%d) + OverflowCount(%d) should equal 25",
			stats.DirectSent, stats.OverflowCount)
	}
}

// TestIndexJobQueue_ConcurrentSend tests concurrent send safety
func TestIndexJobQueue_ConcurrentSend(t *testing.T) {
	cfg := IndexJobQueueConfig{
		MainChannelSize:    100,
		OverflowBufferSize: 10000,
		DropOnOverflow:     true,
		DrainInterval:      time.Millisecond,
	}
	q := NewIndexJobQueue(cfg)
	defer q.Stop()

	// Concurrent sender goroutines
	var wg sync.WaitGroup
	sendersCount := 10
	jobsPerSender := 100

	for s := 0; s < sendersCount; s++ {
		wg.Add(1)
		go func(sender int) {
			defer wg.Done()
			for i := 0; i < jobsPerSender; i++ {
				q.Send(IndexJob{DatasetName: "test", BatchIdx: sender, RowIdx: i})
			}
		}(s)
	}

	wg.Wait()

	stats := q.Stats()
	expected := uint64(sendersCount * jobsPerSender)
	if stats.TotalSent != expected {
		t.Errorf("TotalSent: got %d, want %d", stats.TotalSent, expected)
	}
}

// TestIndexJobQueue_GracefulStop tests that Stop drains remaining jobs
func TestIndexJobQueue_GracefulStop(t *testing.T) {
	cfg := IndexJobQueueConfig{
		MainChannelSize:    10,
		OverflowBufferSize: 100,
		DropOnOverflow:     false,
		DrainInterval:      time.Millisecond,
	}
	q := NewIndexJobQueue(cfg)

	// Send some jobs
	for i := 0; i < 20; i++ {
		q.Send(IndexJob{DatasetName: "test", BatchIdx: 0, RowIdx: i})
	}

	// Consumer goroutine
	var consumed int64
	go func() {
		for range q.Jobs() {
			atomic.AddInt64(&consumed, 1)
		}
	}()

	// Stop should drain and close
	q.Stop()

	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt64(&consumed) != 20 {
		t.Errorf("Expected 20 consumed after Stop, got %d", atomic.LoadInt64(&consumed))
	}
}

// TestIndexJobQueue_BatchSend tests batch send for efficiency
func TestIndexJobQueue_BatchSend(t *testing.T) {
	cfg := IndexJobQueueConfig{
		MainChannelSize:    100,
		OverflowBufferSize: 1000,
		DropOnOverflow:     false,
		DrainInterval:      time.Millisecond,
	}
	q := NewIndexJobQueue(cfg)
	defer q.Stop()

	// Create batch of jobs
	batch := make([]IndexJob, 50)
	for i := range batch {
		batch[i] = IndexJob{DatasetName: "test", BatchIdx: 0, RowIdx: i}
	}

	// Send batch
	q.SendBatch(batch)

	stats := q.Stats()
	if stats.TotalSent != 50 {
		t.Errorf("TotalSent after batch: got %d, want 50", stats.TotalSent)
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

// BenchmarkIndexJobQueue_Send benchmarks non-blocking send
func BenchmarkIndexJobQueue_Send(b *testing.B) {
	cfg := IndexJobQueueConfig{
		MainChannelSize:    10000,
		OverflowBufferSize: 100000,
		DropOnOverflow:     true,
		DrainInterval:      time.Millisecond,
	}
	q := NewIndexJobQueue(cfg)
	defer q.Stop()

	// Consumer to prevent backpressure
	go func() {
		for range q.Jobs() {
		}
	}()

	job := IndexJob{DatasetName: "benchmark", BatchIdx: 0, RowIdx: 0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Send(job)
	}
}

// BenchmarkIndexJobQueue_BlockingSend benchmarks old blocking pattern
func BenchmarkIndexJobQueue_BlockingSend(b *testing.B) {
	ch := make(chan IndexJob, 10000)

	// Consumer
	go func() {
		for range ch {
		}
	}()

	job := IndexJob{DatasetName: "benchmark", BatchIdx: 0, RowIdx: 0}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- job
	}
	close(ch)
}

// BenchmarkIndexJobQueue_BatchSend benchmarks batch sending
func BenchmarkIndexJobQueue_BatchSend(b *testing.B) {
	cfg := IndexJobQueueConfig{
		MainChannelSize:    10000,
		OverflowBufferSize: 100000,
		DropOnOverflow:     true,
		DrainInterval:      time.Millisecond,
	}
	q := NewIndexJobQueue(cfg)
	defer q.Stop()

	// Consumer
	go func() {
		for range q.Jobs() {
		}
	}()

	batch := make([]IndexJob, 100)
	for i := range batch {
		batch[i] = IndexJob{DatasetName: "benchmark", BatchIdx: 0, RowIdx: i}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.SendBatch(batch)
	}
}
