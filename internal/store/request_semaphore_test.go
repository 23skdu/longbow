package store

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// RequestSemaphoreConfig Tests
// =============================================================================

// TestDefaultRequestSemaphoreConfig runs the test default request semaphore config operation.
func TestDefaultRequestSemaphoreConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := DefaultRequestSemaphoreConfig()

	// Default limit should be GOMAXPROCS
	if cfg.MaxConcurrent != runtime.GOMAXPROCS(0) {
		t.Errorf("MaxConcurrent = %d, want %d", cfg.MaxConcurrent, runtime.GOMAXPROCS(0))
	}

	// Default timeout should be reasonable (30s)
	if cfg.AcquireTimeout != 30*time.Second {
		t.Errorf("AcquireTimeout = %v, want 30s", cfg.AcquireTimeout)
	}

	// Should be enabled by default
	if !cfg.Enabled {
		t.Error("Enabled should be true by default")
	}
}

// TestRequestSemaphoreConfig_Validate runs the test request semaphore config_validate operation.
func TestRequestSemaphoreConfig_Validate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	tests := []struct {
		name    string
		cfg     RequestSemaphoreConfig
		wantErr bool
	}{
		{
			name:    "valid config",
			cfg:     RequestSemaphoreConfig{MaxConcurrent: 4, AcquireTimeout: time.Second, Enabled: true},
			wantErr: false,
		},
		{
			name:    "disabled config skips validation",
			cfg:     RequestSemaphoreConfig{MaxConcurrent: 0, AcquireTimeout: 0, Enabled: false},
			wantErr: false,
		},
		{
			name:    "zero MaxConcurrent when enabled",
			cfg:     RequestSemaphoreConfig{MaxConcurrent: 0, AcquireTimeout: time.Second, Enabled: true},
			wantErr: true,
		},
		{
			name:    "negative MaxConcurrent",
			cfg:     RequestSemaphoreConfig{MaxConcurrent: -1, AcquireTimeout: time.Second, Enabled: true},
			wantErr: true,
		},
		{
			name:    "zero timeout when enabled",
			cfg:     RequestSemaphoreConfig{MaxConcurrent: 4, AcquireTimeout: 0, Enabled: true},
			wantErr: true,
		},
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

// =============================================================================
// RequestSemaphore Tests
// =============================================================================

// TestNewRequestSemaphore runs the test new request semaphore operation.
func TestNewRequestSemaphore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  8,
		AcquireTimeout: 5 * time.Second,
		Enabled:        true,
	}

	sem := NewRequestSemaphore(cfg)
	if sem == nil {
		t.Fatal("NewRequestSemaphore returned nil")
	}

	if sem.MaxConcurrent() != 8 {
		t.Errorf("MaxConcurrent() = %d, want 8", sem.MaxConcurrent())
	}

	if sem.Available() != 8 {
		t.Errorf("Available() = %d, want 8", sem.Available())
	}
}

// TestRequestSemaphore_AcquireRelease runs the test request semaphore_acquire release operation.
func TestRequestSemaphore_AcquireRelease(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  2,
		AcquireTimeout: time.Second,
		Enabled:        true,
	}
	sem := NewRequestSemaphore(cfg)

	ctx := context.Background()

	// Acquire first slot
	if err := sem.Acquire(ctx); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if sem.Available() != 1 {
		t.Errorf("Available() = %d, want 1", sem.Available())
	}

	// Acquire second slot
	if err := sem.Acquire(ctx); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if sem.Available() != 0 {
		t.Errorf("Available() = %d, want 0", sem.Available())
	}

	// Release one
	sem.Release()
	if sem.Available() != 1 {
		t.Errorf("Available() after Release = %d, want 1", sem.Available())
	}

	// Release second
	sem.Release()
	if sem.Available() != 2 {
		t.Errorf("Available() after second Release = %d, want 2", sem.Available())
	}
}

// TestRequestSemaphore_TryAcquire runs the test request semaphore_try acquire operation.
func TestRequestSemaphore_TryAcquire(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  1,
		AcquireTimeout: time.Second,
		Enabled:        true,
	}
	sem := NewRequestSemaphore(cfg)

	// First TryAcquire should succeed
	if !sem.TryAcquire() {
		t.Error("First TryAcquire() should succeed")
	}

	// Second TryAcquire should fail (no blocking)
	if sem.TryAcquire() {
		t.Error("Second TryAcquire() should fail when full")
	}

	// After release, should succeed again
	sem.Release()
	if !sem.TryAcquire() {
		t.Error("TryAcquire() after Release should succeed")
	}
}

// TestRequestSemaphore_Timeout runs the test request semaphore_timeout operation.
func TestRequestSemaphore_Timeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  1,
		AcquireTimeout: 50 * time.Millisecond,
		Enabled:        true,
	}
	sem := NewRequestSemaphore(cfg)

	ctx := context.Background()

	// Acquire the only slot
	if err := sem.Acquire(ctx); err != nil {
		t.Fatalf("First Acquire() error = %v", err)
	}

	// Second acquire should timeout
	start := time.Now()
	err := sem.Acquire(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Second Acquire() should timeout")
	}

	// Should have waited approximately the timeout duration
	if elapsed < 40*time.Millisecond || elapsed > 100*time.Millisecond {
		t.Errorf("Timeout duration = %v, expected ~50ms", elapsed)
	}
}

// TestRequestSemaphore_ContextCancellation runs the test request semaphore_context cancellation operation.
func TestRequestSemaphore_ContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  1,
		AcquireTimeout: 10 * time.Second,
		Enabled:        true,
	}
	sem := NewRequestSemaphore(cfg)

	// Fill the semaphore
	ctx := context.Background()
	if err := sem.Acquire(ctx); err != nil {
		t.Fatalf("First Acquire() error = %v", err)
	}

	// Create cancellable context
	ctx2, cancel := context.WithCancel(context.Background())

	// Start acquire in goroutine
	done := make(chan error, 1)
	go func() {
		done <- sem.Acquire(ctx2)
	}()

	// Cancel after short delay
	time.Sleep(20 * time.Millisecond)
	cancel()

	// Should return error due to cancellation
	select {
	case err := <-done:
		if err == nil {
			t.Error("Acquire() should return error on context cancellation")
		}
	case <-time.After(time.Second):
		t.Error("Acquire() did not return after context cancellation")
	}
}

// TestRequestSemaphore_ConcurrentAccess runs the test request semaphore_concurrent access operation.
func TestRequestSemaphore_ConcurrentAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  4,
		AcquireTimeout: 5 * time.Second,
		Enabled:        true,
	}
	sem := NewRequestSemaphore(cfg)

	var maxConcurrent int64
	var current int64
	var wg sync.WaitGroup

	ctx := context.Background()

	// Run 100 goroutines
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := sem.Acquire(ctx); err != nil {
				return
			}
			defer sem.Release()

			// Track max concurrent
			c := atomic.AddInt64(&current, 1)
			for {
				maxVal := atomic.LoadInt64(&maxConcurrent)
				if c <= maxVal {
					break
				}
				if atomic.CompareAndSwapInt64(&maxConcurrent, maxVal, c) {
					break
				}
			}

			// Simulate work
			time.Sleep(time.Millisecond)
			atomic.AddInt64(&current, -1)
		}()
	}

	wg.Wait()

	// Max concurrent should never exceed limit
	if maxConcurrent > 4 {
		t.Errorf("Max concurrent = %d, should not exceed 4", maxConcurrent)
	}
}

// TestRequestSemaphore_Statistics runs the test request semaphore_statistics operation.
func TestRequestSemaphore_Statistics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  2,
		AcquireTimeout: time.Second,
		Enabled:        true,
	}
	sem := NewRequestSemaphore(cfg)

	ctx := context.Background()

	// Perform some acquires
	for i := 0; i < 5; i++ {
		if err := sem.Acquire(ctx); err != nil {
			t.Fatalf("Acquire() error = %v", err)
		}
		sem.Release()
	}

	stats := sem.Stats()

	if stats.TotalAcquires != 5 {
		t.Errorf("TotalAcquires = %d, want 5", stats.TotalAcquires)
	}

	if stats.TotalReleases != 5 {
		t.Errorf("TotalReleases = %d, want 5", stats.TotalReleases)
	}

	if stats.CurrentActive != 0 {
		t.Errorf("CurrentActive = %d, want 0", stats.CurrentActive)
	}
}

// TestRequestSemaphore_DisabledBypassesLimiting runs the test request semaphore_disabled bypasses limiting operation.
func TestRequestSemaphore_DisabledBypassesLimiting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  1,
		AcquireTimeout: time.Second,
		Enabled:        false, // Disabled!
	}
	sem := NewRequestSemaphore(cfg)

	ctx := context.Background()

	// Even with MaxConcurrent=1, multiple acquires should succeed when disabled
	for i := 0; i < 10; i++ {
		if err := sem.Acquire(ctx); err != nil {
			t.Errorf("Acquire() should succeed when disabled, got error = %v", err)
		}
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

// BenchmarkRequestSemaphore_AcquireRelease runs the benchmark request semaphore_acquire release operation.
func BenchmarkRequestSemaphore_AcquireRelease(b *testing.B) {
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  runtime.GOMAXPROCS(0),
		AcquireTimeout: time.Second,
		Enabled:        true,
	}
	sem := NewRequestSemaphore(cfg)
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = sem.Acquire(ctx)
			sem.Release()
		}
	})
}

// BenchmarkRequestSemaphore_TryAcquire runs the benchmark request semaphore_try acquire operation.
func BenchmarkRequestSemaphore_TryAcquire(b *testing.B) {
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  runtime.GOMAXPROCS(0),
		AcquireTimeout: time.Second,
		Enabled:        true,
	}
	sem := NewRequestSemaphore(cfg)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if sem.TryAcquire() {
				sem.Release()
			}
		}
	})
}

// BenchmarkRequestSemaphore_Disabled runs the benchmark request semaphore_disabled operation.
func BenchmarkRequestSemaphore_Disabled(b *testing.B) {
	cfg := RequestSemaphoreConfig{
		MaxConcurrent:  1,
		AcquireTimeout: time.Second,
		Enabled:        false,
	}
	sem := NewRequestSemaphore(cfg)
	ctx := context.Background()

	b.ResetTimer()
	for b.Loop() {
		_ = sem.Acquire(ctx)
		sem.Release()
	}
}
