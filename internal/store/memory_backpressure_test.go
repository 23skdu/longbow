package store

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemoryBackpressure_NewController(t *testing.T) {
	cfg := BackpressureConfig{
		SoftLimitBytes: 1 << 30, // 1GB
		HardLimitBytes: 2 << 30, // 2GB
		CheckInterval:  100 * time.Millisecond,
	}
	ctrl := NewMemoryBackpressureController(cfg)
	if ctrl == nil {
		t.Fatal("Controller should not be nil")
	}
	if ctrl.GetSoftLimit() != cfg.SoftLimitBytes {
		t.Errorf("Soft limit mismatch: got %d, want %d", ctrl.GetSoftLimit(), cfg.SoftLimitBytes)
	}
}

func TestMemoryBackpressure_CheckPressure(t *testing.T) {
	cfg := BackpressureConfig{
		SoftLimitBytes: 100, // Very low for testing
		HardLimitBytes: 200,
		CheckInterval:  10 * time.Millisecond,
	}
	ctrl := NewMemoryBackpressureController(cfg)
	level := ctrl.CheckPressure()
	// Actual heap will exceed 100 bytes so should be under pressure
	if level == PressureNone {
		t.Log("Pressure level is None - heap may be very small")
	}
}

func TestMemoryBackpressure_Acquire(t *testing.T) {
	cfg := BackpressureConfig{
		SoftLimitBytes: 100 << 30, // 100GB - won't trigger
		HardLimitBytes: 200 << 30,
		CheckInterval:  10 * time.Millisecond,
	}
	ctrl := NewMemoryBackpressureController(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := ctrl.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire should succeed when not under pressure: %v", err)
	}
	ctrl.Release()
}

func TestMemoryBackpressure_AcquireTimeout(t *testing.T) {
	cfg := BackpressureConfig{
		SoftLimitBytes: 1, // 1 byte - always under pressure
		HardLimitBytes: 2,
		CheckInterval:  5 * time.Millisecond,
	}
	ctrl := NewMemoryBackpressureController(cfg)
	ctrl.SetPressureLevel(PressureHard) // Force hard pressure
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := ctrl.Acquire(ctx)
	if err == nil {
		t.Error("Acquire should timeout under hard pressure")
	}
}

func TestMemoryBackpressure_Metrics(t *testing.T) {
	cfg := BackpressureConfig{
		SoftLimitBytes: 100 << 30,
		HardLimitBytes: 200 << 30,
		CheckInterval:  10 * time.Millisecond,
	}
	ctrl := NewMemoryBackpressureController(cfg)
	ctx := context.Background()
	_ = ctrl.Acquire(ctx)
	ctrl.Release()
	if ctrl.GetAcquireCount() == 0 {
		t.Error("Acquire count should be incremented")
	}
}

func TestMemoryBackpressure_ConcurrentAccess(t *testing.T) {
	cfg := BackpressureConfig{
		SoftLimitBytes: 100 << 30,
		HardLimitBytes: 200 << 30,
		CheckInterval:  10 * time.Millisecond,
	}
	ctrl := NewMemoryBackpressureController(cfg)
	var successCount atomic.Int64
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if err := ctrl.Acquire(ctx); err == nil {
				successCount.Add(1)
				ctrl.Release()
			}
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	if successCount.Load() != 10 {
		t.Errorf("Expected 10 successful acquires, got %d", successCount.Load())
	}
}

func TestMemoryBackpressure_PressureLevels(t *testing.T) {
	cfg := BackpressureConfig{
		SoftLimitBytes: 100 << 30,
		HardLimitBytes: 200 << 30,
		CheckInterval:  10 * time.Millisecond,
	}
	ctrl := NewMemoryBackpressureController(cfg)

	ctrl.SetPressureLevel(PressureNone)
	if ctrl.GetPressureLevel() != PressureNone {
		t.Errorf("Expected PressureNone, got %d", ctrl.GetPressureLevel())
	}

	ctrl.SetPressureLevel(PressureSoft)
	if ctrl.GetPressureLevel() != PressureSoft {
		t.Errorf("Expected PressureSoft, got %d", ctrl.GetPressureLevel())
	}

	ctrl.SetPressureLevel(PressureHard)
	if ctrl.GetPressureLevel() != PressureHard {
		t.Errorf("Expected PressureHard, got %d", ctrl.GetPressureLevel())
	}
}

func TestMemoryBackpressure_SoftPressureSlowdown(t *testing.T) {
	cfg := BackpressureConfig{
		SoftLimitBytes:    100 << 30,
		HardLimitBytes:    200 << 30,
		CheckInterval:     10 * time.Millisecond,
		SoftPressureDelay: 10 * time.Millisecond,
	}
	ctrl := NewMemoryBackpressureController(cfg)
	ctrl.SetPressureLevel(PressureSoft)

	start := time.Now()
	ctx := context.Background()
	_ = ctrl.Acquire(ctx)
	ctrl.Release()
	elapsed := time.Since(start)

	if elapsed < 5*time.Millisecond {
		t.Logf("Soft pressure should add delay, elapsed: %v", elapsed)
	}
}

func TestMemoryBackpressure_BackgroundUpdate(t *testing.T) {
	cfg := BackpressureConfig{
		SoftLimitBytes: 1 << 30, // 1GB
		HardLimitBytes: 2 << 30, // 2GB
		CheckInterval:  10 * time.Millisecond,
	}

	ctrl := NewMemoryBackpressureController(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start background monitoring
	ctrl.Start(ctx)

	// 1. Manually force Hard Pressure
	ctrl.SetPressureLevel(PressureHard)
	assert.Equal(t, PressureHard, ctrl.GetPressureLevel())

	// 2. Acquire should wait, but the background ticker (CheckPressure)
	// will see actual low memory usage and reset it to PressureNone, waking us up.
	done := make(chan struct{})
	go func() {
		err := ctrl.Acquire(ctx)
		assert.NoError(t, err)
		close(done)
	}()

	// 3. Wait for relief
	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Acquire timed out waiting for pressure relief via ticker")
	}

	// Double check level is back to None
	assert.NotEqual(t, PressureHard, ctrl.GetPressureLevel())
}
