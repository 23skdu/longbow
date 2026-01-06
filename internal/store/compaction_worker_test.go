package store


import (
	"testing"
	"time"
)

// TestNewCompactionWorker verifies worker creation
func TestNewCompactionWorker(t *testing.T) {
	cfg := DefaultCompactionConfig()
	worker := NewCompactionWorker(nil, cfg)
	if worker == nil {
		t.Fatal("expected non-nil worker")
	}
	if worker.config.TargetBatchSize != cfg.TargetBatchSize {
		t.Errorf("config not set correctly")
	}
}

// TestCompactionWorkerStartStop verifies lifecycle
func TestCompactionWorkerStartStop(t *testing.T) {
	cfg := CompactionConfig{
		TargetBatchSize:     1000,
		MinBatchesToCompact: 5,
		CompactionInterval:  10 * time.Millisecond,
		Enabled:             true,
	}
	worker := NewCompactionWorker(nil, cfg)

	// Start should not block
	worker.Start()
	if !worker.IsRunning() {
		t.Error("worker should be running after Start")
	}

	// Stop should be clean
	worker.Stop()
	if worker.IsRunning() {
		t.Error("worker should not be running after Stop")
	}
}

// TestCompactionWorkerDisabled verifies disabled config doesn't start
func TestCompactionWorkerDisabled(t *testing.T) {
	cfg := DefaultCompactionConfig()
	cfg.Enabled = false
	worker := NewCompactionWorker(nil, cfg)

	worker.Start()
	// Should not panic or error, just no-op
	if worker.IsRunning() {
		t.Error("disabled worker should not be running")
	}
	worker.Stop() // Should be safe to call
}

// TestCompactionWorkerMultipleStartStop verifies idempotency
func TestCompactionWorkerMultipleStartStop(t *testing.T) {
	cfg := CompactionConfig{
		TargetBatchSize:     1000,
		MinBatchesToCompact: 5,
		CompactionInterval:  10 * time.Millisecond,
		Enabled:             true,
	}
	worker := NewCompactionWorker(nil, cfg)

	// Multiple starts should be safe
	worker.Start()
	worker.Start()
	if !worker.IsRunning() {
		t.Error("worker should be running")
	}

	// Multiple stops should be safe
	worker.Stop()
	worker.Stop()
	if worker.IsRunning() {
		t.Error("worker should not be running")
	}
}

// TestCompactionWorkerStats verifies statistics tracking
func TestCompactionWorkerStats(t *testing.T) {
	cfg := DefaultCompactionConfig()
	worker := NewCompactionWorker(nil, cfg)

	stats := worker.Stats()
	if stats.CompactionsRun != 0 {
		t.Error("initial compactions should be 0")
	}
	if stats.BatchesMerged != 0 {
		t.Error("initial batches merged should be 0")
	}
}
