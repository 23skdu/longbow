package profiling

import (
	"bytes"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTestLogger() zerolog.Logger {
	return zerolog.New(bytes.NewBuffer(nil))
}

// TestNewMemoryLeakDetector runs the test new memory leak detector operation.
func TestNewMemoryLeakDetector(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	require.NotNil(t, mld)
}

// TestMemoryLeakDetector_SetAlertCallback runs the test memory leak detector_set alert callback operation.
func TestMemoryLeakDetector_SetAlertCallback(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	called := false
	mld.SetAlertCallback(func(r *LeakReport) {
		called = true
	})
	// Callback set, not called yet
	_ = called
}

// TestMemoryLeakDetector_CaptureBaseline runs the test memory leak detector_capture baseline operation.
func TestMemoryLeakDetector_CaptureBaseline(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	// Should not panic
	assert.NotPanics(t, func() {
		mld.CaptureBaseline()
	})
}

// TestMemoryLeakDetector_StartStop runs the test memory leak detector_start stop operation.
func TestMemoryLeakDetector_StartStop(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 10, 50*time.Millisecond)
	mld.CaptureBaseline()
	mld.Start()
	// Give it a brief moment to start
	time.Sleep(10 * time.Millisecond)
	mld.Stop()
	// Double stop should be safe
	mld.Stop()
}

// TestMemoryLeakDetector_StartTwice runs the test memory leak detector_start twice operation.
func TestMemoryLeakDetector_StartTwice(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 10, 50*time.Millisecond)
	mld.Start()
	mld.Start() // second start should be no-op
	mld.Stop()
}

// TestMemoryLeakDetector_GetCurrentStats runs the test memory leak detector_get current stats operation.
func TestMemoryLeakDetector_GetCurrentStats(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	mld.CaptureBaseline()
	heapAlloc, heapObjects, goroutines := mld.GetCurrentStats()
	assert.Greater(t, heapAlloc, uint64(0))
	_ = heapObjects
	_ = goroutines
}

// TestMemoryLeakDetector_WriteHeapProfile runs the test memory leak detector_write heap profile operation.
func TestMemoryLeakDetector_WriteHeapProfile(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	var buf bytes.Buffer
	err := mld.WriteHeapProfile(&buf)
	assert.NoError(t, err)
}

// TestMemoryLeakDetector_WriteGoroutineProfile runs the test memory leak detector_write goroutine profile operation.
func TestMemoryLeakDetector_WriteGoroutineProfile(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	var buf bytes.Buffer
	err := mld.WriteGoroutineProfile(&buf)
	assert.NoError(t, err)
}

// TestMemoryLeakDetector_WriteThreadCreateProfile runs the test memory leak detector_write thread create profile operation.
func TestMemoryLeakDetector_WriteThreadCreateProfile(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	var buf bytes.Buffer
	err := mld.WriteThreadCreateProfile(&buf)
	assert.NoError(t, err)
}

// TestMemoryLeakDetector_WriteBlockProfile runs the test memory leak detector_write block profile operation.
func TestMemoryLeakDetector_WriteBlockProfile(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	var buf bytes.Buffer
	err := mld.WriteBlockProfile(&buf)
	assert.NoError(t, err)
}

// TestMemoryLeakDetector_WriteMutexProfile runs the test memory leak detector_write mutex profile operation.
func TestMemoryLeakDetector_WriteMutexProfile(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	var buf bytes.Buffer
	err := mld.WriteMutexProfile(&buf)
	assert.NoError(t, err)
}

// TestMemoryLeakDetector_GetSnapshots_Empty runs the test memory leak detector_get snapshots_empty operation.
func TestMemoryLeakDetector_GetSnapshots_Empty(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	snaps := mld.GetSnapshots()
	assert.Empty(t, snaps)
}

// TestMemoryLeakDetector_GetSnapshots_AfterBaseline runs the test memory leak detector_get snapshots_after baseline operation.
func TestMemoryLeakDetector_GetSnapshots_AfterBaseline(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	mld.CaptureBaseline()
	snaps := mld.GetSnapshots()
	assert.Len(t, snaps, 1)
}

// TestDefaultLeakDetectorConfig runs the test default leak detector config operation.
func TestDefaultLeakDetectorConfig(t *testing.T) {
	cfg := DefaultLeakDetectorConfig()
	require.NotNil(t, cfg)
	assert.Greater(t, cfg.ThresholdMB, 0)
	assert.Greater(t, cfg.CheckInterval, time.Duration(0))
}

// TestNewLeakDetectorManager runs the test new leak detector manager operation.
func TestNewLeakDetectorManager(t *testing.T) {
	logger := makeTestLogger()
	mgr := NewLeakDetectorManager(logger, DefaultLeakDetectorConfig())
	require.NotNil(t, mgr)
}

// TestLeakDetectorManager_StartStop runs the test leak detector manager_start stop operation.
func TestLeakDetectorManager_StartStop(t *testing.T) {
	logger := makeTestLogger()
	cfg := DefaultLeakDetectorConfig()
	cfg.CheckInterval = 50 * time.Millisecond
	mgr := NewLeakDetectorManager(logger, cfg)
	mgr.Start()
	time.Sleep(10 * time.Millisecond)
	mgr.Stop()
}

// TestLeakDetectorManager_CaptureProfiles runs the test leak detector manager_capture profiles operation.
func TestLeakDetectorManager_CaptureProfiles(t *testing.T) {
	logger := makeTestLogger()
	mgr := NewLeakDetectorManager(logger, DefaultLeakDetectorConfig())
	assert.NotPanics(t, func() {
		_ = mgr.CaptureProfiles()
	})
}

// TestLeakDetectorManager_GetStats runs the test leak detector manager_get stats operation.
func TestLeakDetectorManager_GetStats(t *testing.T) {
	logger := makeTestLogger()
	mgr := NewLeakDetectorManager(logger, DefaultLeakDetectorConfig())
	heapAlloc, heapObjects, goroutines := mgr.GetStats()
	_ = heapAlloc
	_ = heapObjects
	_ = goroutines
}
