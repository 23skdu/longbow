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

func TestNewMemoryLeakDetector(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	require.NotNil(t, mld)
}

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

func TestMemoryLeakDetector_CaptureBaseline(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	// Should not panic
	assert.NotPanics(t, func() {
		mld.CaptureBaseline()
	})
}

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

func TestMemoryLeakDetector_StartTwice(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 10, 50*time.Millisecond)
	mld.Start()
	mld.Start() // second start should be no-op
	mld.Stop()
}

func TestMemoryLeakDetector_GetCurrentStats(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	mld.CaptureBaseline()
	heapAlloc, heapObjects, goroutines := mld.GetCurrentStats()
	assert.Greater(t, heapAlloc, uint64(0))
	_ = heapObjects
	_ = goroutines
}

func TestMemoryLeakDetector_WriteHeapProfile(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	var buf bytes.Buffer
	err := mld.WriteHeapProfile(&buf)
	assert.NoError(t, err)
}

func TestMemoryLeakDetector_WriteGoroutineProfile(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	var buf bytes.Buffer
	err := mld.WriteGoroutineProfile(&buf)
	assert.NoError(t, err)
}

func TestMemoryLeakDetector_WriteThreadCreateProfile(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	var buf bytes.Buffer
	err := mld.WriteThreadCreateProfile(&buf)
	assert.NoError(t, err)
}

func TestMemoryLeakDetector_WriteBlockProfile(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	var buf bytes.Buffer
	err := mld.WriteBlockProfile(&buf)
	assert.NoError(t, err)
}

func TestMemoryLeakDetector_WriteMutexProfile(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	var buf bytes.Buffer
	err := mld.WriteMutexProfile(&buf)
	assert.NoError(t, err)
}

func TestMemoryLeakDetector_GetSnapshots_Empty(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	snaps := mld.GetSnapshots()
	assert.Empty(t, snaps)
}

func TestMemoryLeakDetector_GetSnapshots_AfterBaseline(t *testing.T) {
	logger := makeTestLogger()
	mld := NewMemoryLeakDetector(logger, 100, 100*time.Millisecond)
	mld.CaptureBaseline()
	snaps := mld.GetSnapshots()
	assert.Len(t, snaps, 1)
}

func TestDefaultLeakDetectorConfig(t *testing.T) {
	cfg := DefaultLeakDetectorConfig()
	require.NotNil(t, cfg)
	assert.Greater(t, cfg.ThresholdMB, 0)
	assert.Greater(t, cfg.CheckInterval, time.Duration(0))
}

func TestNewLeakDetectorManager(t *testing.T) {
	logger := makeTestLogger()
	mgr := NewLeakDetectorManager(logger, DefaultLeakDetectorConfig())
	require.NotNil(t, mgr)
}

func TestLeakDetectorManager_StartStop(t *testing.T) {
	logger := makeTestLogger()
	cfg := DefaultLeakDetectorConfig()
	cfg.CheckInterval = 50 * time.Millisecond
	mgr := NewLeakDetectorManager(logger, cfg)
	mgr.Start()
	time.Sleep(10 * time.Millisecond)
	mgr.Stop()
}

func TestLeakDetectorManager_CaptureProfiles(t *testing.T) {
	logger := makeTestLogger()
	mgr := NewLeakDetectorManager(logger, DefaultLeakDetectorConfig())
	assert.NotPanics(t, func() {
		mgr.CaptureProfiles()
	})
}

func TestLeakDetectorManager_GetStats(t *testing.T) {
	logger := makeTestLogger()
	mgr := NewLeakDetectorManager(logger, DefaultLeakDetectorConfig())
	heapAlloc, heapObjects, goroutines := mgr.GetStats()
	_ = heapAlloc
	_ = heapObjects
	_ = goroutines
}
