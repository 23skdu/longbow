package memory

import (
	"strings"
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedArena_NilErrorMetric_AllocSlice verifies that the
// `longbow_arena_nil_error_total{method="AllocSlice"}` counter increments
// each time AllocSlice encounters a nil Slab pointer.  This is the
// regression signal for the P0 `arena is nil` bug fixed in commit
// a2f535ef — the counter must stay at 0 in healthy operation.
//
// The test simulates the bug by storing nil directly into the
// unexported arena pointer (whitebox; this is the only way to exercise
// the error path because the public NewTypedArena always stores a
// non-nil Slab).
func TestTypedArena_NilErrorMetric_AllocSlice(t *testing.T) {
	before := testutil.ToFloat64(metrics.ArenaNilErrorTotal.WithLabelValues("AllocSlice"))

	slab := NewSlabArena(1024 * 1024)
	ta := NewTypedArena[float32](slab)
	ta.arena.Store(nil) // simulate the pre-a2f535ef Release() bug

	_, err := ta.AllocSlice(16)
	require.Error(t, err, "AllocSlice must return an error when the Slab is nil")
	assert.True(t, strings.Contains(err.Error(), "arena is nil"),
		"error must contain the canonical 'arena is nil' string, got: %v", err)

	after := testutil.ToFloat64(metrics.ArenaNilErrorTotal.WithLabelValues("AllocSlice"))
	assert.Equal(t, before+1, after,
		"ArenaNilErrorTotal{method=AllocSlice} must increment by exactly 1")
}

// TestTypedArena_NilErrorMetric_AllocSliceDirty is the AllocSliceDirty
// counterpart of TestTypedArena_NilErrorMetric_AllocSlice.
func TestTypedArena_NilErrorMetric_AllocSliceDirty(t *testing.T) {
	before := testutil.ToFloat64(metrics.ArenaNilErrorTotal.WithLabelValues("AllocSliceDirty"))

	slab := NewSlabArena(1024 * 1024)
	ta := NewTypedArena[float32](slab)
	ta.arena.Store(nil)

	_, err := ta.AllocSliceDirty(16)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "arena is nil"))

	after := testutil.ToFloat64(metrics.ArenaNilErrorTotal.WithLabelValues("AllocSliceDirty"))
	assert.Equal(t, before+1, after,
		"ArenaNilErrorTotal{method=AllocSliceDirty} must increment by exactly 1")
}

// TestTypedArena_NilErrorMetric_AllocSliceAligned is the
// AllocSliceAligned counterpart of TestTypedArena_NilErrorMetric_AllocSlice.
func TestTypedArena_NilErrorMetric_AllocSliceAligned(t *testing.T) {
	before := testutil.ToFloat64(metrics.ArenaNilErrorTotal.WithLabelValues("AllocSliceAligned"))

	slab := NewSlabArena(1024 * 1024)
	ta := NewTypedArena[float32](slab)
	ta.arena.Store(nil)

	_, err := ta.AllocSliceAligned(16, 32)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "arena is nil"))

	after := testutil.ToFloat64(metrics.ArenaNilErrorTotal.WithLabelValues("AllocSliceAligned"))
	assert.Equal(t, before+1, after,
		"ArenaNilErrorTotal{method=AllocSliceAligned} must increment by exactly 1")
}

// TestTypedArena_NilErrorMetric_HealthyOperationStaysAtZero runs a happy-path
// AllocSlice sequence and asserts that the counter does NOT increment
// (i.e. the metric is silent in normal operation, so a non-zero value
// in production is a strong signal of regression).
func TestTypedArena_NilErrorMetric_HealthyOperationStaysAtZero(t *testing.T) {
	slab := NewSlabArena(1024 * 1024)
	ta := NewTypedArena[float32](slab)

	beforeAlloc := testutil.ToFloat64(metrics.ArenaNilErrorTotal.WithLabelValues("AllocSlice"))
	beforeDirty := testutil.ToFloat64(metrics.ArenaNilErrorTotal.WithLabelValues("AllocSliceDirty"))
	beforeAligned := testutil.ToFloat64(metrics.ArenaNilErrorTotal.WithLabelValues("AllocSliceAligned"))

	for i := 0; i < 100; i++ {
		_, err := ta.AllocSlice(16)
		require.NoError(t, err)
		_, err = ta.AllocSliceDirty(16)
		require.NoError(t, err)
		_, err = ta.AllocSliceAligned(16, 32)
		require.NoError(t, err)
	}

	afterAlloc := testutil.ToFloat64(metrics.ArenaNilErrorTotal.WithLabelValues("AllocSlice"))
	afterDirty := testutil.ToFloat64(metrics.ArenaNilErrorTotal.WithLabelValues("AllocSliceDirty"))
	afterAligned := testutil.ToFloat64(metrics.ArenaNilErrorTotal.WithLabelValues("AllocSliceAligned"))

	assert.Equal(t, beforeAlloc, afterAlloc,
		"ArenaNilErrorTotal{method=AllocSlice} must stay at 0 during healthy operation")
	assert.Equal(t, beforeDirty, afterDirty,
		"ArenaNilErrorTotal{method=AllocSliceDirty} must stay at 0 during healthy operation")
	assert.Equal(t, beforeAligned, afterAligned,
		"ArenaNilErrorTotal{method=AllocSliceAligned} must stay at 0 during healthy operation")
}
