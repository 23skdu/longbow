package profiling

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProfiler runs the test profiler operation.
func TestProfiler(t *testing.T) {
	logger := zerolog.New(io.Discard)
	tmpDir, err := os.MkdirTemp("", "profiler-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := ProfilerConfig{
		ProfileDir:      tmpDir,
		CPUProfile:      true,
		MemProfile:      true,
		ProfileInterval: 100 * time.Millisecond,
	}

	p := NewProfiler(config, &logger)
	require.NotNil(t, p)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Test Start/Stop
	err = p.Start(ctx)
	require.NoError(t, err)

	// Test double start
	err = p.Start(ctx)
	require.Error(t, err)

	// Wait for a collection to happen (manually trigger it to avoid waiting)
	p.collectProfiles()

	err = p.Stop()
	require.NoError(t, err)

	// Test double stop
	err = p.Stop()
	require.Error(t, err)

	// Verify files created
	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(entries), 2, "Should have at least CPU and Mem profile")
}

// TestProfiler_WriteProfile runs the test profiler_write profile operation.
func TestProfiler_WriteProfile(t *testing.T) {
	logger := zerolog.New(io.Discard)
	p := NewProfiler(ProfilerConfig{}, &logger)

	types := []string{"heap", "goroutine", "threadcreate", "block", "mutex"}
	for _, pt := range types {
		t.Run(pt, func(t *testing.T) {
			err := p.WriteProfile(pt, io.Discard)
			assert.NoError(t, err)
		})
	}

	err := p.WriteProfile("unknown", io.Discard)
	assert.Error(t, err)
}

// TestGPURecorder runs the test gpu recorder operation.
func TestGPURecorder(t *testing.T) {
	g := NewGPURecorder(5)
	require.NotNil(t, g)

	g.Record("kernel1", 100, 10, 0.5)
	g.Record("kernel2", 200, 20, 0.6)
	g.Record("kernel3", 300, 30, 0.7)
	g.Record("kernel4", 400, 40, 0.8)
	g.Record("kernel5", 500, 50, 0.9)
	g.Record("kernel6", 600, 60, 1.0) // Should drop kernel1

	records := g.GetRecords()
	assert.Equal(t, 5, len(records))
	assert.Equal(t, "kernel2", records[0].KernelName)
	assert.Equal(t, "kernel6", records[4].KernelName)
}

// TestFlameGraphGenerator runs the test flame graph generator operation.
func TestFlameGraphGenerator(t *testing.T) {
	f := NewFlameGraphGenerator()
	require.NotNil(t, f)

	// This is hard to test fully without valid profile data,
	// but we can test the error path or a very minimal mock if possible.
	// For now, just ensure it doesn't panic on empty/invalid data.
	_, err := f.GenerateFlamegraph([]byte("invalid"))
	// It will likely fail when calling pprof
	assert.Error(t, err)
}
