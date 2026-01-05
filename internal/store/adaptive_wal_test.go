package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveWAL_Calculator(t *testing.T) {
	cfg := NewAdaptiveWALConfig()
	cfg.MinInterval = 1 * time.Millisecond
	cfg.MaxInterval = 100 * time.Millisecond
	calc := NewAdaptiveIntervalCalculator(cfg)

	t.Run("ZeroRate", func(t *testing.T) {
		assert.Equal(t, 100*time.Millisecond, calc.CalculateInterval(0))
	})

	t.Run("HighRate", func(t *testing.T) {
		// Rate of 1000 writes/sec should significantly reduce interval
		interval := calc.CalculateInterval(1000)
		assert.True(t, interval < 100*time.Millisecond)
		assert.True(t, interval >= 1*time.Millisecond)
	})

	t.Run("ExtremeRate", func(t *testing.T) {
		// Rate of 1,000,000 writes/sec should hit MinInterval
		interval := calc.CalculateInterval(1000000)
		assert.Equal(t, 1*time.Millisecond, interval)
	})
}

func TestAdaptiveWAL_Tracker(t *testing.T) {
	tracker := NewWriteRateTracker(1 * time.Second)

	// Initially zero
	assert.Equal(t, 0.0, tracker.GetRate())

	// Record some writes
	for i := 0; i < 100; i++ {
		tracker.RecordWrite()
	}

	// We need to wait at least 10ms for rate update trigger
	time.Sleep(15 * time.Millisecond)
	tracker.RecordWrite() // Trigger update

	rate := tracker.GetRate()
	assert.True(t, rate > 0)
}

func TestWALBatcher_FlushLogic(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()
	cfg := DefaultWALBatcherConfig()
	cfg.FlushInterval = 50 * time.Millisecond
	cfg.MaxBatchSize = 10

	batcher := NewWALBatcher(tmpDir, &cfg)
	err := batcher.Start()
	require.NoError(t, err)
	defer func() { _ = batcher.Stop() }()

	schema := arrow.NewSchema([]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewInt32Builder(mem)
	b.Append(1)
	rec := array.NewRecordBatch(schema, []arrow.Array{b.NewArray()}, 1)
	defer rec.Release()

	t.Run("SizeBasedFlush", func(t *testing.T) {
		// Write MaxBatchSize entries
		for i := 0; i < 10; i++ {
			err := batcher.Write(rec, "test", uint64(i+1), time.Now().UnixNano())
			require.NoError(t, err)
		}

		// It should flush immediately (or very quickly)
		// We'll wait a bit and check file size
		time.Sleep(20 * time.Millisecond)

		walPath := filepath.Join(tmpDir, walFileName)
		info, err := os.Stat(walPath)
		require.NoError(t, err)
		assert.True(t, info.Size() > 0)
	})

	t.Run("TimeBasedFlush", func(t *testing.T) {
		// Reset file (actually batcher appends, so we just check it increases)
		walPath := filepath.Join(tmpDir, walFileName)
		initialInfo, _ := os.Stat(walPath)
		initialSize := initialInfo.Size()

		// Write only 1 entry (less than MaxBatchSize)
		err := batcher.Write(rec, "test_time", 100, time.Now().UnixNano())
		require.NoError(t, err)

		// Should not be flushed yet
		info, _ := os.Stat(walPath)
		assert.Equal(t, initialSize, info.Size())

		// Wait for FlushInterval
		time.Sleep(100 * time.Millisecond)

		info, _ = os.Stat(walPath)
		assert.True(t, info.Size() > initialSize)
	})
}
