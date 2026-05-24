package store

import (
	"context"
	"reflect"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/23skdu/longbow/internal/autoscale"
	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Helpers to set private/unexported fields using unsafe pointers
func setPrivateDuration(obj interface{}, name string, d time.Duration) {
	v := reflect.ValueOf(obj).Elem().FieldByName(name)
	ptr := (*time.Duration)(unsafe.Pointer(v.UnsafeAddr()))
	*ptr = d
}

func setPrivateAtomicUint64(obj interface{}, name string, val uint64) {
	v := reflect.ValueOf(obj).Elem().FieldByName(name)
	ptr := (*atomic.Uint64)(unsafe.Pointer(v.UnsafeAddr()))
	ptr.Store(val)
}

func TestAdmissionController(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	basePhys := int64(m.HeapAlloc) + lbmem.GetGlobalOffHeapAllocated()
	if basePhys < 10*1024*1024 {
		basePhys = 10 * 1024 * 1024
	}

	maxMem := atomic.Int64{}
	maxMem.Store(basePhys * 10) // 10x base memory
	currMem := atomic.Int64{}

	ac := NewAdmissionController(&maxMem, &currMem, nil, zerolog.Nop())

	t.Run("Normal Load", func(t *testing.T) {
		currMem.Store(basePhys) // 10%
		err := ac.Admit(context.Background(), "search")
		assert.NoError(t, err)
	})

	t.Run("Throttled Ingestion", func(t *testing.T) {
		// Set currMem above 90% (e.g. 92%) so it dominates basePhys
		currMem.Store(maxMem.Load() * 92 / 100)
		err := ac.Admit(context.Background(), "ingest")
		assert.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("Rejected Search", func(t *testing.T) {
		// Set currMem above 94% (e.g. 96%) so it dominates basePhys
		currMem.Store(maxMem.Load() * 96 / 100)
		err := ac.Admit(context.Background(), "search")
		assert.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("WAL Replay & Sharding Throttling", func(t *testing.T) {
		maxMem.Store(basePhys * 10)
		currMem.Store(100)
		ac.scaler = nil // disable autoscaler for this test

		// Enable WAL Replay
		ac.SetWALReplay(true)
		assert.True(t, ac.IsWALReplay())

		// Acquire 2 slots (the limit querySem buffer is 2)
		err1 := ac.Admit(context.Background(), "search")
		assert.NoError(t, err1)

		err2 := ac.Admit(context.Background(), "search")
		assert.NoError(t, err2)

		// Third search should be throttled because capacity is 2
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // trigger immediate cancel to speed up the test
		err3 := ac.Admit(ctx, "search")
		assert.Error(t, err3)

		// Release one slot
		ac.Release("search")

		// Now we should be able to acquire again
		err4 := ac.Admit(context.Background(), "search")
		assert.NoError(t, err4)

		// Disable WAL replay
		ac.SetWALReplay(false)
		ac.Release("search")
		ac.Release("search")
	})
}

func TestAdmissionController_Expanded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	maxMem := atomic.Int64{}
	currMem := atomic.Int64{}
	ac := NewAdmissionController(&maxMem, &currMem, nil, zerolog.Nop())

	t.Run("Tuner utilization overrides memoryUsage", func(t *testing.T) {
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)
		currPhys := int64(m2.HeapAlloc) + lbmem.GetGlobalOffHeapAllocated()
		if currPhys < 10*1024*1024 {
			currPhys = 10 * 1024 * 1024
		}
		maxMem.Store(currPhys * 10)
		currMem.Store(currPhys) // 10% of maxMem

		logger := zerolog.Nop()
		tuner := lbmem.NewGCTuner(maxMem.Load(), 100, 10, &logger)
		tuner.EnableGPUTuning = false
		tuner.GetPhysicalStats = func() (int64, int64) {
			return maxMem.Load() * 85 / 100, 0
		}

		// Set lastUtilization using direct unsafe cast
		setPrivateAtomicUint64(tuner, "lastUtilization", uint64(920)) // 92% (above 90% soft limit)

		ac.SetTuner(tuner)
		defer ac.SetTuner(nil)

		err := ac.Admit(context.Background(), "ingest")
		assert.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("LONGBOW_MAX_MEMORY_HARD check", func(t *testing.T) {
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)
		currPhys := int64(m2.HeapAlloc) + lbmem.GetGlobalOffHeapAllocated()
		if currPhys < 10*1024*1024 {
			currPhys = 10 * 1024 * 1024
		}
		hardLimit := currPhys * 5
		t.Setenv("LONGBOW_MAX_MEMORY_HARD", strconv.FormatInt(hardLimit, 10))

		maxMem2 := atomic.Int64{}
		maxMem2.Store(currPhys * 10)
		currMem2 := atomic.Int64{}

		ac2 := NewAdmissionController(&maxMem2, &currMem2, nil, zerolog.Nop())
		require.Equal(t, hardLimit, ac2.hardMemory)

		// normal (currPhys * 4 <= currPhys * 5) -> allowed
		currMem2.Store(currPhys * 4)
		err := ac2.Admit(context.Background(), "search")
		assert.NoError(t, err)

		// breached (currPhys * 6 > currPhys * 5) -> rejected
		currMem2.Store(currPhys * 6)
		errIn := ac2.Admit(context.Background(), "ingest")
		assert.Error(t, errIn)
		assert.Equal(t, codes.ResourceExhausted, status.Code(errIn))

		// maintenance/delete/drop -> allowed
		errM := ac2.Admit(context.Background(), "maintenance")
		assert.NoError(t, errM)
		errDel := ac2.Admit(context.Background(), "delete")
		assert.NoError(t, errDel)
		errDrop := ac2.Admit(context.Background(), "drop")
		assert.NoError(t, errDrop)
	})

	t.Run("NewAdmissionController invalid environment variable", func(t *testing.T) {
		t.Setenv("LONGBOW_MAX_MEMORY_HARD", "invalid_value")
		acInvalid := NewAdmissionController(&maxMem, &currMem, nil, zerolog.Nop())
		assert.Equal(t, int64(0), acInvalid.hardMemory)
	})

	t.Run("maxMem <= 0 bypass", func(t *testing.T) {
		maxMemZero := atomic.Int64{}
		acZero := NewAdmissionController(&maxMemZero, &currMem, nil, zerolog.Nop())
		err := acZero.Admit(context.Background(), "ingest")
		assert.NoError(t, err)
	})

	t.Run("MigrationStarted and MigrationFinished", func(t *testing.T) {
		assert.Equal(t, int32(0), ac.migratingCount.Load())
		ac.MigrationStarted()
		assert.Equal(t, int32(1), ac.migratingCount.Load())
		ac.MigrationFinished()
		assert.Equal(t, int32(0), ac.migratingCount.Load())
	})

	t.Run("Tighter memory limits during active migration", func(t *testing.T) {
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)
		currPhys := int64(m2.HeapAlloc) + lbmem.GetGlobalOffHeapAllocated()
		if currPhys < 10*1024*1024 {
			currPhys = 10 * 1024 * 1024
		}
		maxMem.Store(currPhys * 10)
		ac.MigrationStarted()
		defer ac.MigrationFinished()

		// 86% is above 85% ingestLimit during migration -> should be throttled
		currMem.Store(maxMem.Load() * 86 / 100)
		err := ac.Admit(context.Background(), "ingest")
		assert.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))

		// 89% is above 88% hardLimit during migration -> search should be rejected
		currMem.Store(maxMem.Load() * 89 / 100)
		errSearch := ac.Admit(context.Background(), "search")
		assert.Error(t, errSearch)
		assert.Equal(t, codes.ResourceExhausted, status.Code(errSearch))
	})

	t.Run("Adaptive memory backpressure sleep duration", func(t *testing.T) {
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)
		currPhys := int64(m2.HeapAlloc) + lbmem.GetGlobalOffHeapAllocated()
		if currPhys < 10*1024*1024 {
			currPhys = 10 * 1024 * 1024
		}
		maxMem.Store(currPhys * 10)
		currMem.Store(maxMem.Load() * 85 / 100) // 85% (between 80% and 94% hardLimit)

		start := time.Now()
		err := ac.Admit(context.Background(), "ingest")
		assert.NoError(t, err)
		duration := time.Since(start)
		// Should have slept for at least 5ms base delay
		assert.GreaterOrEqual(t, duration, 5*time.Millisecond)
	})

	t.Run("AdmitMigration checks", func(t *testing.T) {
		// When ac.scaler is nil -> returns nil
		acNil := NewAdmissionController(&maxMem, &currMem, nil, zerolog.Nop())
		err := acNil.AdmitMigration(context.Background())
		assert.NoError(t, err)

		t.Run("High search latency", func(t *testing.T) {
			scaler := autoscale.NewAutoScaler(zerolog.Nop())
			setPrivateDuration(scaler, "monitorInterval", 1*time.Millisecond)
			setPrivateDuration(scaler, "cooldown", 1*time.Millisecond)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go scaler.Start(ctx)

			acLocal := NewAdmissionController(&maxMem, &currMem, scaler, zerolog.Nop())
			var m2 runtime.MemStats
			runtime.ReadMemStats(&m2)
			currPhys := int64(m2.HeapAlloc) + lbmem.GetGlobalOffHeapAllocated()
			if currPhys < 10*1024*1024 {
				currPhys = 10 * 1024 * 1024
			}
			maxMem.Store(currPhys * 10)
			currMem.Store(currPhys)

			scaler.RecordSearch(450 * time.Millisecond)
			time.Sleep(5 * time.Millisecond) // Let it sample

			errLat := acLocal.AdmitMigration(context.Background())
			assert.Error(t, errLat)
			assert.Contains(t, errLat.Error(), "search latency")
		})

		t.Run("High Ingest Throughput", func(t *testing.T) {
			scaler := autoscale.NewAutoScaler(zerolog.Nop())
			setPrivateDuration(scaler, "monitorInterval", 1*time.Millisecond)
			setPrivateDuration(scaler, "cooldown", 1*time.Millisecond)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go scaler.Start(ctx)

			acLocal := NewAdmissionController(&maxMem, &currMem, scaler, zerolog.Nop())
			var m2 runtime.MemStats
			runtime.ReadMemStats(&m2)
			currPhys := int64(m2.HeapAlloc) + lbmem.GetGlobalOffHeapAllocated()
			if currPhys < 10*1024*1024 {
				currPhys = 10 * 1024 * 1024
			}
			maxMem.Store(currPhys * 10)
			currMem.Store(currPhys)

			scaler.RecordIngest(130000 * 60) // Multiply by 60 because throughput is calculated as sum / 60.0
			time.Sleep(5 * time.Millisecond) // Let it sample

			errIngest := acLocal.AdmitMigration(context.Background())
			if assert.Error(t, errIngest) {
				assert.Contains(t, errIngest.Error(), "ingestion throughput")
			}
		})

		t.Run("Memory usage > 95%", func(t *testing.T) {
			scaler := autoscale.NewAutoScaler(zerolog.Nop())
			acLocal := NewAdmissionController(&maxMem, &currMem, scaler, zerolog.Nop())

			maxMem.Store(1) // Set maxMem to 1 byte, so physical memory is millions of percent usage!

			errMem := acLocal.AdmitMigration(context.Background())
			if assert.Error(t, errMem) {
				assert.Contains(t, errMem.Error(), "memory usage")
			}
		})
	})

	t.Run("AutoScaler critical health rejection", func(t *testing.T) {
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)
		currPhys := int64(m2.HeapAlloc) + lbmem.GetGlobalOffHeapAllocated()
		if currPhys < 10*1024*1024 {
			currPhys = 10 * 1024 * 1024
		}
		maxMem.Store(currPhys * 10)
		currMem.Store(currPhys) // 10% usage (normal)

		scaler := autoscale.NewAutoScaler(zerolog.Nop())
		setPrivateDuration(scaler, "monitorInterval", 1*time.Millisecond)
		setPrivateDuration(scaler, "cooldown", 1*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go scaler.Start(ctx)

		ac.scaler = scaler

		// Direct injection into rolling window for robustness
		v := reflect.ValueOf(scaler).Elem().FieldByName("searchWindow")
		ptr := (**autoscale.RollingWindow)(unsafe.Pointer(v.UnsafeAddr()))
		searchWindow := *ptr
		searchWindow.Add(60000)

		err := ac.Admit(context.Background(), "search")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "critical capacity")

		// Maintenance is still allowed under critical capacity
		errMaint := ac.Admit(context.Background(), "maintenance")
		assert.NoError(t, errMaint)

		ac.scaler = nil
	})
}
