package profiling

import (
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestMemoryLeakDetector_Basic(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Logger()

	detector := NewMemoryLeakDetector(logger, 1, time.Second)
	detector.CaptureBaseline()

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	for i := 0; i < 1000; i++ {
		_ = make([]byte, 1024)
	}

	runtime.GC()
	time.Sleep(200 * time.Millisecond)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	t.Logf("Heap before: %d bytes", memBefore.HeapAlloc)
	t.Logf("Heap after: %d bytes", memAfter.HeapAlloc)
	t.Logf("Growth: %d bytes", memAfter.HeapAlloc-memBefore.HeapAlloc)
}

func TestMemoryLeakDetector_Goroutines(t *testing.T) {
	logger := zerolog.New(os.Stderr).With().Logger()

	baselineGoroutines := runtime.NumGoroutine()
	t.Logf("Baseline goroutines: %d", baselineGoroutines)

	detector := NewMemoryLeakDetector(logger, 10, time.Second)
	detector.Start()
	defer detector.Stop()

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			<-done
		}()
	}

	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	afterGoroutines := runtime.NumGoroutine()
	t.Logf("After goroutines: %d", afterGoroutines)

	close(done)
	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	finalGoroutines := runtime.NumGoroutine()
	t.Logf("Final goroutines: %d", finalGoroutines)

	if finalGoroutines > baselineGoroutines+5 {
		t.Logf("Potential goroutine leak detected")
	}
}

func TestMemoryLeakDetector_HeapProfile(t *testing.T) {
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	t.Logf("Heap alloc before: %d", memBefore.HeapAlloc)

	for i := 0; i < 100; i++ {
		data := make([]byte, 10240)
		_ = data
	}

	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	t.Logf("Heap alloc after: %d", memAfter.HeapAlloc)
	t.Logf("Growth: %d bytes", memAfter.HeapAlloc-memBefore.HeapAlloc)

	if err := pprof.WriteHeapProfile(os.Stdout); err != nil {
		t.Logf("Could not write heap profile: %v", err)
	}
}

func TestMemoryLeakDetector_GoroutineProfile(t *testing.T) {
	baseline := runtime.NumGoroutine()

	for i := 0; i < 50; i++ {
		go func() {
			time.Sleep(10 * time.Millisecond)
		}()
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	t.Logf("Baseline: %d, After: %d", baseline, after)

	if err := pprof.Lookup("goroutine").WriteTo(os.Stdout, 1); err != nil {
		t.Logf("Could not write goroutine profile: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	final := runtime.NumGoroutine()
	t.Logf("Final: %d", final)
}
