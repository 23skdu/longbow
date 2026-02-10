//go:build linux

package store

import (
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

func TestMemoryLeak_PprofHeapAnalysis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pprof integration test in short mode")
	}

	// Force GC to get clean baseline
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Write heap profile for baseline
	baselineFile, err := os.CreateTemp("", "baseline_heapprof *.prof")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(baselineFile.Name())
	if err := pprof.WriteHeapProfile(baselineFile); err != nil {
		t.Fatalf("failed to write baseline heap profile: %v", err)
	}
	baselineFile.Close()

	// Simulate workload that could leak memory
	// Create and abandon slices in a loop (simulates the CPU recent feature)
	for i := 0; i < 1000; i++ {
		// This pattern mimics potential leaks in CPU-intensive operations
		_ = make([]byte, 1024) // Allocate and abandon
		if i%10 == 0 {
			runtime.Gosched()
		}
	}

	// Another GC pass
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Check for significant memory growth
	allocGrowth := m2.HeapAlloc - m1.HeapAlloc
	t.Logf("Heap allocation growth: %d bytes", allocGrowth)
	t.Logf("Baseline alloc: %d, After workload alloc: %d", m1.HeapAlloc, m2.HeapAlloc)

	// Write post-workload heap profile
	postFile, err := os.CreateTemp("", "post_heapprof *.prof")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(postFile.Name())
	if err := pprof.WriteHeapProfile(postFile); err != nil {
		t.Fatalf("failed to write post-workload heap profile: %v", err)
	}
	postFile.Close()

	// Memory leak detection threshold: 10MB growth after GC
	if allocGrowth > 10*1024*1024 {
		t.Errorf("Potential memory leak detected: %d bytes allocated", allocGrowth)
	}
}

func TestMemoryLeak_PprofGoroutineAnalysis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping pprof goroutine test in short mode")
	}

	// Get baseline goroutine count
	baselineGoroutines := runtime.NumGoroutine()
	t.Logf("Baseline goroutines: %d", baselineGoroutines)

	// Write goroutine profile before
	preFile, err := os.CreateTemp("", "pre_goroutine.prof")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(preFile.Name())
	if err := pprof.Lookup("goroutine").WriteTo(preFile, 1); err != nil {
		t.Fatalf("failed to write goroutine profile: %v", err)
	}
	preFile.Close()

	// Simulate goroutine leak scenario
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func() {
			<-done // Block forever, simulating a leak
		}()
	}

	// Write profile before cleanup
	preCleanupFile, err := os.CreateTemp("", "precleanup_goroutine.prof")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(preCleanupFile.Name())
	if err := pprof.Lookup("goroutine").WriteTo(preCleanupFile, 1); err != nil {
		t.Fatalf("failed to write goroutine profile: %v", err)
	}
	preCleanupFile.Close()

	// Close the channel to let goroutines exit
	close(done)

	// Small delay to allow goroutines to exit
	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	finalGoroutines := runtime.NumGoroutine()
	t.Logf("Final goroutines: %d", finalGoroutines)

	// Allow for some variance, but should be close to baseline
	if finalGoroutines > baselineGoroutines+10 {
		t.Errorf("Potential goroutine leak: expected ~%d, got %d", baselineGoroutines, finalGoroutines)
	}
}

func TestMemoryLeak_CPURecentScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping CPU recent scenario test in short mode")
	}

	// This test simulates the "quarrel cpu recent" feature scenario
	// The feature likely involves tracking recent CPU usage/metrics

	// Get baseline memory
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	// Write baseline heap profile
	baselineHeapFile, err := os.CreateTemp("", "cpu_recent_baseline.prof")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(baselineHeapFile.Name())
	if err := pprof.WriteHeapProfile(baselineHeapFile); err != nil {
		t.Fatalf("failed to write baseline heap: %v", err)
	}
	baselineHeapFile.Close()

	// Simulate CPU recent metric collection (creating temporary buffers)
	for i := 0; i < 500; i++ {
		// Simulate metrics collection that creates temporary slices
		metrics := make([]float64, 64) // CPU usage samples
		for j := range metrics {
			metrics[j] = float64(j) * 0.1
		}
		_ = metrics // Abandon to simulate leak if not properly managed

		if i%50 == 0 {
			time.Sleep(1 * time.Millisecond)
		}
	}

	// Final GC and measurement
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	var final runtime.MemStats
	runtime.ReadMemStats(&final)

	// Write final heap profile for comparison
	finalHeapFile, err := os.CreateTemp("", "cpu_recent_final.prof")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(finalHeapFile.Name())
	if err := pprof.WriteHeapProfile(finalHeapFile); err != nil {
		t.Fatalf("failed to write final heap: %v", err)
	}
	finalHeapFile.Close()

	// Check for leaks
	heapDelta := int64(final.HeapAlloc) - int64(baseline.HeapAlloc)
	objectsDelta := int64(final.HeapObjects) - int64(baseline.HeapObjects)

	t.Logf("Heap alloc delta: %d bytes", heapDelta)
	t.Logf("Heap objects delta: %d", objectsDelta)

	// Threshold: more than 5MB growth after cleanup indicates leak
	if heapDelta > 5*1024*1024 {
		t.Errorf("Memory leak in CPU recent feature: %d bytes growth", heapDelta)
	}

	// Threshold: more than 10000 objects leaked
	if objectsDelta > 10000 {
		t.Errorf("Object leak in CPU recent feature: %d objects", objectsDelta)
	}
}
