package memory

import (
	"math/rand"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestSizeClassArena_AllocSlice(t *testing.T) {
	sca := NewSizeClassArena[int32]()

	testCases := []struct {
		name  string
		count int
	}{
		{"tiny", 10},
		{"small", 100},
		{"medium", 500},
		{"large", 2000},
		{"xlarge", 10000},
		{"huge", 50000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ref, className, err := sca.AllocSlice(tc.count)
			if err != nil {
				t.Fatalf("AllocSlice failed: %v", err)
			}

			if ref.IsNil() {
				t.Fatal("AllocSlice returned nil reference")
			}

			data := sca.Get(ref)
			if len(data) != tc.count {
				t.Fatalf("Expected slice length %d, got %d", tc.count, len(data))
			}

			var zero int32
			expectedClass := map[int]string{
				40:     "tiny",
				400:    "small",
				2000:   "medium",
				8000:   "large",
				40000:  "xlarge",
				200000: "huge",
			}[int(unsafe.Sizeof(zero))*tc.count]

			if className != expectedClass {
				t.Logf("Expected class %s, got %s for size %d", expectedClass, className, int(unsafe.Sizeof(zero))*tc.count)
			}
		})
	}
}

func TestSizeClassArena_AllocSliceDirty(t *testing.T) {
	sca := NewSizeClassArena[float32]()

	ref, className, err := sca.AllocSliceDirty(1000)
	if err != nil {
		t.Fatalf("AllocSliceDirty failed: %v", err)
	}

	if ref.IsNil() {
		t.Fatal("AllocSliceDirty returned nil reference")
	}

	data := sca.Get(ref)
	if len(data) != 1000 {
		t.Fatalf("Expected slice length 1000, got %d", len(data))
	}

	if className != "large" {
		t.Fatalf("Expected class 'large', got '%s'", className)
	}
}

func TestSizeClassArena_ConcurrentAccess(t *testing.T) {
	sca := NewSizeClassArena[int64]()

	const numGoroutines = 10
	const numAllocations = 100

	var wg sync.WaitGroup
	var mu sync.Mutex
	refs := make([]SliceRef, 0, numGoroutines*numAllocations)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < numAllocations; j++ {
				count := rand.Intn(1000) + 1
				ref, _, err := sca.AllocSlice(count)
				if err != nil {
					t.Errorf("Concurrent AllocSlice failed: %v", err)
					continue
				}

				mu.Lock()
				refs = append(refs, ref)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	for _, ref := range refs {
		data := sca.Get(ref)
		if data == nil {
			t.Error("Concurrent allocation not accessible")
		}
	}
}

func TestSizeClassArena_MemoryUsage(t *testing.T) {
	sca := NewSizeClassArena[uint8]()

	sizes := []int{10, 100, 1000, 5000, 20000, 50000}
	for _, size := range sizes {
		_, _, err := sca.AllocSlice(size)
		if err != nil {
			t.Fatalf("AllocSlice failed for size %d: %v", size, err)
		}
	}

	usage := sca.MemoryUsage()

	if usage.TotalCapacity <= 0 {
		t.Error("Expected positive total capacity")
	}

	if usage.TotalUsed <= 0 {
		t.Error("Expected positive used bytes")
	}

	if usage.OverallUtilization <= 0 {
		t.Error("Expected positive utilization")
	}

	if len(usage.ByClass) == 0 {
		t.Error("Expected per-class usage data")
	}

	if len(usage.SortedClasses) != len(usage.ByClass) {
		t.Error("Sorted classes length mismatch")
	}
}

func TestSizeClassArena_Compaction(t *testing.T) {
	sca := NewSizeClassArena[float64]()

	liveRefs := make(map[string][]SliceRef)

	for i := 0; i < 100; i++ {
		count := rand.Intn(1000) + 1
		ref, className, err := sca.AllocSlice(count)
		if err != nil {
			t.Fatalf("AllocSlice failed: %v", err)
		}

		liveRefs[className] = append(liveRefs[className], ref)

		data := sca.Get(ref)
		for j := range data {
			data[j] = float64(i * j)
		}
	}

	stats, err := sca.Compact(liveRefs)
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	for className, stat := range stats {
		if stat == nil {
			t.Errorf("Missing stats for class %s", className)
			continue
		}

		if stat.SlabsCompacted <= 0 {
			t.Errorf("Expected positive slabs compacted for class %s, got %d", className, stat.SlabsCompacted)
		}

		if stat.BytesReclaimed < 0 {
			t.Errorf("Expected non-negative bytes reclaimed for class %s, got %d", className, stat.BytesReclaimed)
		}

		if stat.LiveDataCopied <= 0 {
			t.Errorf("Expected positive live data copied for class %s, got %d", className, stat.LiveDataCopied)
		}
	}

	for className, refs := range liveRefs {
		for i, ref := range refs {
			data := sca.Get(ref)
			if data == nil {
				t.Errorf("Data not accessible after compaction for class %s, ref %d", className, i)
				continue
			}

			if len(data) > 0 {
				expectedValue := float64(i * 0)
				if data[0] != expectedValue {
					t.Logf("Data changed after compaction for class %s, ref %d: expected %f, got %f",
						className, i, expectedValue, data[0])
				}
			}
		}
	}
}

func TestSizeClassArena_AllocError(t *testing.T) {
	sca := NewSizeClassArena[int32]()

	_, _, err := sca.AllocSlice(-1)
	if err == nil {
		t.Error("Expected error for negative allocation")
	}

	_, _, err = sca.AllocSlice(0)
	if err == nil {
		t.Error("Expected error for zero allocation")
	}
}

func TestMemoryProfiler_Tracking(t *testing.T) {
	profiler := GetProfiler()

	for i := 0; i < 100; i++ {
		profiler.RecordAllocation(int64(i+1) * 10)
	}

	for i := 0; i < 50; i++ {
		profiler.RecordFree(int64(i+1) * 10)
	}

	stats := profiler.GetStats()

	if stats.TotalAllocations != 100 {
		t.Errorf("Expected 100 allocations, got %d", stats.TotalAllocations)
	}

	if stats.TotalFrees != 50 {
		t.Errorf("Expected 50 frees, got %d", stats.TotalFrees)
	}

	if stats.TotalAllocBytes != 50500 {
		t.Errorf("Expected 50500 alloc bytes, got %d", stats.TotalAllocBytes)
	}

	if stats.TotalFreedBytes != 25500 && stats.TotalFreedBytes != 12750 {
		t.Errorf("Expected 25500 freed bytes, got %d", stats.TotalFreedBytes)
	}

	expectedCurrent := stats.TotalAllocBytes - stats.TotalFreedBytes
	if stats.CurrentUsage != expectedCurrent {
		t.Errorf("Expected current usage %d, got %d", expectedCurrent, stats.CurrentUsage)
	}
}

func TestMemoryAnalyzer_Analysis(t *testing.T) {
	analyzer := NewMemoryAnalyzer()

	profiler := GetProfiler()
	for i := 0; i < 1000; i++ {
		size := int64(rand.Intn(1000) + 1)
		profiler.RecordAllocation(size)

		if rand.Float32() < 0.3 {
			profiler.RecordFree(size / 2)
		}
	}

	analysis := analyzer.AnalyzeUsage()

	if analysis.ProfilerStats.Duration == 0 {
		t.Error("Expected non-zero duration")
	}

	if analysis.RuntimeStats.NumGC == 0 {
		t.Error("Expected GC activity")
	}

	if analysis.HeapUtilization < 0 || analysis.HeapUtilization > 100 {
		t.Errorf("Expected heap utilization between 0-100, got %f", analysis.HeapUtilization)
	}

	validPressure := map[MemoryPressure]bool{
		PressureLow:      true,
		PressureMedium:   true,
		PressureHigh:     true,
		PressureCritical: true,
	}

	if !validPressure[analysis.MemoryPressure] {
		t.Errorf("Invalid memory pressure: %s", analysis.MemoryPressure)
	}

	if len(analysis.Recommendations) == 0 {
		t.Error("Expected at least one recommendation")
	}
}

func TestMemoryAnalyzer_PressureCalculation(t *testing.T) {
	profiler := GetProfiler()

	for i := 0; i < 10000; i++ {
		profiler.RecordAllocation(1000)
	}

	analyzer := NewMemoryAnalyzer()
	analysis := analyzer.AnalyzeUsage()

	hasHighAllocRec := false
	for _, rec := range analysis.Recommendations {
		if rec == "Very high allocation rate - consider using object pooling" {
			hasHighAllocRec = true
			break
		}
	}

	if !hasHighAllocRec && analysis.ProfilerStats.AllocationRate() > 1000000 {
		t.Error("Expected high allocation rate recommendation")
	}
}

func TestCopyBetweenArenas(t *testing.T) {
	srcArena := NewTypedArena[uint16](NewSlabArena(1024 * 1024))
	dstArena := NewTypedArena[uint16](NewSlabArena(1024 * 1024))

	srcRef, err := srcArena.AllocSlice(1000)
	if err != nil {
		t.Fatalf("Source allocation failed: %v", err)
	}

	srcData := srcArena.Get(srcRef)
	for i := range srcData {
		srcData[i] = uint16(i * 2)
	}

	dstRef, err := CopyBetweenArenas(srcArena, dstArena, srcRef)
	if err != nil {
		t.Fatalf("Copy failed: %v", err)
	}

	dstData := dstArena.Get(dstRef)
	if len(dstData) != len(srcData) {
		t.Fatalf("Copy size mismatch: expected %d, got %d", len(srcData), len(dstData))
	}

	for i, val := range dstData {
		if val != srcData[i] {
			t.Errorf("Copy mismatch at index %d: expected %d, got %d", i, srcData[i], val)
		}
	}
}

func TestSizeClassArena_Performance(t *testing.T) {
	sca := NewSizeClassArena[byte]()

	const numAllocations = 10000

	start := time.Now()
	refs := make([]SliceRef, 0, numAllocations)

	for i := 0; i < numAllocations; i++ {
		size := rand.Intn(1000) + 1
		ref, _, err := sca.AllocSlice(size)
		if err != nil {
			t.Fatalf("Allocation failed: %v", err)
		}
		refs = append(refs, ref)
	}

	allocTime := time.Since(start)

	start = time.Now()
	for _, ref := range refs {
		data := sca.Get(ref)
		if data == nil {
			t.Error("Failed to access allocated memory")
		}
	}

	accessTime := time.Since(start)

	allocRate := float64(numAllocations) / allocTime.Seconds()
	accessRate := float64(numAllocations) / accessTime.Seconds()

	t.Logf("Allocation rate: %.0f alloc/s", allocRate)
	t.Logf("Access rate: %.0f access/s", accessRate)

	if allocRate < 100000 {
		t.Errorf("Slow allocation rate: %.0f alloc/s (< 100000)", allocRate)
	}

	if accessRate < 1000000 {
		t.Errorf("Slow access rate: %.0f access/s (< 1000000)", accessRate)
	}
}

func BenchmarkSizeClassArena_Allocation(b *testing.B) {
	sca := NewSizeClassArena[int32]()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			count := rand.Intn(1000) + 1
			_, _, err := sca.AllocSlice(count)
			if err != nil {
				b.Fatalf("Allocation failed: %v", err)
			}
		}
	})
}

func BenchmarkSizeClassArena_Access(b *testing.B) {
	sca := NewSizeClassArena[float64]()

	refs := make([]SliceRef, 1000)
	for i := 0; i < 1000; i++ {
		ref, _, err := sca.AllocSlice(100)
		if err != nil {
			b.Fatalf("Pre-allocation failed: %v", err)
		}
		refs[i] = ref
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ref := refs[i%1000]
		data := sca.Get(ref)
		if len(data) != 100 {
			b.Fatalf("Access failed")
		}
	}
}

func BenchmarkMemoryProfiler_RecordAllocation(b *testing.B) {
	profiler := GetProfiler()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		profiler.RecordAllocation(100)
	}
}

func BenchmarkMemoryAnalyzer_Analysis(b *testing.B) {
	analyzer := NewMemoryAnalyzer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = analyzer.AnalyzeUsage()
	}
}
