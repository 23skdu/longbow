package benchmark

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/storage"
)

// tryCreateIOUringBackend attempts to create an io_uring WAL backend, returning
// nil and a non-empty skip reason string when io_uring is unavailable on this
// build or kernel.
func tryCreateIOUringBackend(b *testing.B, dir string) (storage.WALBackend, string) {
	b.Helper()
	filePath := fmt.Sprintf("%s/iouring.wal", dir)
	backend, err := storage.NewUringBackend(filePath)
	if err != nil {
		return nil, fmt.Sprintf("io_uring unavailable: %v", err)
	}
	return backend, ""
}

// BenchmarkThroughput measures throughput of I/O operations
func BenchmarkThroughput(b *testing.B) {
	// Create test setup
	tmpDir, err := os.MkdirTemp("", "longbow-throughput-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			b.Logf("failed to remove temp dir %s: %v", tmpDir, err)
		}
	}()

	type testCase struct {
		name      string
		newBackend func() (storage.WALBackend, string) // factory returning (backend, skipReason)
	}

	cases := []testCase{
		{
			name: "Standard Backend",
			newBackend: func() (storage.WALBackend, string) {
				return createStandardBackend(b, tmpDir), ""
			},
		},
		{
			name: "IOUring Backend",
			newBackend: func() (storage.WALBackend, string) {
				return tryCreateIOUringBackend(b, tmpDir)
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			backend, skipReason := tc.newBackend()
			if backend == nil {
				b.Skip(skipReason)
				return
			}
			defer func() {
				if err := backend.Close(); err != nil {
					b.Logf("failed to close backend: %v", err)
				}
			}()

			// Create test data
			testData := make([]byte, 1024) // 1KB per operation

			b.ResetTimer()
			b.ReportAllocs()

			// Measure throughput
			for i := 0; i < b.N; i++ {
				if _, err := backend.Write(testData); err != nil {
					b.Fatalf("write failed: %v", err)
				}
			}

			if err := backend.Sync(); err != nil {
				b.Fatalf("sync failed: %v", err)
			}

			// Calculate metrics
			opsPerSecond := float64(b.N) / b.Elapsed().Seconds()
			bytesPerSecond := opsPerSecond * float64(len(testData))

			b.ReportMetric(opsPerSecond, "ops/sec")
			b.ReportMetric(bytesPerSecond, "bytes/sec")

			// Report to Prometheus for monitoring
			metrics.WalWritesTotal.WithLabelValues("success").Inc()
			metrics.WalFsyncDurationSeconds.WithLabelValues("success").Observe(b.Elapsed().Seconds())
		})
	}
}


// createStandardBackend creates a standard WAL backend for testing
func createStandardBackend(b *testing.B, dir string) storage.WALBackend {
	b.Helper()
	filePath := fmt.Sprintf("%s/standard.wal", dir)
	backend, err := storage.NewFSBackend(filePath)
	if err != nil {
		b.Fatalf("failed to create standard WAL: %v", err)
	}
	return backend
}

// BenchmarkLatency measures latency of individual operations
func BenchmarkLatency(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "longbow-latency-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			b.Logf("failed to remove temp dir %s: %v", tmpDir, err)
		}
	}()

	// Prefer io_uring when available, fall back to standard backend.
	backend, skipReason := tryCreateIOUringBackend(b, tmpDir)
	if backend == nil {
		b.Logf("io_uring unavailable (%s); falling back to standard WAL backend", skipReason)
		backend = createStandardBackend(b, tmpDir)
	}
	defer func() {
		if err := backend.Close(); err != nil {
			b.Logf("failed to close backend: %v", err)
		}
	}()

	testData := make([]byte, 256) // Small operation

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := time.Now()
		if _, err := backend.Write(testData); err != nil {
			b.Fatalf("write failed: %v", err)
		}
		if err := backend.Sync(); err != nil {
			b.Fatalf("sync failed: %v", err)
		}
		latency := time.Since(start)

		// Record latency distribution
		metrics.WalFsyncDurationSeconds.WithLabelValues("success").Observe(latency.Seconds())
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkConcurrentAccess measures concurrent access patterns
func BenchmarkConcurrentAccess(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "longbow-concurrent-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			b.Logf("failed to remove temp dir %s: %v", tmpDir, err)
		}
	}()

	backend := createStandardBackend(b, tmpDir)
	defer func() {
		if err := backend.Close(); err != nil {
			b.Logf("failed to close backend: %v", err)
		}
	}()

	testData := make([]byte, 512)

	b.ResetTimer()
	b.ReportAllocs()

	// Launch concurrent writers
	var workers = runtime.NumCPU()
	done := make(chan bool, workers)

	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < b.N/workers; j++ {
				if _, err := backend.Write(testData); err != nil {
					// Continue on error in stress test
					continue
				}
			}
			done <- true
		}()
	}

	// Wait for all workers to complete
	for i := 0; i < workers; i++ {
		<-done
	}

	b.ReportMetric(float64(workers)*float64(b.N/workers)/b.Elapsed().Seconds(), "ops/sec")
}
