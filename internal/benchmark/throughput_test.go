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

// BenchmarkThroughput measures throughput of I/O operations
func BenchmarkThroughput(b *testing.B) {
	// Create test setup
	tmpDir, err := os.MkdirTemp("", "longbow-throughput-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Test different backends
	testCases := []struct {
		name    string
		backend storage.WALBackend
	}{
		{"Standard Backend", createStandardBackend(b, tmpDir)},
		{"IOUring Backend", createIOUringBackend(b, tmpDir)},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			defer tc.backend.Close()

			// Create test data
			testData := make([]byte, 1024) // 1KB per operation

			b.ResetTimer()
			b.ReportAllocs()

			// Measure throughput
			for i := 0; i < b.N; i++ {
				if _, err := tc.backend.Write(testData); err != nil {
					b.Fatalf("write failed: %v", err)
				}
			}

			if err := tc.backend.Sync(); err != nil {
				b.Fatalf("sync failed: %v", err)
			}

			// Calculate metrics
			opsPerSecond := float64(b.N) / b.Elapsed().Seconds()
			bytesPerSecond := opsPerSecond * float64(len(testData))

			b.ReportMetric(opsPerSecond, "ops/sec")
			b.ReportMetric(bytesPerSecond, "bytes/sec")

			// Report to Prometheus for monitoring
			// Report to Prometheus for monitoring
			metrics.WalWritesTotal.WithLabelValues("success").Inc()
			metrics.WalFsyncDurationSeconds.WithLabelValues("success").Observe(b.Elapsed().Seconds())
		})
	}
}

// createStandardBackend creates a standard WAL backend for testing
func createStandardBackend(b *testing.B, dir string) storage.WALBackend {
	filePath := fmt.Sprintf("%s/standard.wal", dir)
	backend, err := storage.NewFSBackend(filePath)
	if err != nil {
		b.Fatalf("failed to create standard WAL: %v", err)
	}
	return backend
}

// createIOUringBackend creates an io_uring WAL backend for testing
func createIOUringBackend(b *testing.B, dir string) storage.WALBackend {
	filePath := fmt.Sprintf("%s/iouring.wal", dir)
	backend, err := storage.NewUringBackend(filePath)
	if err != nil {
		b.Fatalf("failed to create io_uring WAL: %v", err)
	}
	return backend
}

// BenchmarkLatency measures latency of individual operations
func BenchmarkLatency(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "longbow-latency-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	backend := createIOUringBackend(b, tmpDir)
	defer backend.Close()

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
		// Record latency distribution
		metrics.WalFsyncDurationSeconds.WithLabelValues("success").Observe(latency.Seconds())
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkConcurrentAccess measures concurrent access patterns
func BenchmarkConcurrentAccess(b *testing.B) {
	backend := createStandardBackend(&testing.B{}, "/tmp")
	defer backend.Close()

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
