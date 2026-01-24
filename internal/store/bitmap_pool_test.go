package store

import (
	"testing"
)

// =============================================================================
// Subtask 1: BitmapPoolConfig Tests
// =============================================================================

func TestBitmapPoolConfigDefaults(t *testing.T) {
	cfg := DefaultBitmapPoolConfig()

	if cfg.MaxBufferSize <= 0 {
		t.Error("MaxBufferSize should be positive")
	}
	if len(cfg.SizeBuckets) == 0 {
		t.Error("SizeBuckets should not be empty")
	}
	if cfg.MaxPoolSize < 0 {
		t.Error("MaxPoolSize 0 means unlimited")
	}
	if !cfg.MetricsEnabled {
		t.Error("MetricsEnabled should default to true")
	}
}

func TestBitmapPoolConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     BitmapPoolConfig
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: BitmapPoolConfig{
				SizeBuckets:    []int{1024, 4096, 16384},
				MaxBufferSize:  65536,
				MaxPoolSize:    100,
				MetricsEnabled: true,
			},
			wantErr: false,
		},
		{
			name: "empty buckets",
			cfg: BitmapPoolConfig{
				SizeBuckets:   []int{},
				MaxBufferSize: 65536,
				MaxPoolSize:   100,
			},
			wantErr: true,
		},
		{
			name: "zero max buffer size",
			cfg: BitmapPoolConfig{
				SizeBuckets:   []int{1024},
				MaxBufferSize: 0,
				MaxPoolSize:   100,
			},
			wantErr: true,
		},
		{
			name: "zero max pool size",
			cfg: BitmapPoolConfig{
				SizeBuckets:   []int{1024},
				MaxBufferSize: 65536,
				MaxPoolSize:   0,
			},
			wantErr: false,
		},
		{
			name: "unsorted buckets auto-sorted",
			cfg: BitmapPoolConfig{
				SizeBuckets:   []int{4096, 1024, 16384},
				MaxBufferSize: 65536,
				MaxPoolSize:   100,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// Subtask 2: BitmapPool Tests
// =============================================================================

func TestBitmapPoolCreation(t *testing.T) {
	cfg := DefaultBitmapPoolConfig()
	pool := NewBitmapPool(cfg)
	if pool == nil {
		t.Fatal("NewBitmapPool returned nil")
	}
	pool.Close()
}

func TestBitmapPoolGetPut(t *testing.T) {
	cfg := DefaultBitmapPoolConfig()
	pool := NewBitmapPool(cfg)
	defer pool.Close()

	// Get a buffer for 1000 rows
	buf := pool.Get(1000)
	if buf == nil {
		t.Fatal("Get returned nil buffer")
	}
	if len(buf.Data) < 1000/8 {
		t.Errorf("Buffer too small: got %d bytes, need at least %d", len(buf.Data), 1000/8)
	}

	// Put it back
	pool.Put(buf)
}

func TestBitmapPoolSizeBuckets(t *testing.T) {
	cfg := BitmapPoolConfig{
		SizeBuckets:    []int{1024, 4096, 16384, 65536},
		MaxBufferSize:  65536,
		MaxPoolSize:    10,
		MetricsEnabled: true,
	}
	_ = cfg.Validate()
	pool := NewBitmapPool(cfg)
	defer pool.Close()

	tests := []struct {
		requested int
		expectMin int // minimum bucket size
	}{
		{100, 1024},
		{1024, 1024},
		{1025, 4096},
		{4000, 4096},
		{5000, 16384},
		{20000, 65536},
	}

	for _, tt := range tests {
		buf := pool.Get(tt.requested)
		if buf.Capacity < tt.expectMin {
			t.Errorf("Get(%d): capacity %d < expected min %d",
				tt.requested, buf.Capacity, tt.expectMin)
		}
		pool.Put(buf)
	}
}

func TestBitmapPoolOversizedBuffer(t *testing.T) {
	cfg := BitmapPoolConfig{
		SizeBuckets:    []int{1024, 4096},
		MaxBufferSize:  4096,
		MaxPoolSize:    10,
		MetricsEnabled: true,
	}
	_ = cfg.Validate()
	pool := NewBitmapPool(cfg)
	defer pool.Close()

	// Request larger than max - should still work but not be pooled
	buf := pool.Get(10000)
	if buf == nil {
		t.Fatal("Get returned nil for oversized request")
	}
	if buf.Capacity < 10000 {
		t.Errorf("Oversized buffer too small: %d < 10000", buf.Capacity)
	}

	// Put back - should be discarded
	initialDiscards := pool.Stats().Discards
	pool.Put(buf)
	if pool.Stats().Discards <= initialDiscards {
		t.Error("Oversized buffer should be discarded")
	}
}

func TestBitmapPoolConcurrent(t *testing.T) {
	cfg := DefaultBitmapPoolConfig()
	pool := NewBitmapPool(cfg)
	defer pool.Close()

	const goroutines = 10
	const iterations = 100

	done := make(chan bool, goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			for i := 0; i < iterations; i++ {
				size := (i%4 + 1) * 1024
				buf := pool.Get(size)
				if buf == nil {
					t.Error("Got nil buffer")
				}
				// Simulate work
				for j := 0; j < len(buf.Data) && j < 100; j++ {
					buf.Data[j] = byte(j)
				}
				pool.Put(buf)
			}
			done <- true
		}()
	}

	for g := 0; g < goroutines; g++ {
		<-done
	}

	stats := pool.Stats()
	if stats.Gets != goroutines*iterations {
		t.Errorf("Expected %d gets, got %d", goroutines*iterations, stats.Gets)
	}
}

func TestBitmapPoolStats(t *testing.T) {
	cfg := DefaultBitmapPoolConfig()
	pool := NewBitmapPool(cfg)
	defer pool.Close()

	// Initial stats should be zero
	stats := pool.Stats()
	if stats.Gets != 0 || stats.Puts != 0 {
		t.Error("Initial stats should be zero")
	}

	// Get should increment
	buf := pool.Get(1024)
	stats = pool.Stats()
	if stats.Gets != 1 {
		t.Errorf("Gets should be 1, got %d", stats.Gets)
	}
	if stats.Misses != 1 {
		t.Errorf("First get should be a miss, got %d misses", stats.Misses)
	}

	// Put should increment
	pool.Put(buf)
	stats = pool.Stats()
	if stats.Puts != 1 {
		t.Errorf("Puts should be 1, got %d", stats.Puts)
	}

	// Second get - should be a hit (unless dropped by GC)
	bm2 := pool.Get(1024)
	stats = pool.Stats()
	if stats.Gets != 2 {
		t.Errorf("Gets should be 2, got %d", stats.Gets)
	}

	if stats.Hits == 1 {
		if stats.Misses != 1 {
			t.Errorf("If hit, misses should be 1, got %d", stats.Misses)
		}
	} else { // Dropped by GC
		if stats.Hits != 0 {
			t.Errorf("If dropped, hits should be 0, got %d", stats.Hits)
		}
		if stats.Misses != 2 {
			t.Errorf("If dropped, misses should be 2, got %d", stats.Misses)
		}
	}
	pool.Put(bm2)
}

// =============================================================================
// Subtask 3: PooledBitmap Tests
// =============================================================================

func TestPooledBitmapReset(t *testing.T) {
	cfg := DefaultBitmapPoolConfig()
	pool := NewBitmapPool(cfg)
	defer pool.Close()

	buf := pool.Get(1024)
	// Write some data
	for i := range buf.Data {
		buf.Data[i] = 0xFF
	}

	// Reset should zero the buffer
	buf.Reset()
	for i, b := range buf.Data {
		if b != 0 {
			t.Errorf("Reset failed at index %d: got %d, want 0", i, b)
			break
		}
	}
	pool.Put(buf)
}

func TestPooledBitmapSetBit(t *testing.T) {
	cfg := DefaultBitmapPoolConfig()
	pool := NewBitmapPool(cfg)
	defer pool.Close()

	buf := pool.Get(64)
	buf.Reset()

	// Set some bits
	buf.SetBit(0)
	buf.SetBit(7)
	buf.SetBit(8)

	if !buf.GetBit(0) {
		t.Error("Bit 0 should be set")
	}
	if !buf.GetBit(7) {
		t.Error("Bit 7 should be set")
	}
	if !buf.GetBit(8) {
		t.Error("Bit 8 should be set")
	}
	if buf.GetBit(15) {
		t.Error("Bit 15 should not be set")
	}
	if buf.GetBit(1) {
		t.Error("Bit 1 should not be set")
	}

	pool.Put(buf)
}

// =============================================================================
// Subtask 5: Prometheus Metrics Tests
// =============================================================================

func TestBitmapPoolMetricsExist(t *testing.T) {
	// Just verify the metrics can be incremented without panic
	cfg := DefaultBitmapPoolConfig()
	cfg.MetricsEnabled = true
	pool := NewBitmapPool(cfg)
	defer pool.Close()

	// Operations should update metrics
	buf := pool.Get(1024)
	pool.Put(buf)
	buf = pool.Get(1024)
	pool.Put(buf)

	stats := pool.Stats()
	if stats.Gets != 2 {
		t.Errorf("Expected 2 gets, got %d", stats.Gets)
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkBitmapPoolGetPut(b *testing.B) {
	cfg := DefaultBitmapPoolConfig()
	pool := NewBitmapPool(cfg)
	defer pool.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get(4096)
		pool.Put(buf)
	}
}

func BenchmarkBitmapPoolNoPool(b *testing.B) {
	// Compare against direct allocation
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 4096/8)
		_ = buf
	}
}

func BenchmarkBitmapPoolParallel(b *testing.B) {
	cfg := DefaultBitmapPoolConfig()
	pool := NewBitmapPool(cfg)
	defer pool.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get(4096)
			pool.Put(buf)
		}
	})
}
