package store

import (
	"bytes"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// RecordWriterPool Tests - TDD Red Phase
// =============================================================================

func TestRecordWriterPoolConfig_Defaults(t *testing.T) {
	cfg := DefaultRecordWriterPoolConfig()

	assert.Equal(t, 64*1024, cfg.InitialBufferSize, "default initial buffer should be 64KB")
	assert.Equal(t, 4*1024*1024, cfg.MaxBufferSize, "default max buffer should be 4MB")
	assert.True(t, cfg.UseLZ4, "LZ4 compression should be enabled by default")
}

func TestRecordWriterPoolConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		config  RecordWriterPoolConfig
		wantErr bool
	}{
		{
			name:    "valid config",
			config:  DefaultRecordWriterPoolConfig(),
			wantErr: false,
		},
		{
			name:    "zero initial buffer uses default",
			config:  RecordWriterPoolConfig{InitialBufferSize: 0, MaxBufferSize: 1024 * 1024},
			wantErr: false,
		},
		{
			name:    "initial > max is invalid",
			config:  RecordWriterPoolConfig{InitialBufferSize: 10 * 1024 * 1024, MaxBufferSize: 1024 * 1024},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIPCBufferPool_GetAndPut(t *testing.T) {
	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())
	require.NotNil(t, pool)

	// Get a buffer
	buf := pool.Get()
	require.NotNil(t, buf)
	assert.True(t, buf.Cap() >= 64*1024, "buffer should have at least initial capacity")

	// Write some data
	buf.WriteString("test data")
	assert.Equal(t, 9, buf.Len())

	// Put it back - should reset
	pool.Put(buf)

	// Get again - should be same buffer (reset)
	buf2 := pool.Get()
	assert.Equal(t, 0, buf2.Len(), "returned buffer should be reset")
	pool.Put(buf2)
}

func TestIPCBufferPool_OversizedBufferDiscarded(t *testing.T) {
	cfg := RecordWriterPoolConfig{
		InitialBufferSize: 1024,
		MaxBufferSize:     4096,
		UseLZ4:            true,
	}
	pool := NewIPCBufferPool(cfg)

	// Get and grow beyond max
	buf := pool.Get()
	largeData := make([]byte, 8192) // 8KB > 4KB max
	buf.Write(largeData)

	// Put back - should be discarded (not returned to pool)
	pool.Put(buf)

	// Verify stats
	stats := pool.Stats()
	assert.Equal(t, int64(1), stats.Discarded, "oversized buffer should be discarded")
}

func TestIPCBufferPool_ConcurrentAccess(t *testing.T) {
	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())

	var wg sync.WaitGroup
	iterations := 1000
	goroutines := 10

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(_ int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				buf := pool.Get()
				buf.WriteString("concurrent test")
				pool.Put(buf)
			}
		}(g)
	}

	wg.Wait()
	stats := pool.Stats()
	assert.Equal(t, int64(goroutines*iterations), stats.Gets)
	assert.Equal(t, int64(goroutines*iterations), stats.Puts)
}

func TestIPCBufferPool_Stats(t *testing.T) {
	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())

	// Initial stats
	stats := pool.Stats()
	assert.Equal(t, int64(0), stats.Gets)
	assert.Equal(t, int64(0), stats.Puts)
	assert.Equal(t, int64(0), stats.Hits)
	assert.Equal(t, int64(0), stats.Misses)

	// First get - miss
	buf1 := pool.Get()
	stats = pool.Stats()
	assert.Equal(t, int64(1), stats.Gets)
	assert.Equal(t, int64(1), stats.Misses)

	// Put back
	pool.Put(buf1)
	stats = pool.Stats()
	assert.Equal(t, int64(1), stats.Puts)

	// Second get - hit
	buf2 := pool.Get()
	stats = pool.Stats()
	assert.Equal(t, int64(2), stats.Gets)
	assert.Equal(t, int64(1), stats.Hits)
	pool.Put(buf2)
}

func TestPooledRecordWriter_WriteRecordBatch(t *testing.T) {
	alloc := memory.NewGoAllocator()
	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())

	// Create a simple schema and record batch
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Write using pooled buffer
	buf := pool.Get()
	defer pool.Put(buf)

	w := ipc.NewWriter(buf, ipc.WithSchema(schema))
	err := w.Write(rec)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	assert.True(t, buf.Len() > 0, "buffer should contain IPC data")
}

func TestPooledRecordWriter_BufferReuse(t *testing.T) {
	alloc := memory.NewGoAllocator()
	cfg := RecordWriterPoolConfig{
		InitialBufferSize: 4096,
		MaxBufferSize:     1024 * 1024,
		UseLZ4:            false,
	}
	pool := NewIPCBufferPool(cfg)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// First write
	buf1 := pool.Get()
	bldr := array.NewRecordBuilder(alloc, schema)
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	rec := bldr.NewRecordBatch()

	w1 := ipc.NewWriter(buf1, ipc.WithSchema(schema))
	_ = w1.Write(rec)
	_ = w1.Close()
	rec.Release()
	bldr.Release()

	firstLen := buf1.Len()
	pool.Put(buf1)

	// Second write - should reuse buffer
	buf2 := pool.Get()
	assert.Equal(t, 0, buf2.Len(), "reused buffer should be reset")

	bldr2 := array.NewRecordBuilder(alloc, schema)
	bldr2.Field(0).(*array.Int64Builder).AppendValues([]int64{4, 5, 6, 7, 8}, nil)
	rec2 := bldr2.NewRecordBatch()

	w2 := ipc.NewWriter(buf2, ipc.WithSchema(schema))
	_ = w2.Write(rec2)
	_ = w2.Close()
	rec2.Release()
	bldr2.Release()

	assert.True(t, buf2.Len() > firstLen, "second write with more rows should be larger")
	pool.Put(buf2)

	// Verify pool hit
	stats := pool.Stats()
	assert.Equal(t, int64(1), stats.Hits, "second get should be pool hit")
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkIPCBufferPool_GetPut(b *testing.B) {
	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf.WriteString("benchmark data")
		pool.Put(buf)
	}
}

func BenchmarkIPCBufferPool_NoPool(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := bytes.NewBuffer(make([]byte, 0, 64*1024))
		buf.WriteString("benchmark data")
		// No reuse - GC will collect
	}
}

func BenchmarkPooledRecordWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()
	pool := NewIPCBufferPool(DefaultRecordWriterPoolConfig())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	// Pre-build record
	bldr := array.NewRecordBuilder(alloc, schema)
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	bldr.Field(1).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()
	defer bldr.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		w := ipc.NewWriter(buf, ipc.WithSchema(schema))
		_ = w.Write(rec)
		_ = w.Close()
		pool.Put(buf)
	}
}

func BenchmarkUnpooledRecordWriter(b *testing.B) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	// Pre-build record
	bldr := array.NewRecordBuilder(alloc, schema)
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	bldr.Field(1).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5}, nil)
	rec := bldr.NewRecordBatch()
	defer rec.Release()
	defer bldr.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := bytes.NewBuffer(make([]byte, 0, 64*1024))
		w := ipc.NewWriter(buf, ipc.WithSchema(schema))
		_ = w.Write(rec)
		_ = w.Close()
	}
}
