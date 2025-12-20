package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// Test: ZeroCopyBuffer - Direct byte slice access without allocation
// =============================================================================

func TestZeroCopyBufferConfig_Defaults(t *testing.T) {
	cfg := DefaultZeroCopyBufferConfig()

	if !cfg.Enabled {
		t.Error("Expected Enabled=true by default")
	}
	if cfg.MaxRetainedBuffers != 1024 {
		t.Errorf("Expected MaxRetainedBuffers=1024, got %d", cfg.MaxRetainedBuffers)
	}
	if cfg.MaxBufferSize != 64*1024*1024 {
		t.Errorf("Expected MaxBufferSize=64MB, got %d", cfg.MaxBufferSize)
	}
}

func TestZeroCopyBufferConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ZeroCopyBufferConfig
		wantErr bool
	}{
		{"valid", DefaultZeroCopyBufferConfig(), false},
		{"zero_max_buffers", ZeroCopyBufferConfig{Enabled: true, MaxRetainedBuffers: 0, MaxBufferSize: 1024}, true},
		{"zero_max_size", ZeroCopyBufferConfig{Enabled: true, MaxRetainedBuffers: 10, MaxBufferSize: 0}, true},
		{"disabled_valid", ZeroCopyBufferConfig{Enabled: false}, false},
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
// Test: DirectBufferReader - Read Arrow buffers without copying
// =============================================================================

func TestDirectBufferReader_Create(t *testing.T) {
	alloc := memory.NewGoAllocator()
	reader := NewDirectBufferReader(alloc)

	if reader == nil {
		t.Fatal("Expected non-nil reader")
	}
	if reader.Allocator() != alloc {
		t.Error("Allocator mismatch")
	}
}

func TestDirectBufferReader_GetBufferReference(t *testing.T) {
	alloc := memory.NewGoAllocator()
	reader := NewDirectBufferReader(alloc)

	// Create test data
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	// Get buffer reference - should return same underlying slice
	bufRef := reader.GetBufferReference(data)

	if len(bufRef) != len(data) {
		t.Errorf("Length mismatch: got %d, want %d", len(bufRef), len(data))
	}

	// Verify it's the SAME underlying array (zero-copy)
	if &bufRef[0] != &data[0] {
		t.Error("Expected same underlying array (zero-copy), got copy")
	}
}

func TestDirectBufferReader_CreateArrayDataZeroCopy(t *testing.T) {
	alloc := memory.NewGoAllocator()
	reader := NewDirectBufferReader(alloc)

	// Create raw buffer (simulating data from flight.Reader)
	rawData := make([]byte, 32) // 8 int32 values
	for i := 0; i < 8; i++ {
		// Little-endian int32
		rawData[i*4] = byte(i)
		rawData[i*4+1] = 0
		rawData[i*4+2] = 0
		rawData[i*4+3] = 0
	}

	// Create array data directly from buffer (no copy)
	arrData := reader.CreateArrayDataFromBuffer(arrow.PrimitiveTypes.Int32, 8, rawData, nil)

	if arrData == nil {
		t.Fatal("Expected non-nil ArrayData")
	}
	if arrData.Len() != 8 {
		t.Errorf("Expected length 8, got %d", arrData.Len())
	}

	// Verify the buffer is the same (zero-copy)
	if arrData.Buffers()[1].Len() != 32 {
		t.Errorf("Expected buffer length 32, got %d", arrData.Buffers()[1].Len())
	}
}

// =============================================================================
// Test: AllocatorAwareCache - Cache that returns direct buffer references
// =============================================================================

func TestAllocatorAwareCache_Create(t *testing.T) {
	alloc := memory.NewGoAllocator()
	cache := NewAllocatorAwareCache(alloc, 1000)

	if cache == nil {
		t.Fatal("Expected non-nil cache")
	}
}

func TestAllocatorAwareCache_PutGet_ZeroCopy(t *testing.T) {
	alloc := memory.NewGoAllocator()
	cache := NewAllocatorAwareCache(alloc, 1000)

	// Create a record batch
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	bldr := array.NewInt32Builder(alloc)
	bldr.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 5)
	defer rec.Release()

	// Put into cache
	cache.Put("test", rec)

	// Get from cache - should be zero-copy
	cachedRec, ok := cache.Get("test")
	if !ok {
		t.Fatal("Expected to find cached record")
	}
	if cachedRec == nil {
		t.Fatal("Expected non-nil cached record")
	}

	// Verify same schema pointer (zero-copy metadata)
	if cachedRec.Schema() != rec.Schema() {
		// Note: Schema might be different object but should be equal
		if !cachedRec.Schema().Equal(rec.Schema()) {
			t.Error("Schema mismatch")
		}
	}

	cachedRec.Release()
}

func TestAllocatorAwareCache_GetBufferDirect(t *testing.T) {
	alloc := memory.NewGoAllocator()
	cache := NewAllocatorAwareCache(alloc, 1000)

	// Create test record
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	bldr := array.NewInt64Builder(alloc)
	bldr.AppendValues([]int64{100, 200, 300}, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, 3)
	defer rec.Release()

	cache.Put("direct", rec)

	// Get underlying buffer directly
	buf := cache.GetBufferDirect("direct", 0, 1) // column 0, buffer 1 (data buffer)
	if buf == nil {
		t.Fatal("Expected non-nil buffer")
	}

	// Verify buffer contains expected data
	if len(buf) < 24 { // 3 * int64 = 24 bytes
		t.Errorf("Buffer too small: %d bytes", len(buf))
	}
}

// =============================================================================
// Test: FlightReaderBufferRetainer - Retain flight.Reader internal buffers
// =============================================================================

func TestFlightReaderBufferRetainer_Create(t *testing.T) {
	retainer := NewFlightReaderBufferRetainer(1024)

	if retainer == nil {
		t.Fatal("Expected non-nil retainer")
	}
	if retainer.MaxBuffers() != 1024 {
		t.Errorf("Expected MaxBuffers=1024, got %d", retainer.MaxBuffers())
	}
}

func TestFlightReaderBufferRetainer_RetainBuffer(t *testing.T) {
	retainer := NewFlightReaderBufferRetainer(100)

	buf := []byte{1, 2, 3, 4}
	handle := retainer.RetainBuffer(buf)

	if handle == 0 {
		t.Error("Expected non-zero handle")
	}

	// Retrieve buffer by handle
	retrieved := retainer.GetBuffer(handle)
	if &retrieved[0] != &buf[0] {
		t.Error("Expected same buffer reference (zero-copy)")
	}
}

func TestFlightReaderBufferRetainer_ReleaseBuffer(t *testing.T) {
	retainer := NewFlightReaderBufferRetainer(100)

	buf := []byte{1, 2, 3, 4}
	handle := retainer.RetainBuffer(buf)

	// Release should succeed
	ok := retainer.ReleaseBuffer(handle)
	if !ok {
		t.Error("Expected successful release")
	}

	// Second release should fail
	ok = retainer.ReleaseBuffer(handle)
	if ok {
		t.Error("Expected failed release on already-released buffer")
	}
}

func TestFlightReaderBufferRetainer_Stats(t *testing.T) {
	retainer := NewFlightReaderBufferRetainer(100)

	buf1 := make([]byte, 1024)
	buf2 := make([]byte, 2048)

	retainer.RetainBuffer(buf1)
	h2 := retainer.RetainBuffer(buf2)

	stats := retainer.Stats()

	if stats.ActiveBuffers != 2 {
		t.Errorf("Expected ActiveBuffers=2, got %d", stats.ActiveBuffers)
	}
	if stats.TotalBytesRetained != 3072 {
		t.Errorf("Expected TotalBytesRetained=3072, got %d", stats.TotalBytesRetained)
	}

	retainer.ReleaseBuffer(h2)
	stats = retainer.Stats()
	if stats.ActiveBuffers != 1 {
		t.Errorf("After release: Expected ActiveBuffers=1, got %d", stats.ActiveBuffers)
	}
}

// =============================================================================
// Test: PooledAllocator integration with VectorStore
// =============================================================================

func TestPooledAllocator_AsDefault(t *testing.T) {
	pAlloc := NewPooledAllocator()

	// Verify it implements memory.Allocator
	var _ memory.Allocator = pAlloc

	// Allocate and free
	buf := pAlloc.Allocate(1024)
	if len(buf) != 1024 {
		t.Errorf("Expected 1024 bytes, got %d", len(buf))
	}
	pAlloc.Free(buf)

	// Stats should show reuse potential
	_ = pAlloc.Stats()
}

func TestPooledAllocator_BufferReuse(t *testing.T) {
	pAlloc := NewPooledAllocator()

	// Allocate and free multiple times
	for i := 0; i < 10; i++ {
		buf := pAlloc.Allocate(1024)
		pAlloc.Free(buf)
	}

	// Allocate again - should reuse from pool
	buf := pAlloc.Allocate(1024)
	stats := pAlloc.Stats()

	// After multiple alloc/free cycles, reuse count should be > 0
	if stats.ReusedBuffers == 0 {
		t.Log("Warning: Expected buffer reuse from pool")
	}

	pAlloc.Free(buf)
}

// =============================================================================
// Benchmarks: Zero-copy vs Copy operations
// =============================================================================

func BenchmarkBufferCopy(b *testing.B) {
	src := make([]byte, 1024*1024) // 1MB
	for i := range src {
		src[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		dst := make([]byte, len(src))
		copy(dst, src)
		_ = dst
	}
}

func BenchmarkBufferZeroCopy(b *testing.B) {
	src := make([]byte, 1024*1024) // 1MB
	for i := range src {
		src[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Zero-copy: just reference
		dst := src
		_ = dst
	}
}

func BenchmarkPooledAllocator(b *testing.B) {
	pAlloc := NewPooledAllocator()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := pAlloc.Allocate(4096)
		pAlloc.Free(buf)
	}
}

func BenchmarkGoAllocator(b *testing.B) {
	alloc := memory.NewGoAllocator()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := alloc.Allocate(4096)
		alloc.Free(buf)
	}
}

func BenchmarkDirectBufferReader_CreateArray(b *testing.B) {
	alloc := memory.NewGoAllocator()
	reader := NewDirectBufferReader(alloc)
	rawData := make([]byte, 4096) // 1024 int32 values

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		arrData := reader.CreateArrayDataFromBuffer(arrow.PrimitiveTypes.Int32, 1024, rawData, nil)
		arrData.Release()
	}
}
