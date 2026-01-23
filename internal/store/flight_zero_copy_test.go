package store

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mocks are now in mock_flight_streams_test.go

func TestFlight_ZeroCopyAllocator(t *testing.T) {
	// 1. Setup Allocator
	baseAlloc := memory.NewGoAllocator()
	mockAlloc := &MockAllocator{Allocator: baseAlloc}

	// 2. Create a RecordBatch to send
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "int64s", Type: arrow.PrimitiveTypes.Int64},
			{Name: "f64s", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	// Create decent sized batch (e.g. ~10MB)
	// 10MB ~ 640k rows (16 bytes per row)
	count := 640 * 1024
	b := array.NewRecordBuilder(baseAlloc, schema)
	defer b.Release()

	i64b := b.Field(0).(*array.Int64Builder)
	f64b := b.Field(1).(*array.Float64Builder)

	i64b.Reserve(count)
	f64b.Reserve(count)

	for i := 0; i < count; i++ {
		i64b.Append(int64(i))
		f64b.Append(float64(i))
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	expectedDataSize := int64(count * 16)
	t.Logf("Batch Data Size: %d bytes (%.2f MB)", expectedDataSize, float64(expectedDataSize)/1024/1024)

	// 3. Serialize to Flight Data frames
	chunkChan := make(chan *flight.FlightData, 100)
	mockClient := &mockClientStream{recvChunks: chunkChan}

	var chunks []*flight.FlightData
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for d := range chunkChan {
			chunks = append(chunks, d)
		}
	}()

	w := flight.NewRecordWriter(mockClient, ipc.WithSchema(schema))
	require.NoError(t, w.Write(rec))
	_ = w.Close()
	_ = mockClient.CloseSend()

	wg.Wait()
	t.Logf("Generated %d chunks", len(chunks))
	for i, c := range chunks {
		t.Logf("Chunk %d: HeaderLen=%d, BodyLen=%d", i, len(c.DataHeader), len(c.DataBody))
	}

	// 4. Server Side: Read using Custom Allocator
	mockServer := &mockPutStream{chunks: chunks}

	// Baseline: Using Mock Allocator
	r, err := flight.NewRecordReader(mockServer, ipc.WithAllocator(mockAlloc))
	require.NoError(t, err)
	defer r.Release()

	totalRead := 0
	for r.Next() {
		readRec := r.RecordBatch()
		totalRead++
		assert.Equal(t, count, int(readRec.NumRows()))

		// Verify allocator usage
		allocated := atomic.LoadInt64(&mockAlloc.Allocated)
		t.Logf("Allocated so far: %d", allocated)
	}
	require.NoError(t, r.Err())

	finalAlloc := atomic.LoadInt64(&mockAlloc.Allocated)
	t.Logf("Total Allocated by Reader: %d", finalAlloc)

	// Expectation:
	// In this in-memory test setup, Arrow Flight Reader defaults to Zero-Copy (buffer slicing).
	// We verify this by ensuring allocated bytes are minimal.

	// Sanity verify mockAlloc works
	m := mockAlloc.Allocate(100)
	mockAlloc.Free(m)
	assert.GreaterOrEqual(t, atomic.LoadInt64(&mockAlloc.Allocated), int64(100))

	t.Logf("Total Allocated by Reader: %d (Expected Data if copied: %d)", finalAlloc, expectedDataSize)

	if finalAlloc < expectedDataSize/2 {
		t.Log("SUCCESS: Reader performed Zero-Copy.")
	} else {
		t.Log("INFO: Reader performed copy.")
	}

	// We asserting that it DOES Zero Copy in this scenario.
	assert.Less(t, finalAlloc, expectedDataSize, "Reader performed zero-copy slicing, saving memory")

	if finalAlloc < expectedDataSize/2 {
		t.Log("WARNING: Allocations seem unexpectedly low. Is Zero-Copy already active?")
	}
}
