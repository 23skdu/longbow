package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	lmem "github.com/23skdu/longbow/internal/memory"
)

// TestFlight_Lifecycle verifies that buffers allocated (or sliced) by the Reader
// are properly tracked and released when the Record is Released.
func TestFlight_Lifecycle(t *testing.T) {
	// 1. Setup Tracking Allocator
	baseAlloc := memory.NewGoAllocator()
	trackAlloc := lmem.NewTrackingAllocator(baseAlloc)

	// 2. Create Data
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Int64}}, nil,
	)
	b := array.NewRecordBuilder(baseAlloc, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	// 3. Serialize to Flight Data
	// (Simulate Network)
	chunkChan := make(chan *flight.FlightData, 10)
	mockClient := &mockClientStream{recvChunks: chunkChan}

	w := flight.NewRecordWriter(mockClient, ipc.WithSchema(schema))
	require.NoError(t, w.Write(rec))
	w.Close()
	mockClient.CloseSend() // explicit close

	var chunks []*flight.FlightData
	for d := range chunkChan {
		chunks = append(chunks, d)
	}

	// 4. Read with Tracking Allocator
	mockServer := &mockPutStream{chunks: chunks}
	r, err := flight.NewRecordReader(mockServer, ipc.WithAllocator(trackAlloc))
	require.NoError(t, err)
	defer r.Release()

	// Initial State
	// Initial State
	initialAllocated := trackAlloc.BytesAllocated.Load() // Should be 0 if clean start
	// Note: NewRecordReader might allocate some internal verify buffers? usually minimal.

	records := []arrow.Record{}

	for r.Next() {
		// New Record
		rec := r.Record()
		rec.Retain() // Keep it alive
		records = append(records, rec)
	}
	require.NoError(t, r.Err())

	// Check allocation
	// If Zero-Copy => Low allocation (structs only)
	// If Copy => High allocation
	// Check allocation
	// If Zero-Copy => Low allocation (structs only)
	// If Copy => High allocation
	midAllocated := trackAlloc.BytesAllocated.Load()
	t.Logf("Allocated after Read (Retained): %d (Initial: %d)", midAllocated, initialAllocated)

	// Release all
	for _, rec := range records {
		rec.Release()
	}

	// Check Freed
	// If Copy => Freed should match Allocated (roughly)
	// If Zero-Copy => Freed small

	// CRITICAL: We also want to know if the UNDERLYING memory (the chunks) are released?
	// The TrackingAllocator only tracks what IT allocated.
	// If zero-copy used the input buffers (chunks), the Allocator sees NOTHING about them.
	// So we can't fully verify the *network buffer* lifecycle via the Allocator alone.
	// But we CAN verify that *if* copies happened, they are cleaned up.

	// We mainly want to ensure no "leaks" of things the Allocator DID touch.
	// We mainly want to ensure no "leaks" of things the Allocator DID touch.
	finalAlloc := trackAlloc.BytesAllocated.Load()
	finalFreed := trackAlloc.BytesFreed.Load()

	t.Logf("Total Alloc: %d, Total Freed: %d, Active: %d", finalAlloc, finalFreed, finalAlloc-finalFreed)

	// Assert no active leaks from the allocator's perspective
	// Note: Some global arrow buffers might persist if not fully isolated, but with fresh allocator...
	// Except NewRecordReader might have buffered internal state.
	// r.Release() should clear that.

	// Allow small delta for internal structures that might be cached or pool-managed?
	// GoAllocator is strict.
	assert.Equal(t, finalAlloc, finalFreed, "All allocated memory should be freed")
}
