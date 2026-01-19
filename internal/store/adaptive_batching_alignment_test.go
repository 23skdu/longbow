package store

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoPut_AdaptiveBatchingAlignment(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	logger := zerolog.Nop()
	s := &VectorStore{
		mem:              mem,
		logger:           logger,
		persistenceQueue: make(chan persistenceJob, 100),
		ingestionQueue:   NewIngestionRingBuffer(100),
	}
	emptyMap := make(map[string]*Dataset)
	s.datasets.Store(&emptyMap)
	defer close(s.persistenceQueue)

	// Create a large record (e.g., 2MB)
	dim := 1024    // float32 = 4KB per vector
	numRows := 512 // 512 * 4KB = 2MB

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	vBuilder := builder.Field(0).(*array.FixedSizeListBuilder)
	fBuilder := vBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numRows; i++ {
		vBuilder.Append(true)
		for j := 0; j < dim; j++ {
			fBuilder.Append(float32(j))
		}
	}

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Encode record into FlightData correctly
	recvCh := make(chan *flight.FlightData, 20)
	clientStream := &mockClientStream{recvChunks: recvCh}

	// Create a writer that sends to our mock client stream
	w := flight.NewRecordWriter(clientStream, ipc.WithSchema(schema), ipc.WithAllocator(mem))

	for i := 0; i < 10; i++ {
		err := w.Write(rec)
		require.NoError(t, err)
	}
	err := w.Close()
	require.NoError(t, err)

	// Collect chunks from channel
	chunks := make([]*flight.FlightData, 0)
	for {
		select {
		case fd := <-recvCh:
			if fd == nil {
				goto done_collect
			}
			chunks = append(chunks, fd)
		default:
			goto done_collect
		}
	}
done_collect:

	// Add descriptor to the first chunk (schema)
	if len(chunks) > 0 {
		chunks[0].FlightDescriptor = &flight.FlightDescriptor{Path: []string{"test_ds"}}
	}

	// Use mockPutStream (server-side) to play back these chunks
	mockStream := &mockPutStream{
		chunks: chunks,
	}

	// Run DoPut
	err = s.DoPut(mockStream)
	require.NoError(t, err)

	// Drain persistenceQueue to count flushes
	// We might need to wait a tiny bit since s.DoPut sends to channels
	time.Sleep(100 * time.Millisecond)

	flushCount := 0
drain_p:
	for {
		select {
		case job := <-s.persistenceQueue:
			job.batch.Release()
			flushCount++
		default:
			break drain_p
		}
	}

	// Drain ingestionQueue to avoid leaks
drain_i:
	for {
		job, ok := s.ingestionQueue.Pop()
		if !ok {
			break drain_i
		}
		job.batch.Release()
	}

	// If it flushes every 10MB:
	// Each rec is 2MB. 5 records = 10MB.
	// Total 10 records -> 2 flushes.

	assert.Equal(t, 2, flushCount, "Should have flushed 2 times with 10MB limit")
}

// Helpers for testing
// Methods needed for TestDoPut_AdaptiveBatchingAlignment used structures defined elsewhere (mockClientStream etc)
// or relied on these unused ones?
// Actually TestDoPut_AdaptiveBatchingAlignment uses `mockClientStream` and `mockPutStream` which are NOT these unused ones.
// These seem to be leftover copy-paste artifacts.

// Removed unused types: mockPutStreamRecvOnly, mockPutClient, ipcWriterWrapper
