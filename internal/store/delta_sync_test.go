package store

import (
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// Mock Exchange Stream
type mockExchangeServer struct {
	grpc.ServerStream
	recvChan chan *flight.FlightData
	sendChan chan *flight.FlightData
	ctx      context.Context
}

func (m *mockExchangeServer) Context() context.Context {
	return m.ctx
}

func (m *mockExchangeServer) Send(d *flight.FlightData) error {
	m.sendChan <- d
	return nil
}

func (m *mockExchangeServer) Recv() (*flight.FlightData, error) {
	d, ok := <-m.recvChan
	if !ok {
		return nil, io.EOF
	}
	return d, nil
}

func TestDeltaSync_Integration(t *testing.T) {
	// Setup Store
	pool := memory.NewGoAllocator()
	dir := t.TempDir()
	store := NewVectorStore(pool, zerolog.Nop(), 1024*1024, 1024*1024, 0)
	require.NoError(t, store.InitPersistence(storage.StorageConfig{
		DataPath:         dir,
		SnapshotInterval: 1 * time.Hour,
	}))
	defer func() { _ = store.Close() }()

	// Create Record
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(pool, schema)
	b.Field(0).(*array.Int64Builder).Append(10)
	rec1 := b.NewRecordBatch()

	b.Field(0).(*array.Int64Builder).Append(20)
	rec2 := b.NewRecordBatch()

	b.Field(0).(*array.Int64Builder).Append(30)
	rec3 := b.NewRecordBatch()

	// 1. Write Records (Seq 1, 2, 3)
	startTs := time.Now().UnixNano()
	_ = store.writeToWAL("dataset1", rec1, 1, time.Now().UnixNano())
	_ = store.writeToWAL("dataset1", rec2, 2, time.Now().UnixNano())
	_ = store.writeToWAL("dataset1", rec3, 3, time.Now().UnixNano())

	// Wait for flush
	err := store.FlushWAL()
	require.NoError(t, err)
	// Reopen for read? do_exchange uses NewWALIterator which opens file separately.
	// WALBatcher closing it is fine as long as file exists.

	// 2. Perform Exchange Sync for Seq > 1
	recv := make(chan *flight.FlightData, 10)
	send := make(chan *flight.FlightData, 10)
	stream := &mockExchangeServer{
		recvChan: recv,
		sendChan: send,
		ctx:      context.Background(),
	}

	// Client sends "sync" with last_seq = 1
	lastSeqBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(lastSeqBytes, 1) // we want records > 1 (so 2 and 3)

	recv <- &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Cmd: []byte("sync"),
		},
		DataBody: lastSeqBytes,
	}
	close(recv) // End client stream

	// Run DoExchange
	err = store.DoExchange(stream)
	if err != nil && err != io.EOF {
		// DoExchange returns nil on success (client EOF)
		require.NoError(t, err)
	}

	// Read responses
	close(send)
	responses := make([]*flight.FlightData, 0, 10)
	for fd := range send {
		if string(fd.DataBody) == "ack" {
			continue // ignore ack
		}
		responses = append(responses, fd)
	}

	// Expecting 2 responses (rec2 and rec3)
	assert.Len(t, responses, 2)

	if len(responses) >= 2 {
		// Verify Seq and TS.
		// New Protocol: Check Descriptor.Cmd first, else AppMetadata

		getMeta := func(fd *flight.FlightData) (uint64, int64) {
			var meta []byte
			if len(fd.AppMetadata) >= 16 {
				meta = fd.AppMetadata
			} else if fd.FlightDescriptor != nil && len(fd.FlightDescriptor.Cmd) >= 16 {
				meta = fd.FlightDescriptor.Cmd
			}

			if len(meta) >= 16 {
				seq := binary.LittleEndian.Uint64(meta[0:8])
				ts := int64(binary.LittleEndian.Uint64(meta[8:16]))
				return seq, ts
			}
			return 0, 0
		}

		seqA, tsA := getMeta(responses[0])
		seqB, tsB := getMeta(responses[1])

		assert.Equal(t, uint64(2), seqA)
		assert.Equal(t, uint64(3), seqB)
		assert.GreaterOrEqual(t, tsA, startTs)
		assert.GreaterOrEqual(t, tsB, startTs)
	}
}
