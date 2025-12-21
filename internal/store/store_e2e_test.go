package store

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestStore_EndToEnd_TDD verifies the full lifecycle of data in the VectorStore using real Flight gRPC.
func TestStore_EndToEnd_TDD(t *testing.T) {
	tmpDir := t.TempDir()
	mem := memory.NewGoAllocator()
	logger := zap.NewNop()

	// 1. Initialize Store
	store := NewVectorStore(mem, logger, 1<<30, 0, 0)
	store.dataPath = tmpDir
	// Start WAL/Persistence subsystem
	err := store.InitPersistence(tmpDir, 1*time.Hour)
	require.NoError(t, err)

	// Start Server
	server := flight.NewFlightServer()
	server.RegisterFlightService(store)
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	server.InitListener(ln)

	go server.Serve()
	defer server.Shutdown()

	addr := ln.Addr().String()

	// 2. Initialize Client
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// 3. Put Data
	rec := makeE2ETestRecord(mem, 128, 50)
	defer rec.Release()

	// Create stream
	putStream, err := client.DoPut(ctx)
	require.NoError(t, err)

	writer := flight.NewRecordWriter(putStream, ipc.WithSchema(rec.Schema()))

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"products"},
	}
	writer.SetFlightDescriptor(desc)

	err = writer.Write(rec)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Close send side and drain results (bidirectional stream)
	err = putStream.CloseSend()
	require.NoError(t, err)
	for {
		_, err := putStream.Recv()
		if err != nil {
			break
		}
	}

	// Wait a bit for async indexing if any
	time.Sleep(100 * time.Millisecond)

	// 4. DoGet (Ticket Query)
	// "Get all products"
	ticket := TicketQuery{Name: "products", Limit: 100}
	ticketBytes, _ := json.Marshal(ticket)

	// Flight Client DoGet
	getStream, err := client.DoGet(ctx, &flight.Ticket{Ticket: ticketBytes})
	require.NoError(t, err)

	reader, err := flight.NewRecordReader(getStream)
	require.NoError(t, err)
	defer reader.Release()

	totalRows := 0
	for reader.Next() {
		r := reader.Record()
		totalRows += int(r.NumRows())
	}
	assert.NoError(t, reader.Err())
	assert.Equal(t, 50, totalRows)

	// 6. Persistence & Reload
	server.Shutdown()
	store.Close()

	// Re-open
	store2 := NewVectorStore(mem, logger, 1<<30, 0, 0)
	store2.dataPath = tmpDir
	// InitPersistence requires path and interval
	err = store2.InitPersistence(tmpDir, 1*time.Hour)
	require.NoError(t, err)

	// Start new server
	server2 := flight.NewFlightServer()
	server2.RegisterFlightService(store2)
	ln2, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	server2.InitListener(ln2)
	go server2.Serve()
	defer server2.Shutdown()

	client2, err := flight.NewClientWithMiddleware(ln2.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer client2.Close()

	// Verify data is back via Get
	getStream2, err := client2.DoGet(ctx, &flight.Ticket{Ticket: ticketBytes})
	require.NoError(t, err)

	reader2, err := flight.NewRecordReader(getStream2)
	require.NoError(t, err)
	defer reader2.Release()

	totalRows2 := 0
	for reader2.Next() {
		totalRows2 += int(reader2.Record().NumRows())
	}
	assert.NoError(t, reader2.Err())
	assert.Equal(t, 50, totalRows2, "Data should be recovered")
}

func makeE2ETestRecord(mem memory.Allocator, dims int, numVectors int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	idB := b.Field(0).(*array.Int64Builder)
	catB := b.Field(1).(*array.StringBuilder)
	listB := b.Field(2).(*array.FixedSizeListBuilder)
	valB := listB.ValueBuilder().(*array.Float32Builder)

	categories := []string{"electronics", "clothing", "home", "toys", "sports"}

	idB.Reserve(numVectors)
	catB.Reserve(numVectors)
	listB.Reserve(numVectors)
	valB.Reserve(numVectors * dims)

	for i := 0; i < numVectors; i++ {
		idB.UnsafeAppend(int64(i))
		catB.Append(categories[i%len(categories)])
		listB.Append(true)
		for j := 0; j < dims; j++ {
			valB.UnsafeAppend(float32(0.1 * float32(i)))
		}
	}
	return b.NewRecordBatch()
}
