package store

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestDoExchange_Ingest(t *testing.T) {
	// Setup Server
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1024*1024*100, 0, 0)

	tmpDir := t.TempDir()
	require.NoError(t, s.InitPersistence(storage.StorageConfig{
		DataPath: tmpDir,
	}))
	defer func() { _ = s.Close() }()

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	addr := lis.Addr().String()

	// Client
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := flight.NewClientFromConn(conn, nil)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start DoExchange
	stream, err := client.DoExchange(ctx)
	require.NoError(t, err)

	// Create Descriptor
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"ingest", "test_exchange_ingest"},
	}

	// Create Record Batch
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	vecBuilder := b.Field(1).(*array.FixedSizeListBuilder)
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)
	valBuilder.AppendValues([]float32{0.1, 0.2, 0.3, 0.4}, nil) // 2 vectors of dim 2
	vecBuilder.Append(true)
	vecBuilder.Append(true)

	rec := b.NewRecordBatch()
	defer rec.Release()

	// Writer to stream
	wr := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	wr.SetFlightDescriptor(desc)

	// Write Batch
	err = wr.Write(rec)
	require.NoError(t, err)

	// Read ACK from server
	data, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, data)
	require.NotEmpty(t, data.AppMetadata)

	var ack map[string]interface{}
	err = json.Unmarshal(data.AppMetadata, &ack)
	require.NoError(t, err)

	assert.Equal(t, "acked", ack["status"])
	assert.Equal(t, float64(1), ack["batch_id"]) // float64 due to json unmarshal
	assert.Equal(t, float64(2), ack["rows"])

	// Done
	_ = wr.Close()
	_ = stream.CloseSend()

	// Check store has data (poll for async ingestion)
	ds, ok := s.getDataset("test_exchange_ingest")
	require.True(t, ok)

	assert.Eventually(t, func() bool {
		ds.dataMu.RLock()
		defer ds.dataMu.RUnlock()
		return len(ds.Records) == 1
	}, 1*time.Second, 10*time.Millisecond, "dataset should have 1 record batch")

	require.Len(t, ds.Records, 1)
	assert.Equal(t, int64(2), ds.Records[0].NumRows())
}

// BenchmarkDoExchange_Ingest measures throughput of DoExchange ingestion.
func BenchmarkDoExchange_Ingest(b *testing.B) {
	// Setup Server
	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	s := NewVectorStore(mem, logger, 1024*1024*1024, 0, 0)
	tmpDir := b.TempDir()
	require.NoError(b, s.InitPersistence(storage.StorageConfig{DataPath: tmpDir}))
	defer func() { _ = s.Close() }()

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(b, err)

	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, s)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	// Client
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(b, err)
	defer func() { _ = conn.Close() }()

	client := flight.NewClientFromConn(conn, nil)
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	// Prepare Record Batch (1000 rows, 384 dim)
	rows := 1000
	dim := 384
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dim), arrow.PrimitiveTypes.Float32)},
	}, nil)

	b.ResetTimer()

	// We stream N batches per b.N loop?
	// Or we do b.N iterations where each is one call?
	// Usually strict benchmarks stream many batches in one call.

	// Let's emulate a stream of b.N batches.
	stream, err := client.DoExchange(ctx)
	require.NoError(b, err)

	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{"ingest", "bench_exchange"},
	}

	wr := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	wr.SetFlightDescriptor(desc)

	// Create reusable batch
	recBuilder := array.NewRecordBuilder(mem, schema)
	idB := recBuilder.Field(0).(*array.Int64Builder)
	vecB := recBuilder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	ids := make([]int64, rows)
	vals := make([]float32, rows*dim)
	for i := 0; i < rows; i++ {
		ids[i] = int64(i)
	}
	idB.AppendValues(ids, nil)
	valB.AppendValues(vals, nil)
	for i := 0; i < rows; i++ {
		vecB.Append(true)
	}
	rec := recBuilder.NewRecordBatch()
	defer rec.Release()
	defer recBuilder.Release()

	// Consume ACKs asynchronously
	errChan := make(chan error, 1)
	go func() {
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				errChan <- nil
				return
			}
			if err != nil {
				errChan <- err
				return
			}
		}
	}()

	b.SetBytes(int64(rows * dim * 4))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := wr.Write(rec); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}

	_ = wr.Close()
	if err := stream.CloseSend(); err != nil {
		b.Logf("CloseSend failed: %v", err)
	}

	if err := <-errChan; err != nil {
		b.Fatalf("Recv failed: %v", err)
	}
}
