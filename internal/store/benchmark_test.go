package store

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// setupDataServerBench creates a DataServer with in-memory gRPC for benchmarking
func setupDataServerBench(b *testing.B, cfg StorageConfig) flight.Client {
	lis := bufconn.Listen(testBufSize)

	tmpDir, err := os.MkdirTemp("", "bench_dataserver_*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}

	// Ensure config uses this temp dir
	cfg.DataPath = tmpDir

	mem := memory.NewGoAllocator()
	logger := zerolog.Nop()
	// 512MB memory limit for benchmarks
	vs := NewVectorStore(mem, logger, 1024*1024*512, 0, 0)

	if err := vs.InitPersistence(cfg); err != nil {
		b.Fatalf("Failed to init persistence: %v", err)
	}

	ds := NewDataServer(vs)
	s := grpc.NewServer()
	flight.RegisterFlightServiceServer(s, ds)

	go func() {
		_ = s.Serve(lis)
	}()

	dialer := func(ctx context.Context, address string) (net.Conn, error) {
		return lis.Dial()
	}

	client, err := flight.NewClientWithMiddleware(
		"passthrough:///bufnet",
		nil,
		nil,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}

	b.Cleanup(func() {
		_ = client.Close()
		s.Stop()
		_ = lis.Close()
		_ = vs.Close()
		_ = os.RemoveAll(tmpDir)
	})

	return client
}

func BenchmarkE2EDoGet(b *testing.B) {
	benchmarks := []struct {
		name string
		cfg  StorageConfig
	}{
		{"Buffered", StorageConfig{}},
		{"DirectIO", StorageConfig{UseDirectIO: true}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			client := setupDataServerBench(b, bm.cfg)
			ctx := context.Background()

			// 1. Prepare Data
			schema := arrow.NewSchema(
				[]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int32},
					{Name: "val", Type: arrow.PrimitiveTypes.Float64},
				},
				nil,
			)

			mem := memory.NewGoAllocator()
			builder := array.NewRecordBuilder(mem, schema)
			defer builder.Release()

			numRows := 10000
			ids := make([]int32, numRows)
			vals := make([]float64, numRows)
			for i := 0; i < numRows; i++ {
				ids[i] = int32(i)
				vals[i] = float64(i) * 1.1
			}
			builder.Field(0).(*array.Int32Builder).AppendValues(ids, nil)
			builder.Field(1).(*array.Float64Builder).AppendValues(vals, nil)
			rec := builder.NewRecordBatch()
			defer rec.Release()

			// 2. DoPut (Seed data)
			stream, err := client.DoPut(ctx)
			if err != nil {
				b.Fatalf("DoPut failed: %v", err)
			}
			w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
			w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"bench_e2e"}})
			if err := w.Write(rec); err != nil {
				b.Fatal(err)
			}
			_ = w.Close()
			_ = stream.CloseSend()
			_, _ = stream.Recv()

			ticketBytes, _ := json.Marshal(map[string]interface{}{"name": "bench_e2e"})
			ticket := &flight.Ticket{Ticket: ticketBytes}

			// 3. Benchmark DoGet
			b.ResetTimer()
			b.SetBytes(int64(numRows * (4 + 8))) // 12 bytes per row approx
			for i := 0; i < b.N; i++ {
				rStream, err := client.DoGet(ctx, ticket)
				if err != nil {
					b.Fatal(err)
				}
				reader, err := flight.NewRecordReader(rStream)
				if err != nil {
					b.Fatal(err)
				}

				rowsRead := 0
				for reader.Next() {
					rowsRead += int(reader.RecordBatch().NumRows())
				}
				reader.Release()

				if rowsRead != numRows {
					b.Errorf("Expected %d rows, got %d", numRows, rowsRead)
				}
			}
		})
	}
}

func BenchmarkE2EDoPut(b *testing.B) {
	benchmarks := []struct {
		name string
		cfg  StorageConfig
	}{
		{"Buffered", StorageConfig{}},
		{"DirectIO", StorageConfig{UseDirectIO: true}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			client := setupDataServerBench(b, bm.cfg)
			ctx := context.Background()

			schema := arrow.NewSchema(
				[]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int32},
				},
				nil,
			)

			mem := memory.NewGoAllocator()
			builder := array.NewRecordBuilder(mem, schema)
			defer builder.Release()

			numRows := 1000
			ids := make([]int32, numRows)
			for i := 0; i < numRows; i++ {
				ids[i] = int32(i)
			}
			builder.Field(0).(*array.Int32Builder).AppendValues(ids, nil)
			rec := builder.NewRecordBatch()
			defer rec.Release()

			b.ResetTimer()
			b.SetBytes(int64(numRows * 4))
			for i := 0; i < b.N; i++ {
				stream, err := client.DoPut(ctx)
				if err != nil {
					b.Fatal(err)
				}
				w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
				w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{fmt.Sprintf("bench_put_%d", i)}})
				_ = w.Write(rec)
				_ = w.Close()
				_ = stream.CloseSend()
				_, _ = stream.Recv()
			}
		})
	}
}

func BenchmarkStreamedDoPut(b *testing.B) {
	benchmarks := []struct {
		name string
		cfg  StorageConfig
	}{
		{"Buffered", StorageConfig{}},
		{"DirectIO", StorageConfig{UseDirectIO: true}},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			client := setupDataServerBench(b, bm.cfg)
			ctx := context.Background()

			schema := arrow.NewSchema(
				[]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int32},
				},
				nil,
			)

			mem := memory.NewGoAllocator()
			builder := array.NewRecordBuilder(mem, schema)
			defer builder.Release()

			numRows := 1000
			ids := make([]int32, numRows)
			for i := 0; i < numRows; i++ {
				ids[i] = int32(i)
			}
			builder.Field(0).(*array.Int32Builder).AppendValues(ids, nil)
			rec := builder.NewRecordBatch()
			defer rec.Release()

			stream, err := client.DoPut(ctx)
			if err != nil {
				b.Fatalf("Failed to open DoPut stream: %v", err)
			}
			w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
			w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"bench_streamed"}})

			b.ResetTimer()
			b.SetBytes(int64(numRows * 4))
			for i := 0; i < b.N; i++ {
				if err := w.Write(rec); err != nil {
					b.Fatalf("Write failed at iteration %d: %v", i, err)
				}
			}
			_ = w.Close()
			_ = stream.CloseSend()
			_, _ = stream.Recv()
		})
	}
}
