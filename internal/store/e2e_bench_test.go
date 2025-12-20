package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func BenchmarkE2EDoGet(b *testing.B) {
	_, _, dialer := setupServer(&testing.T{}) // Use &testing.T{} for helper compatibility

	ctx := context.Background()
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
	defer func() { _ = client.Close() }()

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
	stream, _ := client.DoPut(ctx)
	w := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	w.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"bench_e2e"}})
	_ = w.Write(rec)
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

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

func BenchmarkE2EDoPut(b *testing.B) {
	_, _, dialer := setupServer(&testing.T{})

	ctx := context.Background()
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
	defer func() { _ = client.Close() }()

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

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}
