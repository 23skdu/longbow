package sharding

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// MockFlightStream implements flight.FlightService_DoGetClient
type MockFlightStream struct {
	grpc.ClientStream
	batches []arrow.RecordBatch
	curr    int
}

func (m *MockFlightStream) Recv() (*flight.FlightData, error) {
	if m.curr >= len(m.batches) {
		return nil, fmt.Errorf("EOF") // Real EOF is io.EOF but helper expects FlightData
	}
	// We need to convert RecordBatch to FlightData... complex to mock fully.
	// Instead, we mock NewRecordReader in the implementation?
	// The implementation calls flight.NewRecordReader(s).
	// That requires s to send valid IPC messages.
	// This is hard to unit test without real IPC.

	// Alternative: Refactor StreamAggregator to take `RecordReader` interface or `[]SearchResult`.
	// Or use `flight.NewRecordWriter` to write to a pipe.
	return nil, nil
}

func (m *MockFlightStream) Header() (metadata.MD, error) { return nil, nil }
func (m *MockFlightStream) Trailer() metadata.MD         { return nil }
func (m *MockFlightStream) CloseSend() error             { return nil }
func (m *MockFlightStream) Context() context.Context     { return context.Background() }
func (m *MockFlightStream) SendMsg(m2 any) error         { return nil }
func (m *MockFlightStream) RecvMsg(m2 any) error         { return nil }

// Since mocking Flight stream is hard (requires IPC bytes), we verify sortAndSlice directly mostly,
// and basic Aggregate flow.

func TestStreamAggregator_SortAndSlice(t *testing.T) {
	mem := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "score", Type: arrow.PrimitiveTypes.Float32},
		}, nil,
	)

	// Create Batch 1: scores [0.1, 0.9]
	b1 := func() arrow.RecordBatch {
		builder := array.NewRecordBuilder(mem, schema)
		defer builder.Release()
		builder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
		builder.Field(1).(*array.Float32Builder).AppendValues([]float32{0.1, 0.9}, nil)
		return builder.NewRecordBatch()
	}()
	defer b1.Release()

	// Create Batch 2: scores [0.5, 0.8]
	b2 := func() arrow.RecordBatch {
		builder := array.NewRecordBuilder(mem, schema)
		defer builder.Release()
		builder.Field(0).(*array.Int32Builder).AppendValues([]int32{3, 4}, nil)
		builder.Field(1).(*array.Float32Builder).AppendValues([]float32{0.5, 0.8}, nil)
		return builder.NewRecordBatch()
	}()
	defer b2.Release()

	sa := NewStreamAggregator(mem, zerolog.Nop())

	// Manually construct table for test
	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{b1, b2})
	defer tbl.Release()

	// Sort Descending (Top K=3) -> Expected: 0.9 (id 2), 0.8 (id 4), 0.5 (id 3)
	results, err := sa.sortAndSlice(tbl, 1, 3, false)
	if err != nil {
		t.Fatalf("sortAndSlice failed: %v", err)
	}
	defer func() {
		for _, r := range results {
			r.Release()
		}
	}()

	if len(results) != 1 {
		t.Fatalf("Expected 1 result batch, got %d", len(results))
	}

	res := results[0]
	if res.NumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", res.NumRows())
	}

	ids := res.Column(0).(*array.Int32)
	scores := res.Column(1).(*array.Float32)

	expectedIDs := []int32{2, 4, 3}
	expectedScores := []float32{0.9, 0.8, 0.5}

	for i := 0; i < 3; i++ {
		if ids.Value(i) != expectedIDs[i] {
			t.Errorf("Row %d: expected ID %d, got %d", i, expectedIDs[i], ids.Value(i))
		}
		if scores.Value(i) != expectedScores[i] {
			t.Errorf("Row %d: expected Score %f, got %f", i, expectedScores[i], scores.Value(i))
		}
	}
}

func BenchmarkStreamAggregator_Merge(b *testing.B) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "score", Type: arrow.PrimitiveTypes.Float32},
		}, nil,
	)

	// Create 10 batches of 100 rows each
	batches := make([]arrow.RecordBatch, 10)
	for i := 0; i < 10; i++ {
		builder := array.NewRecordBuilder(mem, schema)
		ids := make([]int32, 100)
		scores := make([]float32, 100)
		for j := 0; j < 100; j++ {
			ids[j] = int32(i*100 + j)
			scores[j] = float32(j) / 100.0 // overlapping scores
		}
		builder.Field(0).(*array.Int32Builder).AppendValues(ids, nil)
		builder.Field(1).(*array.Float32Builder).AppendValues(scores, nil)
		batches[i] = builder.NewRecordBatch()
	}

	sa := NewStreamAggregator(mem, zerolog.Nop())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clone input batches because merge might release them or take ownership?
		// StreamAggregator.mergeAndSort releases inputs!
		// So we must increment ref count.
		inputs := make([]arrow.RecordBatch, len(batches))
		for k, batch := range batches {
			batch.Retain()
			inputs[k] = batch
		}

		res, err := sa.mergeAndSort(inputs, 50) // Top 50
		if err != nil {
			b.Fatal(err)
		}
		for _, r := range res {
			r.Release()
		}
	}

	for _, batch := range batches {
		batch.Release()
	}
}
