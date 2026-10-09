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

func TestStreamAggregator_TournamentHeap(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "distance", Type: arrow.PrimitiveTypes.Float32},
			{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
		}, nil,
	)

	// Shard 0: distances [0.1, 0.4, 0.7] (pre-sorted ascending)
	b0 := func() arrow.RecordBatch {
		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()
		b.Field(0).(*array.Int32Builder).AppendValues([]int32{10, 11, 12}, nil)
		b.Field(1).(*array.Float32Builder).AppendValues([]float32{0.1, 0.4, 0.7}, nil)
		vb := b.Field(2).(*array.FixedSizeListBuilder)
		vb.Append(true)
		vb.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{1.0, 1.1}, nil)
		vb.Append(true)
		vb.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{2.0, 2.1}, nil)
		vb.Append(true)
		vb.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{3.0, 3.1}, nil)
		return b.NewRecordBatch()
	}()

	// Shard 1: distances [0.2, 0.3, 0.9] (pre-sorted ascending)
	b1 := func() arrow.RecordBatch {
		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()
		b.Field(0).(*array.Int32Builder).AppendValues([]int32{20, 21, 22}, nil)
		b.Field(1).(*array.Float32Builder).AppendValues([]float32{0.2, 0.3, 0.9}, nil)
		vb := b.Field(2).(*array.FixedSizeListBuilder)
		vb.Append(true)
		vb.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{4.0, 4.1}, nil)
		vb.Append(true)
		vb.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{5.0, 5.1}, nil)
		vb.Append(true)
		vb.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{6.0, 6.1}, nil)
		return b.NewRecordBatch()
	}()

	// Shard 2: distances [0.05, 0.5, 0.8] (pre-sorted ascending)
	b2 := func() arrow.RecordBatch {
		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()
		b.Field(0).(*array.Int32Builder).AppendValues([]int32{30, 31, 32}, nil)
		b.Field(1).(*array.Float32Builder).AppendValues([]float32{0.05, 0.5, 0.8}, nil)
		vb := b.Field(2).(*array.FixedSizeListBuilder)
		vb.Append(true)
		vb.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{7.0, 7.1}, nil)
		vb.Append(true)
		vb.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{8.0, 8.1}, nil)
		vb.Append(true)
		vb.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{9.0, 9.1}, nil)
		return b.NewRecordBatch()
	}()

	sa := NewStreamAggregator(mem, zerolog.Nop())

	// Top K = 4: Expected order of distances: 0.05 (id 30), 0.1 (id 10), 0.2 (id 20), 0.3 (id 21)
	res, err := sa.mergeAndSort([]arrow.RecordBatch{b0, b1, b2}, 4)
	if err != nil {
		t.Fatalf("mergeAndSort failed: %v", err)
	}
	defer func() {
		for _, r := range res {
			r.Release()
		}
	}()

	if len(res) != 1 {
		t.Fatalf("Expected 1 result batch, got %d", len(res))
	}
	rec := res[0]
	if rec.NumRows() != 4 {
		t.Fatalf("Expected 4 rows, got %d", rec.NumRows())
	}

	ids := rec.Column(0).(*array.Int32)
	distances := rec.Column(1).(*array.Float32)
	vectors := rec.Column(2).(*array.FixedSizeList)

	expectedIDs := []int32{30, 10, 20, 21}
	expectedDist := []float32{0.05, 0.1, 0.2, 0.3}

	for i := 0; i < 4; i++ {
		if ids.Value(i) != expectedIDs[i] {
			t.Errorf("Row %d: expected ID %d, got %d", i, expectedIDs[i], ids.Value(i))
		}
		if distances.Value(i) != expectedDist[i] {
			t.Errorf("Row %d: expected distance %f, got %f", i, expectedDist[i], distances.Value(i))
		}
		if vectors.IsNull(i) {
			t.Errorf("Row %d: vector is null", i)
		}
	}
}
