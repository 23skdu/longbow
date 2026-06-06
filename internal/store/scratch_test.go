package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestScratchUint16Distance(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.FixedSizeListOf(16, arrow.PrimitiveTypes.Uint16)},
		},
		nil,
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"500", "501"}, nil)
	vb := b.Field(1).(*array.FixedSizeListBuilder)
	vbVal := vb.ValueBuilder().(*array.Uint16Builder)

	// vector 500: [500, 501, ..., 515]
	for i := 0; i < 16; i++ {
		vbVal.Append(uint16(500 + i))
	}
	// vector 501: [501, 502, ..., 516]
	for i := 0; i < 16; i++ {
		vbVal.Append(uint16(501 + i))
	}

	vb.Append(true)
	vb.Append(true)

	rec := b.NewRecordBatch()
	defer rec.Release()

	ds := &Dataset{
		Name:    "scratch_test",
		Records: NewLockFreeSliceFrom([]arrow.RecordBatch{rec}),
		Schema:  schema,
	}

	config := DefaultArrowHNSWConfig()
	config.DataType = types.VectorTypeUint16
	idx := NewArrowHNSW(ds, &config, nil)

	_, err := idx.AddByLocation(context.Background(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = idx.AddByLocation(context.Background(), 0, 1)
	if err != nil {
		t.Fatal(err)
	}

	vec500, err := idx.GetVector(0)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("SCRATCH: vec500 type = %T, val = %v\n", vec500, vec500)

	opts := types.DefaultSearchOptions()
	res, err := idx.SearchVectors(context.Background(), vec500, 2, nil, opts)
	if err != nil {
		t.Fatal(err)
	}

	for i, r := range res {
		fmt.Printf("SCRATCH: Result[%d]: ID = %d, Dist = %f\n", i, r.ID, r.Distance)
	}
}
