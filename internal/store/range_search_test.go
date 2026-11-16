package store

import (
	"context"
	"math/rand"
	"testing"
	"time"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_RangeSearch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	dims := 128
	count := 100

	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.DataType = lbtypes.VectorTypeFloat32
	config.M = 16
	config.EfConstruction = 64
	config.EfSearch = 64

	idx := NewArrowHNSW(nil, &config, nil)

	vectors := make([][]float32, count)
	for i := 0; i < count; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = rand.Float32()
		}
	}

	err := idx.AddBatchBulk(context.Background(), 0, count, vectors)
	if err != nil {
		t.Fatalf("failed to add vectors: %v", err)
	}

	ctx := context.Background()
	queryVec := vectors[0]

	results, err := idx.SearchVectorsInRange(ctx, queryVec, 0.5, nil, nil)
	if err != nil {
		t.Fatalf("range search failed: %v", err)
	}

	t.Logf("Range search found %d results within threshold 0.5", len(results))

	results, err = idx.SearchVectorsInRange(ctx, queryVec, 100.0, nil, nil)
	if err != nil {
		t.Fatalf("range search failed: %v", err)
	}

	if len(results) == 0 {
		t.Error("expected results with high threshold")
	}

	t.Logf("Range search found %d results within threshold 100.0", len(results))

	_ = idx.Close()
}

func TestArrowHNSW_RangeSearch_Empty(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	dims := 128

	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.DataType = lbtypes.VectorTypeFloat32

	idx := NewArrowHNSW(nil, &config, nil)

	ctx := context.Background()
	queryVec := make([]float32, dims)
	for i := range queryVec {
		queryVec[i] = rand.Float32()
	}

	results, err := idx.SearchVectorsInRange(ctx, queryVec, 50.0, nil, nil)
	if err != nil {
		t.Fatalf("range search failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected no results for empty index, got %d", len(results))
	}

	_ = idx.Close()
}

func TestVectorStore_RangeSearch_Complex64AndComplex128(t *testing.T) {
	const (
		dims  = 8
		nrows = 16
	)
	for _, tc := range []struct {
		name     string
		elemType arrow.DataType
		metaType string
	}{
		{"complex64", arrow.PrimitiveTypes.Float32, "complex64"},
		{"complex128", arrow.PrimitiveTypes.Float64, "complex128"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewGoAllocator()
			vs := NewVectorStore(mem, zerolog.Nop(), 32*1024*1024, 32*1024*1024, 10*time.Millisecond)
			vs.dataPath = t.TempDir()
			vs.StartIngestionWorkers(2)
			vs.StartIndexingWorkers(2)
			defer func() { _ = vs.Close() }()

			vecMeta := arrow.NewMetadata([]string{"longbow.vector_type"}, []string{tc.metaType})
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
				{Name: "vector", Type: arrow.FixedSizeListOf(dims*2, tc.elemType), Nullable: false, Metadata: vecMeta},
			}, nil)

			idB := array.NewStringBuilder(mem)
			defer idB.Release()
			fslB := array.NewFixedSizeListBuilder(mem, dims*2, tc.elemType)
			defer fslB.Release()
			for r := 0; r < nrows; r++ {
				idB.Append(string(rune('a' + r)))
				fslB.Append(true)
				switch vb := fslB.ValueBuilder().(type) {
				case *array.Float32Builder:
					for d := 0; d < dims*2; d++ {
						vb.Append(float32(r*dims*2+d) * 0.01)
					}
				case *array.Float64Builder:
					for d := 0; d < dims*2; d++ {
						vb.Append(float64(r*dims*2+d) * 0.01)
					}
				}
			}
			cols := []arrow.Array{idB.NewArray(), fslB.NewArray()}
			defer cols[0].Release()
			defer cols[1].Release()
			rec := array.NewRecordBatch(schema, cols, nrows)
			defer rec.Release()

			dsName := "rs_" + tc.name
			if err := vs.StoreRecordBatch(context.Background(), dsName, rec); err != nil {
				t.Fatalf("StoreRecordBatch: %v", err)
			}
			require.Eventually(t, func() bool {
				ds, ok := vs.getDataset(dsName)
				return ok && ds.IndexLen() >= nrows
			}, 5*time.Second, 25*time.Millisecond)

			q := make([]float32, dims*2)
			res, err := vs.RangeSearch(context.Background(), dsName, q, 10.0, nil, 5)
			if err != nil {
				t.Fatalf("RangeSearch failed: %v", err)
			}
			if len(res) == 0 {
				t.Fatalf("expected non-empty RangeSearch results for %s", tc.name)
			}
		})
	}
}
