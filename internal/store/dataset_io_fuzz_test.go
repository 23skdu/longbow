package store

import (
	"context"
	"os"
	"testing"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

func FuzzDatasetHeaderValidate(f *testing.F) {
	f.Fuzz(func(t *testing.T, magic string, version int) {
		header := DatasetHeader{
			Magic:   magic,
			Version: version,
		}
		_ = header.Validate()
	})
}

func FuzzDatasetIOExportImport(f *testing.F) {
	f.Fuzz(func(t *testing.T, numVectors int, vectorDim int) {
		if numVectors <= 0 || numVectors > 10000 {
			t.Skip()
		}
		if vectorDim <= 0 || vectorDim > 4096 {
			t.Skip()
		}

		pool := memory.NewGoAllocator()
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(vectorDim), arrow.PrimitiveTypes.Float32)},
			{Name: "metadata", Type: arrow.BinaryTypes.Binary},
			{Name: "created_at", Type: arrow.PrimitiveTypes.Int64},
		}, nil)

		ds := NewDataset("fuzz-dataset", schema)

		b := array.NewRecordBuilder(pool, schema)
		idBldr := b.Field(0).(*array.Int64Builder)
		vecBldr := b.Field(1).(*array.FixedSizeListBuilder)
		vecValBldr := vecBldr.ValueBuilder().(*array.Float32Builder)
		metaBldr := b.Field(2).(*array.BinaryBuilder)
		createdBldr := b.Field(3).(*array.Int64Builder)

		for i := 0; i < numVectors; i++ {
			idBldr.Append(int64(i))
			vecBldr.Append(true)
			for j := 0; j < vectorDim; j++ {
				vecValBldr.Append(float32(i*vectorDim + j))
			}
			metaBldr.Append([]byte(`{"index": ` + string(rune(i+'0')) + `}`))
			createdBldr.Append(int64(i * 1000))
		}

		rec := b.NewRecord()
		ds.Records = append(ds.Records, rec)
		ds.BatchNodes = append(ds.BatchNodes, -1)
		vs := NewVectorStore(pool, zerolog.Nop(), 1<<30, 0, 0)
		vs.datasets.Store(&map[string]*Dataset{"fuzz-dataset": ds})

		tmpDir, _ := os.MkdirTemp("", "fuzz-*")
		defer os.RemoveAll(tmpDir)
		backend, _ := storage.NewFileSnapshotBackend(tmpDir)
		dio := NewDatasetIO(vs)
		ctx := context.Background()

		n, err := dio.ExportToParquet(ctx, "fuzz-dataset", backend)
		if err != nil {
			t.Fatalf("ExportToParquet failed: %v", err)
		}
		if n != int64(numVectors) {
			t.Errorf("expected %d vectors, got %d", numVectors, n)
		}
	})
}

func FuzzDatasetParquetRecord(f *testing.F) {
	f.Fuzz(func(t *testing.T, id int64, vecData []byte, metaData []byte, createdAt int64) {
		record := DatasetParquetRecord{
			ID:        id,
			Vector:    vecData,
			Metadata:  metaData,
			CreatedAt: createdAt,
		}

		if record.ID == 0 && len(record.Vector) == 0 && len(record.Metadata) == 0 && record.CreatedAt == 0 {
			t.Skip("all zero values")
		}
	})
}
