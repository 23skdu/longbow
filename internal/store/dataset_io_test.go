package store

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

type testSnapshotBackend struct {
	files map[string]*bytes.Buffer
}

func newTestSnapshotBackend() *testSnapshotBackend {
	return &testSnapshotBackend{
		files: make(map[string]*bytes.Buffer),
	}
}

func (m *testSnapshotBackend) WriteSnapshot(ctx context.Context, name string, data []byte) error {
	buf := bytes.NewBuffer(data)
	m.files[name] = buf
	return nil
}

func (m *testSnapshotBackend) ReadSnapshot(ctx context.Context, name string) (io.ReadCloser, error) {
	if buf, ok := m.files[name]; ok {
		return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
	}
	return nil, &storage.NotFoundError{Name: name}
}

func (m *testSnapshotBackend) ListSnapshots(ctx context.Context) ([]string, error) {
	var names []string
	for name := range m.files {
		names = append(names, name)
	}
	return names, nil
}

func (m *testSnapshotBackend) DeleteSnapshot(ctx context.Context, name string) error {
	delete(m.files, name)
	return nil
}

func (m *testSnapshotBackend) WriteSnapshotAsync(name string, data []byte) {
	m.files[name] = bytes.NewBuffer(data)
}

func (m *testSnapshotBackend) WriteSnapshotFile(ctx context.Context, name, ext string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.files[name+ext] = bytes.NewBuffer(data)
	return nil
}

func (m *testSnapshotBackend) ReadSnapshotFile(ctx context.Context, name, ext string) (io.ReadCloser, error) {
	if buf, ok := m.files[name+ext]; ok {
		return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
	}
	return nil, &storage.NotFoundError{Name: name + ext}
}

func (m *testSnapshotBackend) Bucket() string { return "test-bucket" }
func (m *testSnapshotBackend) Prefix() string { return "test-prefix" }
func (m *testSnapshotBackend) GetHTTPTransport() *http.Transport {
	return &http.Transport{}
}
func (m *testSnapshotBackend) GetHTTPClient() *http.Client {
	return &http.Client{}
}

func createTestSchemaForDatasetIO() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
		{Name: "metadata", Type: arrow.BinaryTypes.Binary},
		{Name: "created_at", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
}

func createTestRecordForDatasetIO(pool memory.Allocator, schema *arrow.Schema, numRows int) arrow.Record {
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	idBldr := b.Field(0).(*array.Int64Builder)
	vecBldr := b.Field(1).(*array.FixedSizeListBuilder)
	vecValBldr := vecBldr.ValueBuilder().(*array.Float32Builder)
	metaBldr := b.Field(2).(*array.BinaryBuilder)
	createdBldr := b.Field(3).(*array.Int64Builder)

	for i := 0; i < numRows; i++ {
		idBldr.Append(int64(i))

		vecBldr.Append(true)
		for j := 0; j < 4; j++ {
			vecValBldr.Append(float32(i*4 + j))
		}

		metaBldr.Append([]byte(`{"key": "value"}`))
		createdBldr.Append(int64(i * 1000))
	}

	return b.NewRecord()
}

func TestDatasetIOExportToParquet(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := createTestSchemaForDatasetIO()

	ds := NewDataset("test-dataset", schema)
	rec := createTestRecordForDatasetIO(pool, schema, 10)
	ds.Records = append(ds.Records, rec)
	ds.BatchNodes = append(ds.BatchNodes, -1)

	vs := NewVectorStore(pool, zerolog.New(os.Stderr), 1<<30, 0, 0)
	vs.datasets.Store(&map[string]*Dataset{"test-dataset": ds})

	backend := newTestSnapshotBackend()
	dio := NewDatasetIO(vs)

	ctx := context.Background()
	_, err := dio.ExportToParquet(ctx, "test-dataset", backend)
	if err != nil {
		t.Fatalf("ExportToParquet failed: %v", err)
	}

	if len(backend.files) == 0 {
		t.Fatal("expected files to be written to backend")
	}
}

func TestDatasetIOImportFromParquet(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := createTestSchemaForDatasetIO()

	vs := NewVectorStore(pool, zerolog.New(os.Stderr), 1<<30, 0, 0)
	vs.datasets.Store(&map[string]*Dataset{"test-dataset": NewDataset("test-dataset", schema)})

	backend := newTestSnapshotBackend()
	dio := NewDatasetIO(vs)

	ctx := context.Background()
	_, err := dio.ImportFromParquet(ctx, "test-dataset", backend, schema)
	if err != nil {
		t.Logf("ImportFromParquet with no data: %v", err)
	}
}

func TestDatasetHeaderValidate(t *testing.T) {
	tests := []struct {
		name    string
		header  DatasetHeader
		wantErr bool
	}{
		{
			name: "valid header",
			header: DatasetHeader{
				Magic:   DatasetMagic,
				Version: DatasetVersion,
			},
			wantErr: false,
		},
		{
			name: "invalid magic",
			header: DatasetHeader{
				Magic:   "INVALID",
				Version: DatasetVersion,
			},
			wantErr: true,
		},
		{
			name: "invalid version",
			header: DatasetHeader{
				Magic:   DatasetMagic,
				Version: 999,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.header.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDatasetIOEmptyDataset(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := createTestSchemaForDatasetIO()

	vs := NewVectorStore(pool, zerolog.New(os.Stderr), 1<<30, 0, 0)
	vs.datasets.Store(&map[string]*Dataset{"empty-dataset": NewDataset("empty-dataset", schema)})

	backend := newTestSnapshotBackend()
	dio := NewDatasetIO(vs)

	ctx := context.Background()
	_, err := dio.ExportToParquet(ctx, "empty-dataset", backend)
	if err != nil {
		t.Fatalf("ExportToParquet failed: %v", err)
	}
}

func BenchmarkDatasetIOExportToParquet(b *testing.B) {
	pool := memory.NewGoAllocator()
	schema := createTestSchemaForDatasetIO()

	ds := NewDataset("bench-dataset", schema)
	for i := 0; i < 100; i++ {
		rec := createTestRecordForDatasetIO(pool, schema, 100)
		ds.Records = append(ds.Records, rec)
		ds.BatchNodes = append(ds.BatchNodes, -1)
	}

	vs := NewVectorStore(pool, zerolog.New(os.Stderr), 1<<30, 0, 0)
	vs.datasets.Store(&map[string]*Dataset{"bench-dataset": ds})

	backend := newTestSnapshotBackend()
	dio := NewDatasetIO(vs)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dio.ExportToParquet(ctx, "bench-dataset", backend)
	}
}
