package store

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/coder/hnsw"
)

func init() {
	hnsw.RegisterDistanceFunc("euclidean", hnsw.EuclideanDistance)
	hnsw.RegisterDistanceFunc("cosine", hnsw.CosineDistance)
}

func TestHNSWGraphSync_ExportState(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	ds := NewDataset("test", schema)

	config := DefaultArrowHNSWConfig()
	config.Dims = 128
	idx := NewArrowHNSW(ds, &config)

	for i := 0; i < 10; i++ {
		idx.locationStore.Append(Location{BatchIdx: i / 5, RowIdx: i % 5})
	}
	gsync := NewHNSWGraphSync(idx)
	state, err := gsync.ExportState()
	if err != nil {
		t.Fatalf("ExportState failed: %v", err)
	}
	if len(state) == 0 {
		t.Error("ExportState returned empty data")
	}
}

func TestHNSWGraphSync_ImportState(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	ds := NewDataset("test", schema)
	config := DefaultArrowHNSWConfig()
	config.Dims = 128
	idx := NewArrowHNSW(ds, &config)
	for i := 0; i < 10; i++ {
		idx.locationStore.Append(Location{BatchIdx: i / 5, RowIdx: i % 5})
	}
	gsync := NewHNSWGraphSync(idx)
	state, _ := gsync.ExportState()

	ds2 := NewDataset("test2", schema)
	idx2 := NewArrowHNSW(ds2, &config)
	gsync2 := NewHNSWGraphSync(idx2)
	err := gsync2.ImportState(state)
	if err != nil {
		t.Fatalf("ImportState failed: %v", err)
	}
	if idx2.locationStore.Len() != idx.locationStore.Len() {
		t.Errorf("Location count mismatch: got %d, want %d", idx2.locationStore.Len(), idx.locationStore.Len())
	}
	if idx2.config.Dims != idx.config.Dims {
		t.Errorf("Dims mismatch: got %d, want %d", idx2.config.Dims, idx.config.Dims)
	}
	loc, ok := idx2.locationStore.Get(0)
	if !ok {
		t.Errorf("expected location for ID 0")
	}
	if loc.BatchIdx != 0 || loc.RowIdx != 0 {
		t.Errorf("unexpected location for ID 0: %+v", loc)
	}
}

func TestHNSWGraphSync_GraphExportImport(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	ds := NewDataset("test", schema)
	config := DefaultArrowHNSWConfig()
	config.Dims = 128
	idx := NewArrowHNSW(ds, &config)
	gsync := NewHNSWGraphSync(idx)
	var buf bytes.Buffer
	err := gsync.ExportGraph(&buf)
	if err != nil {
		t.Fatalf("ExportGraph failed: %v", err)
	}
	ds2 := NewDataset("test2", schema)
	idx2 := NewArrowHNSW(ds2, &config)
	gsync2 := NewHNSWGraphSync(idx2)
	err = gsync2.ImportGraph(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ImportGraph failed: %v", err)
	}
}

func TestHNSWGraphSync_DeltaSync(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	ds := NewDataset("test", schema)
	config := DefaultArrowHNSWConfig()
	config.Dims = 128
	idx := NewArrowHNSW(ds, &config)
	for i := 0; i < 10; i++ {
		idx.locationStore.Append(Location{BatchIdx: 0, RowIdx: i})
	}
	gsync := NewHNSWGraphSync(idx)
	version1 := gsync.GetVersion()
	for i := 10; i < 20; i++ {
		idx.locationStore.Append(Location{BatchIdx: 1, RowIdx: i - 10})
	}
	gsync.IncrementVersion()
	delta, err := gsync.ExportDelta(version1)
	if err != nil {
		t.Fatalf("ExportDelta failed: %v", err)
	}
	if delta.FromVersion != version1 {
		t.Errorf("FromVersion mismatch: got %d, want %d", delta.FromVersion, version1)
	}
	if delta.ToVersion <= version1 {
		t.Error("ToVersion should be greater than FromVersion")
	}
}

func TestHNSWGraphSync_Metrics(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	ds := NewDataset("test", schema)
	config := DefaultArrowHNSWConfig()
	config.Dims = 128
	idx := NewArrowHNSW(ds, &config)
	for i := 0; i < 10; i++ {
		idx.locationStore.Append(Location{BatchIdx: 0, RowIdx: i})
	}
	gsync := NewHNSWGraphSync(idx)
	_, _ = gsync.ExportState() //nolint:errcheck
	if gsync.GetExportCount() == 0 {
		t.Error("Export count metric should be incremented")
	}
}

func TestHNSWGraphSync_EmptyGraph(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	ds := NewDataset("test", schema)
	config := DefaultArrowHNSWConfig()
	config.Dims = 128
	idx := NewArrowHNSW(ds, &config)
	gsync := NewHNSWGraphSync(idx)
	state, err := gsync.ExportState()
	if err != nil {
		t.Fatalf("ExportState on empty graph failed: %v", err)
	}
	if state == nil {
		t.Error("Empty graph should still produce valid state bytes")
	}
}

func TestHNSWGraphSync_ConcurrentSync(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	ds := NewDataset("test", schema)
	config := DefaultArrowHNSWConfig()
	config.Dims = 128
	idx := NewArrowHNSW(ds, &config)
	for i := 0; i < 100; i++ {
		idx.locationStore.Append(Location{BatchIdx: i / 10, RowIdx: i % 10})
	}
	gsync := NewHNSWGraphSync(idx)
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_, err := gsync.ExportState()
			if err != nil {
				t.Errorf("Concurrent export failed: %v", err)
			}
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestHNSWGraphSync_ApplyDelta(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	ds := NewDataset("test", schema)
	config := DefaultArrowHNSWConfig()
	config.Dims = 128
	idx := NewArrowHNSW(ds, &config)
	for i := 0; i < 10; i++ {
		idx.locationStore.Append(Location{BatchIdx: 0, RowIdx: i})
	}
	gsync := NewHNSWGraphSync(idx)
	version1 := gsync.GetVersion()
	for i := 10; i < 20; i++ {
		idx.locationStore.Append(Location{BatchIdx: 1, RowIdx: i - 10})
	}
	gsync.IncrementVersion()
	delta, _ := gsync.ExportDelta(version1)
	ds2 := NewDataset("test2", schema)
	idx2 := NewArrowHNSW(ds2, &config)
	for i := 0; i < 10; i++ {
		idx2.locationStore.Append(Location{BatchIdx: 0, RowIdx: i})
	}
	gsync2 := NewHNSWGraphSync(idx2)
	err := gsync2.ApplyDelta(delta)
	if err != nil {
		t.Fatalf("ApplyDelta failed: %v", err)
	}
	if idx2.locationStore.Len() != 20 {
		t.Errorf("Expected 20 locations after delta, got %d", idx2.locationStore.Len())
	}
	_, ok := idx2.locationStore.Get(0)
	if !ok {
		t.Errorf("expected location for ID 0")
	}
	_, ok = idx2.locationStore.Get(19)
	if !ok {
		t.Errorf("expected location for ID 19")
	}
}
