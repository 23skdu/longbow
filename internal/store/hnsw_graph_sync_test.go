package store

import (
"bytes"
"testing"
)

func TestHNSWGraphSync_ExportState(t *testing.T) {
ds := &Dataset{Name: "test"}
idx := NewHNSWIndex(ds)
idx.dims = 128
for i := 0; i < 10; i++ {
idx.locations = append(idx.locations, Location{BatchIdx: i / 5, RowIdx: i % 5})
}
sync := NewHNSWGraphSync(idx)
state, err := sync.ExportState()
if err != nil {
t.Fatalf("ExportState failed: %v", err)
}
if len(state) == 0 {
t.Error("ExportState returned empty data")
}
}

func TestHNSWGraphSync_ImportState(t *testing.T) {
ds := &Dataset{Name: "test"}
idx := NewHNSWIndex(ds)
idx.dims = 128
for i := 0; i < 10; i++ {
idx.locations = append(idx.locations, Location{BatchIdx: i / 5, RowIdx: i % 5})
}
sync := NewHNSWGraphSync(idx)
state, _ := sync.ExportState()
ds2 := &Dataset{Name: "test2"}
idx2 := NewHNSWIndex(ds2)
sync2 := NewHNSWGraphSync(idx2)
err := sync2.ImportState(state)
if err != nil {
t.Fatalf("ImportState failed: %v", err)
}
if len(idx2.locations) != len(idx.locations) {
t.Errorf("Location count mismatch: got %d, want %d", len(idx2.locations), len(idx.locations))
}
if idx2.dims != idx.dims {
t.Errorf("Dims mismatch: got %d, want %d", idx2.dims, idx.dims)
}
}

func TestHNSWGraphSync_GraphExportImport(t *testing.T) {
ds := &Dataset{Name: "test"}
idx := NewHNSWIndex(ds)
idx.dims = 128
sync := NewHNSWGraphSync(idx)
var buf bytes.Buffer
err := sync.ExportGraph(&buf)
if err != nil {
t.Fatalf("ExportGraph failed: %v", err)
}
ds2 := &Dataset{Name: "test2"}
idx2 := NewHNSWIndex(ds2)
sync2 := NewHNSWGraphSync(idx2)
err = sync2.ImportGraph(bytes.NewReader(buf.Bytes()))
if err != nil {
t.Fatalf("ImportGraph failed: %v", err)
}
}

func TestHNSWGraphSync_DeltaSync(t *testing.T) {
ds := &Dataset{Name: "test"}
idx := NewHNSWIndex(ds)
idx.dims = 128
for i := 0; i < 10; i++ {
idx.locations = append(idx.locations, Location{BatchIdx: 0, RowIdx: i})
}
sync := NewHNSWGraphSync(idx)
version1 := sync.GetVersion()
for i := 10; i < 20; i++ {
idx.locations = append(idx.locations, Location{BatchIdx: 1, RowIdx: i - 10})
}
sync.IncrementVersion()
delta, err := sync.ExportDelta(version1)
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
ds := &Dataset{Name: "test"}
idx := NewHNSWIndex(ds)
idx.dims = 128
for i := 0; i < 10; i++ {
idx.locations = append(idx.locations, Location{BatchIdx: 0, RowIdx: i})
}
sync := NewHNSWGraphSync(idx)
_, _ = sync.ExportState() //nolint:errcheck
if sync.GetExportCount() == 0 {
t.Error("Export count metric should be incremented")
}
}

func TestHNSWGraphSync_EmptyGraph(t *testing.T) {
ds := &Dataset{Name: "test"}
idx := NewHNSWIndex(ds)
sync := NewHNSWGraphSync(idx)
state, err := sync.ExportState()
if err != nil {
t.Fatalf("ExportState on empty graph failed: %v", err)
}
if state == nil {
t.Error("Empty graph should still produce valid state bytes")
}
}

func TestHNSWGraphSync_ConcurrentSync(t *testing.T) {
ds := &Dataset{Name: "test"}
idx := NewHNSWIndex(ds)
idx.dims = 128
for i := 0; i < 100; i++ {
idx.locations = append(idx.locations, Location{BatchIdx: i / 10, RowIdx: i % 10})
}
sync := NewHNSWGraphSync(idx)
done := make(chan bool, 10)
for i := 0; i < 10; i++ {
go func() {
_, err := sync.ExportState()
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
ds := &Dataset{Name: "test"}
idx := NewHNSWIndex(ds)
idx.dims = 128
for i := 0; i < 10; i++ {
idx.locations = append(idx.locations, Location{BatchIdx: 0, RowIdx: i})
}
sync := NewHNSWGraphSync(idx)
version1 := sync.GetVersion()
for i := 10; i < 20; i++ {
idx.locations = append(idx.locations, Location{BatchIdx: 1, RowIdx: i - 10})
}
sync.IncrementVersion()
delta, _ := sync.ExportDelta(version1)
ds2 := &Dataset{Name: "test2"}
idx2 := NewHNSWIndex(ds2)
idx2.dims = 128
for i := 0; i < 10; i++ {
idx2.locations = append(idx2.locations, Location{BatchIdx: 0, RowIdx: i})
}
sync2 := NewHNSWGraphSync(idx2)
err := sync2.ApplyDelta(delta)
if err != nil {
t.Fatalf("ApplyDelta failed: %v", err)
}
if len(idx2.locations) != 20 {
t.Errorf("Expected 20 locations after delta, got %d", len(idx2.locations))
}
}
