package store

import (
"bytes"
"encoding/binary"
"fmt"
"io"
"os"
"path/filepath"
"time"

"github.com/23skdu/longbow/internal/metrics"
"github.com/apache/arrow/go/v18/arrow"
"github.com/apache/arrow/go/v18/arrow/ipc"

	"golang.org/x/exp/mmap")

const (
walFileName = "wal.log"
snapshotDirName = "snapshots"
)

// InitPersistence initializes the WAL and loads any existing data
func (s *VectorStore) InitPersistence(dataPath string, snapshotInterval time.Duration) error {
s.dataPath = dataPath
if err := os.MkdirAll(s.dataPath, 0755); err != nil {
return fmt.Errorf("failed to create data directory: %w", err)
}

// Load latest snapshot if exists
if err := s.loadSnapshots(); err != nil {
s.logger.Error("Failed to load snapshots", "error", err)
// Continue, maybe partial load or fresh start
}

// Replay WAL
if err := s.replayWAL(); err != nil {
s.logger.Error("Failed to replay WAL", "error", err)
return err
}

// Open WAL for appending
walPath := filepath.Join(s.dataPath, walFileName)
f, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
if err != nil {
return fmt.Errorf("failed to open WAL file: %w", err)
}
s.walFile = f

// Start snapshot ticker
go s.runSnapshotTicker(snapshotInterval)

return nil
}

func (s *VectorStore) writeToWAL(rec arrow.Record, name string) error {
s.walMu.Lock()
defer s.walMu.Unlock()

if rec == nil {
return fmt.Errorf("record is nil")
}
if s.walFile == nil {
return nil // Persistence disabled or not initialized
}

// Format: [NameLen: uint32][Name: bytes][RecordLen: uint64][RecordBytes: bytes]

// 1. Serialize Record to buffer
var buf bytes.Buffer
w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(s.mem))
if err := w.Write(rec); err != nil {
metrics.WalWritesTotal.WithLabelValues("error").Inc()
return fmt.Errorf("failed to serialize record for WAL: %w", err)
}
if err := w.Close(); err != nil {
metrics.WalWritesTotal.WithLabelValues("error").Inc()
return fmt.Errorf("failed to close IPC writer: %w", err)
}
recBytes := buf.Bytes()

// 2. Write Header & Data
nameBytes := []byte(name)
nameLen := uint32(len(nameBytes))
recLen := uint64(len(recBytes))

if err := binary.Write(s.walFile, binary.LittleEndian, nameLen); err != nil {
metrics.WalWritesTotal.WithLabelValues("error").Inc()
return err
}
if _, err := s.walFile.Write(nameBytes); err != nil {
metrics.WalWritesTotal.WithLabelValues("error").Inc()
return err
}
if err := binary.Write(s.walFile, binary.LittleEndian, recLen); err != nil {
metrics.WalWritesTotal.WithLabelValues("error").Inc()
return err
}
n, err := s.walFile.Write(recBytes)
if err != nil {
metrics.WalWritesTotal.WithLabelValues("error").Inc()
return err
}

// Metrics success
metrics.WalWritesTotal.WithLabelValues("ok").Inc()
metrics.WalBytesWritten.Add(float64(4 + len(nameBytes) + 8 + n))

// Ensure it's on disk
// return s.walFile.Sync() // Optional: Sync every write for durability vs performance
return nil
}

func (s *VectorStore) replayWAL() error {
start := time.Now()
defer func() {
metrics.WalReplayDurationSeconds.Observe(time.Since(start).Seconds())
}()

walPath := filepath.Join(s.dataPath, walFileName)
f, err := os.Open(walPath)
if os.IsNotExist(err) {
return nil
}
if err != nil {
return err
}
defer f.Close()

s.logger.Info("Replaying WAL...")
count := 0

for {
var nameLen uint32
if err := binary.Read(f, binary.LittleEndian, &nameLen); err != nil {
if err == io.EOF {
break
}
return fmt.Errorf("wal read error (nameLen): %w", err)
}

nameBytes := make([]byte, nameLen)
if _, err := io.ReadFull(f, nameBytes); err != nil {
return fmt.Errorf("wal read error (name): %w", err)
}
name := string(nameBytes)

var recLen uint64
if err := binary.Read(f, binary.LittleEndian, &recLen); err != nil {
return fmt.Errorf("wal read error (recLen): %w", err)
}

recBytes := make([]byte, recLen)
if _, err := io.ReadFull(f, recBytes); err != nil {
return fmt.Errorf("wal read error (recBytes): %w", err)
}

// Deserialize Record
r, err := ipc.NewReader(bytes.NewReader(recBytes))
if err != nil {
return fmt.Errorf("wal ipc reader error: %w", err)
}
if r.Next() {
rec := r.Record()
rec.Retain()
// Append to store (skipping WAL write)
s.mu.Lock()
if _, ok := s.vectors[name]; !ok {
	s.vectors[name] = &Dataset{Records: []arrow.Record{}, LastAccess: time.Now()}
}
s.vectors[name].Records = append(s.vectors[name].Records, rec)
s.currentMemory += calculateRecordSize(rec)
s.mu.Unlock()
count++
}
r.Release()
}

s.logger.Info("WAL Replay complete", "records_loaded", count)
return nil
}

func (s *VectorStore) Snapshot() error {
start := time.Now()
s.logger.Info("Starting Snapshot...")
s.mu.RLock()
defer s.mu.RUnlock()

snapshotDir := filepath.Join(s.dataPath, snapshotDirName)
tempDir := filepath.Join(s.dataPath, snapshotDirName+"_tmp")

// Clean up any previous temp dir
if err := os.RemoveAll(tempDir); err != nil {
return fmt.Errorf("failed to clean temp snapshot dir: %w", err)
}
if err := os.MkdirAll(tempDir, 0755); err != nil {
metrics.SnapshotTotal.WithLabelValues("error").Inc()
return fmt.Errorf("failed to create temp snapshot dir: %w", err)
}

// Save each dataset to temp dir
for name, ds := range s.vectors {
		recs := ds.Records
if len(recs) == 0 {
continue
}
path := filepath.Join(tempDir, name+".arrow")
f, err := os.Create(path)
if err != nil {
s.logger.Error("Failed to create snapshot file", "name", name, "error", err)
continue
}

// We need to write all records. Since they might have different schemas (unlikely due to validation) or just be chunks.
// We assume same schema for a dataset.
w := ipc.NewWriter(f, ipc.WithSchema(recs[0].Schema()))
for _, rec := range recs {
if err := w.Write(rec); err != nil {
s.logger.Error("Failed to write record to snapshot", "name", name, "error", err)
break
}
}
w.Close()
f.Close()
}

// Atomic swap: Remove old, Rename temp to new
// If we crash here, we have full WAL, so it's safe to lose old snapshot.
if err := os.RemoveAll(snapshotDir); err != nil {
s.logger.Error("Failed to remove old snapshot dir", "error", err)
}
if err := os.Rename(tempDir, snapshotDir); err != nil {
metrics.SnapshotTotal.WithLabelValues("error").Inc()
return fmt.Errorf("failed to rename snapshot dir: %w", err)
}

// Truncate WAL
s.walMu.Lock()
if s.walFile != nil {
s.walFile.Close()
if err := os.Truncate(filepath.Join(s.dataPath, walFileName), 0); err != nil {
s.logger.Error("Failed to truncate WAL", "error", err)
}
// Reopen
f, err := os.OpenFile(filepath.Join(s.dataPath, walFileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
if err == nil {
s.walFile = f
} else {
s.logger.Error("Failed to reopen WAL after snapshot", "error", err)
}
}
s.walMu.Unlock()

metrics.SnapshotTotal.WithLabelValues("ok").Inc()
metrics.SnapshotDurationSeconds.Observe(time.Since(start).Seconds())
s.logger.Info("Snapshot complete")
return nil
}
func (s *VectorStore) loadSnapshots() error {
snapshotDir := filepath.Join(s.dataPath, snapshotDirName)
entries, err := os.ReadDir(snapshotDir)
if os.IsNotExist(err) {
return nil
}
if err != nil {
return err
}

for _, entry := range entries {
if entry.IsDir() || filepath.Ext(entry.Name()) != ".arrow" {
continue
}
name := entry.Name()[:len(entry.Name())-6] // remove .arrow
path := filepath.Join(snapshotDir, entry.Name())

// Use mmap for zero-copy load
mm, err := mmap.Open(path)
if err != nil {
s.logger.Error("Failed to mmap snapshot file", "name", name, "error", err)
continue
}
// We don't close mm here immediately because the records might reference the memory.
// However, in this simple store, we might need to keep track of mapped files to close them on shutdown.
// For now, we let the OS handle cleanup on exit or we leak the fd/map until shutdown (which isn't implemented).
// A better approach would be to add a 'closers' list to VectorStore.

r, err := ipc.NewFileReader(&mmapReader{ReaderAt: mm})
if err != nil {
mm.Close()
s.logger.Error("Failed to create IPC file reader for snapshot", "name", name, "error", err)
continue
}

s.mu.Lock()
for i := 0; i < r.NumRecords(); i++ {
rec, err := r.Record(i)
if err != nil {
s.logger.Error("Failed to read record from snapshot", "name", name, "index", i, "error", err)
continue
}
rec.Retain()
if _, ok := s.vectors[name]; !ok {
	s.vectors[name] = &Dataset{Records: []arrow.Record{}, LastAccess: time.Now()}
}
s.vectors[name].Records = append(s.vectors[name].Records, rec)
s.currentMemory += calculateRecordSize(rec)
}
s.mu.Unlock()
r.Close()
// mm.Close() // Do NOT close mm, as records point to this memory
}
return nil
}

func (s *VectorStore) runSnapshotTicker(initialInterval time.Duration) {
var ticker *time.Ticker
if initialInterval > 0 {
ticker = time.NewTicker(initialInterval)
}

// Helper to get channel safely
getTickChan := func() <-chan time.Time {
if ticker == nil {
return nil
}
return ticker.C
}

for {
select {
case <-getTickChan():
if err := s.Snapshot(); err != nil {
s.logger.Error("Scheduled snapshot failed", "error", err)
}
case newInterval := <-s.snapshotReset:
s.logger.Info("Snapshot ticker updating", "old_interval", initialInterval, "new_interval", newInterval)
if ticker != nil {
ticker.Stop()
ticker = nil
}
if newInterval > 0 {
ticker = time.NewTicker(newInterval)
}
initialInterval = newInterval
}
}
}


// mmapReader wraps mmap.ReaderAt to implement io.ReadSeeker
type mmapReader struct {
*mmap.ReaderAt
offset int64
}

func (r *mmapReader) Read(p []byte) (n int, err error) {
n, err = r.ReaderAt.ReadAt(p, r.offset)
r.offset += int64(n)
return
}

func (r *mmapReader) Seek(offset int64, whence int) (int64, error) {
switch whence {
case io.SeekStart:
r.offset = offset
case io.SeekCurrent:
r.offset += offset
case io.SeekEnd:
r.offset = int64(r.Len()) + offset
}
return r.offset, nil
}


// Close ensures the WAL is flushed and closed properly
func (s *VectorStore) Close() error {
s.logger.Info("Closing VectorStore...")
s.walMu.Lock()
defer s.walMu.Unlock()

if s.walFile != nil {
s.logger.Info("Syncing and closing WAL file")
if err := s.walFile.Sync(); err != nil {
s.logger.Error("Failed to sync WAL", "error", err)
}
if err := s.walFile.Close(); err != nil {
return fmt.Errorf("failed to close WAL: %w", err)
}
s.walFile = nil
}
return nil
}
