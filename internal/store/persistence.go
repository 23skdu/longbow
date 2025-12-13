package store

import (
"bytes"
"encoding/binary"
"fmt"
"io"
"os"
"path/filepath"
"time"

"github.com/apache/arrow/go/v18/arrow"
"github.com/apache/arrow/go/v18/arrow/ipc"
)

const (
walFileName     = "wal.log"
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

if s.walFile == nil {
return nil // Persistence disabled or not initialized
}

// Format: [NameLen: uint32][Name: bytes][RecordLen: uint64][RecordBytes: bytes]

// 1. Serialize Record to buffer
var buf bytes.Buffer
w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
if err := w.Write(rec); err != nil {
return fmt.Errorf("failed to serialize record for WAL: %w", err)
}
if err := w.Close(); err != nil {
return fmt.Errorf("failed to close IPC writer: %w", err)
}
recBytes := buf.Bytes()

// 2. Write Header & Data
nameBytes := []byte(name)
nameLen := uint32(len(nameBytes))
recLen := uint64(len(recBytes))

if err := binary.Write(s.walFile, binary.LittleEndian, nameLen); err != nil {
return err
}
if _, err := s.walFile.Write(nameBytes); err != nil {
return err
}
if err := binary.Write(s.walFile, binary.LittleEndian, recLen); err != nil {
return err
}
if _, err := s.walFile.Write(recBytes); err != nil {
return err
}

// Ensure it's on disk
// return s.walFile.Sync() // Optional: Sync every write for durability vs performance
return nil
}

func (s *VectorStore) replayWAL() error {
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
s.vectors[name] = append(s.vectors[name], rec)
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
s.logger.Info("Starting Snapshot...")
s.mu.RLock()
defer s.mu.RUnlock()

snapshotDir := filepath.Join(s.dataPath, snapshotDirName)
if err := os.MkdirAll(snapshotDir, 0755); err != nil {
return err
}

// Save each dataset
for name, recs := range s.vectors {
if len(recs) == 0 {
continue
}
path := filepath.Join(snapshotDir, name+".arrow")
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

// Truncate WAL
s.walMu.Lock()
if s.walFile != nil {
s.walFile.Close()
os.Truncate(filepath.Join(s.dataPath, walFileName), 0)
// Reopen
f, err := os.OpenFile(filepath.Join(s.dataPath, walFileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
if err == nil {
s.walFile = f
} else {
s.logger.Error("Failed to reopen WAL after snapshot", "error", err)
}
}
s.walMu.Unlock()

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

f, err := os.Open(filepath.Join(snapshotDir, entry.Name()))
if err != nil {
s.logger.Error("Failed to open snapshot file", "name", name, "error", err)
continue
}

r, err := ipc.NewReader(f)
if err != nil {
f.Close()
s.logger.Error("Failed to create IPC reader for snapshot", "name", name, "error", err)
continue
}

s.mu.Lock()
for r.Next() {
rec := r.Record()
rec.Retain()
s.vectors[name] = append(s.vectors[name], rec)
s.currentMemory += calculateRecordSize(rec)
}
s.mu.Unlock()
r.Release()
f.Close()
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
