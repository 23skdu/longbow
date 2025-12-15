package store

import (
"fmt"
"os"
"path/filepath"
"sync"

"github.com/apache/arrow/go/v14/arrow"
"github.com/apache/arrow/go/v14/arrow/memory"
)

// Persistence handles writing and reading snapshots to disk.
// It now uses Parquet for snapshot storage.
type Persistence struct {
dir string
mu  sync.Mutex
}

func NewPersistence(dir string) (*Persistence, error) {
if err := os.MkdirAll(dir, 0755); err != nil {
return nil, fmt.Errorf("failed to create persistence directory: %w", err)
}
return &Persistence{dir: dir}, nil
}

// SaveSnapshot saves the current state to a Parquet file.
func (p *Persistence) SaveSnapshot(record arrow.Record, index int64) error {
p.mu.Lock()
defer p.mu.Unlock()

filename := filepath.Join(p.dir, fmt.Sprintf("snapshot_%d.parquet", index))
file, err := os.Create(filename)
if err != nil {
return fmt.Errorf("failed to create snapshot file: %w", err)
}
defer file.Close()

return writeParquet(file, record)
}

// LoadSnapshot loads the latest snapshot from disk.
func (p *Persistence) LoadSnapshot(pool memory.Allocator) (arrow.Record, int64, error) {
p.mu.Lock()
defer p.mu.Unlock()

files, err := os.ReadDir(p.dir)
if err != nil {
return nil, 0, fmt.Errorf("failed to read persistence directory: %w", err)
}

var latestFile string
var latestIndex int64 = -1

for _, file := range files {
if filepath.Ext(file.Name()) == ".parquet" {
var index int64
_, err := fmt.Sscanf(file.Name(), "snapshot_%d.parquet", &index)
if err == nil && index > latestIndex {
latestIndex = index
latestFile = file.Name()
}
}
}

if latestFile == "" {
return nil, 0, nil // No snapshot found
}

path := filepath.Join(p.dir, latestFile)
f, err := os.Open(path)
if err != nil {
return nil, 0, fmt.Errorf("failed to open snapshot file: %w", err)
}
defer f.Close()

info, err := f.Stat()
if err != nil {
return nil, 0, fmt.Errorf("failed to stat snapshot file: %w", err)
}

record, err := readParquet(f, info.Size(), pool)
if err != nil {
return nil, 0, fmt.Errorf("failed to read parquet snapshot: %w", err)
}

return record, latestIndex, nil
}
