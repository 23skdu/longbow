package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow"
)

// ApplyDelta applies an Arrow record batch as a delta to the specified dataset.
func (s *VectorStore) ApplyDelta(name string, rec arrow.RecordBatch, seq uint64, ts int64) error {
	if s.engine == nil {
		return fmt.Errorf("persistence not initialized")
	}

	// 1. Write to WAL
	if err := s.engine.WriteWAL(name, rec, seq, ts); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// 2. Apply to memory (Reusing re-play logic for consistency)
	return s.applyReplayBatch(name, rec, seq, ts)
}

// Snapshot triggers a snapshot of all state.
func (s *VectorStore) Snapshot(ctx context.Context) error {
	if s.engine == nil {
		return fmt.Errorf("persistence not initialized")
	}

	return s.engine.Snapshot(&storeSnapshotSource{s: s})
}

type storeSnapshotSource struct {
	s *VectorStore
}

func (src *storeSnapshotSource) Iterate(yield func(storage.SnapshotItem) error) error {
	src.s.configMu.RLock()
	datasets := src.s.datasets.Load()
	src.s.configMu.RUnlock()

	if datasets == nil {
		return nil
	}

	for name, ds := range *datasets {
		ds.dataMu.RLock()
		records := make([]arrow.RecordBatch, len(ds.Records))
		copy(records, ds.Records)
		// Access PQEncoder under lock
		var pqBytes []byte
		if ds.PQEncoder != nil {
			pqBytes = ds.PQEncoder.Serialize()
		}
		ds.dataMu.RUnlock()

		item := storage.SnapshotItem{
			Name:       name,
			Records:    records,
			PQCodebook: pqBytes,
		}

		// Export Index Graph
		if ds.Index != nil {
			if hnsw, ok := ds.Index.(*ArrowHNSW); ok {
				// 1. Capture lightweight clone immediately
				snap, meta, err := hnsw.SnapshotGraph()
				if err != nil {
					return fmt.Errorf("failed to snapshot index for %s: %w", name, err)
				}

				// 2. Set Writer Callback
				// This writer will be invoked LATER in a background thread to serialize the clone.
				item.IndexConfigWriter = func(w io.Writer) error {
					// Serialize Metadata (SyncState)
					var metaBuf bytes.Buffer
					if err := gob.NewEncoder(&metaBuf).Encode(meta); err != nil {
						return fmt.Errorf("failed to encode metadata: %w", err)
					}
					metaBytes := metaBuf.Bytes()

					// Write Metadata Length + Bytes
					if err := binary.Write(w, binary.LittleEndian, uint32(len(metaBytes))); err != nil { // #nosec G115
						return err
					}
					if _, err := w.Write(metaBytes); err != nil {
						return err
					}

					// Serialize Graph Data
					return snap.Serialize(w)
				}
			} else {
				// Fallback
				var graphBuf bytes.Buffer
				if err := ds.Index.ExportGraph(&graphBuf); err == nil {
					item.IndexConfig = graphBuf.Bytes()
				} else {
					return fmt.Errorf("failed to export index graph for %s: %w", name, err)
				}
			}
		}

		if err := yield(item); err != nil {
			return err
		}
	}
	return nil
}

// FlushWAL flushes any pending WAL writes.
func (s *VectorStore) FlushWAL() error {
	if s.engine == nil {
		return nil
	}
	return s.engine.FlushWAL()
}

// TruncateWAL truncates the WAL up to the given sequence.
func (s *VectorStore) TruncateWAL(seq uint64) error {
	if s.engine == nil {
		return nil
	}
	return s.engine.TruncateWAL(seq)
}

// writeToWAL is an internal helper for tests that expect it.
func (s *VectorStore) writeToWAL(name string, rec arrow.RecordBatch, seq uint64, ts int64) error {
	return s.ApplyDelta(name, rec, seq, ts)
}
