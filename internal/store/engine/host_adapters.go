package engine

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/store/cluster"
	"github.com/23skdu/longbow/internal/store/streaming"
	"github.com/23skdu/longbow/internal/store/types"
)

// Logger exposes the store logger to domain packages through a host interface,
// avoiding direct access to engine internals.
func (s *VectorStore) Logger() *zerolog.Logger { return &s.logger }

// buildStreamArrowBatch keeps the engine-internal call sites working after the
// streaming encoder moved to its own domain package.
func buildStreamArrowBatch(alloc memory.Allocator, events []streaming.StreamVectorEvent) (arrow.RecordBatch, error) {
	return streaming.BuildStreamArrowBatch(alloc, events)
}

// Allocator exposes the store's Arrow allocator to streaming connectors.
func (s *VectorStore) Allocator() memory.Allocator { return s.mem }

// NewCDCExporter creates the outbound CDC exporter used by streaming
// connectors. The domain only sees the narrow CDCExporter lifecycle.
func (s *VectorStore) NewCDCExporter(logger zerolog.Logger, cdc *cluster.ChangeDataCapture, cfg streaming.CDCExportConfig) (streaming.CDCExporter, error) {
	return NewMessageQueueExporter(logger, cdc, MQConfig{
		Type:           MQType(cfg.Type),
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		BatchSize:      cfg.BatchSize,
		BatchTimeoutMs: cfg.BatchTimeoutMs,
	})
}

// DeleteStreamVector removes a vector ingested through a streaming connector.
// The dataset internals stay inside the engine so the streaming domain does not
// depend on *Dataset.
func (s *VectorStore) DeleteStreamVector(datasetName, rawID string) error {
	ds, ok := s.getDataset(datasetName)
	if !ok {
		return fmt.Errorf("dataset %q not found", datasetName)
	}
	ds.dataMu.Lock()
	defer ds.dataMu.Unlock()
	if loc, exists := ds.PrimaryIndex[rawID]; exists {
		setTombstone(ds, loc.BatchIdx, loc.RowIdx)
	}
	return nil
}

// TextCorpus returns a consistent snapshot of a dataset's text-bearing records.
// It is the TextHost capability used by the textsearch domain.
func (s *VectorStore) TextCorpus(dataset string) ([]arrow.RecordBatch, map[int]*types.Bitset, error) {
	ds, ok := s.getDataset(dataset)
	if !ok {
		return nil, nil, fmt.Errorf("dataset %q not found", dataset)
	}
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()
	return ds.Records.Read(), ds.GetTombstones(), nil
}
