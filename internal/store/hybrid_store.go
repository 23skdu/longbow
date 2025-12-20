package store

import (
	"runtime"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"

	"github.com/23skdu/longbow/internal/metrics"
)

// =============================================================================
// Subtask 4: VectorStore Hybrid Search Integration
// =============================================================================

// NewVectorStoreWithHybridConfig creates a VectorStore with hybrid search enabled.
// When hybrid search is enabled, text columns are indexed using BM25 during DoPut.
func NewVectorStoreWithHybridConfig(mem memory.Allocator, logger *zap.Logger, hybridCfg HybridSearchConfig) (*VectorStore, error) {
	if hybridCfg.Enabled {
		if err := hybridCfg.Validate(); err != nil {
			return nil, err
		}
	}

	s := &VectorStore{
		mem:                mem,
		logger:             logger,
		vectors:            NewShardedMap(),
		ttlDuration:        5 * time.Minute,
		snapshotReset:      make(chan time.Duration, 1),
		indexQueue:         NewIndexJobQueue(DefaultIndexJobQueueConfig()),
		stopChan:           make(chan struct{}),
		columnIndex:        NewColumnInvertedIndex(),
		metadata:           NewCOWMetadataMap(),
		hybridSearchConfig: hybridCfg,
		nsManager:          newNamespaceManager(),
	}

	// Initialize BM25 index if hybrid search is enabled
	if hybridCfg.Enabled {
		s.bm25Index = NewBM25InvertedIndex(hybridCfg.BM25)
	}

	s.maxMemory.Store(1 << 30)  // 1GB default
	s.maxWALSize.Store(1 << 28) // 256MB default
	s.startIndexingWorkers(runtime.NumCPU())
	s.StartMetricsTicker(10 * time.Second)
	s.initCompaction(DefaultCompactionConfig())
	return s, nil
}

// GetHybridSearchConfig returns the hybrid search configuration.
func (s *VectorStore) GetHybridSearchConfig() HybridSearchConfig {
	return s.hybridSearchConfig
}

// GetBM25Index returns the BM25 inverted index for text search.
// Returns nil if hybrid search is not enabled.
func (s *VectorStore) GetBM25Index() *BM25InvertedIndex {
	return s.bm25Index
}

// indexTextColumnsForHybridSearch indexes text columns in a RecordBatch for BM25 search.
// This is called during DoPut when hybrid search is enabled.
func (s *VectorStore) indexTextColumnsForHybridSearch(batch arrow.RecordBatch, baseRowID uint32) {
	if !s.hybridSearchConfig.Enabled || s.bm25Index == nil {
		return
	}

	schema := batch.Schema()
	numRows := batch.NumRows()

	// For each configured text column
	for _, colName := range s.hybridSearchConfig.TextColumns {
		// Find column index
		colIdx := -1
		for i, field := range schema.Fields() {
			if field.Name == colName {
				colIdx = i
				break
			}
		}

		if colIdx < 0 {
			// Column not in schema, skip
			continue
		}

		col := batch.Column(colIdx)

		// Must be string type
		strArr, ok := col.(*array.String)
		if !ok {
			s.logger.Warn("hybrid search: text column is not string type",
				zap.String("column", colName),
				zap.String("actual_type", col.DataType().Name()))
			continue
		}

		// Index each row
		// Index each row
		for row := 0; row < int(numRows); row++ {
			if strArr.IsNull(row) {
				continue
			}

			text := strArr.Value(row)
			if text == "" {
				continue
			}

			// Use baseRowID + row as the VectorID
			// BM25InvertedIndex.Add will tokenize the text internally
			vecID := VectorID(baseRowID + uint32(row)) //nolint:gosec // G115 - row offset safe
			s.bm25Index.Add(vecID, text)
			metrics.BM25DocumentsIndexedTotal.Inc()
		}
	}
}
