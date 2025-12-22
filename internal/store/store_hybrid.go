package store

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.uber.org/zap"

	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// =============================================================================
// Subtask 4: VectorStore Hybrid Search Integration
// =============================================================================

// GetHybridSearchConfig returns the hybrid search configuration.
func (s *VectorStore) GetHybridSearchConfig() HybridSearchConfig {
	return s.hybridSearchConfig
}

// GetBM25Index returns the BM25 inverted index for text search.
// Returns nil if hybrid search is not enabled.
func (s *VectorStore) GetBM25Index() *BM25InvertedIndex {
	return s.bm25Index
}

// NewVectorStoreWithHybridConfig creates a VectorStore with hybrid search enabled.
func NewVectorStoreWithHybridConfig(mem memory.Allocator, logger *zap.Logger, cfg HybridSearchConfig) (*VectorStore, error) {
	if cfg.Enabled {
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
	}

	// Default params for store
	vs := NewVectorStore(mem, logger, 1024*1024*1024, 1024*1024*100, 24*time.Hour)

	vs.hybridSearchConfig = cfg
	if cfg.Enabled {
		vs.bm25Index = NewBM25InvertedIndex(cfg.BM25)
	}

	return vs, nil
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
