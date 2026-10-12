package store

import (
	"github.com/23skdu/longbow/internal/store/index"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/rs/zerolog"

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

// SetHybridSearchConfig updates the hybrid search configuration.
func (s *VectorStore) SetHybridSearchConfig(cfg HybridSearchConfig) {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	s.hybridSearchConfig = cfg
}

// GetBM25Index returns the BM25 inverted index for text search for a dataset.
func (s *VectorStore) GetBM25Index(datasetName string) *BM25InvertedIndex {
	ds, ok := s.getDataset(datasetName)
	if !ok {
		return nil
	}
	return ds.BM25Index
}

// NewVectorStoreWithHybridConfig creates a VectorStore with hybrid search enabled.
//
//nolint:gocritic // Logger passed by value for simplicity
func NewVectorStoreWithHybridConfig(mem memory.Allocator, logger zerolog.Logger, cfg HybridSearchConfig) (*VectorStore, error) {
	if cfg.Enabled {
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
	}

	// Default params for store
	vs := NewVectorStore(mem, logger, 1024*1024*1024, 1024*1024*100, 24*time.Hour)

	vs.hybridSearchConfig = cfg

	return vs, nil
}

// indexTextColumnsForHybridSearch indexes text columns in a RecordBatch for BM25 search.
//
// Every text cell of the batch is tokenised exactly once and the resulting
// token slice is handed to both the legacy inverted index and the arena index.
// The previous shape tokenised the same cell twice (once inside
// BM25InvertedIndex.Add and once again for the arena index) and then ran the
// two writes under the dataset write lock on the ingestion hot path; callers
// now hand this work to the background text indexer instead.
func (s *VectorStore) indexTextColumnsForHybridSearch(ds *Dataset, batch arrow.RecordBatch, baseRowID uint32) {
	if !s.hybridSearchConfig.Enabled || (ds.BM25Index == nil && ds.BM25ArenaIndex == nil) {
		return
	}

	schema := batch.Schema()
	numRows := int(batch.NumRows())

	var legacyDocs []BM25Document
	var arenaDocs []BM25ArenaDocument
	defer func() {
		if len(legacyDocs) > 0 {
			ds.BM25Index.AddBatch(legacyDocs)
		}
		if len(arenaDocs) > 0 {
			if err := ds.BM25ArenaIndex.IndexDocuments(arenaDocs); err != nil {
				s.logger.Error().Err(err).Msg("hybrid search: arena index failed")
			}
		}
		metrics.BM25DocumentsIndexedTotal.Add(float64(len(legacyDocs)))
	}()

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
			s.logger.Warn().
				Str("column", colName).
				Str("actual_type", col.DataType().Name()).
				Msg("hybrid search: text column is not string type")
			continue
		}

		// Index each row
		for row := 0; row < numRows; row++ {
			if strArr.IsNull(row) {
				continue
			}

			text := strArr.Value(row)
			if text == "" {
				continue
			}

			// Use baseRowID + row as the VectorID
			docID := uint32(baseRowID + uint32(row)) //nolint:gosec

			tokens := index.Tokenize(text)
			if len(tokens) == 0 {
				continue
			}

			if ds.BM25Index != nil {
				legacyDocs = append(legacyDocs, BM25Document{ID: VectorID(docID), Tokens: tokens})
			}
			if ds.BM25ArenaIndex != nil {
				arenaDocs = append(arenaDocs, BM25ArenaDocument{ID: docID, Tokens: tokens})
			}
		}
	}
}
