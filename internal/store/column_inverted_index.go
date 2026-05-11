package store

import (
	"context"
	"fmt"
	"sync"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RowPosition identifies a row within a dataset's records
type RowPosition struct {
	RecordIdx int // Index of the record batch
	RowIdx    int // Index of the row within the record
}

// columnIndex holds the inverted index for a single column
// value -> []RowPosition
type columnIndex struct {
	values map[string][]RowPosition
}

func newColumnIndex() *columnIndex {
	return &columnIndex{
		values: make(map[string][]RowPosition),
	}
}

// ColumnInvertedIndex provides O(1) equality lookups on indexed columns for a single dataset
type ColumnInvertedIndex struct {
	mu      sync.RWMutex
	columns map[string]*columnIndex // column_name -> columnIndex
}

// NewColumnInvertedIndex creates a new column-based inverted index
func NewColumnInvertedIndex() *ColumnInvertedIndex {
	return &ColumnInvertedIndex{
		columns: make(map[string]*columnIndex),
	}
}

// Close releases resources associated with the index.
func (idx *ColumnInvertedIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.columns = make(map[string]*columnIndex)
	return nil
}

// IndexRecord indexes specified columns of a record batch
func (idx *ColumnInvertedIndex) IndexRecord(recordIdx int, rec arrow.RecordBatch, columnsToIndex []string) {
	if len(columnsToIndex) == 0 {
		return
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	schema := rec.Schema()
	numRows := int(rec.NumRows())

	for _, colName := range columnsToIndex {
		fieldIndices := schema.FieldIndices(colName)
		if len(fieldIndices) == 0 {
			continue
		}

		colIdx := fieldIndices[0]
		col := rec.Column(colIdx)

		// Get or create column index
		colIndex, exists := idx.columns[colName]
		if !exists {
			colIndex = newColumnIndex()
			idx.columns[colName] = colIndex
		}

		// Index each row based on column type
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if col.IsNull(rowIdx) {
				continue
			}

			var valueStr string
			switch arr := col.(type) {
			case *array.String:
				valueStr = arr.Value(rowIdx)
			case *array.Int64:
				valueStr = fmt.Sprintf("%d", arr.Value(rowIdx))
			case *array.Int32:
				valueStr = fmt.Sprintf("%d", arr.Value(rowIdx))
			case *array.Float64:
				valueStr = fmt.Sprintf("%g", arr.Value(rowIdx))
			case *array.Float32:
				valueStr = fmt.Sprintf("%g", arr.Value(rowIdx))
			case *array.Boolean:
				valueStr = fmt.Sprintf("%t", arr.Value(rowIdx))
			case *array.Date64:
				valueStr = fmt.Sprintf("%d", arr.Value(rowIdx))
			case *array.Timestamp:
				valueStr = fmt.Sprintf("%d", arr.Value(rowIdx))
			default:
				// Skip unsupported types
				continue
			}

			pos := RowPosition{RecordIdx: recordIdx, RowIdx: rowIdx}
			colIndex.values[valueStr] = append(colIndex.values[valueStr], pos)
		}
	}
}

// Lookup returns all row positions matching the given value
// Returns empty slice if not found (O(1) lookup)
func (idx *ColumnInvertedIndex) Lookup(columnName, value string) []RowPosition {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	colIndex, exists := idx.columns[columnName]
	if !exists {
		return nil
	}

	positions, exists := colIndex.values[value]
	if !exists {
		return nil
	}

	// Return a copy to avoid concurrent modification
	result := make([]RowPosition, len(positions))
	copy(result, positions)
	return result
}

// GetMatchingRowIndices returns row indices within a specific record
func (idx *ColumnInvertedIndex) GetMatchingRowIndices(recordIdx int, columnName, value string) []int {
	positions := idx.Lookup(columnName, value)
	if len(positions) == 0 {
		return nil
	}

	var result []int
	for _, pos := range positions {
		if pos.RecordIdx == recordIdx {
			result = append(result, pos.RowIdx)
		}
	}
	return result
}

// HasIndex checks if an index exists for the given column
func (idx *ColumnInvertedIndex) HasIndex(columnName string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	_, exists := idx.columns[columnName]
	return exists
}

// RemoveRecord removes all index entries for a specific record
func (idx *ColumnInvertedIndex) RemoveRecord(recordIdx int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, colIndex := range idx.columns {
		for value, positions := range colIndex.values {
			// Filter out positions from this record
			filtered := positions[:0]
			for _, pos := range positions {
				if pos.RecordIdx != recordIdx {
					filtered = append(filtered, pos)
				}
			}
			if len(filtered) == 0 {
				delete(colIndex.values, value)
			} else {
				colIndex.values[value] = filtered
			}
		}
	}
}

// RemoveDataset is deprecated as index is now dataset-local
func (idx *ColumnInvertedIndex) RemoveDataset(_ string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.columns = make(map[string]*columnIndex)
}

// ColumnInvertedIndexStats contains statistics about the column inverted index.
type ColumnInvertedIndexStats struct {
	Datasets       int
	TotalColumns   int
	TotalValues    int
	TotalPositions int
}

// Stats returns statistics about the index.
func (idx *ColumnInvertedIndex) Stats() ColumnInvertedIndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	stats := ColumnInvertedIndexStats{
		Datasets:     1,
		TotalColumns: len(idx.columns),
	}

	for _, colIndex := range idx.columns {
		stats.TotalValues += len(colIndex.values)
		for _, positions := range colIndex.values {
			stats.TotalPositions += len(positions)
		}
	}

	return stats
}

// BuildFilterMask creates a boolean mask for filtering using indexed lookup
// Returns nil if no index exists for the column
func (idx *ColumnInvertedIndex) BuildFilterMask(recordIdx int, columnName, value string, numRows int, mem memory.Allocator) *array.Boolean {
	positions := idx.GetMatchingRowIndices(recordIdx, columnName, value)
	if positions == nil {
		return nil
	}

	// Build boolean mask
	bldr := array.NewBooleanBuilder(mem)
	defer bldr.Release()

	// Create set for O(1) lookup
	matchSet := make(map[int]bool, len(positions))
	for _, pos := range positions {
		matchSet[pos] = true
	}

	for i := 0; i < numRows; i++ {
		bldr.Append(matchSet[i])
	}

	return bldr.NewBooleanArray()
}

// FilterRecordWithIndex applies an equality filter using the index for O(1) lookup
// Falls back to compute.Filter if no index exists
func (idx *ColumnInvertedIndex) FilterRecordWithIndex(ctx context.Context, recordIdx int, rec arrow.RecordBatch, filter *query.Filter, mem memory.Allocator) (arrow.RecordBatch, error) {
	// Only optimize equality filters
	if filter.Operator != "=" {
		return nil, fmt.Errorf("FilterRecordWithIndex only supports equality filters")
	}

	// Check if we have an index
	if !idx.HasIndex(filter.Field) {
		return nil, fmt.Errorf("no index for column %s", filter.Field)
	}

	// Build filter mask using O(1) index lookup
	mask := idx.BuildFilterMask(recordIdx, filter.Field, filter.Value, int(rec.NumRows()), mem)
	if mask == nil {
		rec.Retain()
		return rec, nil
	}
	defer mask.Release()

	// Apply filter using Arrow compute
	filterRes, err := compute.CallFunction(ctx, "filter", nil, compute.NewDatum(rec), compute.NewDatum(mask.Data()))
	if err != nil {
		return nil, fmt.Errorf("compute filter error: %w", err)
	}

	return filterRes.(*compute.RecordDatum).Value, nil
}
