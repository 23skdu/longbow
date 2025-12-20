package store

import (
	"context"
	"fmt"
	"sync"

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

// datasetIndex holds all column indices for a dataset
type datasetIndex struct {
	mu      sync.RWMutex
	columns map[string]*columnIndex // column_name -> columnIndex
}

func newDatasetIndex() *datasetIndex {
	return &datasetIndex{
		columns: make(map[string]*columnIndex),
	}
}

// ColumnInvertedIndex provides O(1) equality lookups on indexed columns
// Structure: dataset -> column -> value -> []RowPosition
type ColumnInvertedIndex struct {
	mu       sync.RWMutex
	datasets map[string]*datasetIndex
}

// NewColumnInvertedIndex creates a new column-based inverted index
func NewColumnInvertedIndex() *ColumnInvertedIndex {
	return &ColumnInvertedIndex{
		datasets: make(map[string]*datasetIndex),
	}
}

// IndexRecord indexes specified columns of a record batch
func (idx *ColumnInvertedIndex) IndexRecord(datasetName string, recordIdx int, rec arrow.RecordBatch, columnsToIndex []string) {
	if len(columnsToIndex) == 0 {
		return
	}

	idx.mu.Lock()
	dsIdx, exists := idx.datasets[datasetName]
	if !exists {
		dsIdx = newDatasetIndex()
		idx.datasets[datasetName] = dsIdx
	}
	idx.mu.Unlock()

	dsIdx.mu.Lock()
	defer dsIdx.mu.Unlock()

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
		colIndex, exists := dsIdx.columns[colName]
		if !exists {
			colIndex = newColumnIndex()
			dsIdx.columns[colName] = colIndex
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
func (idx *ColumnInvertedIndex) Lookup(datasetName, columnName, value string) []RowPosition {
	idx.mu.RLock()
	dsIdx, exists := idx.datasets[datasetName]
	idx.mu.RUnlock()

	if !exists {
		return nil
	}

	dsIdx.mu.RLock()
	defer dsIdx.mu.RUnlock()

	colIndex, exists := dsIdx.columns[columnName]
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
func (idx *ColumnInvertedIndex) GetMatchingRowIndices(datasetName string, recordIdx int, columnName, value string) []int {
	positions := idx.Lookup(datasetName, columnName, value)
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

// HasIndex checks if an index exists for the given dataset and column
func (idx *ColumnInvertedIndex) HasIndex(datasetName, columnName string) bool {
	idx.mu.RLock()
	dsIdx, exists := idx.datasets[datasetName]
	idx.mu.RUnlock()

	if !exists {
		return false
	}

	dsIdx.mu.RLock()
	defer dsIdx.mu.RUnlock()

	_, exists = dsIdx.columns[columnName]
	return exists
}

// RemoveRecord removes all index entries for a specific record
func (idx *ColumnInvertedIndex) RemoveRecord(datasetName string, recordIdx int) {
	idx.mu.RLock()
	dsIdx, exists := idx.datasets[datasetName]
	idx.mu.RUnlock()

	if !exists {
		return
	}

	dsIdx.mu.Lock()
	defer dsIdx.mu.Unlock()

	for _, colIndex := range dsIdx.columns {
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

// RemoveDataset removes all indices for a dataset
func (idx *ColumnInvertedIndex) RemoveDataset(datasetName string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.datasets, datasetName)
}

// Stats returns statistics about the index
type ColumnInvertedIndexStats struct {
	Datasets       int
	TotalColumns   int
	TotalValues    int
	TotalPositions int
}

func (idx *ColumnInvertedIndex) Stats() ColumnInvertedIndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	stats := ColumnInvertedIndexStats{
		Datasets: len(idx.datasets),
	}

	for _, dsIdx := range idx.datasets {
		dsIdx.mu.RLock()
		stats.TotalColumns += len(dsIdx.columns)
		for _, colIndex := range dsIdx.columns {
			stats.TotalValues += len(colIndex.values)
			for _, positions := range colIndex.values {
				stats.TotalPositions += len(positions)
			}
		}
		dsIdx.mu.RUnlock()
	}

	return stats
}

// BuildFilterMask creates a boolean mask for filtering using indexed lookup
// Returns nil if no index exists for the column
func (idx *ColumnInvertedIndex) BuildFilterMask(datasetName string, recordIdx int, columnName, value string, numRows int, mem memory.Allocator) *array.Boolean {
	positions := idx.GetMatchingRowIndices(datasetName, recordIdx, columnName, value)
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
func (idx *ColumnInvertedIndex) FilterRecordWithIndex(ctx context.Context, datasetName string, recordIdx int, rec arrow.RecordBatch, filter Filter, mem memory.Allocator) (arrow.RecordBatch, error) {
	// Only optimize equality filters
	if filter.Operator != "=" {
		return nil, fmt.Errorf("FilterRecordWithIndex only supports equality filters")
	}

	// Check if we have an index
	if !idx.HasIndex(datasetName, filter.Field) {
		return nil, fmt.Errorf("no index for column %s", filter.Field)
	}

	// Build filter mask using O(1) index lookup
	mask := idx.BuildFilterMask(datasetName, recordIdx, filter.Field, filter.Value, int(rec.NumRows()), mem)
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

// IndexRecordColumns indexes specified columns of a record batch for fast equality lookups
func (s *VectorStore) IndexRecordColumns(dataset string, rec arrow.RecordBatch, batchIdx int) {
	if s.columnIndex == nil || len(s.indexedColumns) == 0 {
		return
	}
	s.columnIndex.IndexRecord(dataset, batchIdx, rec, s.indexedColumns)
}

// SetIndexedColumns configures which columns to index for fast equality lookups
func (s *VectorStore) SetIndexedColumns(columns []string) {
	s.indexedColumns = columns
}

// GetIndexedColumns returns the list of indexed columns
func (s *VectorStore) GetIndexedColumns() []string {
	return s.indexedColumns
}

// filterRecordOptimized uses column index for equality filters when available
// Falls back to Arrow compute for non-indexed columns or non-equality operators
func (s *VectorStore) filterRecordOptimized(ctx context.Context, datasetName string, rec arrow.RecordBatch, batchIdx int, filters []Filter) (arrow.RecordBatch, error) {
	if len(filters) == 0 {
		rec.Retain()
		return rec, nil
	}

	// Check if we can use index for any equality filters
	var indexableFilters []Filter
	var remainingFilters []Filter

	for _, f := range filters {
		if f.Operator == "=" && s.columnIndex != nil && s.columnIndex.HasIndex(datasetName, f.Field) {
			indexableFilters = append(indexableFilters, f)
		} else {
			remainingFilters = append(remainingFilters, f)
		}
	}

	// If no indexable filters, use standard filterRecord
	if len(indexableFilters) == 0 {
		return s.filterRecord(ctx, rec, filters)
	}

	// Use index to get matching row indices for each filter
	var finalMask *array.Boolean
	for _, f := range indexableFilters {
		// Build mask for this filter using the index
		mask := s.columnIndex.BuildFilterMask(datasetName, batchIdx, f.Field, f.Value, int(rec.NumRows()), s.mem)
		if mask == nil {
			// No index data, fall back to full filter
			if finalMask != nil {
				finalMask.Release()
			}
			return s.filterRecord(ctx, rec, filters)
		}

		if finalMask == nil {
			finalMask = mask
		} else {
			// AND the masks together
			andRes, err := compute.CallFunction(ctx, "and", nil, compute.NewDatum(finalMask.Data()), compute.NewDatum(mask.Data()))
			finalMask.Release()
			mask.Release()
			if err != nil {
				return nil, fmt.Errorf("and masks: %w", err)
			}
			finalMask = andRes.(*compute.ArrayDatum).MakeArray().(*array.Boolean)
		}
	}

	// Apply indexed filter
	filterRes, err := compute.CallFunction(ctx, "filter", nil, compute.NewDatum(rec), compute.NewDatum(finalMask.Data()))
	finalMask.Release()
	if err != nil {
		return nil, fmt.Errorf("apply index filter: %w", err)
	}
	filteredRec := filterRes.(*compute.RecordDatum).Value

	// Apply remaining filters using standard method
	if len(remainingFilters) > 0 {
		finalRec, err := s.filterRecord(ctx, filteredRec, remainingFilters)
		filteredRec.Release()
		if err != nil {
			return nil, err
		}
		return finalRec, nil
	}

	return filteredRec, nil
}
