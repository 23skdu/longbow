package index

import (
	"sort"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// trigramIndex stores a trigram inverted index for a single string column.
// Maps each trigram (3-char substring) to the row positions containing that trigram.
// Supports O(1) contains/prefix/suffix lookups via trigram intersection.
type trigramIndex struct {
	mu     sync.RWMutex
	trigrams map[string][]RowPosition
}

func newTrigramIndex() *trigramIndex {
	return &trigramIndex{
		trigrams: make(map[string][]RowPosition),
	}
}

// extractTrigrams returns all unique trigrams from a string.
func extractTrigrams(s string) []string {
	if len(s) < 3 {
		if len(s) == 0 {
			return nil
		}
		// For short strings, use the full string as a single token
		return []string{s}
	}

	// Use map to deduplicate
	seen := make(map[string]struct{}, len(s)-2)
	result := make([]string, 0, len(s)-2)
	for i := 0; i <= len(s)-3; i++ {
		t := s[i : i+3]
		if _, ok := seen[t]; !ok {
			seen[t] = struct{}{}
			result = append(result, t)
		}
	}
	return result
}

// extractPrefixGrams extracts all prefixes of a string as search keys.
// For "hello", returns ["h", "he", "hel", "hell", "hello"].
func extractPrefixGrams(s string) []string {
	if len(s) == 0 {
		return nil
	}
	result := make([]string, 0, len(s))
	for i := 1; i <= len(s); i++ {
		result = append(result, s[:i])
	}
	return result
}

// indexString indexes a single string value at the given row position.
func (idx *trigramIndex) indexString(value string, pos RowPosition) {
	trigrams := extractTrigrams(value)
	for _, tri := range trigrams {
		idx.trigrams[tri] = append(idx.trigrams[tri], pos)
	}
}

// containsLookup returns candidate row positions for a contains query.
// Uses trigram intersection: rows matching ALL trigrams in the query value.
// The result is sorted and deduplicated.
func (idx *trigramIndex) containsLookup(value string) []RowPosition {
	queryTrigrams := extractTrigrams(value)
	if len(queryTrigrams) == 0 {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// For each trigram, get the list of positions
	// Start with the smallest list for efficient intersection
	sort.Slice(queryTrigrams, func(i, j int) bool {
		return len(idx.trigrams[queryTrigrams[i]]) < len(idx.trigrams[queryTrigrams[j]])
	})

	firstSet, ok := idx.trigrams[queryTrigrams[0]]
	if !ok {
		return nil
	}

	// Build frequency map from the first (smallest) trigram
	freq := make(map[RowPosition]int, len(firstSet))
	for _, pos := range firstSet {
		freq[pos] = 1
	}

	// Intersect with remaining trigrams
	for _, tri := range queryTrigrams[1:] {
		positions, ok := idx.trigrams[tri]
		if !ok {
			return nil
		}
		for _, pos := range positions {
			if c, exists := freq[pos]; exists {
				freq[pos] = c + 1
			}
		}
	}

	// Collect positions that matched ALL trigrams
	numTrigrams := len(queryTrigrams)
	result := make([]RowPosition, 0, len(freq))
	for pos, count := range freq {
		if count == numTrigrams {
			result = append(result, pos)
		}
	}

	// Sort by record then by row for deterministic output
	sort.Slice(result, func(i, j int) bool {
		if result[i].RecordIdx != result[j].RecordIdx {
			return result[i].RecordIdx < result[j].RecordIdx
		}
		return result[i].RowIdx < result[j].RowIdx
	})

	return result
}

// StringContainsIndex provides trigram-based contains/prefix search for string columns.
// Works alongside ColumnInvertedIndex for non-equality string operators.
type StringContainsIndex struct {
	mu      sync.RWMutex
	columns map[string]*trigramIndex
}

// NewStringContainsIndex creates a new string contains index.
func NewStringContainsIndex() *StringContainsIndex {
	return &StringContainsIndex{
		columns: make(map[string]*trigramIndex),
	}
}

// IndexRecord indexes string columns of a record batch for contains/prefix search.
func (idx *StringContainsIndex) IndexRecord(recordIdx int, rec arrow.RecordBatch, columnsToIndex []string) {
	if len(columnsToIndex) == 0 {
		return
	}

	schema := rec.Schema()
	numRows := int(rec.NumRows())

	for _, colName := range columnsToIndex {
		fieldIndices := schema.FieldIndices(colName)
		if len(fieldIndices) == 0 {
			continue
		}

		colIdx := fieldIndices[0]
		col := rec.Column(colIdx)

		strArr, ok := col.(*array.String)
		if !ok {
			continue
		}

		idx.mu.Lock()
		triIdx, exists := idx.columns[colName]
		if !exists {
			triIdx = newTrigramIndex()
			idx.columns[colName] = triIdx
		}
		idx.mu.Unlock()

		// Build trigram index
		triIdx.mu.Lock()
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if strArr.IsNull(rowIdx) {
				continue
			}
			value := strArr.Value(rowIdx)
			pos := RowPosition{RecordIdx: recordIdx, RowIdx: rowIdx}
			triIdx.indexString(value, pos)
		}
		triIdx.mu.Unlock()
	}
}

// ContainsLookup returns row positions where the column value contains the query substring.
// Uses trigram intersection for fast candidate generation.
func (idx *StringContainsIndex) ContainsLookup(columnName, value string) []RowPosition {
	idx.mu.RLock()
	triIdx, exists := idx.columns[columnName]
	idx.mu.RUnlock()

	if !exists {
		return nil
	}

	return triIdx.containsLookup(value)
}

// GetMatchingRowIndices returns row indices within a specific record
// where the column value contains the given substring.
func (idx *StringContainsIndex) GetMatchingRowIndices(recordIdx int, columnName, value string) []int {
	positions := idx.ContainsLookup(columnName, value)
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

// BuildFilterMask creates a boolean mask for contains filtering using trigram index.
func (idx *StringContainsIndex) BuildFilterMask(recordIdx int, columnName, value string, numRows int) []int {
	return idx.GetMatchingRowIndices(recordIdx, columnName, value)
}

// HasIndex checks if a contains index exists for the given column.
func (idx *StringContainsIndex) HasIndex(columnName string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	_, exists := idx.columns[columnName]
	return exists
}

// Close releases resources.
func (idx *StringContainsIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.columns = make(map[string]*trigramIndex)
	return nil
}

// RemoveRecord removes all index entries for a specific record.
func (idx *StringContainsIndex) RemoveRecord(recordIdx int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, triIdx := range idx.columns {
		triIdx.mu.Lock()
		for tri, positions := range triIdx.trigrams {
			filtered := positions[:0]
			for _, pos := range positions {
				if pos.RecordIdx != recordIdx {
					filtered = append(filtered, pos)
				}
			}
			if len(filtered) == 0 {
				delete(triIdx.trigrams, tri)
			} else {
				triIdx.trigrams[tri] = filtered
			}
		}
		triIdx.mu.Unlock()
	}
}
