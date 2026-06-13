package index

import (
	"sort"
	"testing"
)

func TestExtractTrigrams(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"", nil},
		{"ab", []string{"ab"}},
		{"abc", []string{"abc"}},
		{"hello", []string{"hel", "ell", "llo"}},
		{"aaaa", []string{"aaa"}},
		{"abcde", []string{"abc", "bcd", "cde"}},
	}
	for _, tt := range tests {
		got := extractTrigrams(tt.input)
		if len(got) != len(tt.want) {
			t.Errorf("extractTrigrams(%q) = %v, want %v", tt.input, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("extractTrigrams(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}

func TestTrigramIndex_ContainsLookup(t *testing.T) {
	idx := newTrigramIndex()

	positions := []RowPosition{
		{0, 0}, {0, 1}, {0, 2}, {1, 0},
	}
	values := []string{
		"hello world",
		"world peace",
		"say hello",
		"goodbye",
	}

	for i, v := range values {
		idx.indexString(v, positions[i])
	}

	tests := []struct {
		query string
		want  []RowPosition
	}{
		{"hello", []RowPosition{{0, 0}, {0, 2}}},
		{"world", []RowPosition{{0, 0}, {0, 1}}},
		{"goodbye", []RowPosition{{1, 0}}},
		{"xyz", nil},
		{"lo wo", []RowPosition{{0, 0}}},
		{"peace", []RowPosition{{0, 1}}},
	}
	for _, tt := range tests {
		got := idx.containsLookup(tt.query)
		if len(got) != len(tt.want) {
			t.Errorf("containsLookup(%q) = %v, want %v", tt.query, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("containsLookup(%q)[%d] = %v, want %v", tt.query, i, got[i], tt.want[i])
			}
		}
	}
}

func TestStringContainsIndex_Integration(t *testing.T) {
	idx := NewStringContainsIndex()

	// Build a simple record with a string column
	// We simulate by manually adding via ContainsLookup test pattern
	_ = idx

	// Verify HasIndex works
	if idx.HasIndex("nonexistent") {
		t.Error("HasIndex should return false for nonexistent column")
	}

	// Close should not panic
	idx.Close()

	// After close, new index is clean
	if idx.HasIndex("anything") {
		t.Error("HasIndex should return false after Close")
	}
}

func TestTrigramIndex_Empty(t *testing.T) {
	idx := newTrigramIndex()
	result := idx.containsLookup("anything")
	if result != nil {
		t.Errorf("containsLookup on empty index should return nil, got %v", result)
	}
}

func TestTrigramIndex_Duplicates(t *testing.T) {
	idx := newTrigramIndex()

	// Same trigram appearing in multiple rows
	idx.indexString("hello", RowPosition{0, 0})
	idx.indexString("help", RowPosition{0, 1})
	idx.indexString("helicopter", RowPosition{1, 0})

	// "hel" is a common trigram
	result := idx.containsLookup("hel")
	if len(result) != 3 {
		t.Errorf("containsLookup('hel') should have 3 results, got %d: %v", len(result), result)
	}

	// "hello" should match only the first
	result = idx.containsLookup("hello")
	if len(result) != 1 {
		t.Errorf("containsLookup('hello') should have 1 result, got %d: %v", len(result), result)
	}
	if len(result) > 0 && result[0] != (RowPosition{0, 0}) {
		t.Errorf("containsLookup('hello')[0] = %v, want {0, 0}", result[0])
	}
}

func TestExtractTrigrams_Deduplicates(t *testing.T) {
	trigrams := extractTrigrams("aaaaa")
	// "aaa" appears 3 times but should be deduplicated to 1
	if len(trigrams) != 1 {
		t.Errorf("extractTrigrams('aaaaa') = %v, want ['aaa'] (len=%d)", trigrams, len(trigrams))
	}
}

func TestTrigramIndex_NoFalsePositives(t *testing.T) {
	idx := newTrigramIndex()

	// These should NOT match "cat"
	idx.indexString("dog", RowPosition{0, 0})
	idx.indexString("car", RowPosition{0, 1})
	idx.indexString("bat", RowPosition{0, 2})

	// "cat" requires trigrams "cat" — none of the values contain it
	result := idx.containsLookup("cat")
	if len(result) != 0 {
		t.Errorf("containsLookup('cat') should have 0 results (no false positives), got %d", len(result))
	}
}

func TestStringContainsIndex_SortedResults(t *testing.T) {
	idx := newTrigramIndex()

	positions := []RowPosition{
		{1, 5}, {1, 3}, {0, 2}, {0, 1}, {0, 0},
	}
	values := []string{
		"apple banana",
		"banana cherry",
		"apple banana cherry",
		"banana date",
		"apple",
	}

	for i, v := range values {
		idx.indexString(v, positions[i])
	}

	result := idx.containsLookup("banana")
	if len(result) != 4 {
		t.Errorf("containsLookup('banana') should have 4 results, got %d: %v", len(result), result)
	}

	// Verify sorted
	if !sort.SliceIsSorted(result, func(i, j int) bool {
		if result[i].RecordIdx != result[j].RecordIdx {
			return result[i].RecordIdx < result[j].RecordIdx
		}
		return result[i].RowIdx < result[j].RowIdx
	}) {
		t.Errorf("results not sorted: %v", result)
	}
}

func TestExtractPrefixGrams(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{"ab", []string{"a", "ab"}},
		{"hello", []string{"h", "he", "hel", "hell", "hello"}},
	}
	for _, tt := range tests {
		got := extractPrefixGrams(tt.input)
		if len(got) != len(tt.want) {
			t.Errorf("extractPrefixGrams(%q) = %v, want %v", tt.input, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("extractPrefixGrams(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
			}
		}
	}
}

func TestStringContainsIndex_RemoveRecord(t *testing.T) {
	idx := NewStringContainsIndex()

	// Add entries via manual trigram index manipulation
	triIdx := newTrigramIndex()
	triIdx.indexString("hello", RowPosition{0, 0})
	triIdx.indexString("hello", RowPosition{0, 1})
	triIdx.indexString("world", RowPosition{1, 0})

	idx.mu.Lock()
	idx.columns["test"] = triIdx
	idx.mu.Unlock()

	// Verify we have data
	if idx.HasIndex("test") {
		result := idx.ContainsLookup("test", "hello")
		if len(result) != 2 {
			t.Errorf("containsLookup should have 2 results before RemoveRecord, got %d", len(result))
		}
	} else {
		t.Error("HasIndex should return true for test column")
	}

	// Remove record 0
	idx.RemoveRecord(0)

	// Check results
	result := idx.ContainsLookup("test", "hello")
	if len(result) != 0 {
		t.Errorf("containsLookup should have 0 results after RemoveRecord, got %d", len(result))
	}

	result = idx.ContainsLookup("test", "world")
	if len(result) != 1 {
		t.Errorf("containsLookup('world') should have 1 result after RemoveRecord, got %d", len(result))
	}
}
