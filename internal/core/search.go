package core

const (
	// SourceDense is a source dense constant.
	SourceDense uint8 = 0
	// SourceSparse is a source sparse constant.
	SourceSparse uint8 = 1
	// SourceFused is a source fused constant.
	SourceFused uint8 = 2
)

// SearchResult represents a single flight search result
type SearchResult struct {
	ID       VectorID
	Distance float32
	Score    float32
	Metadata []byte // Binary metadata payload (zero-copy optimized)
	Vector   []byte // Binary payload for the vector if requested
	Source   uint8  // 0=Dense, 1=Sparse, 2=Fused (see constants)
}

// Candidate represents a search result candidate with ID and distance
type Candidate struct {
	ID    uint32
	Dist  float32
	Level int // for hierarchical structures
}

// ResultIterator provides a streaming interface for search results.
type ResultIterator interface {
	// Next returns the next search result. Returns (SearchResult{}, false) if no more results.
	Next() (SearchResult, bool)
	// Close releases any resources held by the iterator.
	Close() error
}

// ResultSliceIterator implements ResultIterator for a simple slice of results.
type ResultSliceIterator struct {
	results []SearchResult
	pos     int
}

// NewResultSliceIterator constructs a new ResultSliceIterator.
func NewResultSliceIterator(results []SearchResult) *ResultSliceIterator {
	return &ResultSliceIterator{results: results}
}

// Next runs the next operation.
func (it *ResultSliceIterator) Next() (SearchResult, bool) {
	if it.pos >= len(it.results) {
		return SearchResult{}, false
	}
	res := it.results[it.pos]
	it.pos++
	return res, true
}

// Close runs the close operation.
func (it *ResultSliceIterator) Close() error {
	it.results = nil
	return nil
}
