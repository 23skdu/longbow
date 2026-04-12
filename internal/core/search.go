package core

// SearchResult represents a single flight search result
type SearchResult struct {
	ID       VectorID
	Distance float32
	Score    float32
	Metadata map[string]interface{}
	Vector   []byte // Binary payload for the vector if requested
}

// Candidate represents a search result candidate with ID and distance
type Candidate struct {
	ID    uint32
	Dist  float32
	Level int // for hierarchical structures
}
