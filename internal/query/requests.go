package query

// VectorSearchRequest defines the request format for VectorSearch action
type VectorSearchRequest struct {
	Dataset   string      `json:"dataset"`
	Vector    []float32   `json:"vector"`  // Single vector (legacy/simple)
	Vectors   [][]float32 `json:"vectors"` // Multiple vectors for pipelining
	K         int         `json:"k"`
	Filters   []Filter    `json:"filters"`
	LocalOnly bool        `json:"local_only"`
	// Hybrid Search Fields
	TextQuery string  `json:"text_query"`
	Alpha     float32 `json:"alpha"` // 0.0=sparse, 1.0=dense, 0.5=hybrid
}

// VectorSearchResponse defines the response format for VectorSearch action
type VectorSearchResponse struct {
	IDs    []uint64  `json:"ids"`
	Scores []float32 `json:"scores"`
}
