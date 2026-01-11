package query

// VectorSearchRequest defines the request format for VectorSearch action
type VectorSearchRequest struct {
	Dataset   string      `json:"dataset"`
	Vector    []float32   `json:"vector,omitempty"`  // Single vector (legacy/simple)
	Vectors   [][]float32 `json:"vectors,omitempty"` // Multiple vectors for pipelining
	K         int         `json:"k"`
	Filters   []Filter    `json:"filters,omitempty"`
	LocalOnly bool        `json:"local_only,omitempty"`
	// Hybrid Search Fields
	TextQuery string  `json:"text_query,omitempty"`
	Alpha     float32 `json:"alpha,omitempty"` // 0.0=sparse, 1.0=dense, 0.5=hybrid
	// GraphRAG Fields
	GraphAlpha float32 `json:"graph_alpha,omitempty"` // 0.0=disabled, >0 blends graph score

	// Vector Transport
	IncludeVectors bool   `json:"include_vectors,omitempty"`
	VectorFormat   string `json:"vector_format,omitempty"` // "quantized", "f32", "f16"
}

// VectorSearchResponse defines the response format for VectorSearch action
type VectorSearchResponse struct {
	IDs    []uint64  `json:"ids"`
	Scores []float32 `json:"scores"`
}

// VectorSearchByIDRequest defines the request format for searching by User ID
type VectorSearchByIDRequest struct {
	Dataset        string `json:"dataset"`
	ID             string `json:"id"` // User ID (stringified)
	K              int    `json:"k"`
	IncludeVectors bool   `json:"include_vectors,omitempty"`
	VectorFormat   string `json:"vector_format,omitempty"`
}
