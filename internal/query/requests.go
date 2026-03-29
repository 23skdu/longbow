package query

// VectorSearchRequest defines the request format for VectorSearch action
type VectorSearchRequest struct {
	Dataset   string      `json:"dataset"`
	Vector    []float32   `json:"vector,omitempty"`  // Single vector (legacy/simple)
	Vectors   [][]float32 `json:"vectors,omitempty"` // Multiple vectors for pipelining
	K         int         `json:"k"`
	Filters    []Filter               `json:"filters,omitempty"`
	FilterExpr map[string]interface{} `json:"filter_expr,omitempty"` // Rich AST filters
	LocalOnly  bool                   `json:"local_only,omitempty"`
	// Hybrid Search Fields
	TextQuery string  `json:"text_query,omitempty"`
	Alpha     float32 `json:"alpha,omitempty"` // 0.0=sparse, 1.0=dense, 0.5=hybrid
	// GraphRAG Fields
	GraphAlpha float32 `json:"graph_alpha,omitempty"` // 0.0=disabled, >0 blends graph score

	// Vector Type & Quantization (TurboQuant)
	VectorType     string `json:"vector_type,omitempty"`     // "float32", "turboquant", "int8", "binary"
	TurboQuantBits int    `json:"turboquant_bits,omitempty"` // 4, 8 bits (default = 8)

	// Vector Transport
	IncludeVectors bool   `json:"include_vectors,omitempty"`
	VectorFormat   string `json:"vector_format,omitempty"` // "quantized", "f32", "f16", "bq"

	// Consistency Level for distributed queries
	Consistency string `json:"consistency,omitempty"` // "ONE", "QUORUM", "ALL"

	EfSearch int `json:"ef_search,omitempty"`
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
	VectorType     string `json:"vector_type,omitempty"`
	TurboQuantBits int    `json:"turboquant_bits,omitempty"`
}
