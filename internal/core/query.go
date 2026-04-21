package core

import (
	"encoding/json"
)

// Filter defines a filter for search results
type Filter struct {
	Field    string `json:"field,omitempty"`
	Operator string `json:"operator,omitempty"`
	Value    string `json:"value,omitempty"`
	// Logic combines multiple filters: "AND", "OR", "NOT"
	Logic   string   `json:"logic,omitempty"`
	Filters []Filter `json:"filters,omitempty"`
	// Subquery allows filtering based on the results of another query
	Subquery *TicketQuery `json:"subquery,omitempty"`
	// ResolvedValues stores the results of a subquery after it's executed
	ResolvedValues []any `json:"-"`
}

// WindowFunction defines an analytical function to be applied over result sets
type WindowFunction struct {
	Name  string     `json:"name"`            // row_number, rank, dense_rank
	Field string     `json:"field,omitempty"` // Field to aggregate (for avg, sum, etc.)
	Over  WindowSpec `json:"over"`
	As    string     `json:"as"` // Output column name
}

// WindowSpec defines the partition and ordering for a window function
type WindowSpec struct {
	PartitionBy []string      `json:"partition_by,omitempty"`
	OrderBy     []WindowOrder `json:"order_by,omitempty"`
}

// WindowOrder defines Sort order within a window
type WindowOrder struct {
	Field      string `json:"field"`
	Descending bool   `json:"descending,omitempty"`
}

// GeoPoint represents a coordinate on Earth
type GeoPoint struct {
	Lat  float64 `json:"lat"`
	Lon  float64 `json:"lon"`
	Name string  `json:"name,omitempty"`
}

func (g *GeoPoint) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Lat  float64 `json:"lat"`
		Lon  float64 `json:"lon"`
		Name string  `json:"name,omitempty"`
	}{
		Lat:  g.Lat,
		Lon:  g.Lon,
		Name: g.Name,
	})
}

func (g *GeoPoint) UnmarshalJSON(data []byte) error {
	var parsed struct {
		Lat  float64 `json:"lat"`
		Lon  float64 `json:"lon"`
		Name string  `json:"name,omitempty"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	g.Lat = parsed.Lat
	g.Lon = parsed.Lon
	g.Name = parsed.Name
	return nil
}

// GeoBoundingBox represents a rectangular geographic area
type GeoBoundingBox struct {
	MinLat float64 `json:"min_lat"`
	MaxLat float64 `json:"max_lat"`
	MinLon float64 `json:"min_lon"`
	MaxLon float64 `json:"max_lon"`
}

// GeoSearchRequest defines the request format for Geospatial search
type GeoSearchRequest struct {
	Dataset    string                 `json:"dataset"`
	Center     GeoPoint               `json:"center"`
	RadiusKm   float64                `json:"radius_km"`
	Box        *GeoBoundingBox        `json:"box,omitempty"`
	K          int                    `json:"k"`
	Filters    []Filter               `json:"filters,omitempty"`
	SearchType string                 `json:"search_type"` // "radius", "box", "hybrid"
}

// VectorSearchRequest defines the request format for VectorSearch action
type VectorSearchRequest struct {
	Dataset         string                 `json:"dataset"`
	Vector          []float32              `json:"vector,omitempty"`
	Vectors         [][]float32            `json:"vectors,omitempty"`
	K               int                    `json:"k"`
	Filters         []Filter               `json:"filters,omitempty"`
	FilterExpr      map[string]interface{} `json:"filter_expr,omitempty"`
	LocalOnly       bool                   `json:"local_only,omitempty"`
	TextQuery       string                 `json:"text_query,omitempty"`
	Alpha           float32                `json:"alpha,omitempty"`
	GraphAlpha      float32                `json:"graph_alpha,omitempty"`
	IncludeVectors  bool                   `json:"include_vectors,omitempty"`
	VectorFormat    string                 `json:"vector_format,omitempty"`
	VectorType      string                 `json:"vector_type,omitempty"`     // "float32", "turboquant", etc.
	TurboQuantBits  int                    `json:"turboquant_bits,omitempty"` // 4, 8 bits
	Consistency     string                 `json:"consistency,omitempty"`     // "ONE", "QUORUM", "ALL"
	EfSearch        int                    `json:"ef_search,omitempty"`
	WindowFunctions []WindowFunction       `json:"window_functions,omitempty"`
}

// VectorSearchByIDRequest defines the request format for searching by User ID
type VectorSearchByIDRequest struct {
	Dataset        string `json:"dataset"`
	ID             string `json:"id"`
	K              int    `json:"k"`
	IncludeVectors bool   `json:"include_vectors,omitempty"`
	VectorFormat   string `json:"vector_format,omitempty"`
	VectorType     string `json:"vector_type,omitempty"`
	TurboQuantBits int    `json:"turboquant_bits,omitempty"`
}

// RecommendRequest defines the request for recommendation
type RecommendRequest struct {
	Dataset string   `json:"dataset"`
	SeedIDs []string `json:"seed_ids"`
	K       int      `json:"k"`
	Alpha   float32  `json:"alpha"`
	MaxHops int      `json:"max_hops,omitempty"`
	Decay   float32  `json:"decay,omitempty"`
}

// CTE defines a Common Table Expression
type CTE struct {
	Name    string               `json:"name"`
	Search  *VectorSearchRequest `json:"search"`
	Columns []string             `json:"columns,omitempty"`
}

// TicketQuery defines the structure for a ticket based query
type TicketQuery struct {
	Name            string                   `json:"name"`
	Limit           int64                    `json:"limit"`
	Filters         []Filter                 `json:"filters"`
	WindowFunctions []WindowFunction         `json:"window_functions,omitempty"`
	Search          *VectorSearchRequest     `json:"search,omitempty"`
	SearchByID      *VectorSearchByIDRequest `json:"search_by_id,omitempty"`
	Recommend       *RecommendRequest        `json:"recommend,omitempty"`
	GeoSearch       *GeoSearchRequest        `json:"geo_search,omitempty"`
	CTEs            []CTE                    `json:"ctes,omitempty"`

	// Backwards compatibility / Direct shortcut fields
	Vector []float32 `json:"vector,omitempty"`
	K      int       `json:"k,omitempty"`
}

// VectorSearchResponse defines the response format for VectorSearch action
type VectorSearchResponse struct {
	IDs    []uint64  `json:"ids"`
	Scores []float32 `json:"scores"`
}
