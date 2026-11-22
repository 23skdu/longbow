package query

import "github.com/23skdu/longbow/internal/core"

// VectorSearchRequest aliases to resolve circular dependencies and maintain backward compatibility
type VectorSearchRequest = core.VectorSearchRequest

// VectorSearchByIDRequest represents a vector search by id request value.
type VectorSearchByIDRequest = core.VectorSearchByIDRequest

// RecommendRequest represents a recommend request value.
type RecommendRequest = core.RecommendRequest

// VectorSearchResponse represents a vector search response value.
type VectorSearchResponse = core.VectorSearchResponse

// Filter represents a filter value.
type Filter = core.Filter

// WindowFunction represents a window function value.
type WindowFunction = core.WindowFunction

// WindowSpec represents a window spec value.
type WindowSpec = core.WindowSpec

// WindowOrder represents a window order value.
type WindowOrder = core.WindowOrder

// CTE represents a cte value.
type CTE = core.CTE

// GeoPoint represents a geo point value.
type GeoPoint = core.GeoPoint

// GeoBoundingBox represents a geo bounding box value.
type GeoBoundingBox = core.GeoBoundingBox

// GeoSearchRequest represents a geo search request value.
type GeoSearchRequest = core.GeoSearchRequest

// TicketQuery is also aliased for convenience
type TicketQuery = core.TicketQuery

// TemporalSearchRequest represents a temporal search request value.
type TemporalSearchRequest = core.TemporalSearchRequest

// TemporalAggregationRequest represents a temporal aggregation request value.
type TemporalAggregationRequest = core.TemporalAggregationRequest

// TemporalVersionHistoryRequest represents a temporal version history request value.
type TemporalVersionHistoryRequest = core.TemporalVersionHistoryRequest

// RangeSearchRequest represents a range search request value.
type RangeSearchRequest = core.RangeSearchRequest

// FederatedSearchRequest represents a federated search request value.
type FederatedSearchRequest = core.FederatedSearchRequest
