package query

import "github.com/23skdu/longbow/internal/core"

// Type aliases to resolve circular dependencies and maintain backward compatibility
type VectorSearchRequest = core.VectorSearchRequest
type VectorSearchByIDRequest = core.VectorSearchByIDRequest
type RecommendRequest = core.RecommendRequest
type VectorSearchResponse = core.VectorSearchResponse
type Filter = core.Filter
type WindowFunction = core.WindowFunction
type WindowSpec = core.WindowSpec
type WindowOrder = core.WindowOrder
type CTE = core.CTE
type GeoPoint = core.GeoPoint
type GeoBoundingBox = core.GeoBoundingBox
type GeoSearchRequest = core.GeoSearchRequest

// TicketQuery is also aliased for convenience
type TicketQuery = core.TicketQuery

type TemporalSearchRequest = core.TemporalSearchRequest
type TemporalAggregationRequest = core.TemporalAggregationRequest
type TemporalVersionHistoryRequest = core.TemporalVersionHistoryRequest
