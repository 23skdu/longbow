package store

import (
	"math"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/apache/arrow-go/v18/arrow/flight"

	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/mesh"
	qry "github.com/23skdu/longbow/internal/query"
	types "github.com/23skdu/longbow/internal/store/types"
)

// ListFlights returns a list of available datasets as FlightInfo objects.

func (s *VectorStore) handleDoGetGeoSearch(req *types.GeoSearchRequest, wfs []qry.WindowFunction, stream flight.FlightService_DoGetServer, mem *lbmem.ArenaAllocator) error {
	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
	}

	if ds.GeoIndex == nil {
		return status.Error(codes.FailedPrecondition, "dataset has no geospatial index")
	}

	// Lock dataset for search
	ds.dataMu.RLock()
	var results []types.SearchResult
	var err error

	switch req.SearchType {
	case "radius":
		results, err = ds.GeoIndex.SearchRadius(stream.Context(), req.Center, req.RadiusKm, req.K)
	case "box":
		if req.Box == nil {
			ds.dataMu.RUnlock()
			return status.Error(codes.InvalidArgument, "bounding box required for 'box' search")
		}
		results, err = ds.GeoIndex.SearchBox(stream.Context(), *req.Box, req.K)
	case "hybrid":
		results, err = ds.GeoIndex.HybridSearch(stream.Context(), req.QueryVector, req.Center, req.RadiusKm, req.K)
	default:
		ds.dataMu.RUnlock()
		return status.Errorf(codes.InvalidArgument, "invalid search_type: %s", req.SearchType)
	}

	if err != nil {
		ds.dataMu.RUnlock()
		return status.Errorf(codes.Internal, "geospatial search failed: %v", err)
	}

	// Map internal IDs to user IDs if primary index exists
	results = s.mapInternalToUserIDsLocked(ds, results)
	ds.dataMu.RUnlock()

	// 2. Global Scatter-Gather if enabled
	md, _ := metadata.FromIncomingContext(stream.Context())
	isGlobal := false
	if vals := md.Get("x-longbow-global"); len(vals) > 0 && vals[0] == "true" {
		isGlobal = true
	}

	if isGlobal && s.Mesh != nil {
		var matchedNodeIDs []string
		if s.Mesh.Config.Delegate != nil {
			if router, ok := s.Mesh.Config.Delegate.(interface {
				RouteGeo(dataset string, lat, lon float64, radiusKm float64) []string
			}); ok {
				lat := req.Center.Lat
				lon := req.Center.Lon
				radius := req.RadiusKm
				if req.SearchType == "box" && req.Box != nil {
					lat = (req.Box.MinLat + req.Box.MaxLat) / 2
					lon = (req.Box.MinLon + req.Box.MaxLon) / 2
					dLat := req.Box.MaxLat - req.Box.MinLat
					dLon := req.Box.MaxLon - req.Box.MinLon
					radius = math.Sqrt(dLat*dLat+dLon*dLon) * 111.0 / 2
				}
				matchedNodeIDs = router.RouteGeo(req.Dataset, lat, lon, radius)
			}
		}

		var remotePeers []mesh.Member
		selfID := s.Mesh.GetIdentity().ID
		matchedSet := make(map[string]bool)
		for _, id := range matchedNodeIDs {
			matchedSet[id] = true
		}

		for _, p := range s.Mesh.GetMembers() {
			if p.ID != selfID && (len(matchedNodeIDs) == 0 || matchedSet[p.ID]) {
				remotePeers = append(remotePeers, p)
			}
		}

		var globalErr error
		results, globalErr = s.coordinator.GlobalGeoSearch(stream.Context(), results, req, remotePeers)
		if globalErr != nil {
			s.logger.Warn().Err(globalErr).Msg("DoGet GlobalGeoSearch partial failure")
		}
	}

	return s.streamSearchResults(results, wfs, stream, mem)
}

func (s *VectorStore) handleDoGetTemporalSearch(req *types.TemporalSearchRequest, wfs []qry.WindowFunction, stream flight.FlightService_DoGetServer, mem *lbmem.ArenaAllocator) error {
	if !s.temporalConfig.Enabled {
		return status.Error(codes.FailedPrecondition, "temporal index not enabled")
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return status.Errorf(codes.NotFound, "dataset %s not found", req.Dataset)
	}

	var results []types.SearchResult
	var err error

	switch req.SearchType {
	case "as_of":
		results, err = ds.TemporalIndex.SearchAsOf(stream.Context(), req.Timestamp, req.K)
	case "range":
		results, err = ds.TemporalIndex.SearchRange(stream.Context(), req.StartTime, req.EndTime, req.K)
	case "sliding_window":
		results, err = ds.TemporalIndex.SearchSlidingWindow(stream.Context(), req.WindowSize, req.K)
	case "sliding_window_time":
		results, err = ds.TemporalIndex.SearchSlidingWindowByTime(stream.Context(), req.Duration, req.K)
	default:
		return status.Errorf(codes.InvalidArgument, "invalid temporal search_type: %s", req.SearchType)
	}

	if err != nil {
		return status.Errorf(codes.Internal, "temporal search failed: %v", err)
	}

	// Lock dataset for ID mapping (requires dataMu)
	ds.dataMu.RLock()
	results = s.mapInternalToUserIDsLocked(ds, results)
	ds.dataMu.RUnlock()

	// 2. Global Scatter-Gather if enabled
	md, _ := metadata.FromIncomingContext(stream.Context())
	isGlobal := false
	if vals := md.Get("x-longbow-global"); len(vals) > 0 && vals[0] == "true" {
		isGlobal = true
	}

	if isGlobal && s.Mesh != nil {
		var matchedNodeIDs []string
		if s.Mesh.Config.Delegate != nil {
			if router, ok := s.Mesh.Config.Delegate.(interface {
				RouteTemporal(dataset string, startTime, endTime int64) []string
			}); ok {
				var startTime, endTime int64
				switch req.SearchType {
				case "as_of":
					startTime = 0
					endTime = req.Timestamp
				case "range":
					startTime = req.StartTime
					endTime = req.EndTime
				case "sliding_window", "sliding_window_time":
					startTime = 0
					endTime = time.Now().UnixNano()
				}
				matchedNodeIDs = router.RouteTemporal(req.Dataset, startTime, endTime)
			}
		}

		var remotePeers []mesh.Member
		selfID := s.Mesh.GetIdentity().ID
		matchedSet := make(map[string]bool)
		for _, id := range matchedNodeIDs {
			matchedSet[id] = true
		}

		for _, p := range s.Mesh.GetMembers() {
			if p.ID != selfID && (len(matchedNodeIDs) == 0 || matchedSet[p.ID]) {
				remotePeers = append(remotePeers, p)
			}
		}

		var globalErr error
		results, globalErr = s.coordinator.GlobalTemporalSearch(stream.Context(), results, req, remotePeers)
		if globalErr != nil {
			s.logger.Warn().Err(globalErr).Msg("DoGet GlobalTemporalSearch partial failure")
		}
	}

	return s.streamSearchResults(results, wfs, stream, mem)
}
