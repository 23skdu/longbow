package store

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	lbmem "github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	qry "github.com/23skdu/longbow/internal/query"
	types "github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RangeSearch returns all vectors within the given distance threshold from queryVec.
func (s *VectorStore) RangeSearch(ctx context.Context, dataset string, queryVec []float32, threshold float32, filters []qry.Filter, maxResults int) ([]SearchResult, error) {
	defer func(start time.Time) {
		metrics.SearchLatencySeconds.WithLabelValues(dataset, "range").Observe(time.Since(start).Seconds())
	}(time.Now())

	if dataset == "" {
		return nil, status.Error(codes.InvalidArgument, "dataset name is required")
	}
	if len(queryVec) == 0 {
		return nil, status.Error(codes.InvalidArgument, "query vector must not be empty")
	}
	if threshold < 0 {
		return nil, status.Error(codes.InvalidArgument, "threshold must be non-negative")
	}

	ds, ok := s.getDataset(dataset)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "dataset not found: %s", dataset)
	}

	ds.dataMu.RLock()
	if ds.evicting.Load() {
		ds.dataMu.RUnlock()
		return nil, status.Errorf(codes.Unavailable, "dataset %s is being evicted", dataset)
	}
	if ds.Index == nil {
		ds.dataMu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, "dataset has no vector index")
	}
	expectedDim := ds.Index.GetDimension()
	actualDim := uint32(len(queryVec)) // #nosec G115
	dsType := InferVectorDataType(ds.Schema, "vector")
	if dsType == VectorTypeComplex64 || dsType == VectorTypeComplex128 {
		actualDim /= 2
	}
	if expectedDim > 0 && actualDim != expectedDim {
		ds.dataMu.RUnlock()
		return nil, status.Errorf(codes.InvalidArgument, "dimension mismatch: expected %d, got %d", expectedDim, actualDim)
	}

	results, err := ds.Index.SearchVectorsInRange(ctx, queryVec, threshold, filters, types.SearchOptions{})
	if err != nil {
		ds.dataMu.RUnlock()
		return nil, status.Errorf(codes.Internal, "range search failed: %v", err)
	}
	results = s.mapInternalToUserIDsLocked(ds, results)
	ds.dataMu.RUnlock()

	sort.Slice(results, func(i, j int) bool {
		if results[i].Distance == results[j].Distance {
			return results[i].ID < results[j].ID
		}
		return results[i].Distance < results[j].Distance
	})
	if maxResults > 0 && len(results) > maxResults {
		results = results[:maxResults]
	}

	return s.FilterResultsByRLS(ctx, dataset, results), nil
}

func (s *VectorStore) handleDoGetRangeSearch(req *types.RangeSearchRequest, wfs []qry.WindowFunction, stream flight.FlightService_DoGetServer, mem *lbmem.ArenaAllocator) error {
	results, err := s.RangeSearch(stream.Context(), req.Dataset, req.Vector, req.Threshold, req.Filters, req.MaxResults)
	if err != nil {
		return err
	}
	return s.streamSearchResults(results, wfs, stream, mem)
}

func (s *VectorStore) handleRangeSearchAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req qry.RangeSearchRequest
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid range search request: %v", err)
	}
	if _, err := s.AuthorizeDatasetAccess(stream.Context(), PermRead, req.Dataset); err != nil {
		return err
	}
	results, err := s.RangeSearch(stream.Context(), req.Dataset, req.Vector, req.Threshold, req.Filters, req.MaxResults)
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{
		"dataset":   req.Dataset,
		"threshold": req.Threshold,
		"count":     len(results),
		"results":   results,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal range search results: %v", err)
	}
	return stream.Send(&flight.Result{Body: body})
}
