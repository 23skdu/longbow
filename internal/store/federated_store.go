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

type storeDatasetSearcher struct {
	store   *VectorStore
	dataset string
}

func (a *storeDatasetSearcher) Search(ctx context.Context, query []float32, k int, _ types.SearchOptions) ([]types.SearchResult, error) {
	return a.store.SearchDatasetVectors(ctx, a.dataset, query, k, nil)
}

func (a *storeDatasetSearcher) GetDimension() int {
	ds, ok := a.store.getDataset(a.dataset)
	if !ok {
		return 0
	}
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()
	if ds.Index == nil {
		return 0
	}
	return int(ds.Index.GetDimension())
}

func datasetCollectionMetrics(ds *Dataset) (vecCount, dim int) {
	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()
	if ds.Index != nil {
		vecCount = ds.Index.Len()
		dim = int(ds.Index.GetDimension())
	}
	return vecCount, dim
}

// syncDatasetToFederatedRouter ensures live store datasets are indexed in the federated router.
func (s *VectorStore) syncDatasetToFederatedRouter(router *FederatedQueryRouter, names []string) {
	for _, name := range names {
		ds, ok := s.getDataset(name)
		if !ok {
			continue
		}
		vecCount, dim := datasetCollectionMetrics(ds)
		router.UpsertCollection(name, &storeDatasetSearcher{store: s, dataset: name}, &CollectionInfo{
			Name:        name,
			Dimension:   dim,
			VectorCount: vecCount,
		})
	}
}

// FederatedSearch executes parallel k-NN across multiple collections or tag-matched collections
// and merges results via Reciprocal Rank Fusion (RRF).
func (s *VectorStore) FederatedSearch(ctx context.Context, collections, tags []string, queryVec []float32, k, rrfK int) ([]SearchResult, error) {
	defer func(start time.Time) {
		metrics.SearchLatencySeconds.WithLabelValues("federated", "federated").Observe(time.Since(start).Seconds())
	}(time.Now())

	if len(queryVec) == 0 {
		return nil, status.Error(codes.InvalidArgument, "query vector must not be empty")
	}
	if k <= 0 {
		k = 10
	}

	router := s.GetFederatedRouter()
	active := s.GetActiveDatasets()
	s.syncDatasetToFederatedRouter(router, active)

	if len(tags) > 0 && len(collections) == 0 {
		// Authorize all readable datasets before routing by tag.
		candidates := router.getCollectionsForTags(tags)
		authorized := make([]string, 0, len(candidates))
		for _, c := range candidates {
			if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, c); err == nil {
				authorized = append(authorized, c)
			}
		}
		if len(authorized) == 0 {
			return []SearchResult{}, nil
		}
		return router.SearchWithRRFK(ctx, queryVec, authorized, k, rrfK)
	}

	targets := collections
	if len(targets) == 0 {
		targets = active
		sort.Strings(targets)
	}
	authorized := make([]string, 0, len(targets))
	for _, col := range targets {
		if _, err := s.AuthorizeDatasetAccess(ctx, PermRead, col); err != nil {
			if len(collections) > 0 {
				return nil, err
			}
			continue
		}
		if _, ok := s.getDataset(col); ok {
			authorized = append(authorized, col)
		}
	}
	if len(authorized) == 0 {
		return []SearchResult{}, nil
	}
	return router.SearchWithRRFK(ctx, queryVec, authorized, k, rrfK)
}

func (s *VectorStore) handleDoGetFederatedSearch(req *types.FederatedSearchRequest, wfs []qry.WindowFunction, stream flight.FlightService_DoGetServer, mem *lbmem.ArenaAllocator) error {
	results, err := s.FederatedSearch(stream.Context(), req.Collections, req.Tags, req.Vector, req.K, req.RRFK)
	if err != nil {
		return err
	}
	return s.streamSearchResults(results, wfs, stream, mem)
}

func (s *VectorStore) handleFederatedAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	switch action.Type {
	case "FederatedSearch":
		var req qry.FederatedSearchRequest
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid federated search request: %v", err)
		}
		results, err := s.FederatedSearch(stream.Context(), req.Collections, req.Tags, req.Vector, req.K, req.RRFK)
		if err != nil {
			return true, err
		}
		body, _ := json.Marshal(map[string]any{
			"count":   len(results),
			"results": results,
		})
		return true, stream.Send(&flight.Result{Body: body})

	case "RegisterFederatedCollection":
		var req struct {
			Dataset     string            `json:"dataset"`
			Name        string            `json:"name"`
			Description string            `json:"description,omitempty"`
			Tags        []string          `json:"tags,omitempty"`
			Metadata    map[string]string `json:"metadata,omitempty"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid register collection request: %v", err)
		}
		name := req.Dataset
		if name == "" {
			name = req.Name
		}
		if name == "" {
			return true, status.Error(codes.InvalidArgument, "dataset or name is required")
		}
		if _, err := s.AuthorizeDatasetAccess(stream.Context(), PermWrite, name); err != nil {
			return true, err
		}
		router := s.GetFederatedRouter()
		ds, ok := s.getDataset(name)
		vecCount := 0
		dim := 0
		if ok {
			vecCount, dim = datasetCollectionMetrics(ds)
		}
		info := &CollectionInfo{
			Name:        name,
			Description: req.Description,
			Tags:        req.Tags,
			Dimension:   dim,
			VectorCount: vecCount,
			Metadata:    req.Metadata,
		}
		router.UpsertCollection(name, &storeDatasetSearcher{store: s, dataset: name}, info)
		for _, tag := range req.Tags {
			_ = router.RegisterRoutingRule(&RoutingRule{
				Tag:        tag,
				Collection: name,
				Priority:   1,
			})
		}
		body, _ := json.Marshal(map[string]any{
			"status":     "registered",
			"collection": name,
			"tags":       req.Tags,
		})
		return true, stream.Send(&flight.Result{Body: body})

	case "ListFederatedCollections":
		router := s.GetFederatedRouter()
		s.syncDatasetToFederatedRouter(router, s.GetActiveDatasets())
		cols := router.ListCollections()
		visible := make([]*CollectionInfo, 0, len(cols))
		for _, c := range cols {
			if _, err := s.AuthorizeDatasetAccess(stream.Context(), PermRead, c.Name); err == nil {
				visible = append(visible, c)
			}
		}
		body, _ := json.Marshal(map[string]any{
			"count":       len(visible),
			"collections": visible,
		})
		return true, stream.Send(&flight.Result{Body: body})
	}
	return false, nil
}
