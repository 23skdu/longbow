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

// Search runs the search operation.
func (a *storeDatasetSearcher) Search(ctx context.Context, query []float32, k int, _ types.SearchOptions) ([]types.SearchResult, error) {
	return a.store.SearchDatasetVectors(ctx, a.dataset, query, k, nil)
}

// GetDimension returns the dimension.
func (a *storeDatasetSearcher) GetDimension() int {
	ds, ok := a.store.getDataset(a.dataset)
	if !ok {
		return 0
	}
	a.store.lockDatasetForRead(ds)
	defer a.store.unlockDatasetForRead(ds)
	if ds.Index == nil {
		return 0
	}
	return int(ds.Index.GetDimension())
}

func datasetCollectionMetrics(s *VectorStore, ds *Dataset) (vecCount, dim int) {
	s.lockDatasetForRead(ds)
	defer s.unlockDatasetForRead(ds)
	if ds.Index != nil {
		vecCount = ds.Index.Len()
		dim = int(ds.Index.GetDimension())
	}
	return vecCount, dim
}

// syncDatasetToFederatedRouter reconciles store-owned federated collections
// with the live dataset map. Deleted datasets are removed, recreated datasets
// get a fresh searcher, and existing registrations have their metrics
// refreshed. All expensive work happens before any map publication, so
// searches never wait for a full resync under an exclusive router lock.
func (s *VectorStore) syncDatasetToFederatedRouter(router *FederatedQueryRouter, names []string) {
	live := make(map[string]struct{}, len(names))
	for _, name := range names {
		ds, ok := s.getDataset(name)
		if !ok || ds == nil {
			continue
		}
		live[name] = struct{}{}

		vecCount, dim := datasetCollectionMetrics(s, ds)
		searcher, registered := router.searcherFor(name)
		if storeSearcher, ok := searcher.(*storeDatasetSearcher); !registered || !ok || storeSearcher.store != s || storeSearcher.dataset != name {
			searcher = &storeDatasetSearcher{store: s, dataset: name}
		}
		router.RefreshCollection(name, searcher, &CollectionInfo{
			Name:        name,
			Dimension:   dim,
			VectorCount: vecCount,
		})
	}

	// Drop registrations whose dataset disappeared. Only collections owned by
	// this store are removed; externally registered searchers are left alone.
	for _, info := range router.ListCollections() {
		if info == nil {
			continue
		}
		if _, ok := live[info.Name]; ok {
			continue
		}
		searcher, ok := router.searcherFor(info.Name)
		if !ok {
			continue
		}
		storeSearcher, ok := searcher.(*storeDatasetSearcher)
		if ok && storeSearcher.store == s {
			router.RemoveCollection(info.Name)
		}
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
	// Only re-sync when the store has changed or a recreated dataset is
	// missing. Searcher lookup uses a COW snapshot, so the sync never holds an
	// exclusive lock while searches are in flight.
	if s.federatedDirty.Load() > 0 || router.SearcherCount() == 0 || router.hasMissingCollection(active) {
		s.syncDatasetToFederatedRouter(router, active)
		s.federatedDirty.Store(0)
	}

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
			vecCount, dim = datasetCollectionMetrics(s, ds)
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
