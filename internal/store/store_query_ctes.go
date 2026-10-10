package store

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/23skdu/longbow/internal/core"
	qry "github.com/23skdu/longbow/internal/query"
	types "github.com/23skdu/longbow/internal/store/types"
)

// ListFlights returns a list of available datasets as FlightInfo objects.

func (s *VectorStore) resolveCTEs(ctx context.Context, ctes []qry.CTE, results map[string][]types.SearchResult) error {
	for _, cte := range ctes {
		if cte.Search == nil {
			continue
		}
		// Execute the search for this CTE
		// Wrap Search in a TicketQuery to use executeInternalTicket with fallback support
		tkt := &qry.TicketQuery{
			Name:   cte.Search.Dataset,
			Search: cte.Search,
		}
		res, err := s.executeInternalTicket(ctx, tkt)
		if err != nil {
			return err
		}
		s.logger.Debug().Str("cte", cte.Name).Int("results", len(res)).Msg("CTE resolved")
		results[cte.Name] = res
	}
	return nil
}

func (s *VectorStore) resolveSubqueries(ctx context.Context, filters []qry.Filter) error {
	for i := range filters {
		f := &filters[i]
		// Recursive check
		if len(f.Filters) > 0 {
			if err := s.resolveSubqueries(ctx, f.Filters); err != nil {
				return err
			}
		}

		if f.Subquery != nil {
			// Execute subquery
			res, err := s.executeInternalTicket(ctx, f.Subquery)
			if err != nil {
				return err
			}
			s.logger.Debug().Str("field", f.Field).Int("results", len(res)).Msg("Subquery resolved")
			// Extract IDs (or first column) into ResolvedValues
			resolved := make([]any, len(res))
			for j, r := range res {
				resolved[j] = uint64(r.ID)
			}
			f.ResolvedValues = resolved
		}
	}
	return nil
}

func (s *VectorStore) executeInternalSearch(ctx context.Context, req *qry.VectorSearchRequest) ([]types.SearchResult, error) {
	isHybrid := req.TextQuery != "" || (req.Alpha > 0 && req.Alpha < 1.0)
	var queryVec []float32
	if len(req.Vector) > 0 {
		queryVec = req.Vector
	}

	if isHybrid {
		return s.SearchHybrid(ctx, req.Dataset, queryVec, req.TextQuery, req.K, req.Alpha, 60, req.GraphAlpha, 2, req.RawHybrid)
	}

	ds, ok := s.getDataset(req.Dataset)
	if !ok {
		return nil, fmt.Errorf("dataset %s not found", req.Dataset)
	}

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	if ds.Index == nil {
		return nil, fmt.Errorf("index not initialized for %s", req.Dataset)
	}

	res, err := ds.Index.SearchVectors(ctx, queryVec, req.K, req.Filters, SearchOptions{
		IncludeVectors: req.IncludeVectors,
		VectorFormat:   types.MapStringToVectorDataType(req.VectorFormat),
		FilterExpr:     ParseFilter(req.FilterExpr),
	})
	if err != nil {
		return nil, err
	}

	return s.mapInternalToUserIDsLocked(ds, res), nil
}

func (s *VectorStore) executeInternalTicket(ctx context.Context, query *qry.TicketQuery) ([]types.SearchResult, error) {
	// If search is present AND has a vector/text, use vector search path
	if query.Search != nil && (len(query.Search.Vector) > 0 || query.Search.TextQuery != "") {
		return s.executeInternalSearch(ctx, query.Search)
	}

	// If search is present but has NO vector (metadata only),
	// copy its parameters to the main query for table scan.
	if query.Search != nil {
		if query.Name == "" {
			query.Name = query.Search.Dataset
		}
		if query.Limit == 0 {
			query.Limit = int64(query.Search.K)
		}
		if len(query.Filters) == 0 && len(query.Search.Filters) > 0 {
			query.Filters = query.Search.Filters
		}
	}

	// Table scan fallback for metadata-only internal queries
	return s.executeInternalTable(query)
}

func (s *VectorStore) executeInternalTable(query *qry.TicketQuery) ([]types.SearchResult, error) {
	ds, ok := s.getDataset(query.Name)
	if !ok {
		return nil, fmt.Errorf("dataset %s not found", query.Name)
	}

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	var results []types.SearchResult
	limit := int(query.Limit)
	if limit <= 0 {
		limit = 1000 // Default internal limit
	}

	idColIdx := -1
	if ds.Schema != nil {
		for j, field := range ds.Schema.Fields() {
			if field.Name == "id" {
				idColIdx = j
				break
			}
		}
	}

	for i, rec := range ds.Records.Read() {
		if len(results) >= limit {
			break
		}

		// Apply filters
		var eval *qry.FilterEvaluator
		if len(query.Filters) > 0 {
			var err error
			eval, err = s.evaluateFilters(ds, i, query.Filters)
			if err != nil {
				return nil, err
			}
		}

		// Apply tombstones
		ts := ds.Tombstones[i]

		numRows := int(rec.NumRows())
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if len(results) >= limit {
				break
			}

			// Check filters
			if eval != nil && !eval.Matches(rowIdx) {
				continue
			}

			// Check tombstones
			if ts != nil && ts.Contains(rowIdx) {
				continue
			}

			var res types.SearchResult
			// We need a numeric ID for SearchResult.
			// If 'id' column exists, try to extract it.
			if idColIdx != -1 {
				col := rec.Column(idColIdx)
				switch c := col.(type) {
				case *array.Uint32:
					res.ID = types.VectorID(c.Value(rowIdx))
				case *array.Uint64:
					res.ID = types.VectorID(c.Value(rowIdx)) // #nosec G115
				case *array.Int64:
					res.ID = types.VectorID(c.Value(rowIdx)) // #nosec G115
				case *array.Int32:
					res.ID = types.VectorID(c.Value(rowIdx)) // #nosec G115
				default:
					// Fallback to internal ID (constructed from batch/row)
					res.ID = types.VectorID(uint32(i)<<16 | uint32(rowIdx))
				}
			} else {
				res.ID = types.VectorID(uint32(i)<<16 | uint32(rowIdx))
			}

			results = append(results, res)
		}
	}

	return results, nil
}

func (s *VectorStore) evaluateFilters(ds *Dataset, batchIdx int, filters []core.Filter) (*qry.FilterEvaluator, error) {
	rec := ds.Records.Read()[batchIdx]
	eval, err := qry.NewFilterEvaluator(rec, filters)
	if err != nil {
		return nil, err
	}
	return eval, nil
}

// streamSearchResults is a helper to stream a list of search results as RecordBatches
