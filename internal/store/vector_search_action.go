package store

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Global zero-alloc parser pool for VectorSearch
var vectorSearchParserPool = sync.Pool{
	New: func() interface{} {
		return query.NewZeroAllocVectorSearchParser(768)
	},
}

// handleVectorSearchAction handles the VectorSearch DoAction request
func (s *VectorStore) handleVectorSearchAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	start := time.Now()

	var req query.VectorSearchRequest
	var parseErr error

	parser := vectorSearchParserPool.Get().(*query.ZeroAllocVectorSearchParser)
	defer vectorSearchParserPool.Put(parser)

	req, parseErr = parser.Parse(action.Body)
	if parseErr != nil {
		metrics.VectorSearchParseFallbackTotal.Inc()
		req = query.VectorSearchRequest{}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			metrics.VectorSearchActionErrors.Inc()
			s.logger.Warn().Err(err).Msg("VectorSearch JSON parse failed")
			return status.Errorf(codes.InvalidArgument, "invalid JSON request: %v", err)
		}
	} else {
		metrics.ZeroAllocVectorSearchParseTotal.Inc()
	}
	// Validate K
	if req.K < 1 {
		metrics.VectorSearchActionErrors.Inc()
		return status.Error(codes.InvalidArgument, "k must be at least 1")
	}

	// Determine if Hybrid Search
	isHybrid := req.TextQuery != "" || (req.Alpha > 0 && req.Alpha < 1.0)

	// Combine Vector and Vectors into a single slice for processing
	var queryVectors [][]float32
	if len(req.Vector) > 0 {
		queryVectors = append(queryVectors, req.Vector)
	}
	if len(req.Vectors) > 0 {
		queryVectors = append(queryVectors, req.Vectors...)
	}

	if len(queryVectors) == 0 {
		metrics.VectorSearchActionErrors.Inc()
		return status.Error(codes.InvalidArgument, "no query vector(s) provided")
	}
	// Process each query vector
	for _, queryVec := range queryVectors {
		var searchResults []SearchResult
		var err error

		if isHybrid {
			// Perform Hybrid Search
			searchResults, err = s.SearchHybrid(stream.Context(), req.Dataset, queryVec, req.TextQuery, req.K, req.Alpha, 60)
			if err != nil {
				metrics.VectorSearchActionErrors.Inc()
				continue // For pipelining, we might want to continue or return error? Let's return error for now to be safe, or log it.
				// For now, let's stop on first error for simplicity unless user wants full partial results.
			}
		} else {
			// Standard Vector Search
			ds, err := s.getDataset(req.Dataset)
			if err != nil {
				metrics.VectorSearchActionErrors.Inc()
				return status.Errorf(codes.NotFound, "dataset not found: %s", req.Dataset)
			}

			// Acquire read lock
			ds.dataMu.RLock()
			if ds.evicting.Load() {
				ds.dataMu.RUnlock()
				metrics.EvictionRejectedQueries.Inc()
				return status.Errorf(codes.Unavailable, "dataset %s is being evicted", req.Dataset)
			}

			if ds.Index == nil {
				ds.dataMu.RUnlock()
				metrics.VectorSearchActionErrors.Inc()
				return status.Error(codes.FailedPrecondition, "dataset has no HNSW index")
			}

			// Validate dimension
			expectedDim := ds.Index.GetDimension()
			if uint32(len(queryVec)) != expectedDim {
				ds.dataMu.RUnlock()
				metrics.VectorSearchActionErrors.Inc()
				s.logger.Warn().
					Str("dataset", req.Dataset).
					Uint32("expected", expectedDim).
					Int("got", len(queryVec)).
					Int("index_len", ds.Index.Len()).
					Msg("Dimension mismatch in VectorSearch")
				return status.Errorf(codes.InvalidArgument, "dimension mismatch: expected %d, got %d", expectedDim, len(queryVec))
			}

			// Perform search
			var errSearch error
			searchResults, errSearch = ds.Index.SearchVectors(queryVec, req.K, req.Filters)
			if errSearch != nil {
				ds.dataMu.RUnlock()
				metrics.VectorSearchActionErrors.Inc()
				return status.Errorf(codes.Internal, "vector search failed: %v", errSearch)
			}

			// Map internal IDs to User IDs
			searchResults = s.MapInternalToUserIDs(ds, searchResults)
			ds.dataMu.RUnlock()
		}

		// Perform Global Search (Scatter-Gather) if not local-only
		if !req.LocalOnly && s.Mesh != nil {
			peers := s.Mesh.GetMembers()
			var remotePeers []mesh.Member
			selfID := s.Mesh.GetIdentity().ID
			for _, p := range peers {
				if p.ID != selfID {
					remotePeers = append(remotePeers, p)
				}
			}

			// Override Vector in request for remote peers
			batchReq := req
			batchReq.Vector = queryVec
			batchReq.Vectors = nil // Don't forward the whole batch to each peer for each call?
			// Wait, the aggregator/coordinator might need adjustment too.
			// For now, let's just forward the single vector.
			searchResults, err = s.coordinator.GlobalSearch(stream.Context(), searchResults, batchReq, remotePeers)
			if err != nil {
				s.logger.Warn().Err(err).Msg("Global search partial failure")
			}
		}

		// Build and send response for this vector
		resp := query.VectorSearchResponse{
			IDs:    make([]uint64, len(searchResults)),
			Scores: make([]float32, len(searchResults)),
		}
		for i, res := range searchResults {
			resp.IDs[i] = uint64(res.ID)
			resp.Scores[i] = res.Score
		}

		respBytes, _ := json.Marshal(resp)
		if err := stream.Send(&flight.Result{Body: respBytes}); err != nil {
			metrics.VectorSearchActionErrors.Inc()
			return err
		}
	}

	metrics.VectorSearchActionTotal.Inc()
	metrics.VectorSearchActionDuration.Observe(time.Since(start).Seconds())
	return nil
}

// handleVectorSearchByIDAction handles the VectorSearchByID DoAction request
func (s *VectorStore) handleVectorSearchByIDAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	var req query.VectorSearchByIDRequest
	if err := json.Unmarshal(action.Body, &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid json body: %v", err)
	}

	ds, err := s.getDataset(req.Dataset)
	if err != nil {
		return err
	}

	ds.dataMu.RLock()
	defer ds.dataMu.RUnlock()

	if ds.Index == nil {
		return status.Error(codes.FailedPrecondition, "dataset has no index")
	}

	// 1. Find the vector by User ID (Scan)
	// TODO: Optimize with an inverted index or map if available
	var targetVec []float32
	found := false

	for i, rec := range ds.Records {
		// Check for "id" column
		idColIdx := -1
		for j, field := range rec.Schema().Fields() {
			if field.Name == "id" {
				idColIdx = j
				break
			}
		}
		if idColIdx == -1 {
			continue // No ID column in this batch
		}

		// Support String and Int64 IDs
		// Comparison logic to match req.ID (which is string)
		// If column is Int64, we parse req.ID
		col := rec.Column(idColIdx)
		rowIdx := -1

		switch arr := col.(type) {
		case *array.String:
			for j := 0; j < arr.Len(); j++ {
				if arr.Value(j) == req.ID {
					rowIdx = j
					break
				}
			}
		case *array.Int64:
			var intID int64
			if n, _ := fmt.Sscanf(req.ID, "%d", &intID); n == 1 {
				for j := 0; j < arr.Len(); j++ {
					if arr.Value(j) == intID {
						rowIdx = j
						break
					}
				}
			}
		case *array.Uint64:
			var uintID uint64
			if n, _ := fmt.Sscanf(req.ID, "%d", &uintID); n == 1 {
				for j := 0; j < arr.Len(); j++ {
					if arr.Value(j) == uintID {
						rowIdx = j
						break
					}
				}
			}
		}

		// Fallback: Check using String() formatted representation if specific type match failed or wasn't covered
		if rowIdx == -1 {
			for j := 0; j < col.Len(); j++ {
				if i64Arr, ok := col.(*array.Int64); ok {
					var val int64
					if n, _ := fmt.Sscanf(req.ID, "%d", &val); n == 1 {
						if i64Arr.Value(j) == val {
							rowIdx = j
							break
						}
					}
				} else if u64Arr, ok := col.(*array.Uint64); ok {
					var val uint64
					if n, _ := fmt.Sscanf(req.ID, "%d", &val); n == 1 {
						if u64Arr.Value(j) == val {
							rowIdx = j
							break
						}
					}
				} else if strArr, ok := col.(*array.String); ok {
					if strArr.Value(j) == req.ID {
						rowIdx = j
						break
					}
				}
			}
		}

		if rowIdx != -1 {
			// Found it! Check if deleted.
			ts := ds.Tombstones[i]
			if ts != nil && ts.Contains(rowIdx) {
				continue // Deleted
			}

			// Extract Vector
			vec, err := ExtractVectorFromArrow(rec, rowIdx, -1) // -1 auto-detects vector column
			if err != nil {
				return status.Errorf(codes.Internal, "failed to extract vector: %v", err)
			}
			targetVec = vec
			found = true
			break
		}
	}

	if !found {
		return status.Errorf(codes.NotFound, "id '%s' not found in dataset '%s'", req.ID, req.Dataset)
	}

	// 2. Perform Search
	results, err := ds.Index.SearchVectors(targetVec, req.K, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "search failed: %v", err)
	}

	// 3. Map Results
	results = s.MapInternalToUserIDs(ds, results)

	resp := query.VectorSearchResponse{
		IDs:    make([]uint64, len(results)),
		Scores: make([]float32, len(results)),
	}
	for i, res := range results {
		resp.IDs[i] = uint64(res.ID)
		resp.Scores[i] = res.Score
	}

	respBytes, _ := json.Marshal(resp)
	if err := stream.Send(&flight.Result{Body: respBytes}); err != nil {
		return err
	}

	return nil
}
