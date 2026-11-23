package engine

import (
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *VectorStore) handleSearchExtrasAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	switch action.Type {
	case "AdvancedTextSearch":
		var req AdvancedTextQueryRequest
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		resp, err := s.GetTextSearchManager().Search(stream.Context(), req)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(resp)
		return true, stream.Send(&flight.Result{Body: body})

	case "SearchFacets":
		var req AdvancedTextQueryRequest
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		resp, err := s.GetTextSearchManager().Search(stream.Context(), req)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(map[string]any{"facets": resp.Facets, "total_hits": resp.TotalHits})
		return true, stream.Send(&flight.Result{Body: body})

	case "SearchAnalytics":
		var req struct {
			Limit int `json:"limit"`
		}
		_ = json.Unmarshal(action.Body, &req)
		snap := s.GetTextSearchManager().Analytics(req.Limit)
		body, _ := json.Marshal(snap)
		return true, stream.Send(&flight.Result{Body: body})

	case "TuneBM25":
		var req struct {
			Dataset string  `json:"dataset"`
			K1      float64 `json:"k1"`
			B       float64 `json:"b"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		if err := s.GetTextSearchManager().SetBM25Params(req.Dataset, BM25TuningParams{K1: req.K1, B: req.B}); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, stream.Send(&flight.Result{Body: []byte(`{"status":"bm25_tuned"}`)})
	}
	return false, nil
}
