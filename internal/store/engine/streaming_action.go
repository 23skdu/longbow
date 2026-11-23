package engine

import (
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// handleStreamAction serves Flight DoAction calls for Item 5 streaming connectors & schema registry.
func (s *VectorStore) handleStreamAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	switch action.Type {
	case "CreateStreamConnector":
		var req struct {
			StreamConnectorConfig
			Start bool `json:"start"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid connector json: %v", err)
		}
		conn, err := s.GetStreamManager().CreateConnector(req.StreamConnectorConfig, req.Start)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(conn.Stats())
		return true, stream.Send(&flight.Result{Body: body})

	case "StopStreamConnector":
		var req struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		if err := s.GetStreamManager().StopConnector(req.Name); err != nil {
			return true, status.Errorf(codes.NotFound, "%v", err)
		}
		return true, stream.Send(&flight.Result{Body: []byte(`{"status":"stopped"}`)})

	case "ListStreamConnectors", "GetStreamStats":
		stats := s.GetStreamManager().ListStats()
		body, _ := json.Marshal(map[string]any{"connectors": stats})
		return true, stream.Send(&flight.Result{Body: body})

	case "RegisterStreamSchema":
		var req RegisteredStreamSchema
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid schema json: %v", err)
		}
		id := s.GetStreamManager().Registry().RegisterLocal(req.Subject, req)
		body, _ := json.Marshal(map[string]any{"schema_id": id, "subject": req.Subject})
		return true, stream.Send(&flight.Result{Body: body})

	case "IngestStreamEvent":
		var req struct {
			Connector string           `json:"connector"`
			Envelopes []StreamEnvelope `json:"envelopes"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid ingest json: %v", err)
		}
		conn, ok := s.GetStreamManager().GetConnector(req.Connector)
		if !ok {
			return true, status.Errorf(codes.NotFound, "connector %q not found", req.Connector)
		}
		committed, deduped, err := conn.ProcessEnvelopes(stream.Context(), req.Envelopes)
		if err != nil {
			return true, status.Errorf(codes.Internal, "stream ingestion failed: %v", err)
		}
		body, _ := json.Marshal(map[string]any{
			"committed":    committed,
			"deduplicated": deduped,
			"stats":        conn.Stats(),
		})
		return true, stream.Send(&flight.Result{Body: body})
	}
	return false, nil
}
