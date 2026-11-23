package engine

import "github.com/23skdu/longbow/internal/store/controlplane"

import (
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *VectorStore) handleCloudControlAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	ctrl := s.GetCloudControlPlane()
	switch action.Type {
	case "CreateCloudCluster":
		var req CreateCloudClusterRequest
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		cl, err := ctrl.CreateCluster(req)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, sendJSONActionResult(stream, cl)

	case "GetCloudCluster":
		var req struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		cl, ok := ctrl.GetCluster(req.ID)
		if !ok {
			return true, status.Errorf(codes.NotFound, "cluster %s not found", req.ID)
		}
		return true, sendJSONActionResult(stream, cl)

	case "ListCloudClusters":
		var req struct {
			TenantID string `json:"tenant_id"`
		}
		if len(action.Body) > 0 {
			_ = json.Unmarshal(action.Body, &req)
		}
		return true, sendJSONActionResult(stream, map[string]any{"clusters": ctrl.ListClusters(req.TenantID)})

	case "ScaleCloudCluster":
		var req struct {
			ID    string    `json:"id"`
			Nodes int       `json:"nodes"`
			Tier  CloudTier `json:"tier"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		cl, err := ctrl.ScaleCluster(req.ID, req.Nodes, req.Tier)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, sendJSONActionResult(stream, cl)

	case "AddReplicaRegion":
		var req struct {
			ID       string `json:"id"`
			Region   string `json:"region"`
			Endpoint string `json:"endpoint"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		cl, err := ctrl.AddReplicaRegion(req.ID, req.Region, req.Endpoint)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, sendJSONActionResult(stream, cl)

	case "ReplicateClusterMutation":
		var req struct {
			ClusterID string           `json:"cluster_id"`
			Event     ReplicationEvent `json:"event"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		n, err := ctrl.ReplicateMutation(stream.Context(), req.ClusterID, req.Event)
		if err != nil {
			return true, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return true, sendJSONActionResult(stream, map[string]any{"replicated_regions": n})

	case "PromoteClusterRegion":
		var req struct {
			ClusterID    string `json:"cluster_id"`
			TargetRegion string `json:"target_region"`
			Reason       string `json:"reason"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		cl, err := ctrl.PromoteRegion(stream.Context(), req.ClusterID, req.TargetRegion, req.Reason)
		if err != nil {
			return true, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return true, sendJSONActionResult(stream, cl)

	case "GetCloudSLAReport":
		var req struct {
			ClusterID     string  `json:"cluster_id"`
			ObservedP99Ms float64 `json:"observed_p99_ms"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		rep, err := ctrl.EvaluateSLA(req.ClusterID, req.ObservedP99Ms)
		if err != nil {
			return true, status.Errorf(codes.NotFound, "%v", err)
		}
		return true, sendJSONActionResult(stream, rep)

	case "DeleteCloudCluster":
		var req struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		if err := ctrl.DeleteCluster(req.ID); err != nil {
			return true, status.Errorf(codes.NotFound, "%v", err)
		}
		return true, sendJSONActionResult(stream, map[string]any{"deleted": req.ID})

	default:
		return false, nil
	}
}

// sendJSONActionResult keeps existing action handlers working after the
// control plane domain moved.
func sendJSONActionResult(stream flight.FlightService_DoActionServer, payload any) error {
	return controlplane.SendJSONActionResult(stream, payload)
}
