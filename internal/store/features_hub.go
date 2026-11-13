package store

import (
	"net/http"

	"github.com/apache/arrow-go/v18/arrow/flight"
)

// FeatureSuite holds lazily-initialized enterprise and extended feature managers
// attached to a VectorStore instance.
type FeatureSuite struct {
	metering    *UsageMeter
	streaming   *StreamIngestionManager
	multimodal  *MultiModalPipeline
	reranker    *AdvancedRerankManager
	elasticCtrl *ElasticShardingController
	schemaReg   *SchemaConstraintRegistry
	matViews    *MaterializedViewManager
	textSearch  *AdvancedTextSearchManager
	provenance  *ProvenanceManager
	cloudCtrl   *CloudControlPlane
	federated   *FederatedQueryRouter
}

func (s *VectorStore) ensureFeatures() *FeatureSuite {
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if s.features == nil {
		s.features = &FeatureSuite{}
	}
	return s.features
}

// GetUsageMeter returns the VectorStore's tenant usage & billing meter, initializing it lazily.
func (s *VectorStore) GetUsageMeter() *UsageMeter {
	fs := s.ensureFeatures()
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if fs.metering == nil {
		fs.metering = NewUsageMeter(s.logger)
	}
	return fs.metering
}

// GetStreamManager returns the VectorStore's streaming CDC & connector manager, initializing it lazily.
func (s *VectorStore) GetStreamManager() *StreamIngestionManager {
	fs := s.ensureFeatures()
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if fs.streaming == nil {
		fs.streaming = NewStreamIngestionManager(s)
	}
	return fs.streaming
}

// GetMultiModalPipeline returns the VectorStore's multi-modal embedding & search pipeline.
func (s *VectorStore) GetMultiModalPipeline() *MultiModalPipeline {
	fs := s.ensureFeatures()
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if fs.multimodal == nil {
		p, _ := NewMultiModalPipeline(s, MultiModalConfig{Dimension: 128})
		fs.multimodal = p
	}
	return fs.multimodal
}

// GetRerankManager returns the VectorStore's advanced reranking & query rewriting manager.
func (s *VectorStore) GetRerankManager() *AdvancedRerankManager {
	fs := s.ensureFeatures()
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if fs.reranker == nil {
		fs.reranker = NewAdvancedRerankManager(s)
	}
	return fs.reranker
}

// GetElasticController returns the VectorStore's elastic sharding & auto-scaling controller.
func (s *VectorStore) GetElasticController() *ElasticShardingController {
	fs := s.ensureFeatures()
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if fs.elasticCtrl == nil {
		fs.elasticCtrl = NewElasticShardingController(s)
	}
	return fs.elasticCtrl
}

// GetSchemaRegistry returns the VectorStore's schema constraint & evolution registry.
func (s *VectorStore) GetSchemaRegistry() *SchemaConstraintRegistry {
	fs := s.ensureFeatures()
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if fs.schemaReg == nil {
		fs.schemaReg = NewSchemaConstraintRegistry(s)
	}
	return fs.schemaReg
}

// GetMaterializedViewManager returns the VectorStore's materialized view & aggregation manager.
func (s *VectorStore) GetMaterializedViewManager() *MaterializedViewManager {
	fs := s.ensureFeatures()
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if fs.matViews == nil {
		fs.matViews = NewMaterializedViewManager(s)
	}
	return fs.matViews
}

// GetTextSearchManager returns the VectorStore's advanced full-text & analytics engine.
func (s *VectorStore) GetTextSearchManager() *AdvancedTextSearchManager {
	fs := s.ensureFeatures()
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if fs.textSearch == nil {
		fs.textSearch = NewAdvancedTextSearchManager(s)
	}
	return fs.textSearch
}

// GetProvenanceManager returns the VectorStore's vector provenance & explainability manager.
func (s *VectorStore) GetProvenanceManager() *ProvenanceManager {
	fs := s.ensureFeatures()
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if fs.provenance == nil {
		fs.provenance = NewProvenanceManager(s)
	}
	return fs.provenance
}

// GetCloudControlPlane returns the lazily initialized managed cloud & multi-region control plane.
func (s *VectorStore) GetCloudControlPlane() *CloudControlPlane {
	fs := s.ensureFeatures()
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if fs.cloudCtrl == nil {
		fs.cloudCtrl = NewCloudControlPlane(s)
	}
	return fs.cloudCtrl
}

// GetFederatedRouter returns the lazily initialized federated cross-collection search router.
func (s *VectorStore) GetFederatedRouter() *FederatedQueryRouter {
	fs := s.ensureFeatures()
	s.featuresMu.Lock()
	defer s.featuresMu.Unlock()
	if fs.federated == nil {
		fs.federated = NewFederatedQueryRouter()
	}
	return fs.federated
}

// handleFeatureAction dispatches Flight DoAction calls for extended roadmap features.
func (s *VectorStore) handleFeatureAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	if action == nil {
		return false, nil
	}
	if handled, err := s.handleUsageAction(action, stream); handled {
		return true, err
	}
	if handled, err := s.handleStreamAction(action, stream); handled {
		return true, err
	}
	if handled, err := s.handleMultiModalAction(action, stream); handled {
		return true, err
	}
	if handled, err := s.handleRerankAction(action, stream); handled {
		return true, err
	}
	if handled, err := s.handleAutoScaleShardAction(action, stream); handled {
		return true, err
	}
	if handled, err := s.handleSchemaRegistryAction(action, stream); handled {
		return true, err
	}
	if handled, err := s.handleMaterializedViewAction(action, stream); handled {
		return true, err
	}
	if handled, err := s.handleSearchExtrasAction(action, stream); handled {
		return true, err
	}
	if handled, err := s.handleProvenanceAction(action, stream); handled {
		return true, err
	}
	if handled, err := s.handleCloudControlAction(action, stream); handled {
		return true, err
	}
	if handled, err := s.handleFederatedAction(action, stream); handled {
		return true, err
	}
	if handled, err := s.handleGraphQLAction(action, stream); handled {
		return true, err
	}
	return false, nil
}

// RegisterHTTPHandlers mounts feature HTTP API endpoints (REST v1, GraphQL,
// Cloud Control Plane, Usage Metering, and Edge Transport) onto the server's HTTP mux,
// wrapped with RBAC HTTP authentication when RBAC is enabled.
func (s *VectorStore) RegisterHTTPHandlers(mux *http.ServeMux) {
	if mux == nil {
		return
	}
	mux.Handle("/api/v1/health", s.RESTHealthHTTPHandler())
	mux.Handle("/api/v1/openapi.json", s.OpenAPIHandler())
	mux.Handle("/api/v1/graphql", s.httpAuthWrap(PermRead, s.GraphQLHTTPHandler()))

	mux.Handle("/api/v1/namespaces", s.httpMethodAuthWrap(PermRead, PermWrite, s.namespacesHTTPHandler()))
	mux.Handle("/api/v1/namespaces/", s.httpMethodAuthWrap(PermRead, PermWrite, s.namespaceItemHTTPHandler()))
	mux.Handle("/api/v1/datasets", s.httpMethodAuthWrap(PermRead, PermWrite, s.datasetsCollectionHTTPHandler()))
	mux.Handle("/api/v1/datasets/", s.httpAuthWrap(PermRead, s.datasetItemHTTPHandler()))

	mux.Handle("/api/v1/usage", s.httpAuthWrap(PermRead, s.GetUsageMeter().HTTPHandler()))
	mux.Handle("/api/v1/usage/export", s.httpAuthWrap(PermRead, s.GetUsageMeter().ExportHTTPHandler()))
	mux.Handle("/api/v1/admin/usage", s.httpAuthWrap(PermManageTenant, s.adminUsageHTTPHandler()))

	mux.Handle("/api/v1/search", s.httpAuthWrap(PermRead, s.EdgeSearchHTTPHandler()))
	mux.Handle("/api/v1/search/federated", s.httpAuthWrap(PermRead, s.federatedSearchHTTPHandler()))
	mux.Handle("/api/v1/federated/collections", s.httpMethodAuthWrap(PermRead, PermWrite, s.federatedCollectionsHTTPHandler()))
	mux.Handle("/api/v1/edge/sync", s.httpAuthWrap(PermWrite, s.EdgeSyncHTTPHandler()))

	mux.Handle("/api/v1/cloud/clusters", s.httpMethodAuthWrap(PermRead, PermManageTenant, s.cloudClustersCollectionHTTPHandler()))
	mux.Handle("/api/v1/cloud/clusters/", s.httpAuthWrap(PermRead, s.cloudClusterItemHTTPHandler()))
	mux.Handle("/api/v1/cloud/internal/replicate", s.httpAuthWrap(PermWrite, s.cloudInternalReplicateHTTPHandler()))
}
