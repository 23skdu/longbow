package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/23skdu/longbow/internal/core"
)

// VectorProvenance tracks lineage, embedding model/version, source URI, and timestamps per vector.
type VectorProvenance struct {
	Dataset      string         `json:"dataset"`
	VectorID     string         `json:"vector_id"`
	SourceURI    string         `json:"source_uri,omitempty"`
	Model        string         `json:"model,omitempty"`
	ModelVersion string         `json:"model_version,omitempty"`
	ChunkIndex   int            `json:"chunk_index,omitempty"`
	ContentHash  string         `json:"content_hash,omitempty"`
	TenantID     string         `json:"tenant_id,omitempty"`
	Lineage      []string       `json:"lineage,omitempty"`
	IngestedAt   time.Time      `json:"ingested_at"`
	Extra        map[string]any `json:"extra,omitempty"`
}

// ScoreBreakdown decomposes dense, sparse/BM25, graph expansion, RRF, and rerank score contributions.
type ScoreBreakdown struct {
	DenseScore  float64            `json:"dense_score"`
	SparseScore float64            `json:"sparse_score"`
	GraphScore  float64            `json:"graph_score"`
	RRFScore    float64            `json:"rrf_score"`
	RerankScore float64            `json:"rerank_score"`
	FinalScore  float64            `json:"final_score"`
	Weights     map[string]float64 `json:"weights,omitempty"`
}

// TraversalHop captures one HNSW graph navigation hop during DebugSearchPath / ExplainSearch.
type TraversalHop struct {
	Step            int      `json:"step"`
	NodeID          uint32   `json:"node_id"`
	NeighborCount   int      `json:"neighbor_count"`
	SampleNeighbors []uint32 `json:"sample_neighbors,omitempty"`
	EstimatedDist   float32  `json:"estimated_dist,omitempty"`
	ImprovedBest    bool     `json:"improved_best"`
}

// ExplainedCandidate augments a ranked result with score components and tracked provenance.
type ExplainedCandidate struct {
	ID         core.VectorID     `json:"id"`
	UserID     string            `json:"user_id,omitempty"`
	Distance   float32           `json:"distance"`
	Breakdown  ScoreBreakdown    `json:"breakdown"`
	Provenance *VectorProvenance `json:"provenance,omitempty"`
}

// SearchExplanation describes full ranking transparency, graph traversal trace, and provenance.
type SearchExplanation struct {
	Dataset    string               `json:"dataset"`
	QueryK     int                  `json:"query_k"`
	Alpha      float64              `json:"alpha"`
	GraphAlpha float64              `json:"graph_alpha"`
	IndexType  string               `json:"index_type"`
	EntryPoint uint32               `json:"entry_point"`
	TotalHops  int                  `json:"total_hops"`
	ElapsedMs  float64              `json:"elapsed_ms"`
	Hops       []TraversalHop       `json:"hops"`
	Candidates []ExplainedCandidate `json:"candidates"`
}

// ExplainSearchRequest configures ExplainSearch and DebugSearchPath Flight actions.
type ExplainSearchRequest struct {
	Dataset    string    `json:"dataset"`
	Vector     []float32 `json:"vector"`
	TextQuery  string    `json:"text_query,omitempty"`
	K          int       `json:"k"`
	Alpha      float64   `json:"alpha"`
	GraphAlpha float64   `json:"graph_alpha,omitempty"`
	MaxHops    int       `json:"max_hops,omitempty"`
}

// ProvenanceManager maintains vector lineage records and produces search explanations.
type ProvenanceManager struct {
	mu      sync.RWMutex
	store   *VectorStore
	records map[string]map[string]VectorProvenance // dataset -> vector_id -> provenance
}

// NewProvenanceManager creates a vector provenance & search explainability manager.
func NewProvenanceManager(s *VectorStore) *ProvenanceManager {
	return &ProvenanceManager{
		store:   s,
		records: make(map[string]map[string]VectorProvenance),
	}
}

// Record stores or updates provenance metadata for a vector, computing a content hash if absent.
func (pm *ProvenanceManager) Record(prov VectorProvenance) error {
	if prov.Dataset == "" || prov.VectorID == "" {
		return errors.New("dataset and vector_id are required")
	}
	if prov.IngestedAt.IsZero() {
		prov.IngestedAt = time.Now().UTC()
	}
	if prov.ContentHash == "" && prov.SourceURI != "" {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d:%s", prov.SourceURI, prov.Model, prov.ChunkIndex, prov.ModelVersion)))
		prov.ContentHash = hex.EncodeToString(h[:16])
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.records[prov.Dataset] == nil {
		pm.records[prov.Dataset] = make(map[string]VectorProvenance)
	}
	pm.records[prov.Dataset][prov.VectorID] = prov
	return nil
}

// Get retrieves provenance metadata for one vector.
func (pm *ProvenanceManager) Get(dataset, vectorID string) (VectorProvenance, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	dsMap := pm.records[dataset]
	if dsMap == nil {
		return VectorProvenance{}, false
	}
	p, ok := dsMap[vectorID]
	return p, ok
}

// ListByDataset returns tracked provenance entries for a dataset sorted by vector_id.
func (pm *ProvenanceManager) ListByDataset(dataset string, limit int) []VectorProvenance {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	dsMap := pm.records[dataset]
	if len(dsMap) == 0 {
		return nil
	}
	out := make([]VectorProvenance, 0, len(dsMap))
	for _, p := range dsMap {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VectorID < out[j].VectorID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// ExplainSearch runs dense + optional sparse/BM25 search alongside an HNSW graph traversal trace
// and returns per-candidate score breakdowns + provenance.
func (pm *ProvenanceManager) ExplainSearch(ctx context.Context, req ExplainSearchRequest) (*SearchExplanation, error) {
	start := time.Now()
	if req.Dataset == "" {
		return nil, errors.New("dataset is required")
	}
	if len(req.Vector) == 0 && req.TextQuery == "" {
		return nil, errors.New("provide query vector or text_query")
	}
	if req.K <= 0 {
		req.K = 10
	}
	if req.Alpha == 0 && req.TextQuery == "" {
		req.Alpha = 1.0
	} else if req.Alpha == 0 {
		req.Alpha = 0.7
	}
	if req.MaxHops <= 0 {
		req.MaxHops = 16
	}

	var indexType string
	var entryPoint uint32
	var hops []TraversalHop
	var denseRes []SearchResult
	var userIDs map[core.VectorID]string

	if pm.store != nil {
		ds, ok := pm.store.getDataset(req.Dataset)
		if !ok {
			return nil, fmt.Errorf("dataset %q not found", req.Dataset)
		}
		ds.dataMu.RLock()
		if ds.Index != nil {
			indexType = ds.Index.GetIndexType()
			entryPoint = ds.Index.GetEntryPoint()
			if len(req.Vector) > 0 {
				denseRes, _ = ds.Index.SearchVectors(ctx, req.Vector, req.K, nil, SearchOptions{})
				hops = traceIndexHops(ds.Index, entryPoint, denseRes, req.MaxHops)
			}
			userIDs = make(map[core.VectorID]string, len(denseRes))
			for _, r := range denseRes {
				if len(r.Metadata) > 0 {
					if m, _ := core.DecodeMetadata(r.Metadata); m != nil {
						if uid, ok := m["id"].(string); ok {
							userIDs[r.ID] = uid
						}
					}
				}
			}
		}
		ds.dataMu.RUnlock()
	}

	// Sparse BM25 contribution if TextQuery specified
	sparseScores := make(map[core.VectorID]float64)
	if req.TextQuery != "" && pm.store != nil {
		if textResp, err := pm.store.GetTextSearchManager().Search(ctx, AdvancedTextQueryRequest{
			Dataset: req.Dataset,
			Query:   req.TextQuery,
			K:       req.K * 2,
		}); err == nil {
			for _, hit := range textResp.Hits {
				sparseScores[core.VectorID(hit.VectorID)] = hit.Score
			}
		}
	}

	// Build candidate breakdowns and attach provenance
	denseRank := make(map[core.VectorID]int, len(denseRes))
	for i, r := range denseRes {
		denseRank[r.ID] = i + 1
	}

	candidates := make([]ExplainedCandidate, 0, len(denseRes))
	for _, r := range denseRes {
		denseS := 1.0 / (1.0 + float64(r.Distance))
		sparseS := sparseScores[r.ID]
		rrf := req.Alpha*(1.0/float64(60+denseRank[r.ID])) + (1.0-req.Alpha)*sparseS
		graphS := 0.0
		if req.GraphAlpha > 0 {
			graphS = req.GraphAlpha * denseS
		}
		finalS := req.Alpha*denseS + (1.0-req.Alpha)*sparseS + graphS

		uid := userIDs[r.ID]
		if uid == "" {
			uid = fmt.Sprintf("%d", r.ID)
		}
		var provCopy *VectorProvenance
		if p, ok := pm.Get(req.Dataset, uid); ok {
			cp := p
			provCopy = &cp
		} else if p, ok := pm.Get(req.Dataset, fmt.Sprintf("%d", r.ID)); ok {
			cp := p
			provCopy = &cp
		}

		candidates = append(candidates, ExplainedCandidate{
			ID:       r.ID,
			UserID:   uid,
			Distance: r.Distance,
			Breakdown: ScoreBreakdown{
				DenseScore:  denseS,
				SparseScore: sparseS,
				GraphScore:  graphS,
				RRFScore:    rrf,
				FinalScore:  finalS,
				Weights: map[string]float64{
					"dense":  req.Alpha,
					"sparse": 1.0 - req.Alpha,
					"graph":  req.GraphAlpha,
				},
			},
			Provenance: provCopy,
		})
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].Breakdown.FinalScore > candidates[j].Breakdown.FinalScore
	})
	if len(candidates) > req.K {
		candidates = candidates[:req.K]
	}

	return &SearchExplanation{
		Dataset:    req.Dataset,
		QueryK:     req.K,
		Alpha:      req.Alpha,
		GraphAlpha: req.GraphAlpha,
		IndexType:  indexType,
		EntryPoint: entryPoint,
		TotalHops:  len(hops),
		ElapsedMs:  float64(time.Since(start).Microseconds()) / 1000.0,
		Hops:       hops,
		Candidates: candidates,
	}, nil
}

func traceIndexHops(idx VectorIndex, entryPoint uint32, targets []SearchResult, maxHops int) []TraversalHop {
	if idx == nil || idx.Len() == 0 {
		return nil
	}
	targetDists := make(map[uint32]float32, len(targets))
	for _, t := range targets {
		targetDists[uint32(t.ID)] = t.Distance
	}

	visited := make(map[uint32]bool)
	hops := make([]TraversalHop, 0, maxHops)
	cur := entryPoint
	bestDist := float32(1e9)

	for step := 0; step < maxHops; step++ {
		if visited[cur] {
			break
		}
		visited[cur] = true
		nbs, err := idx.GetRawNeighbors(cur)
		if err != nil {
			break
		}
		sample := nbs
		if len(sample) > 8 {
			sample = sample[:8]
		}
		curDist := float32(1.0)
		if d, ok := targetDists[cur]; ok {
			curDist = d
		}
		improved := curDist < bestDist
		if improved {
			bestDist = curDist
		}
		hops = append(hops, TraversalHop{
			Step:            step,
			NodeID:          cur,
			NeighborCount:   len(nbs),
			SampleNeighbors: sample,
			EstimatedDist:   curDist,
			ImprovedBest:    improved,
		})
		// Advance toward any unvisited neighbor (preferring target hit if reachable)
		nextNode := uint32(0)
		foundNext := false
		for _, nb := range nbs {
			if visited[nb] {
				continue
			}
			if _, isTarget := targetDists[nb]; isTarget {
				nextNode = nb
				foundNext = true
				break
			}
			if !foundNext {
				nextNode = nb
				foundNext = true
			}
		}
		if !foundNext {
			break
		}
		cur = nextNode
	}
	return hops
}

func (s *VectorStore) handleProvenanceAction(action *flight.Action, stream flight.FlightService_DoActionServer) (bool, error) {
	switch action.Type {
	case "RecordProvenance":
		var prov VectorProvenance
		if err := json.Unmarshal(action.Body, &prov); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		if err := s.GetProvenanceManager().Record(prov); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		return true, stream.Send(&flight.Result{Body: []byte(`{"status":"recorded"}`)})

	case "GetProvenance":
		var req struct {
			Dataset  string `json:"dataset"`
			VectorID string `json:"vector_id"`
			Limit    int    `json:"limit"`
		}
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		if req.VectorID != "" {
			prov, ok := s.GetProvenanceManager().Get(req.Dataset, req.VectorID)
			if !ok {
				return true, status.Errorf(codes.NotFound, "provenance not found for %s/%s", req.Dataset, req.VectorID)
			}
			body, _ := json.Marshal(prov)
			return true, stream.Send(&flight.Result{Body: body})
		}
		list := s.GetProvenanceManager().ListByDataset(req.Dataset, req.Limit)
		body, _ := json.Marshal(map[string]any{"provenance": list})
		return true, stream.Send(&flight.Result{Body: body})

	case "ExplainSearch", "DebugSearchPath":
		var req ExplainSearchRequest
		if err := json.Unmarshal(action.Body, &req); err != nil {
			return true, status.Errorf(codes.InvalidArgument, "invalid json: %v", err)
		}
		exp, err := s.GetProvenanceManager().ExplainSearch(stream.Context(), req)
		if err != nil {
			return true, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		body, _ := json.Marshal(exp)
		return true, stream.Send(&flight.Result{Body: body})
	}
	return false, nil
}
