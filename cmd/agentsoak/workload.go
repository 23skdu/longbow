package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/23skdu/longbow/client"
	"github.com/apache/arrow-go/v18/arrow/flight"
)

func finiteWeight(w float64) bool {
	return !math.IsNaN(w) && !math.IsInf(w, 0)
}

// workloadOps is the canonical set of operations in the shared soak workload.
// Both Go and Python simulators use the same names in
// scripts/configs/multiagent_soak.json.
var workloadOps = []string{
	"dense", "hybrid", "sparse", "filtered", "range", "byid",
	"temporal_asof", "temporal_range", "temporal_window",
	"geo_radius", "geo_box", "geo_hybrid",
	"graphrag", "traverse", "recommend",
	"multimodal", "federated", "text",
	"insert", "upsert", "delete", "delete_ns",
	"pagerank", "communities", "get_vector", "graph_stats",
	"cross_read",
}

func knownWorkloadOp(op string) bool {
	for _, candidate := range workloadOps {
		if candidate == op {
			return true
		}
	}
	return false
}

// weightedSelector maps operation names to a cumulative distribution. Keys are
// sorted so a seed yields the same mix across Go runs and across processes.
type weightedSelector struct {
	ops   []string
	cum   []float64
	total float64
}

func newWeightedSelector(weights map[string]float64) (*weightedSelector, error) {
	if len(weights) == 0 {
		return nil, fmt.Errorf("empty workload")
	}
	names := make([]string, 0, len(weights))
	total := 0.0
	for op, weight := range weights {
		if !knownWorkloadOp(op) {
			return nil, fmt.Errorf("unknown workload op %q", op)
		}
		if !finiteWeight(weight) || weight < 0 {
			return nil, fmt.Errorf("invalid weight for %q", op)
		}
		if weight == 0 {
			continue
		}
		names = append(names, op)
		total += weight
	}
	if total <= 0 {
		return nil, fmt.Errorf("workload weights must sum to a positive value")
	}
	sort.Strings(names)
	cum := make([]float64, len(names))
	running := 0.0
	for i, name := range names {
		running += weights[name]
		cum[i] = running
	}
	return &weightedSelector{ops: names, cum: cum, total: total}, nil
}

// pick draws an operation from the weighted distribution.
func (w *weightedSelector) pick(rng *rand.Rand) string {
	if w == nil || len(w.ops) == 0 {
		return "dense"
	}
	x := rng.Float64() * w.total
	for i, cutoff := range w.cum {
		if x < cutoff {
			return w.ops[i]
		}
	}
	return w.ops[len(w.ops)-1]
}

// parseWorkloadOverride parses a comma-separated op=weight override, e.g.
// -workload dense=25,hybrid=10.
func parseWorkloadOverride(s string) (map[string]float64, error) {
	out := make(map[string]float64)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid workload entry %q (want op=weight)", part)
		}
		op := strings.TrimSpace(kv[0])
		if !knownWorkloadOp(op) {
			return nil, fmt.Errorf("unknown workload operation %q", op)
		}
		weight, err := strconv.ParseFloat(strings.TrimSpace(kv[1]), 64)
		if err != nil || !finiteWeight(weight) || weight < 0 {
			return nil, fmt.Errorf("invalid weight for %q", op)
		}
		out[op] = weight
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty workload override")
	}
	return out, nil
}

func doActionDrain(ctx context.Context, c *client.SmartClient, actionType string, req any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	stream, err := c.DoAction(c.WithAuthContext(ctx), &flight.Action{Type: actionType, Body: body})
	if err != nil {
		return err
	}
	defer stream.CloseSend() // #nosec G104
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func doSearchWith(ctx context.Context, c *client.SmartClient, dataset string, vec []float32, k int, extra map[string]any) error {
	req := map[string]any{"dataset": dataset, "vector": vec, "k": k}
	for key, value := range extra {
		req[key] = value
	}
	return doActionDrain(ctx, c, "VectorSearch", req)
}

func doByID(ctx context.Context, c *client.SmartClient, dataset, id string, k int) error {
	return doActionDrain(ctx, c, "VectorSearchByID", map[string]any{"dataset": dataset, "id": id, "k": k})
}

func doRangeSearch(ctx context.Context, c *client.SmartClient, dataset string, vec []float32, k int) error {
	return doActionDrain(ctx, c, "RangeSearch", map[string]any{
		"dataset": dataset, "vector": vec, "threshold": 0.85, "max_results": k,
	})
}

// temporalSearchType maps workload operation names to server search types.
func temporalSearchType(op string) string {
	switch op {
	case "temporal_asof":
		return "as_of"
	case "temporal_range":
		return "range"
	case "temporal_window":
		return "sliding_window"
	default:
		return op
	}
}

func doTemporalSearch(ctx context.Context, c *client.SmartClient, dataset, searchType string, windowSize int, vec []float32, k int) error {
	req := map[string]any{"dataset": dataset, "search_type": searchType, "k": k}
	switch searchType {
	case "as_of":
		req["timestamp"] = time.Now().UnixNano()
	case "range":
		now := time.Now().UnixNano()
		req["start_time"] = now - int64(24*time.Hour)
		req["end_time"] = now
	default:
		req["window_size"] = windowSize
		req["vector"] = vec
	}
	return doActionDrain(ctx, c, "TemporalSearch", req)
}

func doGeoSearch(ctx context.Context, c *client.SmartClient, dataset, searchType string, opt soakGeoOptions, vec []float32, k int) error {
	req := map[string]any{
		"dataset": dataset, "search_type": searchType, "k": k,
		"center": map[string]any{"lat": opt.Lat, "lon": opt.Lon},
	}
	switch searchType {
	case "radius":
		req["radius_km"] = opt.RadiusKM
	case "box":
		req["box"] = map[string]any{
			"min_lat": opt.Lat - 0.1, "max_lat": opt.Lat + 0.1,
			"min_lon": opt.Lon - 0.1, "max_lon": opt.Lon + 0.1,
		}
	default:
		req["radius_km"] = opt.RadiusKM
		req["query_vector"] = vec
	}
	return doActionDrain(ctx, c, "GeoSearch", req)
}

func doGraphSearch(ctx context.Context, c *client.SmartClient, dataset, action string, opt soakGraphOptions, nodeID uint32) error {
	switch action {
	case "graphrag":
		return doActionDrain(ctx, c, "GraphRAGExpand", map[string]any{
			"dataset": dataset, "node_ids": []uint32{nodeID},
		})
	case "traverse":
		return doActionDrain(ctx, c, "traverse-graph", map[string]any{
			"dataset": dataset, "start": nodeID, "max_hops": opt.MaxHops,
			"incoming": false, "weighted": true, "decay": 0.5,
		})
	case "pagerank":
		return doActionDrain(ctx, c, "calculate-pagerank", map[string]any{
			"dataset": dataset, "damping_factor": 0.85,
			"max_iterations": 20, "tolerance": 0.0001,
		})
	case "communities":
		return doActionDrain(ctx, c, "detect-communities", map[string]any{
			"dataset": dataset, "max_iterations": 10,
		})
	case "graph_stats":
		return doActionDrain(ctx, c, "GetGraphStats", map[string]any{"dataset": dataset})
	default:
		return fmt.Errorf("unsupported graph op %q", action)
	}
}

func doRecommend(ctx context.Context, c *client.SmartClient, dataset, id string, k int, alpha float64, hops int) error {
	return doActionDrain(ctx, c, "Recommend", map[string]any{
		"dataset": dataset, "seed_ids": []string{id},
		"k": k, "alpha": alpha, "max_hops": hops, "decay": 0.5,
	})
}

func doTextSearch(ctx context.Context, c *client.SmartClient, dataset string, k int) error {
	return doActionDrain(ctx, c, "AdvancedTextSearch", map[string]any{
		"dataset": dataset, "query": "memory AND agent", "k": k,
		"fuzzy_max_dist": 1, "snippet_chars": 160,
	})
}

func doMultiModal(ctx context.Context, c *client.SmartClient, dataset string, k int) error {
	return doActionDrain(ctx, c, "MultiModalSearch", map[string]any{
		"dataset": dataset, "modality": "text", "text_query": "memory", "k": k,
	})
}

func doFederated(ctx context.Context, c *client.SmartClient, collections, tags []string, vec []float32, k, rrfk int) error {
	req := map[string]any{"collections": collections, "tags": tags, "vector": vec, "k": k, "rrf_k": rrfk}
	return doActionDrain(ctx, c, "FederatedSearch", req)
}
