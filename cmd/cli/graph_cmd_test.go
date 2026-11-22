package main

import (
	"encoding/json"
	"testing"
)

// TestGraphActionNames_AreKebabCase server dispatch (internal/store/store_actions.go for the data plane,
// store_meta_actions.go for the meta plane) and the RBAC allowlist
// (internal/store/rbac_actions.go) only recognise kebab-case action types for
// graph operations; anything else falls through to codes.Unimplemented (or,
// worse, RBAC's default PermAdmin). The Python SDK already sends kebab-case
// via the meta client. These tests pin the literals on the CLI side; the
// mirror assertions live in internal/store (TestGraphActionWiring_*).
func TestGraphActionNames_AreKebabCase(t *testing.T) {
	cases := map[string]string{
		"add-edge":           actionAddEdge,
		"traverse-graph":     actionTraverseGraph,
		"calculate-pagerank": actionCalculatePageRank,
		"detect-communities": actionDetectCommunities,
	}
	for want, got := range cases {
		if got != want {
			t.Errorf("action type = %q, want %q (server dispatch rejects anything else with Unimplemented)", got, want)
		}
	}
	// GetGraphStats is historically PascalCase on both planes; do not "fix" it.
	if actionGetGraphStats != "GetGraphStats" {
		t.Errorf("action type = %q, want %q", actionGetGraphStats, "GetGraphStats")
	}
}

// TestBuildAddEdgeAction runs the test build add edge action operation.
func TestBuildAddEdgeAction(t *testing.T) {
	a, err := buildAddEdgeAction("kb", 1, "knows", 2, 0.5)
	if err != nil {
		t.Fatalf("buildAddEdgeAction: %v", err)
	}
	if a.Type != "add-edge" {
		t.Fatalf("Type = %q, want add-edge", a.Type)
	}
	var body map[string]any
	if err := json.Unmarshal(a.Body, &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if body["dataset"] != "kb" || body["predicate"] != "knows" {
		t.Errorf("unexpected body: %v", body)
	}
	// Server parses subject/object as int64 and weight as float32.
	if _, ok := body["subject"].(float64); !ok {
		t.Errorf("subject must be numeric, got %T", body["subject"])
	}
	if _, ok := body["weight"].(float64); !ok {
		t.Errorf("weight must be numeric, got %T", body["weight"])
	}
}

// TestBuildTraverseAction runs the test build traverse action operation.
func TestBuildTraverseAction(t *testing.T) {
	a, err := buildTraverseAction("kb", 3, 4, true, true)
	if err != nil {
		t.Fatalf("buildTraverseAction: %v", err)
	}
	if a.Type != "traverse-graph" {
		t.Fatalf("Type = %q, want traverse-graph", a.Type)
	}
	var body struct {
		Dataset  string `json:"dataset"`
		Start    int    `json:"start"`
		MaxHops  int    `json:"max_hops"`
		Incoming bool   `json:"incoming"`
		Weighted bool   `json:"weighted"`
	}
	if err := json.Unmarshal(a.Body, &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	// The handler reads these exact keys; a mismatch silently falls back to
	// defaults (MaxHops=2, outgoing, unweighted).
	if body.Dataset != "kb" || body.Start != 3 || body.MaxHops != 4 || !body.Incoming || !body.Weighted {
		t.Errorf("unexpected body: %+v", body)
	}
}

// TestBuildPageRankAction runs the test build page rank action operation.
func TestBuildPageRankAction(t *testing.T) {
	a, err := buildPageRankAction("kb", 0.7, 42)
	if err != nil {
		t.Fatalf("buildPageRankAction: %v", err)
	}
	if a.Type != "calculate-pagerank" {
		t.Fatalf("Type = %q, want calculate-pagerank", a.Type)
	}
	var body struct {
		Dataset       string  `json:"dataset"`
		DampingFactor float64 `json:"damping_factor"`
		MaxIterations int     `json:"max_iterations"`
	}
	if err := json.Unmarshal(a.Body, &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if body.DampingFactor != 0.7 || body.MaxIterations != 42 {
		t.Errorf("unexpected body: %+v", body)
	}
}

// TestBuildDetectCommunitiesAction runs the test build detect communities action operation.
func TestBuildDetectCommunitiesAction(t *testing.T) {
	a, err := buildDetectCommunitiesAction("kb", 25)
	if err != nil {
		t.Fatalf("buildDetectCommunitiesAction: %v", err)
	}
	if a.Type != "detect-communities" {
		t.Fatalf("Type = %q, want detect-communities", a.Type)
	}
	var body struct {
		Dataset       string `json:"dataset"`
		MaxIterations int    `json:"max_iterations"`
	}
	if err := json.Unmarshal(a.Body, &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if body.Dataset != "kb" || body.MaxIterations != 25 {
		t.Errorf("unexpected body: %+v", body)
	}
}

// TestBuildGetGraphStatsAction runs the test build get graph stats action operation.
func TestBuildGetGraphStatsAction(t *testing.T) {
	a, err := buildGetGraphStatsAction("kb")
	if err != nil {
		t.Fatalf("buildGetGraphStatsAction: %v", err)
	}
	if a.Type != "GetGraphStats" {
		t.Fatalf("Type = %q, want GetGraphStats", a.Type)
	}
	var body map[string]string
	if err := json.Unmarshal(a.Body, &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if body["dataset"] != "kb" {
		t.Errorf("unexpected body: %v", body)
	}
}
