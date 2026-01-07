package store

import "testing"

// TestGraphStore_TraverseIncoming tests traversing incoming edges
func TestGraphStore_TraverseIncoming(t *testing.T) {
	gs := NewGraphStore()

	// Connect: PaperA(2) -> cites -> PaperB(1)
	// Connect: PaperC(3) -> cites -> PaperB(1)
	_ = gs.AddEdge(Edge{Subject: VectorID(2), Predicate: "cites", Object: VectorID(1), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(3), Predicate: "cites", Object: VectorID(1), Weight: 1.0})

	// Traverse Incoming from PaperB(1)
	opts := DefaultTraverseOptions()
	opts.MaxHops = 1
	opts.Direction = DirectionIncoming
	paths := gs.Traverse(VectorID(1), opts)

	if len(paths) != 2 {
		t.Errorf("expected 2 incoming paths, got %d", len(paths))
	}

	found2 := false
	found3 := false
	for _, p := range paths {
		if len(p.Nodes) > 1 {
			switch p.Nodes[1] {
			case 2:
				found2 = true
			case 3:
				found3 = true
			}
		}
	}

	if !found2 || !found3 {
		t.Errorf("expected to find nodes 2 and 3 via incoming support traversal")
	}
}

// TestGraphStore_TraverseBidirectional tests traversing both directions
func TestGraphStore_TraverseBidirectional(t *testing.T) {
	gs := NewGraphStore()

	// Center(1) -> Out(2)
	// In(3) -> Center(1)
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "out", Object: VectorID(2), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(3), Predicate: "in", Object: VectorID(1), Weight: 1.0})

	// Traverse Both from Center(1)
	opts := DefaultTraverseOptions()
	opts.MaxHops = 1
	opts.Direction = DirectionBoth
	paths := gs.Traverse(VectorID(1), opts)

	if len(paths) != 2 {
		t.Errorf("expected 2 bidirectional paths, got %d", len(paths))
	}

	foundOut := false
	foundIn := false
	for _, p := range paths {
		if len(p.Nodes) > 1 {
			switch p.Nodes[1] {
			case 2:
				foundOut = true
			case 3:
				foundIn = true
			}
		}
	}

	if !foundOut {
		t.Errorf("failed to traverse outgoing edge to 2")
	}
	if !foundIn {
		t.Errorf("failed to traverse incoming edge from 3")
	}
}

// TestGraphStore_RankWithGraph verifies the re-ranking logic
func TestGraphStore_RankWithGraph(t *testing.T) {
	gs := NewGraphStore()

	// Graph:
	// 1 <-> 2 (Strong connection)
	// 3 (Isolated)
	//
	// Initial Search Results: [3 (0.9), 1 (0.8)]
	//
	// If we re-rank with graph starting from {1, 3}, node 2 should appear.
	// Also if 1 is connected to many things, it might get boosted?
	//
	// Let's test "Boosting":
	// Result: A(10), B(20). Score A > B.
	// Graph: B -> C, B -> D, B -> E. A is isolated.
	// Spreading activation from {A, B} might boost neighbors of B.
	// But RankWithGraph reranks the *Return Set*?
	// The implementation returns "expanded set" (deduplicated).

	_ = gs.AddEdge(Edge{Subject: VectorID(10), Predicate: "related", Object: VectorID(11), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(11), Predicate: "related", Object: VectorID(10), Weight: 1.0})

	initial := []SearchResult{
		{ID: 10, Score: 0.5}, // Less relevance initially
		{ID: 20, Score: 0.9}, // High relevance, isolated
	}

	// 10 is connected to 11. 20 is isolated.
	// RankWithGraph with Alpha=0.5
	reranked := gs.RankWithGraph(initial, 0.5, 1)

	// We expect 11 to appear in results because of connection to 10.
	found11 := false
	for _, res := range reranked {
		if res.ID == 11 {
			found11 = true
		}
	}
	if !found11 {
		t.Errorf("expected neighbor 11 to be included in reranked results")
	}
}
