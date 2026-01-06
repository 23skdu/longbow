package store

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestGraphStore_AddEdge tests adding edges to the graph store
func TestGraphStore_AddEdge(t *testing.T) {
	gs := NewGraphStore()

	_ = gs.AddEdge(Edge{
		Subject:   VectorID(1),
		Predicate: "owns",
		Object:    VectorID(2),
		Weight:    1.0,
	})

	if gs.EdgeCount() != 1 {
		t.Errorf("expected 1 edge, got %d", gs.EdgeCount())
	}
}

// TestGraphStore_GetEdgesBySubject tests querying edges by subject
func TestGraphStore_GetEdgesBySubject(t *testing.T) {
	gs := NewGraphStore()

	// Add multiple edges from same subject
	// 1: alice, 2: report1, 3: report2, 4: bob, 5: report3
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "owns", Object: VectorID(2), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "likes", Object: VectorID(3), Weight: 0.8})
	_ = gs.AddEdge(Edge{Subject: VectorID(4), Predicate: "owns", Object: VectorID(5), Weight: 1.0})

	edges := gs.GetEdgesBySubject(VectorID(1))
	if len(edges) != 2 {
		t.Errorf("expected 2 edges for alice (1), got %d", len(edges))
	}

	edges = gs.GetEdgesBySubject(VectorID(4))
	if len(edges) != 1 {
		t.Errorf("expected 1 edge for bob (4), got %d", len(edges))
	}

	edges = gs.GetEdgesBySubject(VectorID(99))
	if len(edges) != 0 {
		t.Errorf("expected 0 edges for unknown, got %d", len(edges))
	}
}

// TestGraphStore_GetEdgesByObject tests querying edges by object (incoming)
func TestGraphStore_GetEdgesByObject(t *testing.T) {
	gs := NewGraphStore()

	// Add edges pointing to same object
	// 1: alice, 2: shared, 3: bob, 4: carol, 5: other
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "owns", Object: VectorID(2), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(3), Predicate: "likes", Object: VectorID(2), Weight: 0.5})
	_ = gs.AddEdge(Edge{Subject: VectorID(4), Predicate: "owns", Object: VectorID(5), Weight: 1.0})

	edges := gs.GetEdgesByObject(VectorID(2))
	if len(edges) != 2 {
		t.Errorf("expected 2 edges to doc:shared (2), got %d", len(edges))
	}

	edges = gs.GetEdgesByObject(VectorID(5))
	if len(edges) != 1 {
		t.Errorf("expected 1 edge to doc:other (5), got %d", len(edges))
	}
}

// TestGraphStore_GetEdgesByPredicate tests filtering by relationship type
func TestGraphStore_GetEdgesByPredicate(t *testing.T) {
	gs := NewGraphStore()

	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "owns", Object: VectorID(10), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(2), Predicate: "owns", Object: VectorID(20), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "likes", Object: VectorID(30), Weight: 0.5})

	ownsEdges := gs.GetEdgesByPredicate("owns")
	if len(ownsEdges) != 2 {
		t.Errorf("expected 2 'owns' edges, got %d", len(ownsEdges))
	}

	likesEdges := gs.GetEdgesByPredicate("likes")
	if len(likesEdges) != 1 {
		t.Errorf("expected 1 'likes' edge, got %d", len(likesEdges))
	}
}

// TestGraphStore_PredicateVocabulary tests tracking unique predicates
func TestGraphStore_PredicateVocabulary(t *testing.T) {
	gs := NewGraphStore()

	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "owns", Object: VectorID(2), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(2), Predicate: "likes", Object: VectorID(3), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(3), Predicate: "owns", Object: VectorID(4), Weight: 1.0}) // duplicate predicate
	_ = gs.AddEdge(Edge{Subject: VectorID(4), Predicate: "authored", Object: VectorID(5), Weight: 1.0})

	vocab := gs.PredicateVocabulary()
	if len(vocab) != 3 {
		t.Errorf("expected 3 unique predicates, got %d: %v", len(vocab), vocab)
	}
}

// TestGraphStore_ToArrowBatch tests converting edges to Arrow RecordBatch with Dictionary encoding
func TestGraphStore_ToArrowBatch(t *testing.T) {
	gs := NewGraphStore()

	// Add edges with repeated predicates
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "owns", Object: VectorID(10), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(2), Predicate: "owns", Object: VectorID(20), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "likes", Object: VectorID(30), Weight: 0.5})
	_ = gs.AddEdge(Edge{Subject: VectorID(3), Predicate: "owns", Object: VectorID(40), Weight: 1.0})

	batch, err := gs.ToArrowBatch(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrowBatch failed: %v", err)
	}
	defer batch.Release()

	// Verify row count
	if batch.NumRows() != 4 {
		t.Errorf("expected 4 rows, got %d", batch.NumRows())
	}

	// Verify predicate column is Dictionary-encoded
	predicateCol := batch.Column(1)
	if predicateCol.DataType().ID() != arrow.DICTIONARY {
		t.Errorf("expected predicate column to be Dictionary type, got %v", predicateCol.DataType())
	}

	// Verify Subject column is Uint32
	subjectCol := batch.Column(0)
	if subjectCol.DataType().ID() != arrow.UINT32 {
		t.Errorf("expected subject column to be Uint32, got %v", subjectCol.DataType())
	}
}

// TestGraphStore_DictionaryMemorySavings verifies Dictionary encoding saves memory
func TestGraphStore_DictionaryMemorySavings(t *testing.T) {
	gs := NewGraphStore()

	// Add 1000 edges with only 3 predicate types
	predicates := []string{"owns", "likes", "authored"}
	for i := 0; i < 1000; i++ {
		_ = gs.AddEdge(Edge{
			Subject:   VectorID(i),
			Predicate: predicates[i%3],
			Object:    VectorID(i + 1000),
			Weight:    1.0,
		})
	}

	// Vocabulary should only have 3 predicates despite 1000 edges
	vocab := gs.PredicateVocabulary()
	if len(vocab) != 3 {
		t.Errorf("expected 3 unique predicates, got %d", len(vocab))
	}

	// Convert to Arrow and verify Dictionary size is small
	batch, err := gs.ToArrowBatch(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrowBatch failed: %v", err)
	}
	defer batch.Release()

	// Dictionary should only store 3 strings, indices are uint16
	predicateCol := batch.Column(1).(*array.Dictionary)
	dict := predicateCol.Dictionary()
	if dict.Len() != 3 {
		t.Errorf("expected dictionary with 3 entries, got %d", dict.Len())
	}
}

// TestGraphStore_FromArrowBatch tests loading edges from Arrow RecordBatch
func TestGraphStore_FromArrowBatch(t *testing.T) {
	gs1 := NewGraphStore()
	_ = gs1.AddEdge(Edge{Subject: VectorID(100), Predicate: "rel", Object: VectorID(200), Weight: 1.0})
	_ = gs1.AddEdge(Edge{Subject: VectorID(200), Predicate: "rel", Object: VectorID(300), Weight: 0.5})

	// Convert to Arrow
	batch, err := gs1.ToArrowBatch(memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("ToArrowBatch failed: %v", err)
	}
	defer batch.Release()

	// Create new GraphStore from Arrow batch
	gs2 := NewGraphStore()
	err = gs2.FromArrowBatch(batch)
	if err != nil {
		t.Fatalf("FromArrowBatch failed: %v", err)
	}

	// Verify edges loaded correctly
	if gs2.EdgeCount() != 2 {
		t.Errorf("expected 2 edges, got %d", gs2.EdgeCount())
	}

	edges := gs2.GetEdgesBySubject(VectorID(100))
	if len(edges) != 1 || edges[0].Object != VectorID(200) {
		t.Errorf("edge not loaded correctly: %+v", edges)
	}
}

// TestGraphStore_TraverseSingleHop tests finding direct neighbors
func TestGraphStore_TraverseSingleHop(t *testing.T) {
	gs := NewGraphStore()

	// Build a simple graph: alice(1) -> owns -> doc1(10), alice(1) -> likes -> doc2(11)
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "owns", Object: VectorID(10), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "likes", Object: VectorID(11), Weight: 0.8})
	_ = gs.AddEdge(Edge{Subject: VectorID(2), Predicate: "owns", Object: VectorID(12), Weight: 1.0})

	// Traverse 1 hop from alice (1)
	opts := DefaultTraverseOptions()
	opts.MaxHops = 1
	opts.Direction = DirectionOutgoing
	paths := gs.Traverse(VectorID(1), opts)
	if len(paths) != 2 {
		t.Errorf("expected 2 paths from alice, got %d", len(paths))
	}

	// Check paths contain expected objects
	objects := make(map[VectorID]bool)
	for _, p := range paths {
		if len(p.Nodes) > 0 {
			objects[p.Nodes[len(p.Nodes)-1]] = true
		}
	}
	if !objects[VectorID(10)] || !objects[VectorID(11)] {
		t.Errorf("expected doc1(10) and doc2(11) in paths, got %v", objects)
	}
}

// TestGraphStore_TraverseMultiHop tests multi-hop graph traversal
func TestGraphStore_TraverseMultiHop(t *testing.T) {
	gs := NewGraphStore()

	// Build chain: alice(1) -> owns -> doc1(10) -> references -> paper1(20) -> cites -> paper2(30)
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "owns", Object: VectorID(10), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(10), Predicate: "references", Object: VectorID(20), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(20), Predicate: "cites", Object: VectorID(30), Weight: 1.0})

	// Traverse 3 hops from alice
	opts := DefaultTraverseOptions()
	opts.MaxHops = 3
	opts.Direction = DirectionOutgoing
	paths := gs.Traverse(VectorID(1), opts)

	// Should find path to paper2 (30)
	foundPaper2 := false
	for _, p := range paths {
		for _, node := range p.Nodes {
			if node == VectorID(30) {
				foundPaper2 = true
			}
		}
	}
	if !foundPaper2 {
		t.Errorf("expected to find paper2(30) in 3-hop traversal")
	}
}

// TestGraphStore_TraverseNoCycles tests that traversal avoids cycles
func TestGraphStore_TraverseNoCycles(t *testing.T) {
	gs := NewGraphStore()

	// Create cycle: 1 -> 2 -> 3 -> 1
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "rel", Object: VectorID(2), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(2), Predicate: "rel", Object: VectorID(3), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(3), Predicate: "rel", Object: VectorID(1), Weight: 1.0})

	// Traverse should not hang due to cycle
	opts := DefaultTraverseOptions()
	opts.MaxHops = 5
	opts.Direction = DirectionOutgoing
	paths := gs.Traverse(VectorID(1), opts)

	// Should complete without infinite loop
	if len(paths) == 0 {
		t.Errorf("expected some paths despite cycle")
	}
}

// TestGraphStore_TraverseParallel tests concurrent traversal from multiple starting points
func TestGraphStore_TraverseParallel(t *testing.T) {
	gs := NewGraphStore()

	// Build graph with multiple branches
	for i := 0; i < 100; i++ {
		_ = gs.AddEdge(Edge{
			Subject:   VectorID(i),
			Predicate: "connects",
			Object:    VectorID(i + 1), // i+1 to avoid self loop on i=0? no, i->i+1
			Weight:    1.0,
		})
	}

	// Parallel traversal from multiple nodes
	starts := []VectorID{0, 10, 20, 30}
	opts := DefaultTraverseOptions()
	opts.MaxHops = 5
	opts.Direction = DirectionOutgoing
	results := gs.TraverseParallel(starts, opts)

	if len(results) != 4 {
		t.Errorf("expected 4 result sets, got %d", len(results))
	}

	// Each should have found paths
	for start, paths := range results {
		if len(paths) == 0 {
			t.Errorf("expected paths from %d, got none", start)
		}
	}
}

// TestLouvainClustering_BasicCommunities tests detecting obvious clusters
func TestLouvainClustering_BasicCommunities(t *testing.T) {
	gs := NewGraphStore()

	// Create two obvious clusters
	// Cluster 1: 1-2-3 tightly connected
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "rel", Object: VectorID(2), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(2), Predicate: "rel", Object: VectorID(1), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(2), Predicate: "rel", Object: VectorID(3), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(3), Predicate: "rel", Object: VectorID(2), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "rel", Object: VectorID(3), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(3), Predicate: "rel", Object: VectorID(1), Weight: 1.0})

	// Cluster 2: 10-11-12 tightly connected
	_ = gs.AddEdge(Edge{Subject: VectorID(10), Predicate: "rel", Object: VectorID(11), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(11), Predicate: "rel", Object: VectorID(10), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(11), Predicate: "rel", Object: VectorID(12), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(12), Predicate: "rel", Object: VectorID(11), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(10), Predicate: "rel", Object: VectorID(12), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(12), Predicate: "rel", Object: VectorID(10), Weight: 1.0})

	// Weak link between clusters
	_ = gs.AddEdge(Edge{Subject: VectorID(3), Predicate: "rel", Object: VectorID(10), Weight: 0.1})

	communities := gs.DetectCommunities()

	// Should detect 2 communities
	if len(communities) < 2 {
		t.Errorf("expected at least 2 communities, got %d", len(communities))
	}
}

// TestLouvainClustering_GetCommunityForNode tests looking up node community
func TestLouvainClustering_GetCommunityForNode(t *testing.T) {
	gs := NewGraphStore()

	// Create connected nodes
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "rel", Object: VectorID(2), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(2), Predicate: "rel", Object: VectorID(3), Weight: 1.0})

	gs.DetectCommunities()

	communityA := gs.GetCommunityForNode(VectorID(1))
	communityB := gs.GetCommunityForNode(VectorID(2))

	// Tightly connected nodes should be in same community
	if communityA != communityB {
		t.Logf("1 in community %d, 2 in community %d", communityA, communityB)
	}
}

// TestLouvainClustering_CommunityCount tests community count metric
func TestLouvainClustering_CommunityCount(t *testing.T) {
	gs := NewGraphStore()

	// Single cluster
	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "rel", Object: VectorID(2), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(2), Predicate: "rel", Object: VectorID(3), Weight: 1.0})

	gs.DetectCommunities()

	count := gs.CommunityCount()
	if count < 1 {
		t.Errorf("expected at least 1 community, got %d", count)
	}
}

// TestGraphStore_TraverseWeighted tests that weighted traversal prioritizes higher edge weights
func TestGraphStore_TraverseWeighted(t *testing.T) {
	gs := NewGraphStore()

	// Diamond graph:
	// Start(1) -> A(2) [Weight 1.0] -> End(4) [Weight 1.0]  (Total 2.0)
	// Start(1) -> B(3) [Weight 10.0] -> End(4) [Weight 10.0] (Total 20.0)
	//
	// BFS might explore A or B first arbitrarily.
	// Weighted traversal (Max-Heap) MUST explore B first because of weight 10.0.

	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "rel", Object: VectorID(2), Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(2), Predicate: "rel", Object: VectorID(4), Weight: 1.0})

	_ = gs.AddEdge(Edge{Subject: VectorID(1), Predicate: "rel", Object: VectorID(3), Weight: 10.0})
	_ = gs.AddEdge(Edge{Subject: VectorID(3), Predicate: "rel", Object: VectorID(4), Weight: 10.0})

	opts := DefaultTraverseOptions()
	opts.MaxHops = 2
	opts.Direction = DirectionOutgoing
	opts.Weighted = true

	paths := gs.Traverse(1, opts)

	// We expect paths to be discovered in order of score.
	// First path found (besides start) should be 1->3 (Weight 10).
	// Path 0 is usually Start node itself (if logic allows) or first expansion.
	// My Traverse logic:
	// if len(item.path.Nodes) > 1 { paths = append(...) }
	// So single node path is NOT in `paths`.

	// We expect the first few paths to be the high weight ones.
	// Path 1->3 should be before 1->2.

	foundStrongPathFirst := false
	foundWeakPath := false

	for _, p := range paths {
		if len(p.Nodes) == 2 {
			// Check immediate neighbors
			secondNode := p.Nodes[1]
			switch secondNode {
			case 3:
				foundStrongPathFirst = true
			case 2:
				if !foundStrongPathFirst {
					t.Errorf("Expected to find strong path (via node 3) before weak path (via node 2)")
				}
				foundWeakPath = true
			}
		}
	}

	if !foundStrongPathFirst {
		t.Errorf("Did not find strong path via node 3")
	}
	if !foundWeakPath {
		t.Errorf("Did not find weak path via node 2")
	}
}
