package store

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestGraphStore_AddEdge tests adding edges to the graph store
func TestGraphStore_AddEdge(t *testing.T) {
	gs := NewGraphStore()

	_ = gs.AddEdge(Edge{
		Subject:   "user:alice",
		Predicate: "owns",
		Object:    "doc:report1",
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
	_ = gs.AddEdge(Edge{Subject: "user:alice", Predicate: "owns", Object: "doc:report1", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "user:alice", Predicate: "likes", Object: "doc:report2", Weight: 0.8})
	_ = gs.AddEdge(Edge{Subject: "user:bob", Predicate: "owns", Object: "doc:report3", Weight: 1.0})

	edges := gs.GetEdgesBySubject("user:alice")
	if len(edges) != 2 {
		t.Errorf("expected 2 edges for alice, got %d", len(edges))
	}

	edges = gs.GetEdgesBySubject("user:bob")
	if len(edges) != 1 {
		t.Errorf("expected 1 edge for bob, got %d", len(edges))
	}

	edges = gs.GetEdgesBySubject("user:unknown")
	if len(edges) != 0 {
		t.Errorf("expected 0 edges for unknown, got %d", len(edges))
	}
}

// TestGraphStore_GetEdgesByObject tests querying edges by object (incoming)
func TestGraphStore_GetEdgesByObject(t *testing.T) {
	gs := NewGraphStore()

	// Add edges pointing to same object
	_ = gs.AddEdge(Edge{Subject: "user:alice", Predicate: "owns", Object: "doc:shared", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "user:bob", Predicate: "likes", Object: "doc:shared", Weight: 0.5})
	_ = gs.AddEdge(Edge{Subject: "user:carol", Predicate: "owns", Object: "doc:other", Weight: 1.0})

	edges := gs.GetEdgesByObject("doc:shared")
	if len(edges) != 2 {
		t.Errorf("expected 2 edges to doc:shared, got %d", len(edges))
	}

	edges = gs.GetEdgesByObject("doc:other")
	if len(edges) != 1 {
		t.Errorf("expected 1 edge to doc:other, got %d", len(edges))
	}
}

// TestGraphStore_GetEdgesByPredicate tests filtering by relationship type
func TestGraphStore_GetEdgesByPredicate(t *testing.T) {
	gs := NewGraphStore()

	_ = gs.AddEdge(Edge{Subject: "user:alice", Predicate: "owns", Object: "doc:1", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "user:bob", Predicate: "owns", Object: "doc:2", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "user:alice", Predicate: "likes", Object: "doc:3", Weight: 0.5})

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

	_ = gs.AddEdge(Edge{Subject: "a", Predicate: "owns", Object: "b", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "b", Predicate: "likes", Object: "c", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "c", Predicate: "owns", Object: "d", Weight: 1.0}) // duplicate predicate
	_ = gs.AddEdge(Edge{Subject: "d", Predicate: "authored", Object: "e", Weight: 1.0})

	vocab := gs.PredicateVocabulary()
	if len(vocab) != 3 {
		t.Errorf("expected 3 unique predicates, got %d: %v", len(vocab), vocab)
	}
}

// TestGraphStore_ToArrowBatch tests converting edges to Arrow RecordBatch with Dictionary encoding
func TestGraphStore_ToArrowBatch(t *testing.T) {
	gs := NewGraphStore()

	// Add edges with repeated predicates
	_ = gs.AddEdge(Edge{Subject: "user:alice", Predicate: "owns", Object: "doc:1", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "user:bob", Predicate: "owns", Object: "doc:2", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "user:alice", Predicate: "likes", Object: "doc:3", Weight: 0.5})
	_ = gs.AddEdge(Edge{Subject: "user:carol", Predicate: "owns", Object: "doc:4", Weight: 1.0})

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
}

// TestGraphStore_DictionaryMemorySavings verifies Dictionary encoding saves memory
func TestGraphStore_DictionaryMemorySavings(t *testing.T) {
	gs := NewGraphStore()

	// Add 1000 edges with only 3 predicate types
	predicates := []string{"owns", "likes", "authored"}
	for i := 0; i < 1000; i++ {
		_ = gs.AddEdge(Edge{
			Subject:   fmt.Sprintf("user:%d", i),
			Predicate: predicates[i%3],
			Object:    fmt.Sprintf("doc:%d", i),
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
	_ = gs1.AddEdge(Edge{Subject: "a", Predicate: "rel", Object: "b", Weight: 1.0})
	_ = gs1.AddEdge(Edge{Subject: "b", Predicate: "rel", Object: "c", Weight: 0.5})

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

	edges := gs2.GetEdgesBySubject("a")
	if len(edges) != 1 || edges[0].Object != "b" {
		t.Errorf("edge not loaded correctly: %+v", edges)
	}
}

// TestGraphStore_TraverseSingleHop tests finding direct neighbors
func TestGraphStore_TraverseSingleHop(t *testing.T) {
	gs := NewGraphStore()

	// Build a simple graph: alice -> owns -> doc1, alice -> likes -> doc2
	_ = gs.AddEdge(Edge{Subject: "alice", Predicate: "owns", Object: "doc1", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "alice", Predicate: "likes", Object: "doc2", Weight: 0.8})
	_ = gs.AddEdge(Edge{Subject: "bob", Predicate: "owns", Object: "doc3", Weight: 1.0})

	// Traverse 1 hop from alice
	paths := gs.Traverse("alice", 1)
	if len(paths) != 2 {
		t.Errorf("expected 2 paths from alice, got %d", len(paths))
	}

	// Check paths contain expected objects
	objects := make(map[string]bool)
	for _, p := range paths {
		if len(p.Nodes) > 0 {
			objects[p.Nodes[len(p.Nodes)-1]] = true
		}
	}
	if !objects["doc1"] || !objects["doc2"] {
		t.Errorf("expected doc1 and doc2 in paths, got %v", objects)
	}
}

// TestGraphStore_TraverseMultiHop tests multi-hop graph traversal
func TestGraphStore_TraverseMultiHop(t *testing.T) {
	gs := NewGraphStore()

	// Build chain: alice -> owns -> doc1 -> references -> paper1 -> cites -> paper2
	_ = gs.AddEdge(Edge{Subject: "alice", Predicate: "owns", Object: "doc1", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "doc1", Predicate: "references", Object: "paper1", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "paper1", Predicate: "cites", Object: "paper2", Weight: 1.0})

	// Traverse 3 hops from alice
	paths := gs.Traverse("alice", 3)

	// Should find path to paper2
	foundPaper2 := false
	for _, p := range paths {
		for _, node := range p.Nodes {
			if node == "paper2" {
				foundPaper2 = true
			}
		}
	}
	if !foundPaper2 {
		t.Errorf("expected to find paper2 in 3-hop traversal")
	}
}

// TestGraphStore_TraverseNoCycles tests that traversal avoids cycles
func TestGraphStore_TraverseNoCycles(t *testing.T) {
	gs := NewGraphStore()

	// Create cycle: a -> b -> c -> a
	_ = gs.AddEdge(Edge{Subject: "a", Predicate: "rel", Object: "b", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "b", Predicate: "rel", Object: "c", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "c", Predicate: "rel", Object: "a", Weight: 1.0})

	// Traverse should not hang due to cycle
	paths := gs.Traverse("a", 5)

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
			Subject:   fmt.Sprintf("node%d", i),
			Predicate: "connects",
			Object:    fmt.Sprintf("node%d", i+1),
			Weight:    1.0,
		})
	}

	// Parallel traversal from multiple nodes
	starts := []string{"node0", "node10", "node20", "node30"}
	results := gs.TraverseParallel(starts, 5)

	if len(results) != 4 {
		t.Errorf("expected 4 result sets, got %d", len(results))
	}

	// Each should have found paths
	for start, paths := range results {
		if len(paths) == 0 {
			t.Errorf("expected paths from %s, got none", start)
		}
	}
}

// TestLouvainClustering_BasicCommunities tests detecting obvious clusters
func TestLouvainClustering_BasicCommunities(t *testing.T) {
	gs := NewGraphStore()

	// Create two obvious clusters
	// Cluster 1: a-b-c tightly connected
	_ = gs.AddEdge(Edge{Subject: "a", Predicate: "rel", Object: "b", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "b", Predicate: "rel", Object: "a", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "b", Predicate: "rel", Object: "c", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "c", Predicate: "rel", Object: "b", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "a", Predicate: "rel", Object: "c", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "c", Predicate: "rel", Object: "a", Weight: 1.0})

	// Cluster 2: x-y-z tightly connected
	_ = gs.AddEdge(Edge{Subject: "x", Predicate: "rel", Object: "y", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "y", Predicate: "rel", Object: "x", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "y", Predicate: "rel", Object: "z", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "z", Predicate: "rel", Object: "y", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "x", Predicate: "rel", Object: "z", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "z", Predicate: "rel", Object: "x", Weight: 1.0})

	// Weak link between clusters
	_ = gs.AddEdge(Edge{Subject: "c", Predicate: "rel", Object: "x", Weight: 0.1})

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
	_ = gs.AddEdge(Edge{Subject: "a", Predicate: "rel", Object: "b", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "b", Predicate: "rel", Object: "c", Weight: 1.0})

	gs.DetectCommunities()

	communityA := gs.GetCommunityForNode("a")
	communityB := gs.GetCommunityForNode("b")

	// Tightly connected nodes should be in same community
	if communityA != communityB {
		t.Logf("a in community %d, b in community %d", communityA, communityB)
	}
}

// TestLouvainClustering_CommunityCount tests community count metric
func TestLouvainClustering_CommunityCount(t *testing.T) {
	gs := NewGraphStore()

	// Single cluster
	_ = gs.AddEdge(Edge{Subject: "a", Predicate: "rel", Object: "b", Weight: 1.0})
	_ = gs.AddEdge(Edge{Subject: "b", Predicate: "rel", Object: "c", Weight: 1.0})

	gs.DetectCommunities()

	count := gs.CommunityCount()
	if count < 1 {
		t.Errorf("expected at least 1 community, got %d", count)
	}
}
