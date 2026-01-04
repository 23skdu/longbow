package store

// GraphBackend abstracts the storage of the HNSW graph structure (adjacency list).
// Implementations can be in-memory (GraphData) or on-disk (DiskGraph).
type GraphBackend interface {
	// GetNeighbors returns the neighbors of the given node at the specified layer.
	// usage of 'buf' allows reducing allocations. If buf is large enough, it is used.
	// Returns a slice containing the neighbors (safe/stable copy).
	GetNeighbors(layer int, nodeID uint32, buf []uint32) []uint32

	// GetLevel returns the maximum level of the node.
	// Returns -1 if the node does not exist.
	GetLevel(nodeID uint32) int

	// Size returns the total number of nodes in the graph.
	Size() int

	// GetCapacity returns the current capacity of the backend.
	GetCapacity() int

	// GetVectorSQ8 returns the SQ8 encoded vector for the given node.
	// Returns nil if not present.
	GetVectorSQ8(nodeID uint32) []byte

	// GetVectorPQ returns the PQ encoded vector for the given node.
	// Returns nil if not present.
	GetVectorPQ(nodeID uint32) []byte

	// Close cleans up any resources associated with the backend (e.g., file handles).
	Close() error
}

// MutableGraphBackend extends GraphBackend with write operations.
// In-memory implementations usually support this.
type MutableGraphBackend interface {
	GraphBackend

	// SetNeighbors updates the neighbors of the given node at the specified layer.
	SetNeighbors(layer int, nodeID uint32, neighbors []uint32)

	// SetLevel sets the level of a node.
	SetLevel(nodeID uint32, level int)

	// Grow ensures the backend has capacity for at least minCap nodes.
	// returns true if growth happened.
	Grow(minCap int) bool
}
