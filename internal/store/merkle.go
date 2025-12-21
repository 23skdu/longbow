package store

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"
)

const (
	MerkleFanout = 16
	MerkleDepth  = 4
)

// MerkleNode represents a node in the Merkle Tree
type MerkleNode struct {
	Hash     [32]byte
	Children [MerkleFanout]*MerkleNode
}

// MerkleTree manages consistency hashes for a Dataset
type MerkleTree struct {
	root *MerkleNode
	mu   sync.RWMutex
}

func NewMerkleTree() *MerkleTree {
	return &MerkleTree{
		root: &MerkleNode{},
	}
}

// Update updates the hash for a given VectorID with its latest timestamp
func (t *MerkleTree) Update(id VectorID, ts int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 1. Traverse to leaf
	node := t.root
	path := t.getPath(id)

	nodes := make([]*MerkleNode, MerkleDepth+1)
	nodes[0] = node

	for i := 0; i < MerkleDepth; i++ {
		slot := path[i]
		if node.Children[slot] == nil {
			node.Children[slot] = &MerkleNode{}
		}
		node = node.Children[slot]
		nodes[i+1] = node
	}

	// 2. Update Leaf Hash (XOR or hash atomic updates)
	// For simplicity, we hash (ID | TS) and XOR into leaf
	// Using XOR allows commutativity and easier updates
	h := sha256.New()
	binary.Write(h, binary.LittleEndian, uint32(id))
	binary.Write(h, binary.LittleEndian, ts)
	itemHash := h.Sum(nil)

	for i := 0; i < 32; i++ {
		node.Hash[i] ^= itemHash[i]
	}

	// 3. Re-hash parents
	for i := MerkleDepth - 1; i >= 0; i-- {
		parent := nodes[i]
		h := sha256.New()
		for _, child := range parent.Children {
			if child != nil {
				h.Write(child.Hash[:])
			} else {
				h.Write(make([]byte, 32)) // Empty child
			}
		}
		copy(parent.Hash[:], h.Sum(nil))
	}
}

func (t *MerkleTree) RootHash() [32]byte {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.root.Hash
}

func (t *MerkleTree) GetNode(path []int) ([32]byte, [][32]byte, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	node := t.root
	for _, slot := range path {
		if node.Children[slot] == nil {
			return [32]byte{}, nil, false
		}
		node = node.Children[slot]
	}

	childHashes := make([][32]byte, MerkleFanout)
	for i, child := range node.Children {
		if child != nil {
			childHashes[i] = child.Hash
		}
	}
	return node.Hash, childHashes, true
}

func (t *MerkleTree) getPath(id VectorID) []int {
	path := make([]int, MerkleDepth)
	val := uint32(id)
	for i := 0; i < MerkleDepth; i++ {
		path[MerkleDepth-1-i] = int(val % MerkleFanout)
		val /= MerkleFanout
	}
	return path
}
