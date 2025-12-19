package store

import (
"context"
"crypto/sha256"
"sync"
"time"

"github.com/23skdu/longbow/internal/metrics"
)

// =============================================================================
// MerkleNode - Binary tree node for content verification
// =============================================================================

// MerkleNode represents a node in the Merkle tree
type MerkleNode struct {
hash  []byte
left  *MerkleNode
right *MerkleNode
isLeaf bool
}

// NewMerkleLeaf creates a leaf node with the given hash
func NewMerkleLeaf(hash []byte) *MerkleNode {
return &MerkleNode{
hash:   hash,
isLeaf: true,
}
}

// NewMerkleBranch creates a branch node from two children
func NewMerkleBranch(left, right *MerkleNode) *MerkleNode {
// Compute combined hash from left and right children
combined := make([]byte, 0, 64)
combined = append(combined, left.hash...)
if right != nil {
combined = append(combined, right.hash...)
} else {
// For odd number of leaves, duplicate the left hash
combined = append(combined, left.hash...)
}
hash := sha256.Sum256(combined)

return &MerkleNode{
hash:   hash[:],
left:   left,
right:  right,
isLeaf: false,
}
}

// Hash returns the node's hash
func (n *MerkleNode) Hash() []byte {
return n.hash
}

// IsLeaf returns true if this is a leaf node
func (n *MerkleNode) IsLeaf() bool {
return n.isLeaf
}

// Left returns the left child
func (n *MerkleNode) Left() *MerkleNode {
return n.left
}

// Right returns the right child
func (n *MerkleNode) Right() *MerkleNode {
return n.right
}

// =============================================================================
// MerkleTree - Complete binary tree for dataset verification
// =============================================================================

// MerkleTree represents a complete Merkle tree
type MerkleTree struct {
root      *MerkleNode
leafCount int
leaves    []*MerkleNode
}

// BuildMerkleTree constructs a Merkle tree from leaf hashes
func BuildMerkleTree(hashes [][]byte) *MerkleTree {
if len(hashes) == 0 {
return &MerkleTree{}
}

// Create leaf nodes
leaves := make([]*MerkleNode, len(hashes))
for i, h := range hashes {
leaves[i] = NewMerkleLeaf(h)
}

// Build tree bottom-up
nodes := leaves
for len(nodes) > 1 {
nextLevel := make([]*MerkleNode, 0, (len(nodes)+1)/2)
for i := 0; i < len(nodes); i += 2 {
left := nodes[i]
var right *MerkleNode
if i+1 < len(nodes) {
right = nodes[i+1]
}
nextLevel = append(nextLevel, NewMerkleBranch(left, right))
}
nodes = nextLevel
}

return &MerkleTree{
root:      nodes[0],
leafCount: len(hashes),
leaves:    leaves,
}
}

// Root returns the root node
func (t *MerkleTree) Root() *MerkleNode {
return t.root
}

// RootHash returns the root hash
func (t *MerkleTree) RootHash() []byte {
if t.root == nil {
return nil
}
return t.root.Hash()
}

// LeafCount returns the number of leaves
func (t *MerkleTree) LeafCount() int {
return t.leafCount
}

// =============================================================================
// Tree Comparison - Find divergent records efficiently
// =============================================================================

// MerkleDiff represents a difference between two trees
type MerkleDiff struct {
LeafIndex int
LocalHash []byte
RemoteHash []byte
}

// CompareMerkleTrees finds differences between two Merkle trees
func CompareMerkleTrees(local, remote *MerkleTree) []MerkleDiff {
if local == nil || remote == nil {
return nil
}

// Quick check: if roots match, trees are identical
if hashesEqual(local.RootHash(), remote.RootHash()) {
return nil
}

// Find divergent leaf indices by comparing leaves
var diffs []MerkleDiff
maxLeaves := local.LeafCount()
if remote.LeafCount() > maxLeaves {
maxLeaves = remote.LeafCount()
}

for i := 0; i < maxLeaves; i++ {
var localHash, remoteHash []byte
if i < len(local.leaves) && local.leaves[i] != nil {
localHash = local.leaves[i].Hash()
}
if i < len(remote.leaves) && remote.leaves[i] != nil {
remoteHash = remote.leaves[i].Hash()
}

if !hashesEqual(localHash, remoteHash) {
diffs = append(diffs, MerkleDiff{
LeafIndex:  i,
LocalHash:  localHash,
RemoteHash: remoteHash,
})
}
}

return diffs
}

func hashesEqual(a, b []byte) bool {
if len(a) != len(b) {
return false
}
for i := range a {
if a[i] != b[i] {
return false
}
}
return true
}

// =============================================================================
// MerkleTreeManager - Manages trees per dataset with metrics
// =============================================================================

// MerkleTreeStats holds statistics about tree operations
type MerkleTreeStats struct {
TreesBuilt     int64
Comparisons    int64
DivergencesFound int64
}

// MerkleTreeManager manages Merkle trees for datasets
type MerkleTreeManager struct {
mu     sync.RWMutex
trees  map[string]*MerkleTree
stats  MerkleTreeStats
}

// NewMerkleTreeManager creates a new manager
func NewMerkleTreeManager() *MerkleTreeManager {
return &MerkleTreeManager{
trees: make(map[string]*MerkleTree),
}
}

// BuildTree builds and stores a Merkle tree for a dataset
func (m *MerkleTreeManager) BuildTree(dataset string, hashes [][]byte) error {
start := time.Now()
tree := BuildMerkleTree(hashes)

m.mu.Lock()
m.trees[dataset] = tree
m.stats.TreesBuilt++
m.mu.Unlock()

metrics.MerkleTreeBuilds.Inc()
metrics.MerkleTreeBuildDuration.Observe(time.Since(start).Seconds())

return nil
}

// GetTree retrieves a tree for a dataset
func (m *MerkleTreeManager) GetTree(dataset string) *MerkleTree {
m.mu.RLock()
defer m.mu.RUnlock()
return m.trees[dataset]
}

// CompareWithRemote compares local tree with a remote tree
func (m *MerkleTreeManager) CompareWithRemote(dataset string, remote *MerkleTree) []MerkleDiff {
m.mu.RLock()
local := m.trees[dataset]
m.mu.RUnlock()

if local == nil {
return nil
}

m.mu.Lock()
m.stats.Comparisons++
m.mu.Unlock()

diffs := CompareMerkleTrees(local, remote)

if len(diffs) > 0 {
m.mu.Lock()
m.stats.DivergencesFound += int64(len(diffs))
m.mu.Unlock()
metrics.MerkleDivergencesFound.Add(float64(len(diffs)))
}

return diffs
}

// GetStats returns current statistics
func (m *MerkleTreeManager) GetStats() MerkleTreeStats {
m.mu.RLock()
defer m.mu.RUnlock()
return m.stats
}

// =============================================================================
// AntiEntropySync - Orchestrates anti-entropy synchronization
// =============================================================================

// AntiEntropySyncResult holds the result of a sync operation
type AntiEntropySyncResult struct {
DivergentCount   int
DivergentIndices []int
}

// AntiEntropySync handles anti-entropy synchronization between peers
type AntiEntropySync struct {
}

// NewAntiEntropySync creates a new anti-entropy sync handler
func NewAntiEntropySync() *AntiEntropySync {
return &AntiEntropySync{}
}

// FindDivergentRecords identifies records that differ between local and remote
func (s *AntiEntropySync) FindDivergentRecords(
ctx context.Context,
local, remote *MerkleTree,
) AntiEntropySyncResult {
start := time.Now()
defer func() {
metrics.AntiEntropySyncDuration.Observe(time.Since(start).Seconds())
}()

if local == nil || remote == nil {
return AntiEntropySyncResult{}
}

// Check for context cancellation
select {
case <-ctx.Done():
return AntiEntropySyncResult{}
default:
}

diffs := CompareMerkleTrees(local, remote)

indices := make([]int, len(diffs))
for i, d := range diffs {
indices[i] = d.LeafIndex
}

return AntiEntropySyncResult{
DivergentCount:   len(diffs),
DivergentIndices: indices,
}
}
