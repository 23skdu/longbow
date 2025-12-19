package store

import (
"context"
"crypto/sha256"
"testing"
)

// =============================================================================
// TDD Red Phase: Merkle Tree Anti-Entropy Tests
// =============================================================================

func TestMerkleNode_Leaf(t *testing.T) {
data := []byte("test-record-data")
hash := sha256.Sum256(data)

node := NewMerkleLeaf(hash[:])

if node == nil {
t.Fatal("NewMerkleLeaf returned nil")
}
if !node.IsLeaf() {
t.Error("leaf node should return true for IsLeaf()")
}
if len(node.Hash()) != 32 {
t.Errorf("expected 32 byte hash, got %d", len(node.Hash()))
}
}

func TestMerkleNode_Branch(t *testing.T) {
leftHash := sha256.Sum256([]byte("left"))
rightHash := sha256.Sum256([]byte("right"))

left := NewMerkleLeaf(leftHash[:])
right := NewMerkleLeaf(rightHash[:])

branch := NewMerkleBranch(left, right)

if branch == nil {
t.Fatal("NewMerkleBranch returned nil")
}
if branch.IsLeaf() {
t.Error("branch node should return false for IsLeaf()")
}
if branch.Left() != left {
t.Error("Left() should return left child")
}
if branch.Right() != right {
t.Error("Right() should return right child")
}
}

func TestMerkleTree_Build(t *testing.T) {
// Build tree from list of record hashes
hashes := make([][]byte, 4)
for i := range hashes {
h := sha256.Sum256([]byte{byte(i)})
hashes[i] = h[:]
}

tree := BuildMerkleTree(hashes)

if tree == nil {
t.Fatal("BuildMerkleTree returned nil")
}
if tree.Root() == nil {
t.Fatal("tree Root() is nil")
}
if tree.LeafCount() != 4 {
t.Errorf("expected 4 leaves, got %d", tree.LeafCount())
}
}

func TestMerkleTree_RootHash(t *testing.T) {
hashes := make([][]byte, 2)
for i := range hashes {
h := sha256.Sum256([]byte{byte(i)})
hashes[i] = h[:]
}

tree1 := BuildMerkleTree(hashes)
tree2 := BuildMerkleTree(hashes)

// Same data should produce same root hash
root1 := tree1.RootHash()
root2 := tree2.RootHash()

if len(root1) != 32 {
t.Errorf("expected 32 byte root hash, got %d", len(root1))
}

for i := range root1 {
if root1[i] != root2[i] {
t.Error("identical data should produce identical root hash")
break
}
}
}

func TestMerkleTree_Compare(t *testing.T) {
// Tree 1: records 0,1,2,3
hashes1 := make([][]byte, 4)
for i := range hashes1 {
h := sha256.Sum256([]byte{byte(i)})
hashes1[i] = h[:]
}

// Tree 2: records 0,1,2,X (last record different)
hashes2 := make([][]byte, 4)
for i := range hashes2 {
if i == 3 {
h := sha256.Sum256([]byte{99}) // Different
hashes2[i] = h[:]
} else {
h := sha256.Sum256([]byte{byte(i)})
hashes2[i] = h[:]
}
}

tree1 := BuildMerkleTree(hashes1)
tree2 := BuildMerkleTree(hashes2)

diffs := CompareMerkleTrees(tree1, tree2)

if len(diffs) == 0 {
t.Error("expected differences, got none")
}
// Should identify leaf index 3 as different
found := false
for _, d := range diffs {
if d.LeafIndex == 3 {
found = true
break
}
}
if !found {
t.Error("expected leaf index 3 to be in differences")
}
}

func TestMerkleTree_IdenticalRoots(t *testing.T) {
hashes := make([][]byte, 4)
for i := range hashes {
h := sha256.Sum256([]byte{byte(i)})
hashes[i] = h[:]
}

tree1 := BuildMerkleTree(hashes)
tree2 := BuildMerkleTree(hashes)

diffs := CompareMerkleTrees(tree1, tree2)

if len(diffs) != 0 {
t.Errorf("identical trees should have no differences, got %d", len(diffs))
}
}

func TestMerkleTreeManager_BuildForDataset(t *testing.T) {
mgr := NewMerkleTreeManager()

// Mock record hashes for dataset
hashes := make([][]byte, 8)
for i := range hashes {
h := sha256.Sum256([]byte{byte(i)})
hashes[i] = h[:]
}

err := mgr.BuildTree("test-dataset", hashes)
if err != nil {
t.Fatalf("BuildTree failed: %v", err)
}

tree := mgr.GetTree("test-dataset")
if tree == nil {
t.Fatal("GetTree returned nil")
}
}

func TestMerkleTreeManager_ComparePeers(t *testing.T) {
mgr := NewMerkleTreeManager()

// Local tree
localHashes := make([][]byte, 4)
for i := range localHashes {
h := sha256.Sum256([]byte{byte(i)})
localHashes[i] = h[:]
}
_ = mgr.BuildTree("dataset1", localHashes)

// Remote tree (different record at index 2)
remoteHashes := make([][]byte, 4)
for i := range remoteHashes {
if i == 2 {
h := sha256.Sum256([]byte{50})
remoteHashes[i] = h[:]
} else {
h := sha256.Sum256([]byte{byte(i)})
remoteHashes[i] = h[:]
}
}
remoteTree := BuildMerkleTree(remoteHashes)

diffs := mgr.CompareWithRemote("dataset1", remoteTree)

if len(diffs) == 0 {
t.Error("expected differences")
}
}

func TestMerkleTreeManager_Metrics(t *testing.T) {
mgr := NewMerkleTreeManager()

hashes := make([][]byte, 4)
for i := range hashes {
h := sha256.Sum256([]byte{byte(i)})
hashes[i] = h[:]
}
_ = mgr.BuildTree("test", hashes)

stats := mgr.GetStats()

if stats.TreesBuilt == 0 {
t.Error("TreesBuilt should be > 0")
}
}

func TestAntiEntropySync_FindDivergent(t *testing.T) {
sync := NewAntiEntropySync()

// Local and remote trees with divergence
localHashes := make([][]byte, 4)
for i := range localHashes {
h := sha256.Sum256([]byte{byte(i)})
localHashes[i] = h[:]
}

remoteHashes := make([][]byte, 4)
for i := range remoteHashes {
if i == 1 {
h := sha256.Sum256([]byte{100})
remoteHashes[i] = h[:]
} else {
h := sha256.Sum256([]byte{byte(i)})
remoteHashes[i] = h[:]
}
}

localTree := BuildMerkleTree(localHashes)
remoteTree := BuildMerkleTree(remoteHashes)

ctx := context.Background()
result := sync.FindDivergentRecords(ctx, localTree, remoteTree)

if result.DivergentCount == 0 {
t.Error("expected divergent records")
}
if len(result.DivergentIndices) == 0 {
t.Error("expected divergent indices")
}
}

func TestAntiEntropySync_Concurrent(t *testing.T) {
sync := NewAntiEntropySync()

hashes := make([][]byte, 16)
for i := range hashes {
h := sha256.Sum256([]byte{byte(i)})
hashes[i] = h[:]
}
tree := BuildMerkleTree(hashes)

// Concurrent builds should be safe
done := make(chan bool, 10)
for i := 0; i < 10; i++ {
go func() {
ctx := context.Background()
_ = sync.FindDivergentRecords(ctx, tree, tree)
done <- true
}()
}

for i := 0; i < 10; i++ {
<-done
}
}
