package store

import (
"testing"
)

// =============================================================================
// Inverted Index Tests
// =============================================================================

func TestInvertedIndex_AddAndSearch(t *testing.T) {
idx := NewInvertedIndex()

// Add documents with text content
idx.Add(VectorID(0), "error code 500 internal server error")
idx.Add(VectorID(1), "warning timeout exceeded")
idx.Add(VectorID(2), "error code 404 not found")
idx.Add(VectorID(3), "success operation completed")
idx.Add(VectorID(4), "error fatal crash dump")

// Search for "error" - should match docs 0, 2, 4
results := idx.Search("error", 10)
if len(results) != 3 {
t.Errorf("expected 3 results for 'error', got %d", len(results))
}

// Verify results contain expected IDs
found := make(map[VectorID]bool)
for _, r := range results {
found[r.ID] = true
}
for _, id := range []VectorID{0, 2, 4} {
if !found[id] {
t.Errorf("expected ID %d in results", id)
}
}
}

func TestInvertedIndex_MultiTermSearch(t *testing.T) {
idx := NewInvertedIndex()

idx.Add(VectorID(0), "error code 500 internal server error")
idx.Add(VectorID(1), "error code 404 not found")
idx.Add(VectorID(2), "warning code timeout")

// Search for "error code" - should match docs 0, 1 with higher scores
results := idx.Search("error code", 10)
if len(results) < 2 {
t.Errorf("expected at least 2 results for 'error code', got %d", len(results))
}

// Docs 0 and 1 should have higher scores than doc 2
if len(results) >= 2 {
// First two should be docs with both terms
topIDs := make(map[VectorID]bool)
topIDs[results[0].ID] = true
topIDs[results[1].ID] = true
if !topIDs[VectorID(0)] || !topIDs[VectorID(1)] {
t.Errorf("expected docs 0 and 1 to be top results")
}
}
}

func TestInvertedIndex_Delete(t *testing.T) {
idx := NewInvertedIndex()

idx.Add(VectorID(0), "error code 500")
idx.Add(VectorID(1), "error code 404")

// Delete doc 0
idx.Delete(VectorID(0))

// Search should only return doc 1
results := idx.Search("error", 10)
if len(results) != 1 {
t.Errorf("expected 1 result after delete, got %d", len(results))
}
if len(results) > 0 && results[0].ID != VectorID(1) {
t.Errorf("expected ID 1, got %d", results[0].ID)
}
}

func TestInvertedIndex_EmptySearch(t *testing.T) {
idx := NewInvertedIndex()

idx.Add(VectorID(0), "hello world")

// Search for non-existent term
results := idx.Search("nonexistent", 10)
if len(results) != 0 {
t.Errorf("expected 0 results for non-existent term, got %d", len(results))
}
}

func TestInvertedIndex_Limit(t *testing.T) {
idx := NewInvertedIndex()

for i := 0; i < 100; i++ {
idx.Add(VectorID(i), "common term")
}

// Search with limit
results := idx.Search("common", 5)
if len(results) != 5 {
t.Errorf("expected 5 results with limit, got %d", len(results))
}
}

// =============================================================================
// Reciprocal Rank Fusion Tests
// =============================================================================

func TestRRF_BasicFusion(t *testing.T) {
// Dense results: [A, B, C, D] (A is rank 1, B is rank 2, etc.)
denseResults := []SearchResult{
{ID: VectorID(0), Score: 0.9}, // A - rank 1
{ID: VectorID(1), Score: 0.8}, // B - rank 2
{ID: VectorID(2), Score: 0.7}, // C - rank 3
{ID: VectorID(3), Score: 0.6}, // D - rank 4
}

// Sparse results: [C, A, E, B] (C is rank 1, A is rank 2, etc.)
sparseResults := []SearchResult{
{ID: VectorID(2), Score: 5.0}, // C - rank 1
{ID: VectorID(0), Score: 4.0}, // A - rank 2
{ID: VectorID(4), Score: 3.0}, // E - rank 3
{ID: VectorID(1), Score: 2.0}, // B - rank 4
}

// RRF with k=60 (standard)
// A: 1/(60+1) + 1/(60+2) = 0.0164 + 0.0161 = 0.0325
// B: 1/(60+2) + 1/(60+4) = 0.0161 + 0.0156 = 0.0317
// C: 1/(60+3) + 1/(60+1) = 0.0159 + 0.0164 = 0.0323
// D: 1/(60+4) + 0 = 0.0156
// E: 0 + 1/(60+3) = 0.0159

fused := ReciprocalRankFusion(denseResults, sparseResults, 60, 10)

if len(fused) == 0 {
t.Fatal("expected non-empty fused results")
}

// A should be ranked highest (appears in both with good ranks)
if fused[0].ID != VectorID(0) {
t.Errorf("expected ID 0 (A) to be top result, got %d", fused[0].ID)
}
}

func TestRRF_EmptyInputs(t *testing.T) {
// Both empty
result := ReciprocalRankFusion(nil, nil, 60, 10)
if len(result) != 0 {
t.Errorf("expected empty result for empty inputs")
}

// One empty
dense := []SearchResult{{ID: VectorID(0), Score: 1.0}}
result = ReciprocalRankFusion(dense, nil, 60, 10)
if len(result) != 1 {
t.Errorf("expected 1 result when sparse is empty")
}
}

func TestRRF_KParameter(t *testing.T) {
dense := []SearchResult{
{ID: VectorID(0), Score: 0.9},
{ID: VectorID(1), Score: 0.8},
}
sparse := []SearchResult{
{ID: VectorID(1), Score: 5.0},
{ID: VectorID(0), Score: 4.0},
}

// With k=1: ranks matter more
// A: 1/(1+1) + 1/(1+2) = 0.5 + 0.33 = 0.83
// B: 1/(1+2) + 1/(1+1) = 0.33 + 0.5 = 0.83
// Should be tied or very close

// With k=60: ranks matter less, more uniform
// Results should still be similar but scores closer

resultK1 := ReciprocalRankFusion(dense, sparse, 1, 10)
resultK60 := ReciprocalRankFusion(dense, sparse, 60, 10)

if len(resultK1) != 2 || len(resultK60) != 2 {
t.Error("expected 2 results for each k value")
}
}

// =============================================================================
// Hybrid Search Tests
// =============================================================================

func TestHybridSearch_Integration(t *testing.T) {
// Create a hybrid searcher
hs := NewHybridSearcher()

// Add vectors with associated text
vectors := [][]float32{
{1.0, 0.0, 0.0}, // ID 0
{0.9, 0.1, 0.0}, // ID 1 - close to 0
{0.0, 1.0, 0.0}, // ID 2
{0.0, 0.9, 0.1}, // ID 3 - close to 2
{0.0, 0.0, 1.0}, // ID 4
}
texts := []string{
"error code 500 server crash",
"warning timeout issue",
"error code 404 page not found",
"success completed operation",
"error fatal exception dump",
}

for i, v := range vectors {
hs.Add(VectorID(i), v, texts[i])
}

// Dense-only search (vector similar to ID 0)
query := []float32{0.95, 0.05, 0.0}
denseResults := hs.SearchDense(query, 5)
if len(denseResults) == 0 {
t.Error("expected dense results")
}
// ID 0 or 1 should be top result (closest vectors)
if denseResults[0].ID != VectorID(0) && denseResults[0].ID != VectorID(1) {
t.Errorf("expected ID 0 or 1 as top dense result, got %d", denseResults[0].ID)
}

// Sparse-only search
sparseResults := hs.SearchSparse("error code", 5)
if len(sparseResults) < 2 {
t.Error("expected at least 2 sparse results for 'error code'")
}

// Hybrid search
hybridResults := hs.SearchHybrid(query, "error", 5, 0.5, 60)
if len(hybridResults) == 0 {
t.Error("expected hybrid results")
}
}

func TestHybridSearch_WeightedFusion(t *testing.T) {
hs := NewHybridSearcher()

// Add test data
hs.Add(VectorID(0), []float32{1.0, 0.0}, "rare unique term")
hs.Add(VectorID(1), []float32{0.9, 0.1}, "common common common")
hs.Add(VectorID(2), []float32{0.0, 1.0}, "rare unique special")

query := []float32{0.95, 0.05}

// With alpha=1.0 (dense only), ID 0 or 1 should be top
denseOnly := hs.SearchHybridWeighted(query, "rare unique", 3, 1.0, 60)
if len(denseOnly) > 0 && denseOnly[0].ID == VectorID(2) {
t.Error("dense-only should not favor ID 2 (far vector)")
}

// With alpha=0.0 (sparse only), ID 0 or 2 should be top (have 'rare unique')
sparseOnly := hs.SearchHybridWeighted(query, "rare unique", 3, 0.0, 60)
if len(sparseOnly) > 0 {
if sparseOnly[0].ID != VectorID(0) && sparseOnly[0].ID != VectorID(2) {
t.Errorf("sparse-only should favor ID 0 or 2, got %d", sparseOnly[0].ID)
}
}
}

func TestHybridSearch_Delete(t *testing.T) {
hs := NewHybridSearcher()

hs.Add(VectorID(0), []float32{1.0, 0.0}, "test document")
hs.Add(VectorID(1), []float32{0.0, 1.0}, "test another")

// Delete ID 0
hs.Delete(VectorID(0))

// Sparse search should only find ID 1
results := hs.SearchSparse("test", 5)
if len(results) != 1 {
t.Errorf("expected 1 result after delete, got %d", len(results))
}
if len(results) > 0 && results[0].ID != VectorID(1) {
t.Errorf("expected ID 1 after delete, got %d", results[0].ID)
}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkInvertedIndex_Add(b *testing.B) {
idx := NewInvertedIndex()
text := "this is a sample document with several words for indexing"

b.ResetTimer()
for i := 0; i < b.N; i++ {
idx.Add(VectorID(i), text)
}
}

func BenchmarkInvertedIndex_Search(b *testing.B) {
idx := NewInvertedIndex()
// Pre-populate with 10k documents
for i := 0; i < 10000; i++ {
idx.Add(VectorID(i), "error code warning timeout server client database query")
}

b.ResetTimer()
for i := 0; i < b.N; i++ {
idx.Search("error code", 10)
}
}

func BenchmarkRRF(b *testing.B) {
dense := make([]SearchResult, 100)
sparse := make([]SearchResult, 100)
for i := 0; i < 100; i++ {
dense[i] = SearchResult{ID: VectorID(i), Score: float32(100 - i)}
sparse[i] = SearchResult{ID: VectorID(99 - i), Score: float32(100 - i)}
}

b.ResetTimer()
for i := 0; i < b.N; i++ {
ReciprocalRankFusion(dense, sparse, 60, 10)
}
}
