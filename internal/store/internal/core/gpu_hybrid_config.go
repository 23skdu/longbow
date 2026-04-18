package core

import (
	"github.com/23skdu/longbow/internal/store/types"
	"errors"
)

// GPUHybridSearchConfig configures GPU/CPU hybrid search behavior
// This is separate from the text+vector HybridSearchConfig
type GPUHybridSearchConfig struct {
	// CandidateMultiplier determines how many candidates GPU generates
	// GPU returns k * CandidateMultiplier candidates for CPU refinement
	// Higher values improve recall but increase CPU work
	// Default: 10, Range: [2, 50]
	CandidateMultiplier int

	// RefineTopK determines how many top results to refine with CPU
	// If 0, uses the requested k value
	// Default: 0 (use k)
	RefineTopK int

	// EnableGPUCache enables caching of GPU search results
	// Useful for repeated queries
	// Default: false
	EnableGPUCache bool

	// MaxGPUCacheSize limits the GPU result cache size
	// Only used when EnableGPUCache is true
	// Default: 1000
	MaxGPUCacheSize int

	// MinVectorsForGPU minimum vectors required to use GPU
	// Below this threshold, pure CPU search is used
	// Default: 1000
	MinVectorsForGPU int

	// UseGPUPrefetch enables GPU prefetching for better throughput
	// Default: true
	UseGPUPrefetch bool
}

// DefaultGPUHybridSearchConfig returns default configuration for GPU/CPU hybrid search
func DefaultGPUHybridSearchConfig() GPUHybridSearchConfig {
	return GPUHybridSearchConfig{
		CandidateMultiplier: 10,
		RefineTopK:          0, // Use k by default
		EnableGPUCache:      false,
		MaxGPUCacheSize:     1000,
		MinVectorsForGPU:    1000,
		UseGPUPrefetch:      true,
	}
}

// Validate checks if the GPU hybrid search configuration is valid
func (c *GPUHybridSearchConfig) Validate() error {
	if c.CandidateMultiplier < 2 {
		return errors.New("GPUHybridSearchConfig: CandidateMultiplier must be at least 2")
	}
	if c.CandidateMultiplier > 50 {
		return errors.New("GPUHybridSearchConfig: CandidateMultiplier must be at most 50")
	}
	if c.RefineTopK < 0 {
		return errors.New("GPUHybridSearchConfig: RefineTopK must be non-negative")
	}
	if c.MaxGPUCacheSize < 0 {
		return errors.New("GPUHybridSearchConfig: MaxGPUCacheSize must be non-negative")
	}
	if c.MinVectorsForGPU < 0 {
		return errors.New("GPUHybridSearchConfig: MinVectorsForGPU must be non-negative")
	}
	return nil
}

// GetRefineTopK returns the number of results to refine
// If RefineTopK is 0, returns k
func (c *GPUHybridSearchConfig) GetRefineTopK(k int) int {
	if c.RefineTopK == 0 {
		return k
	}
	return c.RefineTopK
}

// GetCandidateCount returns the number of candidates to generate
func (c *GPUHybridSearchConfig) GetCandidateCount(k int) int {
	return k * c.CandidateMultiplier
}

// candidateResult represents a candidate from GPU search
type candidateResult struct {
	id       types.VectorID
	distance float32
	index    int // Original position in GPU results
}

// candidateHeap is a min-heap for efficient top-k selection
type candidateHeap []candidateResult

func (h candidateHeap) Len() int           { return len(h) }
func (h candidateHeap) Less(i, j int) bool { return h[i].distance < h[j].distance }
func (h candidateHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *candidateHeap) Push(x interface{}) {
	*h = append(*h, x.(candidateResult))
}

func (h *candidateHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// deduplicateCandidates removes duplicate vector IDs from candidates
// Returns unique candidates preserving best (lowest) distances
func deduplicateCandidates(candidates []candidateResult) []candidateResult {
	seen := make(map[types.VectorID]bool, len(candidates))
	unique := make([]candidateResult, 0, len(candidates))

	for _, c := range candidates {
		if !seen[c.id] {
			seen[c.id] = true
			unique = append(unique, c)
		}
	}

	return unique
}
