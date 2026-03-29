package store

// nosec G404 - math/rand is used for IVF centroid selection, not security-sensitive
import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"

	"github.com/23skdu/longbow/internal/simd"
	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

const (
	ivfFlatMagic   = 0x49564646 // "IVFF" in hex
	ivfFlatVersion = 1
)

// =============================================================================
// IVF-Flat Index Implementation
// =============================================================================

// IVFFlatIndex implements PluggableVectorIndex for IVF-Flat algorithm
// IVF-Flat (Inverted File with Flat quantization) partitions vectors into clusters
// using k-means and searches only the nearest clusters (n_probe).
type IVFFlatIndex struct {
	mu          sync.RWMutex
	dimension   int
	vectors     map[uint64][]float32
	centroids   [][]float32    // K cluster centroids
	assignments map[uint64]int // Vector ID -> Cluster ID
	config      *IVFFlatConfig
	built       bool
}

// NewIVFFlatIndex creates a new IVF-Flat index
func NewIVFFlatIndex(cfg IndexConfig) (*IVFFlatIndex, error) {
	if cfg.Dimension <= 0 {
		return nil, fmt.Errorf("invalid dimension: %d", cfg.Dimension)
	}

	if cfg.IVFFlatConfig == nil {
		cfg.IVFFlatConfig = &IVFFlatConfig{
			NClusters: 100, // Default: 100 clusters
			NProbe:    10,  // Default: probe 10 clusters
		}
	}

	return &IVFFlatIndex{
		dimension:   cfg.Dimension,
		vectors:     make(map[uint64][]float32),
		centroids:   make([][]float32, 0),
		assignments: make(map[uint64]int),
		config:      cfg.IVFFlatConfig,
		built:       false,
	}, nil
}

func (ivf *IVFFlatIndex) Type() IndexType {
	return IndexTypeIVFFlat
}

func (ivf *IVFFlatIndex) Dimension() int {
	return ivf.dimension
}

func (ivf *IVFFlatIndex) Size() int {
	ivf.mu.RLock()
	defer ivf.mu.RUnlock()
	return len(ivf.vectors)
}

func (ivf *IVFFlatIndex) NeedsBuild() bool {
	return true // IVF requires training/clustering
}

func (ivf *IVFFlatIndex) Add(id uint64, vector []float32) error {
	ivf.mu.Lock()
	defer ivf.mu.Unlock()

	if len(vector) != ivf.dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", ivf.dimension, len(vector))
	}

	ivf.vectors[id] = vector

	// If already built, assign to nearest cluster
	if ivf.built {
		clusterID := ivf.findNearestCluster(vector)
		ivf.assignments[id] = clusterID
	}

	return nil
}

func (ivf *IVFFlatIndex) AddBatch(ids []uint64, vectors [][]float32) error {
	ivf.mu.Lock()
	defer ivf.mu.Unlock()

	if len(ids) != len(vectors) {
		return fmt.Errorf("ids and vectors length mismatch: %d vs %d", len(ids), len(vectors))
	}

	for i, id := range ids {
		vector := vectors[i]
		if len(vector) != ivf.dimension {
			return fmt.Errorf("vector dimension mismatch: expected %d, got %d", ivf.dimension, len(vector))
		}

		ivf.vectors[id] = vector

		// If already built, assign to nearest cluster
		if ivf.built {
			clusterID := ivf.findNearestCluster(vector)
			ivf.assignments[id] = clusterID
		}
	}

	return nil
}

// Build trains the k-means clustering model
func (ivf *IVFFlatIndex) Build() error {
	ivf.mu.Lock()
	defer ivf.mu.Unlock()

	if len(ivf.vectors) == 0 {
		return fmt.Errorf("no vectors to build index")
	}

	// Extract vectors for training
	vectors := make([][]float32, 0, len(ivf.vectors))
	for _, vec := range ivf.vectors {
		vectors = append(vectors, vec)
	}

	// Run k-means clustering
	centroids, assignments, err := ivf.kMeansClustering(vectors, ivf.config.NClusters)
	if err != nil {
		return err
	}

	ivf.centroids = centroids
	ivf.assignments = assignments
	ivf.built = true

	return nil
}

// Search finds k nearest neighbors by probing n_probe nearest clusters
func (ivf *IVFFlatIndex) Search(query []float32, k int) ([]IndexSearchResult, error) {
	ivf.mu.RLock()
	defer ivf.mu.RUnlock()

	if !ivf.built {
		return nil, fmt.Errorf("index not built. Call Build() first")
	}

	if len(ivf.vectors) == 0 {
		return []IndexSearchResult{}, nil
	}

	// Find n_probe nearest clusters
	probeClusters := ivf.findNearestClusters(query, ivf.config.NProbe)

	// Search vectors in probed clusters
	type result struct {
		id       uint64
		distance float32
	}

	results := make([]result, 0, k*2)

	for _, clusterID := range probeClusters {
		// Get all vectors assigned to this cluster
		for id, assignedCluster := range ivf.assignments {
			if assignedCluster == clusterID {
				vector := ivf.vectors[id]
				dist, _ := simd.L2Squared(query, vector)
				results = append(results, result{id: id, distance: dist})
			}
		}
	}

	// Sort by distance and return top k
	sort.Slice(results, func(a, b int) bool {
		return results[a].distance < results[b].distance
	})

	if k > len(results) {
		k = len(results)
	}

	searchResults := make([]IndexSearchResult, k)
	for i := 0; i < k; i++ {
		searchResults[i] = IndexSearchResult{
			ID:       results[i].id,
			Distance: results[i].distance,
		}
	}

	return searchResults, nil
}

func (ivf *IVFFlatIndex) SearchBatch(queries [][]float32, k int) ([][]IndexSearchResult, error) {
	results := make([][]IndexSearchResult, len(queries))
	for i, query := range queries {
		r, err := ivf.Search(query, k)
		if err != nil {
			return nil, err
		}
		results[i] = r
	}
	return results, nil
}

// GetNeighbors is not supported for IVF-Flat index
func (ivf *IVFFlatIndex) GetNeighbors(ctx context.Context, id lbtypes.VectorID, k int) ([]lbtypes.SearchResult, error) {
	return nil, fmt.Errorf("graph-based neighbor retrieval not supported for IVF-Flat: %w", os.ErrPermission)
}

func (ivf *IVFFlatIndex) Save(path string) error {
	ivf.mu.RLock()
	defer ivf.mu.RUnlock()

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	// Write magic number and version
	if err := binary.Write(f, binary.LittleEndian, uint32(ivfFlatMagic)); err != nil {
		return fmt.Errorf("failed to write magic: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(ivfFlatVersion)); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}

	// Write dimension
	if err := binary.Write(f, binary.LittleEndian, uint32(ivf.dimension)); err != nil {
		return fmt.Errorf("failed to write dimension: %w", err)
	}

	// Write config as JSON
	configData, err := json.Marshal(ivf.config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(len(configData))); err != nil {
		return fmt.Errorf("failed to write config length: %w", err)
	}
	if _, err := f.Write(configData); err != nil {
		return fmt.Errorf("failed to write config: %w", err)
	}

	// Write cluster count
	clusterCount := uint32(len(ivf.centroids))
	if err := binary.Write(f, binary.LittleEndian, clusterCount); err != nil {
		return fmt.Errorf("failed to write cluster count: %w", err)
	}

	// Write centroids
	for _, centroid := range ivf.centroids {
		centroidBytes := make([]byte, len(centroid)*4)
		for i, v := range centroid {
			binary.LittleEndian.PutUint32(centroidBytes[i*4:], math.Float32bits(v))
		}
		if _, err := f.Write(centroidBytes); err != nil {
			return fmt.Errorf("failed to write centroid: %w", err)
		}
	}

	// Write vector count
	vecCount := uint32(len(ivf.vectors))
	if err := binary.Write(f, binary.LittleEndian, vecCount); err != nil {
		return fmt.Errorf("failed to write vector count: %w", err)
	}

	// Sort IDs for deterministic ordering
	ids := make([]uint64, 0, len(ivf.vectors))
	for id := range ivf.vectors {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Write vectors with assignments: (id, cluster_id, vector)
	for _, id := range ids {
		vector := ivf.vectors[id]
		clusterID := ivf.assignments[id]

		if err := binary.Write(f, binary.LittleEndian, id); err != nil {
			return fmt.Errorf("failed to write vector id: %w", err)
		}
		if err := binary.Write(f, binary.LittleEndian, uint32(clusterID)); err != nil {
			return fmt.Errorf("failed to write cluster id: %w", err)
		}

		vecBytes := make([]byte, len(vector)*4)
		for i, v := range vector {
			binary.LittleEndian.PutUint32(vecBytes[i*4:], math.Float32bits(v))
		}
		if _, err := f.Write(vecBytes); err != nil {
			return fmt.Errorf("failed to write vector: %w", err)
		}
	}

	// Write built flag
	builtFlag := uint8(0)
	if ivf.built {
		builtFlag = 1
	}
	if err := binary.Write(f, binary.LittleEndian, builtFlag); err != nil {
		return fmt.Errorf("failed to write built flag: %w", err)
	}

	return nil
}

func (ivf *IVFFlatIndex) Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Read and verify magic number
	var magic uint32
	if err := binary.Read(f, binary.LittleEndian, &magic); err != nil {
		return fmt.Errorf("failed to read magic: %w", err)
	}
	if magic != ivfFlatMagic {
		return fmt.Errorf("invalid magic number: got %x, expected %x", magic, ivfFlatMagic)
	}

	// Read version
	var version uint32
	if err := binary.Read(f, binary.LittleEndian, &version); err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	if version > ivfFlatVersion {
		return fmt.Errorf("unsupported version: %d", version)
	}

	// Read dimension
	var dimension uint32
	if err := binary.Read(f, binary.LittleEndian, &dimension); err != nil {
		return fmt.Errorf("failed to read dimension: %w", err)
	}
	ivf.dimension = int(dimension)

	// Read config
	var configLen uint32
	if err := binary.Read(f, binary.LittleEndian, &configLen); err != nil {
		return fmt.Errorf("failed to read config length: %w", err)
	}
	configData := make([]byte, configLen)
	if _, err := f.Read(configData); err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}
	ivf.config = &IVFFlatConfig{}
	if err := json.Unmarshal(configData, ivf.config); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Read cluster count
	var clusterCount uint32
	if err := binary.Read(f, binary.LittleEndian, &clusterCount); err != nil {
		return fmt.Errorf("failed to read cluster count: %w", err)
	}

	// Read centroids
	ivf.centroids = make([][]float32, clusterCount)
	for i := uint32(0); i < clusterCount; i++ {
		centroid := make([]float32, ivf.dimension)
		centroidBytes := make([]byte, ivf.dimension*4)
		if _, err := f.Read(centroidBytes); err != nil {
			return fmt.Errorf("failed to read centroid: %w", err)
		}
		for j := 0; j < ivf.dimension; j++ {
			centroid[j] = math.Float32frombits(binary.LittleEndian.Uint32(centroidBytes[j*4:]))
		}
		ivf.centroids[i] = centroid
	}

	// Read vectors
	var vecCount uint32
	if err := binary.Read(f, binary.LittleEndian, &vecCount); err != nil {
		return fmt.Errorf("failed to read vector count: %w", err)
	}
	ivf.vectors = make(map[uint64][]float32, vecCount)
	ivf.assignments = make(map[uint64]int, vecCount)

	for i := uint32(0); i < vecCount; i++ {
		var id uint64
		if err := binary.Read(f, binary.LittleEndian, &id); err != nil {
			return fmt.Errorf("failed to read vector id: %w", err)
		}
		var clusterID uint32
		if err := binary.Read(f, binary.LittleEndian, &clusterID); err != nil {
			return fmt.Errorf("failed to read cluster id: %w", err)
		}

		vec := make([]float32, ivf.dimension)
		vecBytes := make([]byte, ivf.dimension*4)
		if _, err := f.Read(vecBytes); err != nil {
			return fmt.Errorf("failed to read vector: %w", err)
		}
		for j := 0; j < ivf.dimension; j++ {
			vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(vecBytes[j*4:]))
		}
		ivf.vectors[id] = vec
		ivf.assignments[id] = int(clusterID)
	}

	// Read built flag
	var builtFlag uint8
	if err := binary.Read(f, binary.LittleEndian, &builtFlag); err != nil {
		return fmt.Errorf("failed to read built flag: %w", err)
	}
	ivf.built = builtFlag == 1

	return nil
}

func (ivf *IVFFlatIndex) Close() error {
	return nil
}

func (ivf *IVFFlatIndex) AddByLocation(batchIdx, rowIdx int) error {
	return nil
}

func (ivf *IVFFlatIndex) GetVectorID(loc Location) (uint64, bool) {
	// Not supported for IVFFlat adapter
	return 0, false
}

func (ivf *IVFFlatIndex) SearchVectors(query []float32, k int, options SearchOptions) []lbtypes.SearchResult {
	results, _ := ivf.Search(query, k)
	searchResults := make([]lbtypes.SearchResult, len(results))
	for i, r := range results {
		id := r.ID
		if id > 4294967295 {
			id = 4294967295
		}
		searchResults[i] = lbtypes.SearchResult{
			ID:       lbtypes.VectorID(id),
			Distance: r.Distance,
			Score:    1.0 / (1.0 + r.Distance),
		}
	}
	return searchResults
}

func (ivf *IVFFlatIndex) Len() int {
	return ivf.Size()
}

// =============================================================================
// K-Means Clustering Implementation
// =============================================================================

// kMeansClustering performs k-means clustering on the input vectors
func (ivf *IVFFlatIndex) kMeansClustering(vectors [][]float32, k int) ([][]float32, map[uint64]int, error) {
	n := len(vectors)
	if n == 0 {
		return nil, nil, fmt.Errorf("no vectors for clustering")
	}

	if k > n {
		k = n
	}

	// Initialize centroids using k-means++ initialization
	centroids := ivf.kMeansPlusPlusInit(vectors, k)

	// Run k-means iterations
	maxIterations := 100
	tolerance := 1e-4

	for iter := 0; iter < maxIterations; iter++ {
		// Assign vectors to nearest centroids
		assignments := make(map[uint64]int)
		for i, vector := range vectors {
			id := uint64(i) // Use index as temporary ID
			nearestCluster := ivf.findNearestCentroid(vector, centroids)
			assignments[id] = nearestCluster
		}

		// Update centroids
		newCentroids := make([][]float32, k)
		clusterCounts := make([]int, k)

		for i := 0; i < k; i++ {
			newCentroids[i] = make([]float32, ivf.dimension)
		}

		for id, clusterID := range assignments {
			vector := vectors[id]
			for d := 0; d < ivf.dimension; d++ {
				newCentroids[clusterID][d] += vector[d]
			}
			clusterCounts[clusterID]++
		}

		// Calculate new centroids and check convergence
		maxDelta := 0.0
		for i := 0; i < k; i++ {
			if clusterCounts[i] > 0 {
				for d := 0; d < ivf.dimension; d++ {
					newCentroids[i][d] /= float32(clusterCounts[i])
				}
			} else {
				// Reinitialize empty cluster
				newCentroids[i] = vectors[rand.Intn(n)]
			}

			// Calculate maximum centroid movement
			if ivf.centroids != nil && len(ivf.centroids) > i {
				deltaFloat32, _ := simd.L2Squared(newCentroids[i], centroids[i])
				delta := float64(deltaFloat32)
				if delta > maxDelta {
					maxDelta = delta
				}
			}
		}

		centroids = newCentroids

		// Check convergence
		if maxDelta < tolerance {
			break
		}
	}

	// Final assignment with actual vector IDs
	finalAssignments := make(map[uint64]int)
	for i, vector := range vectors {
		// Find the original vector ID
		var originalID uint64
		found := false
		for id, vec := range ivf.vectors {
			if vecEqual(vector, vec) {
				originalID = id
				found = true
				break
			}
		}
		if !found {
			originalID = uint64(i)
		}
		finalAssignments[originalID] = ivf.findNearestCentroid(vector, centroids)
	}

	return centroids, finalAssignments, nil
}

// kMeansPlusPlusInit initializes centroids using k-means++ algorithm
func (ivf *IVFFlatIndex) kMeansPlusPlusInit(vectors [][]float32, k int) [][]float32 {
	n := len(vectors)
	centroids := make([][]float32, 0, k)

	// Choose first centroid randomly
	firstIdx := rand.Intn(n)
	centroids = append(centroids, vectors[firstIdx])

	// Choose remaining centroids
	for len(centroids) < k {
		// Calculate distances to nearest centroid for each vector
		distances := make([]float64, n)
		totalDist := 0.0

		for i, vector := range vectors {
			minDist := math.MaxFloat64
			for _, centroid := range centroids {
				dist, _ := simd.L2Squared(vector, centroid)
				dist64 := float64(dist)
				if dist64 < minDist {
					minDist = dist64
				}
			}
			distances[i] = minDist * minDist // Square distance
			totalDist += distances[i]
		}

		// Choose next centroid with probability proportional to distance squared
		r := rand.Float64() * totalDist
		cumsum := 0.0
		nextIdx := 0
		for i, dist := range distances {
			cumsum += dist
			if cumsum >= r {
				nextIdx = i
				break
			}
		}

		centroids = append(centroids, vectors[nextIdx])
	}

	return centroids
}

// findNearestCentroid finds the nearest centroid for a vector
func (ivf *IVFFlatIndex) findNearestCentroid(vector []float32, centroids [][]float32) int {
	minDist := float32(math.MaxFloat32)
	minIdx := 0

	for i, centroid := range centroids {
		dist, _ := simd.L2Squared(vector, centroid)
		if dist < minDist {
			minDist = dist
			minIdx = i
		}
	}

	return minIdx
}

// findNearestCluster finds the nearest cluster for a vector (when already built)
func (ivf *IVFFlatIndex) findNearestCluster(vector []float32) int {
	return ivf.findNearestCentroid(vector, ivf.centroids)
}

// findNearestClusters finds the n nearest clusters for a query vector
func (ivf *IVFFlatIndex) findNearestClusters(query []float32, n int) []int {
	type clusterDist struct {
		id       int
		distance float32
	}

	dists := make([]clusterDist, len(ivf.centroids))
	for i, centroid := range ivf.centroids {
		dist, _ := simd.L2Squared(query, centroid)
		dists[i] = clusterDist{
			id:       i,
			distance: dist,
		}
	}

	// Sort by distance
	sort.Slice(dists, func(a, b int) bool {
		return dists[a].distance < dists[b].distance
	})

	// Return n nearest cluster IDs
	result := make([]int, min(n, len(dists)))
	for i := 0; i < len(result); i++ {
		result[i] = dists[i].id
	}

	return result
}

// vecEqual checks if two vectors are equal
func vecEqual(a, b []float32) bool {
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

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
