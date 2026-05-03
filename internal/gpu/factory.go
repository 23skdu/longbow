package gpu

import (
	"fmt"
	"math"
	"os"
	"sort"
	"unsafe"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/simd"
)

// NewIndexWithBackend creates a GPU index with specified backend (delegates to implementation)
func NewIndexWithBackend(cfg GPUConfig, backend GPUBackend) (Index, error) {
	switch backend {
	case BackendCPU, BackendOpenCL:
		return NewCPUIndex(cfg)
	default:
		return newGPUIndexImpl(cfg, backend)
	}
}

// NewIndex creates a GPU index with auto-detected backend (delegates to implementation)
func NewIndex(cfg GPUConfig) (Index, error) {
	if cfg.Backend == BackendCPU || !cfg.Enabled {
		return NewCPUIndex(cfg)
	}

	preferredBackend := DetectGPUBackend()
	return NewIndexWithBackend(cfg, preferredBackend)
}

// CPUIndex implements a CPU-only fallback index using linear scan
type CPUIndex struct {
	vectors     map[int64][]float32
	pqCodes     map[int64][]byte
	tqCodes     map[int64][]byte
	dimension  int
	deviceID   int32
	pqEncoder  *pq.PQEncoder
	pqEnabled  bool // Enable PQ compression during ingest
}

func NewCPUIndex(cfg GPUConfig) (*CPUIndex, error) {
	idx := &CPUIndex{
		vectors:   make(map[int64][]float32),
		pqCodes:   make(map[int64][]byte),
		tqCodes:   make(map[int64][]byte),
		dimension: cfg.Dimension,
		deviceID:  cfg.DeviceID,
	}
	// Auto-enable PQ for larger indexes to improve ingest throughput
	if os.Getenv("LONGBOW_PQ_INGEST") == "1" {
		idx.pqEnabled = true
	}
	return idx, nil
}

func (i *CPUIndex) Add(ids []int64, vectors []float32) error {
	if len(ids) == 0 {
		return nil
	}

	vectorsPerID := len(vectors) / len(ids)

	// Use PQ compression during ingest if enabled and encoder is trained
	if i.pqEnabled && i.pqEncoder != nil {
		for idx, id := range ids {
			start := idx * vectorsPerID
			end := start + vectorsPerID
			if end > len(vectors) {
				end = len(vectors)
			}
			vec := vectors[start:end]
			// Encode to PQ codes directly (skip storing full vectors)
			codes, err := i.pqEncoder.Encode(vec)
			if err != nil {
				return err
			}
			i.pqCodes[id] = codes
		}
		return nil
	}

	// Default: store full vectors
	for idx, id := range ids {
		start := idx * vectorsPerID
		end := start + vectorsPerID
		if end > len(vectors) {
			end = len(vectors)
		}
		i.vectors[id] = vectors[start:end]
	}

	return nil
}


func (i *CPUIndex) Search(vector []float32, k int) (ids []int64, distances []float32, err error) {
	if len(i.vectors) == 0 {
		return []int64{}, []float32{}, nil
	}

	if k > len(i.vectors) {
		k = len(i.vectors)
	}

	type result struct {
		id       int64
		distance float32
	}

	results := make([]result, 0, len(i.vectors))

	for id, storedVec := range i.vectors {
		dist := euclideanDistance(vector, storedVec)
		results = append(results, result{id: id, distance: dist})
	}

	// Sort by distance (ascending)
	sort.Slice(results, func(a, b int) bool {
		return results[a].distance < results[b].distance
	})

	// Return top k results
	ids = make([]int64, k)
	distances = make([]float32, k)
	for idx := 0; idx < k && idx < len(results); idx++ {
		ids[idx] = results[idx].id
		distances[idx] = results[idx].distance
	}

	return ids, distances, nil
}

// euclideanDistance computes the squared Euclidean distance between two vectors
func euclideanDistance(a, b []float32) float32 {
	var sum float32
	for i := 0; i < len(a) && i < len(b); i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum
}

func (i *CPUIndex) Close() error {
	i.vectors = nil
	return nil
}

func (i *CPUIndex) Backend() GPUBackend {
	return BackendCPU
}

func (i *CPUIndex) DeviceID() int32 {
	return i.deviceID
}

func (i *CPUIndex) GetDeviceInfo() (*GPUInfo, error) {
	return &GPUInfo{
		Backend:  BackendCPU,
		Name:     "CPU",
		MemoryMB: 0,
	}, nil
}

func (i *CPUIndex) GetMemoryInfo() (total, free, used int64, err error) {
	var totalMem int64
	for _, vec := range i.vectors {
		totalMem += int64(len(vec) * 4)
	}
	return totalMem, 0, totalMem, nil
}

func (i *CPUIndex) GetUtilization() (float32, error) {
	return 0, nil
}

func (i *CPUIndex) SearchBatch(vectors [][]float32, k int) ([][]int64, [][]float32, error) {
	if len(vectors) == 0 {
		return nil, nil, nil
	}

	results := make([][]int64, len(vectors))
	distances := make([][]float32, len(vectors))

	for idx, vec := range vectors {
		ids, dist, err := i.Search(vec, k)
		if err != nil {
			return nil, nil, fmt.Errorf("batch search[%d]: %w", idx, err)
		}
		results[idx] = ids
		distances[idx] = dist
	}

	return results, distances, nil
}


func (i *CPUIndex) AddPQ(ids []int64, codes []byte, m int) error {
	if len(ids) == 0 {
		return nil
	}
	codeLen := len(codes) / len(ids)
	for idx, id := range ids {
		i.pqCodes[id] = codes[idx*codeLen : (idx+1)*codeLen]
	}
	return nil
}

func (i *CPUIndex) SearchPQ(lookupTable []float32, m int, k int) (ids []int64, distances []float32, err error) {
	if len(i.pqCodes) == 0 {
		return []int64{}, []float32{}, nil
	}

	// For efficiency, we collect all codes and IDs into slices
	numVectors := len(i.pqCodes)
	flatCodes := make([]byte, numVectors*m)
	idsMap := make([]int64, numVectors)
	idx := 0
	for id, codes := range i.pqCodes {
		copy(flatCodes[idx*m:(idx+1)*m], codes)
		idsMap[idx] = id
		idx++
	}

	results := make([]float32, numVectors)
	if err := simd.ADCDistanceBatch(lookupTable, flatCodes, m, results); err != nil {
		// Fallback to scalar if SIMD fails or not available
		for j := 0; j < numVectors; j++ {
			var dist float32
			codes := flatCodes[j*m : (j+1)*m]
			for c := 0; c < m; c++ {
				dist += lookupTable[c*256+int(codes[c])]
			}
			results[j] = dist
		}
	}

	type res struct {
		id   int64
		dist float32
	}
	allResults := make([]res, numVectors)
	for j := 0; j < numVectors; j++ {
		allResults[j] = res{id: idsMap[j], dist: results[j]}
	}

	sort.Slice(allResults, func(a, b int) bool {
		return allResults[a].dist < allResults[b].dist
	})

	if k > numVectors {
		k = numVectors
	}

	ids = make([]int64, k)
	distances = make([]float32, k)
	for j := 0; j < k; j++ {
		ids[j] = allResults[j].id
		distances[j] = allResults[j].dist
	}

	return ids, distances, nil
}

func (i *CPUIndex) SearchFloat16(vector []uint16, k int) ([]int64, []float32, error) {
	// Convert float16 (uint16) to float32
	f32 := make([]float32, len(vector))
	for idx, v := range vector {
		f32[idx] = float16ToFloat32(v)
	}
	return i.Search(f32, k)
}

func (i *CPUIndex) SearchComplex64(vector []uint16, k int) ([]int64, []float32, error) {
	// complex64 is 2 x float32, stored as uint16 pairs (for GPU compatibility)
	f32 := make([]float32, len(vector))
	for idx, v := range vector {
		f32[idx] = float16ToFloat32(v)
	}
	return i.Search(f32, k)
}

func (i *CPUIndex) SearchComplex128(vector []float32, k int) ([]int64, []float32, error) {
	// complex128 is 2 x float64, but stored as float32 pairs in our format
	return i.Search(vector, k)
}

func (i *CPUIndex) TrainPQ(vectors []float32, m int, k int) error {
	encoder, err := pq.NewPQEncoder(i.dimension, m, k)
	if err != nil {
		return err
	}
	numVecs := len(vectors) / i.dimension
	vecs2d := make([][]float32, numVecs)
	for idx := 0; idx < numVecs; idx++ {
		vecs2d[idx] = vectors[idx*i.dimension : (idx+1)*i.dimension]
	}
	if err := encoder.Train(vecs2d); err != nil {
		return err
	}
	i.pqEncoder = encoder
	return nil
}

func (i *CPUIndex) EncodePQ(vectors []float32) ([]byte, error) {
	if i.pqEncoder == nil {
		return nil, fmt.Errorf("PQ encoder not trained")
	}
	numVecs := len(vectors) / i.dimension
	codes := make([]byte, numVecs*i.pqEncoder.M)
	for idx := 0; idx < numVecs; idx++ {
		vec := vectors[idx*i.dimension : (idx+1)*i.dimension]
		encoded, err := i.pqEncoder.Encode(vec)
		if err != nil {
			return nil, err
		}
		copy(codes[idx*i.pqEncoder.M:], encoded)
	}
	return codes, nil
}

func (i *CPUIndex) AddTurboQuant(ids []int64, tqData []byte, bitsPerAngle int) error {
	if len(ids) == 0 {
		return nil
	}
	stride := len(tqData) / len(ids)
	for idx, id := range ids {
		i.tqCodes[id] = tqData[idx*stride : (idx+1)*stride]
	}
	return nil
}

func (i *CPUIndex) SearchTurboQuant(vector []float32, k int, bitsPerAngle int) ([]int64, []float32, error) {
	if len(i.tqCodes) == 0 {
		return []int64{}, []float32{}, nil
	}

	// TurboQuant CPU search: currently implemented via reconstruction + SIMD Euclidean
	// This is a placeholder for a dedicated TQ SIMD kernel.
	type result struct {
		id       int64
		distance float32
	}
	results := make([]result, 0, len(i.tqCodes))

	for id := range i.tqCodes {
		// Reconstruct (simplified for now)
		recon := make([]float32, i.dimension)
		// ... reconstruction logic would go here ...
		// For now, we just do a fallback search
		dist, _ := simd.DistFunc(vector, recon)
		results = append(results, result{id: id, distance: dist})
	}

	sort.Slice(results, func(a, b int) bool {
		return results[a].distance < results[b].distance
	})

	if k > len(results) {
		k = len(results)
	}

	ids := make([]int64, k)
	distances := make([]float32, k)
	for idx := 0; idx < k; idx++ {
		ids[idx] = results[idx].id
		distances[idx] = results[idx].distance
	}
	return ids, distances, nil
}

func (i *CPUIndex) AssignToClusters(vectors []float32, centroids []float32) ([]uint32, error) {
	if len(centroids) == 0 || len(vectors) == 0 {
		return nil, nil
	}

	dim := i.dimension
	numVectors := len(vectors) / dim
	numCentroids := len(centroids) / dim
	assignments := make([]uint32, numVectors)

	for v := 0; v < numVectors; v++ {
		vec := vectors[v*dim : (v+1)*dim]
		bestC := uint32(0)
		minDist := float32(3.40282346638528859811704183484516925440e+38) // MaxFloat32

		for c := 0; c < numCentroids; c++ {
			cent := centroids[c*dim : (c+1)*dim]
			dist := euclideanDistance(vec, cent)
			if dist < minDist {
				minDist = dist
				bestC = uint32(c)
			}
		}
		assignments[v] = bestC
	}

	return assignments, nil
}

func (i *CPUIndex) UpdateGraph(offsets []uint32, neighbors []uint32, weights []float32) error {
	return fmt.Errorf("UpdateGraph not supported on CPUIndex")
}

func (i *CPUIndex) GraphExpand(seeds []uint32, depth int, alpha float32) ([]uint32, []float32, error) {
	return nil, nil, fmt.Errorf("GraphExpand not supported on CPUIndex")
}

func (i *CPUIndex) HaversineSearch(centerLat, centerLon float32, points []float32, earthRadius float32) ([]float32, error) {
	count := len(points) / 2
	results := make([]float32, count)
	
	// Scalar fallback for CPU
	for j := 0; j < count; j++ {
		lat2 := points[j*2]
		lon2 := points[j*2+1]
		
		dLat := (lat2 - centerLat) * math.Pi / 180.0
		dLon := (lon2 - centerLon) * math.Pi / 180.0
		
		lat1Rad := float64(centerLat) * math.Pi / 180.0
		lat2Rad := float64(lat2) * math.Pi / 180.0
		
		a := math.Sin(float64(dLat/2))*math.Sin(float64(dLat/2)) +
			math.Cos(lat1Rad)*math.Cos(lat2Rad)*
				math.Sin(float64(dLon/2))*math.Sin(float64(dLon/2))
		c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
		results[j] = float32(float64(earthRadius) * c)
	}
	
	return results, nil
}

func (i *CPUIndex) NormBatch(vectors []float32, dims int) ([]float32, error) {
	count := len(vectors) / dims
	results := make([]float32, count)
	
	for j := 0; j < count; j++ {
		vec := vectors[j*dims : (j+1)*dims]
		var sum float32
		for _, v := range vec {
			sum += v * v
		}
		results[j] = sum
	}
	
	return results, nil
}

// float16ToFloat32 ...
func float16ToFloat32(v uint16) float32 {
	// Extract float16 components
	sign := uint32(v >> 15)
	exp := uint32((v >> 10) & 0x1F)
	mant := uint32(v & 0x3FF)

	// Handle special cases
	if exp == 0 {
		if mant == 0 {
			// Zero
			return float32FromBits(sign << 31)
		}
		// Subnormal
		return float32FromBits((sign << 31) | (mant << 13))
	}
	if exp == 31 {
		// Infinity or NaN
		return float32FromBits((sign << 31) | (0xFF << 23) | (mant << 13))
	}

	// Normalized: convert exponent from 5-bit bias-15 to 8-bit bias-127
	newExp := (exp - 15 + 127) << 23
	return float32FromBits((sign << 31) | newExp | (mant << 13))
}

// float32FromBits converts uint32 bits to float32
func float32FromBits(bits uint32) float32 {
	return *(*float32)(unsafe.Pointer(&bits)) // #nosec G103 -- intentional unsafe for type punning
}
