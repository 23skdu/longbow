package store

import (
"math"
"math/rand"
"testing"
)

// =============================================================================
// Subtask 5: Product Quantization (PQ) Tests - TDD Red Phase
// =============================================================================

// TestDefaultPQConfig verifies default configuration values
func TestDefaultPQConfig(t *testing.T) {
cfg := DefaultPQConfig(128) // 128-dim vectors

if cfg == nil {
t.Fatal("DefaultPQConfig returned nil")
}

// Default M=8 subvectors
if cfg.M != 8 {
t.Errorf("M = %d, want 8", cfg.M)
}

// Default Ksub=256 centroids per subvector
if cfg.Ksub != 256 {
t.Errorf("Ksub = %d, want 256", cfg.Ksub)
}

// Dim should match input
if cfg.Dim != 128 {
t.Errorf("Dim = %d, want 128", cfg.Dim)
}

// SubDim = Dim / M = 128 / 8 = 16
if cfg.SubDim != 16 {
t.Errorf("SubDim = %d, want 16", cfg.SubDim)
}
}

// TestPQConfigValidation tests configuration validation rules
func TestPQConfigValidation(t *testing.T) {
tests := []struct {
name    string
cfg     *PQConfig
wantErr bool
}{
{
name:    "nil config",
cfg:     nil,
wantErr: true,
},
{
name:    "valid config",
cfg:     &PQConfig{M: 8, Ksub: 256, Dim: 128, SubDim: 16},
wantErr: false,
},
{
name:    "M must be positive",
cfg:     &PQConfig{M: 0, Ksub: 256, Dim: 128, SubDim: 16},
wantErr: true,
},
{
name:    "Ksub must be positive",
cfg:     &PQConfig{M: 8, Ksub: 0, Dim: 128, SubDim: 16},
wantErr: true,
},
{
name:    "Dim not divisible by M",
cfg:     &PQConfig{M: 7, Ksub: 256, Dim: 128, SubDim: 18},
wantErr: true,
},
{
name:    "Ksub > 256 invalid for uint8 codes",
cfg:     &PQConfig{M: 8, Ksub: 257, Dim: 128, SubDim: 16},
wantErr: true,
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
err := tt.cfg.Validate()
if (err != nil) != tt.wantErr {
t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
}
})
}
}

// TestPQEncoderCreation tests encoder creation with pre-trained codebook
func TestPQEncoderCreation(t *testing.T) {
cfg := &PQConfig{M: 4, Ksub: 256, Dim: 16, SubDim: 4}

// Create mock codebook: M subspaces, each with Ksub centroids of SubDim floats
codebook := make([][][]float32, cfg.M)
for m := 0; m < cfg.M; m++ {
codebook[m] = make([][]float32, cfg.Ksub)
for k := 0; k < cfg.Ksub; k++ {
codebook[m][k] = make([]float32, cfg.SubDim)
}
}

enc, err := NewPQEncoder(cfg, codebook)
if err != nil {
t.Fatalf("NewPQEncoder failed: %v", err)
}

if enc == nil {
t.Fatal("NewPQEncoder returned nil")
}

if enc.Dims() != 16 {
t.Errorf("Dims() = %d, want 16", enc.Dims())
}

if enc.CodeSize() != 4 {
t.Errorf("CodeSize() = %d, want 4 (M bytes)", enc.CodeSize())
}
}

// TestPQEncoderTrainFromData tests training codebook via k-means
func TestPQEncoderTrainFromData(t *testing.T) {
// Generate random training vectors
rng := rand.New(rand.NewSource(42))
numVectors := 1000
dim := 16
vectors := make([][]float32, numVectors)
for i := 0; i < numVectors; i++ {
vectors[i] = make([]float32, dim)
for j := 0; j < dim; j++ {
vectors[i][j] = rng.Float32()*2 - 1 // [-1, 1]
}
}

cfg := &PQConfig{M: 4, Ksub: 16, Dim: dim, SubDim: 4} // Small for test

enc, err := TrainPQEncoder(cfg, vectors, 10) // 10 k-means iterations
if err != nil {
t.Fatalf("TrainPQEncoder failed: %v", err)
}

if enc == nil {
t.Fatal("TrainPQEncoder returned nil")
}

// Verify codebook dimensions
codebook := enc.GetCodebook()
if len(codebook) != cfg.M {
t.Errorf("Codebook has %d subspaces, want %d", len(codebook), cfg.M)
}
for m, subspace := range codebook {
if len(subspace) != cfg.Ksub {
t.Errorf("Subspace %d has %d centroids, want %d", m, len(subspace), cfg.Ksub)
}
for k, centroid := range subspace {
if len(centroid) != cfg.SubDim {
t.Errorf("Centroid [%d][%d] has %d dims, want %d", m, k, len(centroid), cfg.SubDim)
}
}
}
}

// TestPQEncodeVector tests encoding float32 to PQ codes
func TestPQEncodeVector(t *testing.T) {
// Create simple codebook where centroid k is all k values
cfg := &PQConfig{M: 4, Ksub: 4, Dim: 8, SubDim: 2}
codebook := make([][][]float32, cfg.M)
for m := 0; m < cfg.M; m++ {
codebook[m] = make([][]float32, cfg.Ksub)
for k := 0; k < cfg.Ksub; k++ {
codebook[m][k] = []float32{float32(k), float32(k)}
}
}

enc, _ := NewPQEncoder(cfg, codebook)

// Vector closest to centroid 0 in all subspaces
v1 := []float32{0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1}
code1 := enc.Encode(v1)
for i, c := range code1 {
if c != 0 {
t.Errorf("Encode(v1)[%d] = %d, want 0", i, c)
}
}

// Vector closest to centroid 3 in all subspaces
v2 := []float32{2.9, 2.9, 2.9, 2.9, 2.9, 2.9, 2.9, 2.9}
code2 := enc.Encode(v2)
for i, c := range code2 {
if c != 3 {
t.Errorf("Encode(v2)[%d] = %d, want 3", i, c)
}
}
}

// TestPQDecodeVector tests approximate reconstruction from codes
func TestPQDecodeVector(t *testing.T) {
cfg := &PQConfig{M: 4, Ksub: 4, Dim: 8, SubDim: 2}
codebook := make([][][]float32, cfg.M)
for m := 0; m < cfg.M; m++ {
codebook[m] = make([][]float32, cfg.Ksub)
for k := 0; k < cfg.Ksub; k++ {
codebook[m][k] = []float32{float32(k) * 0.5, float32(k) * 0.5}
}
}

enc, _ := NewPQEncoder(cfg, codebook)

// Code [2, 1, 3, 0] should decode to centroids
code := []uint8{2, 1, 3, 0}
decoded := enc.Decode(code)

// Expected: centroid[0][2], centroid[1][1], centroid[2][3], centroid[3][0]
// = [1.0, 1.0, 0.5, 0.5, 1.5, 1.5, 0.0, 0.0]
expected := []float32{1.0, 1.0, 0.5, 0.5, 1.5, 1.5, 0.0, 0.0}

for i, v := range decoded {
if math.Abs(float64(v-expected[i])) > 0.001 {
t.Errorf("Decode()[%d] = %f, want %f", i, v, expected[i])
}
}
}

// TestPQAsymmetricDistance tests ADC (Asymmetric Distance Computation)
func TestPQAsymmetricDistance(t *testing.T) {
cfg := &PQConfig{M: 4, Ksub: 4, Dim: 8, SubDim: 2}
codebook := make([][][]float32, cfg.M)
for m := 0; m < cfg.M; m++ {
codebook[m] = make([][]float32, cfg.Ksub)
for k := 0; k < cfg.Ksub; k++ {
codebook[m][k] = []float32{float32(k), float32(k)}
}
}

enc, _ := NewPQEncoder(cfg, codebook)

// Query vector
query := []float32{0, 0, 1, 1, 2, 2, 3, 3}

// Precompute distance table for query
table := enc.ComputeDistanceTable(query)

// Code that exactly matches query subvectors
code := []uint8{0, 1, 2, 3}
dist := enc.ADCDistance(table, code)

// Should be ~0 since code matches query
if dist > 0.1 {
t.Errorf("ADCDistance for matching code = %f, want ~0", dist)
}

// Code that doesn't match
code2 := []uint8{3, 2, 1, 0}
dist2 := enc.ADCDistance(table, code2)

// Should be larger
if dist2 <= dist {
t.Errorf("ADCDistance for non-matching code = %f, should be > %f", dist2, dist)
}
}

// TestPQCompressionRatio tests memory savings calculation
func TestPQCompressionRatio(t *testing.T) {
tests := []struct {
name  string
dim   int
m     int
ratio float64
}{
{"128-dim M=8", 128, 8, 64.0},   // 512 bytes -> 8 bytes
{"128-dim M=16", 128, 16, 32.0}, // 512 bytes -> 16 bytes
{"128-dim M=32", 128, 32, 16.0}, // 512 bytes -> 32 bytes
{"256-dim M=8", 256, 8, 128.0},  // 1024 bytes -> 8 bytes
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
originalSize := tt.dim * 4 // float32
pqSize := tt.m            // M bytes (one uint8 per subvector)
ratio := float64(originalSize) / float64(pqSize)

if math.Abs(ratio-tt.ratio) > 0.1 {
t.Errorf("Compression ratio = %f, want %f", ratio, tt.ratio)
}
})
}
}

// TestPQEncodeInto tests in-place encoding
func TestPQEncodeInto(t *testing.T) {
cfg := &PQConfig{M: 4, Ksub: 4, Dim: 8, SubDim: 2}
codebook := make([][][]float32, cfg.M)
for m := 0; m < cfg.M; m++ {
codebook[m] = make([][]float32, cfg.Ksub)
for k := 0; k < cfg.Ksub; k++ {
codebook[m][k] = []float32{float32(k), float32(k)}
}
}

enc, _ := NewPQEncoder(cfg, codebook)
vec := []float32{0, 0, 1, 1, 2, 2, 3, 3}
dst := make([]uint8, cfg.M)

enc.EncodeInto(vec, dst)

expected := []uint8{0, 1, 2, 3}
for i, c := range dst {
if c != expected[i] {
t.Errorf("EncodeInto()[%d] = %d, want %d", i, c, expected[i])
}
}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkPQEncode(b *testing.B) {
    rng := rand.New(rand.NewSource(42))
cfg := &PQConfig{M: 8, Ksub: 256, Dim: 128, SubDim: 16}
codebook := make([][][]float32, cfg.M)
for m := 0; m < cfg.M; m++ {
codebook[m] = make([][]float32, cfg.Ksub)
for k := 0; k < cfg.Ksub; k++ {
codebook[m][k] = make([]float32, cfg.SubDim)
for d := 0; d < cfg.SubDim; d++ {
codebook[m][k][d] = rng.Float32()
}
}
}

enc, _ := NewPQEncoder(cfg, codebook)
vec := make([]float32, 128)
for i := range vec {
vec[i] = rng.Float32()
}

b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = enc.Encode(vec)
}
}

func BenchmarkPQADCDistance(b *testing.B) {
    rng := rand.New(rand.NewSource(42))
cfg := &PQConfig{M: 8, Ksub: 256, Dim: 128, SubDim: 16}
codebook := make([][][]float32, cfg.M)
for m := 0; m < cfg.M; m++ {
codebook[m] = make([][]float32, cfg.Ksub)
for k := 0; k < cfg.Ksub; k++ {
codebook[m][k] = make([]float32, cfg.SubDim)
for d := 0; d < cfg.SubDim; d++ {
codebook[m][k][d] = rng.Float32()
}
}
}

enc, _ := NewPQEncoder(cfg, codebook)
query := make([]float32, 128)
for i := range query {
query[i] = rng.Float32()
}
table := enc.ComputeDistanceTable(query)

code := make([]uint8, cfg.M)
for i := range code {
code[i] = uint8(rand.Intn(256))
}

b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = enc.ADCDistance(table, code)
}
}

func BenchmarkPQDistanceTable(b *testing.B) {
    rng := rand.New(rand.NewSource(42))
cfg := &PQConfig{M: 8, Ksub: 256, Dim: 128, SubDim: 16}
codebook := make([][][]float32, cfg.M)
for m := 0; m < cfg.M; m++ {
codebook[m] = make([][]float32, cfg.Ksub)
for k := 0; k < cfg.Ksub; k++ {
codebook[m][k] = make([]float32, cfg.SubDim)
for d := 0; d < cfg.SubDim; d++ {
codebook[m][k][d] = rng.Float32()
}
}
}

enc, _ := NewPQEncoder(cfg, codebook)
query := make([]float32, 128)
for i := range query {
query[i] = rng.Float32()
}

b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = enc.ComputeDistanceTable(query)
}
}
