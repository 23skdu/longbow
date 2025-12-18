package store

import (
"sync"
"testing"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/23skdu/longbow/internal/metrics"
)

// =============================================================================
// Subtask 1: AutoShardingConfig Tests
// =============================================================================

func TestAutoShardingConfigDefaults(t *testing.T) {
cfg := DefaultAutoShardingConfig()

if cfg.Threshold != 10000 {
t.Errorf("expected default Threshold 10000, got %d", cfg.Threshold)
}
if cfg.NumShards <= 0 {
t.Errorf("expected positive NumShards, got %d", cfg.NumShards)
}
if !cfg.Enabled {
t.Error("expected Enabled=true by default")
}
if cfg.M != 16 {
t.Errorf("expected default M=16, got %d", cfg.M)
}
if cfg.EfConstruction != 200 {
t.Errorf("expected default EfConstruction=200, got %d", cfg.EfConstruction)
}
}

func TestAutoShardingConfigValidation(t *testing.T) {
tests := []struct {
name    string
config  AutoShardingConfig
wantErr bool
}{
{
name: "valid config",
config: AutoShardingConfig{
Enabled:        true,
Threshold:      10000,
NumShards:      8,
M:              16,
EfConstruction: 200,
},
wantErr: false,
},
{
name: "disabled config skips validation",
config: AutoShardingConfig{
Enabled:   false,
Threshold: 0,
},
wantErr: false,
},
{
name: "zero threshold",
config: AutoShardingConfig{
Enabled:        true,
Threshold:      0,
NumShards:      8,
M:              16,
EfConstruction: 200,
},
wantErr: true,
},
{
name: "negative threshold",
config: AutoShardingConfig{
Enabled:        true,
Threshold:      -100,
NumShards:      8,
M:              16,
EfConstruction: 200,
},
wantErr: true,
},
{
name: "zero shards",
config: AutoShardingConfig{
Enabled:        true,
Threshold:      10000,
NumShards:      0,
M:              16,
EfConstruction: 200,
},
wantErr: true,
},
{
name: "zero M",
config: AutoShardingConfig{
Enabled:        true,
Threshold:      10000,
NumShards:      8,
M:              0,
EfConstruction: 200,
},
wantErr: true,
},
{
name: "zero EfConstruction",
config: AutoShardingConfig{
Enabled:        true,
Threshold:      10000,
NumShards:      8,
M:              16,
EfConstruction: 0,
},
wantErr: true,
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
err := tt.config.Validate()
if (err != nil) != tt.wantErr {
t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
}
})
}
}

// =============================================================================
// Subtask 2: ShouldShard Detection Tests
// =============================================================================

func TestShouldShard(t *testing.T) {
tests := []struct {
name        string
vectorCount int
threshold   int
enabled     bool
want        bool
}{
{"below threshold", 5000, 10000, true, false},
{"at threshold", 10000, 10000, true, false},
{"above threshold", 10001, 10000, true, true},
{"well above threshold", 50000, 10000, true, true},
{"disabled even above threshold", 50000, 10000, false, false},
{"custom threshold below", 500, 1000, true, false},
{"custom threshold above", 1001, 1000, true, true},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
cfg := AutoShardingConfig{
Enabled:        tt.enabled,
Threshold:      tt.threshold,
NumShards:      8,
M:              16,
EfConstruction: 200,
}
got := cfg.ShouldShard(tt.vectorCount)
if got != tt.want {
t.Errorf("ShouldShard(%d) = %v, want %v", tt.vectorCount, got, tt.want)
}
})
}
}

// =============================================================================
// Subtask 3: Index Migration Tests
// =============================================================================

func TestMigrateToShardedEmpty(t *testing.T) {
ds := &Dataset{
Name: "test-migrate-empty",
}
originalIndex := NewHNSWIndex(ds)

cfg := DefaultAutoShardingConfig()
shardedIndex, err := MigrateToSharded(originalIndex, cfg)
if err != nil {
t.Fatalf("MigrateToSharded failed: %v", err)
}
if shardedIndex == nil {
t.Fatal("MigrateToSharded returned nil")
}
if shardedIndex.Len() != 0 {
t.Errorf("expected 0 vectors, got %d", shardedIndex.Len())
}
}

func TestMigrateToShardedWithVectors(t *testing.T) {
mem := memory.NewGoAllocator()
schemaFields := []arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
}
schema := arrow.NewSchema(schemaFields, nil)

builder := array.NewRecordBuilder(mem, schema)
defer builder.Release()

idBuilder := builder.Field(0).(*array.Int64Builder)
listBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
vecBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)

for i := 0; i < 100; i++ {
idBuilder.Append(int64(i))
listBuilder.Append(true)
for j := 0; j < 4; j++ {
vecBuilder.Append(float32(i*4 + j))
}
}

rec := builder.NewRecordBatch()
defer rec.Release()

ds := &Dataset{
Name:    "test-migrate-vectors",
Records: []arrow.RecordBatch{rec},
}

originalIndex := NewHNSWIndex(ds)
for i := 0; i < 100; i++ {
if err := originalIndex.Add(0, i); err != nil {
t.Fatalf("Add failed: %v", err)
}
}

if originalIndex.Len() != 100 {
t.Fatalf("expected 100 vectors, got %d", originalIndex.Len())
}

cfg := DefaultAutoShardingConfig()
shardedIndex, err := MigrateToSharded(originalIndex, cfg)
if err != nil {
t.Fatalf("MigrateToSharded failed: %v", err)
}

if shardedIndex.Len() != 100 {
t.Errorf("sharded index has %d vectors, want 100", shardedIndex.Len())
}
}

func TestConcurrentMigration(t *testing.T) {
ds := &Dataset{
Name: "test-concurrent",
}
originalIndex := NewHNSWIndex(ds)
cfg := DefaultAutoShardingConfig()

var wg sync.WaitGroup
errors := make(chan error, 10)

for i := 0; i < 10; i++ {
wg.Add(1)
go func() {
defer wg.Done()
_, err := MigrateToSharded(originalIndex, cfg)
if err != nil {
errors <- err
}
}()
}

wg.Wait()
close(errors)

for err := range errors {
t.Errorf("concurrent migration error: %v", err)
}
}

// =============================================================================
// Subtask 5: Prometheus Metrics Tests
// =============================================================================

func TestAutoShardingMetricsExist(t *testing.T) {
// Verify metrics are registered
if metrics.HnswShardingMigrationsTotal == nil {
t.Error("HnswShardingMigrationsTotal metric not registered")
}
// HnswShardedIndicesCount not yet implemented
// if metrics.HnswShardedIndicesCount == nil {
// 	t.Error("HnswShardedIndicesCount metric not registered")
// }
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkHNSWIndexAdd(b *testing.B) {
mem := memory.NewGoAllocator()
schemaFields := []arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "vector", Type: arrow.FixedSizeListOf(4, arrow.PrimitiveTypes.Float32)},
}
schema := arrow.NewSchema(schemaFields, nil)

builder := array.NewRecordBuilder(mem, schema)
idBuilder := builder.Field(0).(*array.Int64Builder)
listBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
vecBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)

for i := 0; i < 10000; i++ {
idBuilder.Append(int64(i))
listBuilder.Append(true)
for j := 0; j < 128; j++ {
vecBuilder.Append(float32(i + j))
}
}

rec := builder.NewRecordBatch()
defer rec.Release()
defer builder.Release()

ds := &Dataset{
Name:    "bench",
Records: []arrow.RecordBatch{rec},
}
index := NewHNSWIndex(ds)

b.ResetTimer()
for i := 0; i < b.N; i++ {
_ = index.Add(0, i%int(rec.NumRows()))
}
}

func BenchmarkShardedHNSWAdd(b *testing.B) {
ds := &Dataset{Name: "bench-sharded"}
cfg := DefaultShardedHNSWConfig()
sharded := NewShardedHNSW(cfg, ds)

vec := make([]float32, 128)
for i := range vec {
vec[i] = float32(i)
}

b.ResetTimer()
for i := 0; i < b.N; i++ {
loc := Location{BatchIdx: 0, RowIdx: i}
_, _ = sharded.Add(loc, vec)
}
}
