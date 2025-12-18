package store

import (
"errors"
"hash/fnv"
"runtime"
"sync"
"sync/atomic"

"github.com/apache/arrow-go/v18/arrow"
)

// =============================================================================
// PartitionedRecords - Subtask 2: Partitioned RecordBatch Storage
// =============================================================================

// PartitionedRecordsConfig configures partitioned records storage.
type PartitionedRecordsConfig struct {
NumPartitions int // Number of partitions (default: runtime.NumCPU())
}

// DefaultPartitionedRecordsConfig returns sensible defaults.
func DefaultPartitionedRecordsConfig() PartitionedRecordsConfig {
return PartitionedRecordsConfig{
NumPartitions: runtime.NumCPU(),
}
}

// Validate checks configuration validity.
func (c PartitionedRecordsConfig) Validate() error {
if c.NumPartitions <= 0 {
return errors.New("NumPartitions must be positive")
}
return nil
}

// partition represents a single partition with its own lock and records.
type partition struct {
mu      sync.RWMutex
records []arrow.RecordBatch
}

// PartitionedRecordsStats holds statistics for partitioned records.
type PartitionedRecordsStats struct {
TotalBatches   int
NumPartitions  int
BatchesPerPart []int
}

// PartitionedRecords provides partitioned storage for Arrow RecordBatches.
// Each partition has its own lock, enabling concurrent access to different partitions.
type PartitionedRecords struct {
partitions    []partition
numPartitions int
totalBatches  atomic.Int64
}

// NewPartitionedRecords creates new partitioned records with given config.
func NewPartitionedRecords(cfg PartitionedRecordsConfig) *PartitionedRecords {
if cfg.NumPartitions <= 0 {
cfg.NumPartitions = runtime.NumCPU()
}
return &PartitionedRecords{
partitions:    make([]partition, cfg.NumPartitions),
numPartitions: cfg.NumPartitions,
}
}

// NewPartitionedRecordsDefault creates partitioned records with default config.
func NewPartitionedRecordsDefault() *PartitionedRecords {
return NewPartitionedRecords(DefaultPartitionedRecordsConfig())
}

// NumPartitions returns the number of partitions.
func (pr *PartitionedRecords) NumPartitions() int {
return pr.numPartitions
}

// TotalBatches returns the total number of batches across all partitions.
func (pr *PartitionedRecords) TotalBatches() int {
return int(pr.totalBatches.Load())
}

// PartitionBatches returns the number of batches in a specific partition.
func (pr *PartitionedRecords) PartitionBatches(partitionIdx int) int {
if partitionIdx < 0 || partitionIdx >= pr.numPartitions {
return 0
}
pr.partitions[partitionIdx].mu.RLock()
count := len(pr.partitions[partitionIdx].records)
pr.partitions[partitionIdx].mu.RUnlock()
return count
}

// hashKey returns the partition index for a given key using FNV-1a.
func (pr *PartitionedRecords) hashKey(key uint64) int {
h := fnv.New64a()
b := make([]byte, 8)
b[0] = byte(key)
b[1] = byte(key >> 8)
b[2] = byte(key >> 16)
b[3] = byte(key >> 24)
b[4] = byte(key >> 32)
b[5] = byte(key >> 40)
b[6] = byte(key >> 48)
b[7] = byte(key >> 56)
h.Write(b)
return int(h.Sum64() % uint64(pr.numPartitions))
}

// Append adds a batch to a partition based on the provided routing key.
// The batch is retained (ref count incremented).
func (pr *PartitionedRecords) Append(batch arrow.RecordBatch, routingKey uint64) {
partIdx := int(routingKey % uint64(pr.numPartitions))
pr.AppendToPartition(batch, partIdx)
}

// AppendWithKey adds a batch using hash-based routing, returns partition index.
func (pr *PartitionedRecords) AppendWithKey(batch arrow.RecordBatch, key uint64) int {
partIdx := pr.hashKey(key)
pr.AppendToPartition(batch, partIdx)
return partIdx
}

// AppendToPartition adds a batch directly to a specific partition.
func (pr *PartitionedRecords) AppendToPartition(batch arrow.RecordBatch, partIdx int) {
if partIdx < 0 || partIdx >= pr.numPartitions {
partIdx = 0
}

batch.Retain()

pr.partitions[partIdx].mu.Lock()
pr.partitions[partIdx].records = append(pr.partitions[partIdx].records, batch)
pr.partitions[partIdx].mu.Unlock()

pr.totalBatches.Add(1)
}

// GetAll returns all batches across all partitions.
// Caller should NOT release returned batches - they are still owned by PartitionedRecords.
func (pr *PartitionedRecords) GetAll() []arrow.RecordBatch {
// First pass: count total
total := 0
for i := range pr.partitions {
pr.partitions[i].mu.RLock()
total += len(pr.partitions[i].records)
pr.partitions[i].mu.RUnlock()
}

result := make([]arrow.RecordBatch, 0, total)

// Second pass: collect
for i := range pr.partitions {
pr.partitions[i].mu.RLock()
result = append(result, pr.partitions[i].records...)
pr.partitions[i].mu.RUnlock()
}

return result
}

// GetPartition returns all batches from a specific partition.
func (pr *PartitionedRecords) GetPartition(partIdx int) []arrow.RecordBatch {
if partIdx < 0 || partIdx >= pr.numPartitions {
return nil
}

pr.partitions[partIdx].mu.RLock()
defer pr.partitions[partIdx].mu.RUnlock()

// Return a copy of the slice
result := make([]arrow.RecordBatch, len(pr.partitions[partIdx].records))
copy(result, pr.partitions[partIdx].records)
return result
}

// ForEach iterates over all batches, calling fn for each.
// If fn returns false, iteration stops early.
func (pr *PartitionedRecords) ForEach(fn func(batch arrow.RecordBatch, partition int) bool) {
for partIdx := range pr.partitions {
pr.partitions[partIdx].mu.RLock()
records := pr.partitions[partIdx].records
pr.partitions[partIdx].mu.RUnlock()

for _, batch := range records {
if !fn(batch, partIdx) {
return
}
}
}
}

// ForEachInPartition iterates over batches in a specific partition.
func (pr *PartitionedRecords) ForEachInPartition(partIdx int, fn func(batch arrow.RecordBatch) bool) {
if partIdx < 0 || partIdx >= pr.numPartitions {
return
}

pr.partitions[partIdx].mu.RLock()
records := pr.partitions[partIdx].records
pr.partitions[partIdx].mu.RUnlock()

for _, batch := range records {
if !fn(batch) {
return
}
}
}

// Stats returns current statistics.
func (pr *PartitionedRecords) Stats() PartitionedRecordsStats {
batchesPerPart := make([]int, pr.numPartitions)
total := 0

for i := range pr.partitions {
pr.partitions[i].mu.RLock()
count := len(pr.partitions[i].records)
pr.partitions[i].mu.RUnlock()
batchesPerPart[i] = count
total += count
}

return PartitionedRecordsStats{
TotalBatches:   total,
NumPartitions:  pr.numPartitions,
BatchesPerPart: batchesPerPart,
}
}

// Clear removes all batches from all partitions.
func (pr *PartitionedRecords) Clear() {
for i := range pr.partitions {
pr.partitions[i].mu.Lock()
// Release all batches
for _, batch := range pr.partitions[i].records {
batch.Release()
}
pr.partitions[i].records = nil
pr.partitions[i].mu.Unlock()
}
pr.totalBatches.Store(0)
}

// ReplaceAll atomically replaces all batches in a partition.
func (pr *PartitionedRecords) ReplaceAll(partIdx int, batches []arrow.RecordBatch) {
if partIdx < 0 || partIdx >= pr.numPartitions {
return
}

// Retain new batches
for _, b := range batches {
b.Retain()
}

pr.partitions[partIdx].mu.Lock()

// Release old batches
oldCount := len(pr.partitions[partIdx].records)
for _, batch := range pr.partitions[partIdx].records {
batch.Release()
}

// Set new batches
pr.partitions[partIdx].records = batches
newCount := len(batches)

pr.partitions[partIdx].mu.Unlock()

// Update total count
pr.totalBatches.Add(int64(newCount - oldCount))
}
