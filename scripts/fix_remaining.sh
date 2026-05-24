#!/bin/bash
sed -i '' 's/sharded\.shards\[0\]\.index/sharded.GetShardIndex(0)/g' internal/store/graph_api.go
sed -i '' 's/TurboQuantBits      int/turboQuantBits      int/g' internal/store/dataset.go
cat << 'M' >> internal/store/store.go
func (d *Dataset) TurboQuantBits() int {
	return d.turboQuantBits
}
M

cat << 'M' >> internal/store/aliases.go
func NewLockFreeMap[K comparable, V any]() *LockFreeMap[K, V] { return types.NewLockFreeMap[K, V]() }
const IndexTypeHNSW = index.IndexTypeHNSW
const IndexTypeIVFFlat = index.IndexTypeIVFFlat
const IndexTypeIVFPQ = index.IndexTypeIVFPQ
const IndexTypeIVFOPQ = index.IndexTypeIVFOPQ
const IndexTypeDiskANN = index.IndexTypeDiskANN
const IndexTypeBM25 = index.IndexTypeBM25
const IndexTypeComposite = index.IndexTypeComposite
const IndexTypeLearned = index.IndexTypeLearned
M

cat << 'M' >> internal/store/index/sharded_hnsw.go
// LocationStore returns the location store
func (s *ShardedHNSW) LocationStore() *ChunkedLocationStore {
	return s.locationStore
}
M

sed -i '' 's/sh\.locationStore/sh.LocationStore()/g' internal/store/hnsw_autoshard.go
