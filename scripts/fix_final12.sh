#!/bin/bash
cat << 'M' >> internal/store/store.go

// GetFlightClientPool returns the client pool
func (s *VectorStore) GetFlightClientPool() *cluster.FlightClientPool {
	return s.clients
}
M

cat << 'M' >> internal/store/index/sharded_hnsw.go

// Dataset returns the underlying index data provider
func (s *ShardedHNSW) Dataset() types.IndexDataProvider {
	return s.dataset
}
M

sed -i '' 's/ds.Index.(\*ShardedHNSW).dataset/ds.Index.(*index.ShardedHNSW).Dataset()/g' internal/store/doput_sharding_test.go
sed -i '' 's/ds.Index.(\*index.ShardedHNSW).dataset/ds.Index.(*index.ShardedHNSW).Dataset()/g' internal/store/doput_sharding_test.go

sed -i '' '/const testBufSize = 1024 \* 1024/d' internal/store/benchmark_test.go

sed -i '' 's/NewDataServer/cluster.NewDataServer/g' internal/store/do_get_search_verify_test.go
sed -i '' 's/vs\.handleVectorSearchAction/vs.HandleVectorSearchAction/g' internal/store/efsearch_test.go
sed -i '' 's/ToGRPCStatus/types.ToGRPCStatus/g' internal/store/errors_test.go
