#!/bin/bash

# Ensure we are in the root or can find the directory
cd internal/store/hnsw2 || exit 1

# Move and rename files
mv adaptive_ef.go ../arrow_adaptive_ef.go
mv adaptive_ef_test.go ../arrow_adaptive_ef_test.go
mv aggregation.go ../arrow_aggregation.go
mv aggregation_test.go ../arrow_aggregation_test.go
mv arrow.go ../arrow_utils.go
mv arrow_test.go ../arrow_utils_test.go
mv batch.go ../arrow_batch.go
mv batch_test.go ../arrow_batch_test.go
mv bitset.go ../arrow_bitset.go
mv bitset_simd_amd64.go ../arrow_bitset_simd_amd64.go
mv bitset_simd_amd64.s ../arrow_bitset_simd_amd64.s
mv bitset_simd_arm64.go ../arrow_bitset_simd_arm64.go
mv bitset_simd_generic.go ../arrow_bitset_simd_generic.go
mv bitset_simd_test.go ../arrow_bitset_simd_test.go
mv bitset_test.go ../arrow_bitset_simd_base_test.go
mv chunk_test.go ../arrow_chunk_test.go
mv concurrent_test.go ../arrow_concurrent_test.go
mv delete_test.go ../arrow_delete_test.go
mv distance_arrow.go ../arrow_distance.go
mv distance_arrow_test.go ../arrow_distance_test.go
mv distance_simd.go ../arrow_distance_simd.go
mv distance_test.go ../arrow_distance_base_test.go
mv graph.go ../arrow_hnsw_graph.go
mv graph_metrics.go ../arrow_graph_metrics.go
mv graph_test.go ../arrow_graph_test.go
mv heap.go ../arrow_heap.go
mv heap_test.go ../arrow_heap_test.go
mv index.go ../arrow_hnsw_index.go
mv insert.go ../arrow_hnsw_insert.go
mv insert_bench_test.go ../arrow_insert_bench_test.go
mv insert_concurrent_test.go ../arrow_insert_concurrent_test.go
mv insert_pool.go ../arrow_insert_pool.go
mv insert_properties_test.go ../arrow_insert_properties_test.go
mv insert_test.go ../arrow_insert_test.go
mv kernels.go ../arrow_kernels.go
mv kernels_test.go ../arrow_kernels_test.go
mv maxheap.go ../arrow_maxheap.go
mv maxheap_test.go ../arrow_maxheap_test.go
mv memory_test.go ../arrow_memory_test.go
mv neighbors_arrow.go ../arrow_neighbors.go
mv pq.go ../arrow_pq.go
mv properties_test.go ../arrow_properties_test.go
mv quantization.go ../arrow_quantization.go
mv search.go ../arrow_hnsw_search.go
mv search_bench_test.go ../arrow_search_bench_test.go
mv search_test.go ../arrow_search_test.go
mv sharded.go ../arrow_sharded_wrapper.go
mv sharded_test.go ../arrow_sharded_wrapper_test.go
mv sq8_test.go ../arrow_sq8_test.go
mv validation_test.go ../arrow_validation_test.go

# Handle testdata
if [ -d "testdata" ]; then
    cp -r testdata/* ../testdata/
    rm -rf testdata
fi

cd ..

# Refactor package name
# Cross-platform sed
sed -i '' 's/package hnsw2/package store/g' arrow_*.go
sed -i '' 's/package hnsw2_test/package store_test/g' arrow_*_test.go

# Remove store imports
sed -i '' '/github.com\/23skdu\/longbow\/internal\/store"/d' arrow_*.go

# Rename Config to ArrowHNSWConfig
sed -i '' 's/type Config struct/type ArrowHNSWConfig struct/g' arrow_*.go
sed -i '' 's/Config/ArrowHNSWConfig/g' arrow_*.go

# Fix store. references
sed -i '' 's/store\.VectorID/VectorID/g' arrow_*.go
sed -i '' 's/store\.Location/Location/g' arrow_*.go
sed -i '' 's/store\.Dataset/Dataset/g' arrow_*.go
sed -i '' 's/store\.ChunkedLocationStore/ChunkedLocationStore/g' arrow_*.go
sed -i '' 's/store\.Filter/Filter/g' arrow_*.go
sed -i '' 's/store\.SearchResult/SearchResult/g' arrow_*.go
sed -i '' 's/store\.VectorIndex/VectorIndex/g' arrow_*.go
sed -i '' 's/store\.Bitset/Bitset/g' arrow_*.go
sed -i '' 's/store\.NewChunkedLocationStore/NewChunkedLocationStore/g' arrow_*.go

