#!/bin/bash
sed -i '' 's/cluster.CDCEvent/CDCEvent/g' internal/store/store.go

sed -i '' 's/NewMetaServer/cluster.NewMetaServer/g' internal/store/mesh_actions_bench_test.go
sed -i '' 's/NewMetaServer/cluster.NewMetaServer/g' internal/store/mesh_actions_test.go

sed -i '' 's/core\./index./g' internal/store/repro_graphrag_race_test.go
sed -i '' 's/core\./index./g' internal/store/repro_sharded_race_test.go

mv internal/store/sharded_indexing_test.go internal/store/index/
sed -i '' 's/package store/package index/g' internal/store/index/sharded_indexing_test.go

