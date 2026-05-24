#!/bin/bash
sed -i '' 's/func NewIVFHNSWCompositeIndex(cfg IVFHNSWConfig) \*index.IVFHNSWCompositeIndex { return index.NewIVFHNSWCompositeIndex(cfg) }/func NewIVFHNSWCompositeIndex(d int, cfg IVFHNSWConfig) (\*index.IVFHNSWCompositeIndex, error) { return index.NewIVFHNSWCompositeIndex(d, cfg) }/g' internal/store/aliases.go

cat << 'M' >> internal/store/aliases.go
type IVFOPQConfig = index.IVFOPQConfig
func NewIVFOPQIndex(cfg IVFOPQConfig) *index.IVFOPQIndex { return index.NewIVFOPQIndex(cfg) }
M

sed -i '' 's/s\.FlightBackend\.GetNeighborsBulk/s.GetNeighborsBulk/g' internal/store/store_meta_actions.go

sed -i '' 's/NewDataServer/cluster.NewDataServer/g' internal/store/benchmark_test.go

cat << 'M' >> internal/store/benchmark_test.go
const testBufSize = 1024 * 1024
M

