#!/bin/bash
cat << 'M' >> internal/store/store.go

// GetMesh returns the mesh
func (s *VectorStore) GetMesh() *mesh.Gossip {
	return s.Mesh
}
M

cat << 'M' >> internal/store/index/sharded_hnsw.go

// SetDataset sets the dataset for testing
func (s *ShardedHNSW) SetDataset(dp types.IndexDataProvider) {
	s.dataset = dp
}
M

sed -i '' 's/ds.Index.(\*index.ShardedHNSW).Dataset() = ds/ds.Index.(*index.ShardedHNSW).SetDataset(ds)/g' internal/store/doput_sharding_test.go

sed -i '' 's/core\./index./g' internal/store/generic_quantizer_test.go

sed -i '' '/"testing"/a\
	"github.com/23skdu/longbow/internal/store/index"\
' internal/store/generic_quantizer_test.go

