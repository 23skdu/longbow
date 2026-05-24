#!/bin/bash
# Remove the duplicate import
sed -i '' '9d' internal/store/generic_quantizer_test.go

sed -i '' 's/NewMetaServer/cluster.NewMetaServer/g' internal/store/graph_api_test.go

sed -i '' 's/NewShardedInvertedIndex/index.NewShardedInvertedIndex/g' internal/store/inverted_index_sharded_test.go

# Also ensure inverted_index_sharded_test.go imports index
sed -i '' '/"testing"/a\
	"github.com/23skdu/longbow/internal/store/index"\
' internal/store/inverted_index_sharded_test.go

