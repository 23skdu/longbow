#!/bin/bash
sed -i '' 's/type IndexType = index.IndexType/type IndexType = types.IndexType/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeHNSW = index.IndexTypeHNSW/const IndexTypeHNSW = types.IndexTypeHNSW/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeIVFFlat = index.IndexTypeIVFFlat/const IndexTypeIVFFlat = types.IndexTypeIVFFlat/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeIVFPQ = index.IndexTypeIVFPQ/const IndexTypeIVFPQ = types.IndexTypeIVFPQ/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeIVFOPQ = index.IndexTypeIVFOPQ/const IndexTypeIVFOPQ = types.IndexTypeIVFOPQ/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeDiskANN = index.IndexTypeDiskANN/const IndexTypeDiskANN = types.IndexTypeDiskANN/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeBM25 = index.IndexTypeBM25/const IndexTypeBM25 = types.IndexTypeBM25/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeComposite = index.IndexTypeComposite/const IndexTypeComposite = types.IndexTypeComposite/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeLearned = index.IndexTypeLearned/const IndexTypeLearned = types.IndexTypeLearned/g' internal/store/aliases.go

sed -i '' 's/ds.TurboQuantBits = /ds.turboQuantBits = /g' internal/store/dataset.go

sed -i '' '/"github.com\/23skdu\/longbow\/internal\/query"/a\
	"github.com/23skdu/longbow/internal/store/index"\
' internal/store/hybrid_search_arena.go

cat << 'M' >> internal/store/aliases.go
type CDCFilter = cluster.CDCFilter
type CDCEventType = cluster.CDCEventType
const CDCEventInsert = cluster.CDCEventInsert
const CDCEventUpdate = cluster.CDCEventUpdate
const CDCEventDelete = cluster.CDCEventDelete
M

