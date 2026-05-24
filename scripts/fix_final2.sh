#!/bin/bash
sed -i '' 's/type IndexType = types.IndexType/type IndexType = index.IndexType/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeHNSW = types.IndexTypeHNSW/const IndexTypeHNSW = index.IndexTypeHNSW/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeIVFFlat = types.IndexTypeIVFFlat/const IndexTypeIVFFlat = index.IndexTypeIVFFlat/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeIVFPQ = types.IndexTypeIVFPQ/const IndexTypeIVFPQ = index.IndexTypeIVFPQ/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeIVFOPQ = types.IndexTypeIVFOPQ/const IndexTypeIVFOPQ = index.IndexTypeIVFOPQ/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeDiskANN = types.IndexTypeDiskANN/const IndexTypeDiskANN = index.IndexTypeDiskANN/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeBM25 = types.IndexTypeBM25/const IndexTypeBM25 = index.IndexTypeBM25/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeComposite = types.IndexTypeComposite/const IndexTypeComposite = index.IndexTypeComposite/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeLearned = types.IndexTypeLearned/const IndexTypeLearned = index.IndexTypeLearned/g' internal/store/aliases.go

# Check what the import is in hybrid_search_arena.go
sed -i '' '/"github.com\/23skdu\/longbow\/internal\/store\/index"/d' internal/store/hybrid_search_arena.go
