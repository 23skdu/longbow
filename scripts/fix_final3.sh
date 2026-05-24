#!/bin/bash
sed -i '' 's/type IndexType = types.IndexType/type IndexType = index.IndexType/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeHNSW = types.IndexTypeHNSW/const IndexTypeHNSW = index.IndexTypeHNSW/g' internal/store/aliases.go
sed -i '' 's/const IndexTypeIVFFlat = types.IndexTypeIVFFlat/const IndexTypeIVFFlat = index.IndexTypeIVFFlat/g' internal/store/aliases.go

# Remove the ones that don't exist in index package
sed -i '' '/const IndexTypeIVFPQ = types.IndexTypeIVFPQ/d' internal/store/aliases.go
sed -i '' '/const IndexTypeIVFOPQ = types.IndexTypeIVFOPQ/d' internal/store/aliases.go
sed -i '' '/const IndexTypeDiskANN = types.IndexTypeDiskANN/d' internal/store/aliases.go
sed -i '' '/const IndexTypeBM25 = types.IndexTypeBM25/d' internal/store/aliases.go
sed -i '' '/const IndexTypeComposite = types.IndexTypeComposite/d' internal/store/aliases.go
sed -i '' '/const IndexTypeLearned = types.IndexTypeLearned/d' internal/store/aliases.go

# Add to aliases.go
cat << 'M' >> internal/store/aliases.go
type CDCEvent = cluster.CDCEvent
type LearnedIndexConfig = index.LearnedIndexConfig

func NewCircuitBreakerRegistry(cfg cluster.CircuitBreakerConfig) *CircuitBreakerRegistry { return cluster.NewCircuitBreakerRegistry(cfg) }
func DefaultCircuitBreakerConfig() cluster.CircuitBreakerConfig { return cluster.DefaultCircuitBreakerConfig() }
M

