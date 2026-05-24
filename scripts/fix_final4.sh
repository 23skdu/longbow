#!/bin/bash
sed -i '' '/IndexTypeIVFPQ/d' internal/store/aliases.go
sed -i '' '/IndexTypeIVFOPQ/d' internal/store/aliases.go
sed -i '' '/IndexTypeBM25/d' internal/store/aliases.go
sed -i '' '/IndexTypeComposite/d' internal/store/aliases.go
sed -i '' '/IndexTypeLearned/d' internal/store/aliases.go

cat << 'M' >> internal/store/aliases.go
type IndexAdaptationConfig = index.IndexAdaptationConfig

func NewIndexPerformancePredictor() *IndexPerformancePredictor { return index.NewIndexPerformancePredictor() }
func NewLearnedIndexRateLimiter() *LearnedIndexRateLimiter { return index.NewLearnedIndexRateLimiter() }
func NewRuntimeIndexAdapter(cfg IndexAdaptationConfig) *RuntimeIndexAdapter { return index.NewRuntimeIndexAdapter(cfg) }
M

sed -i '' '/package store/a\
import "github.com/23skdu/longbow/internal/store/index"
' internal/store/hybrid_search_arena.go
