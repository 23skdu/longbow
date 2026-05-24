#!/bin/bash
sed -i '' 's/func NewIndexPerformancePredictor() \*IndexPerformancePredictor { return index.NewIndexPerformancePredictor() }/func NewIndexPerformancePredictor(l zerolog.Logger, cfg index.LearnedIndexConfig) \*IndexPerformancePredictor { return index.NewIndexPerformancePredictor(l, cfg) }/g' internal/store/aliases.go
sed -i '' 's/func NewLearnedIndexRateLimiter() \*LearnedIndexRateLimiter { return index.NewLearnedIndexRateLimiter() }/func NewLearnedIndexRateLimiter(p \*IndexPerformancePredictor, l zerolog.Logger) \*LearnedIndexRateLimiter { return index.NewLearnedIndexRateLimiter(p, l) }/g' internal/store/aliases.go
sed -i '' 's/func NewRuntimeIndexAdapter(cfg IndexAdaptationConfig) \*RuntimeIndexAdapter { return index.NewRuntimeIndexAdapter(cfg) }/func NewRuntimeIndexAdapter(l zerolog.Logger, p \*IndexPerformancePredictor, cfg IndexAdaptationConfig, m index.MetricsCollector) \*RuntimeIndexAdapter { return index.NewRuntimeIndexAdapter(l, p, cfg, m) }/g' internal/store/aliases.go

sed -i '' 's/ToGRPCStatus/types.ToGRPCStatus/g' internal/store/store_actions.go
sed -i '' 's/s\.handleVectorSearchAction/s.HandleVectorSearchAction/g' internal/store/store_actions.go
sed -i '' 's/ds\.TurboQuantBits >/ds.TurboQuantBits() >/g' internal/store/store_actions.go
sed -i '' 's/ds\.TurboQuantBits,/ds.TurboQuantBits(),/g' internal/store/store_actions.go
