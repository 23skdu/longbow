#!/bin/bash
sed -i '' 's/func NewBM25ArenaIndex(cfg index.BM25Config) \*BM25ArenaIndex { return index.NewBM25ArenaIndex(cfg) }/func NewBM25ArenaIndex(arena *github.com\/23skdu\/longbow\/internal\/memory.SlabArena, offset int) \*BM25ArenaIndex { return index.NewBM25ArenaIndex(arena, offset) }/g' internal/store/aliases.go

sed -i '' 's/func NewShardedHNSW(m interface{}, v interface{}, p interface{}, d int, m_param int, efc int, t int) \*ShardedHNSW { return index.NewShardedHNSW(m, v, p, d, m_param, efc, t) }/func NewShardedHNSW(cfg index.ShardedHNSWConfig, dp types.IndexDataProvider) index.VectorIndex { return index.NewShardedHNSW(cfg, dp) }\
func DefaultShardedHNSWConfig() index.ShardedHNSWConfig { return index.DefaultShardedHNSWConfig() }/g' internal/store/aliases.go

sed -i '' 's/handleVectorSearchExchange/HandleVectorSearchExchange/g' internal/store/do_exchange.go
sed -i '' 's/sharded.shards\[i\]/sharded.Shards()[i]/g' internal/store/graph_api.go
sed -i '' 's/len(sharded.shards)/len(sharded.Shards())/g' internal/store/graph_api.go

