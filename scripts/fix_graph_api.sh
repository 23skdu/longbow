#!/bin/bash
sed -i '' 's/sharded.Shards()\[0\]\.index/sharded.GetShardIndex(0)/g' internal/store/graph_api.go
sed -i '' 's/len(sharded.Shards())/sharded.NumShards()/g' internal/store/graph_api.go
