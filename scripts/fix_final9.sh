#!/bin/bash
sed -i '' 's/func ParseConsistencyLevel(s string) ConsistencyLevel { return cluster.ParseConsistencyLevel(s) }/func ParseConsistencyLevel(s string) (ConsistencyLevel, error) { return cluster.ParseConsistencyLevel(s) }/g' internal/store/aliases.go

cat << 'M' >> internal/store/aliases.go
type IVFHNSWConfig = index.IVFHNSWConfig
func NewIVFHNSWCompositeIndex(cfg IVFHNSWConfig) *index.IVFHNSWCompositeIndex { return index.NewIVFHNSWCompositeIndex(cfg) }
M

python3 -c "
import re
with open('internal/store/store_meta_actions.go', 'r') as f:
    content = f.read()
content = re.sub(r's\.FlightBackend\.DoAction\(action,\s*stream\)', 'status.Error(codes.Unimplemented, \"unimplemented action\")', content)
with open('internal/store/store_meta_actions.go', 'w') as f:
    f.write(content)

with open('internal/store/batched_indexing_test.go', 'r') as f:
    content = f.read()
content = content.replace('&map[string]types.IndexDataProvider', '&map[string]*Dataset')
with open('internal/store/batched_indexing_test.go', 'w') as f:
    f.write(content)

with open('internal/store/binary_search_test.go', 'r') as f:
    content = f.read()
content = content.replace('store.handleVectorSearchExchange', 'store.HandleVectorSearchExchange')
with open('internal/store/binary_search_test.go', 'w') as f:
    f.write(content)
"
