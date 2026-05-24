#!/bin/bash
cat << 'M' >> internal/store/aliases.go
type ConsistencyLevel = cluster.ConsistencyLevel
func ParseConsistencyLevel(s string) ConsistencyLevel { return cluster.ParseConsistencyLevel(s) }
func NewLockFreeSliceFrom[T any](items []T) *LockFreeSlice[T] { return types.NewLockFreeSliceFrom(items) }
M

python3 -c "
with open('internal/store/store_meta_actions.go', 'r') as f:
    content = f.read()

content = content.replace('s.FlightBackend.DoAction(action, stream)', 'status.Error(codes.Unimplemented, \"unimplemented action\")')
content = content.replace('s.handleVectorSearchAction', 's.HandleVectorSearchAction')

with open('internal/store/store_meta_actions.go', 'w') as f:
    f.write(content)
"
