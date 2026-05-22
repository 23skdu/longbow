import re

with open('/Users/rsd/REPOS/longbow/internal/store/types/graph_data.go', 'r') as f:
    content = f.read()

# Fix initArenaSafe signature
content = content.replace(
    "func initArenaSafe[T any](arenaPtr **memory.TypedArena[T], slabSize int, alloc memory.Allocator)",
    "func initArenaSafe[T any](arenaPtr **memory.TypedArena[T], slabSize int, alloc arrowmemory.Allocator)"
)

# Fix remaining initArenaSafe calls that only have 2 arguments
# pattern: initArenaSafe(&g.SomethingArena, slabSize)
content = re.sub(
    r'initArenaSafe\(&g\.([A-Za-z0-9]+Arena),\s*([^,]+)\)',
    r'initArenaSafe(&g.\1, \2, g.Allocator)',
    content
)

with open('/Users/rsd/REPOS/longbow/internal/store/types/graph_data.go', 'w') as f:
    f.write(content)
