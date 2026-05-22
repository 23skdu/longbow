import re

with open('/Users/rsd/REPOS/longbow/internal/store/types/graph_data.go', 'r') as f:
    content = f.read()

# Replace previous initArenaSafe calls
content = re.sub(
    r'initArenaSafe\(&g\.([A-Za-z0-9]+Arena),\s*slabSize\)',
    r'initArenaSafe(&g.\1, slabSize, g.Allocator)',
    content
)

# Now replace the remaining unsafe blocks:
# if g.Float32Arena == nil { ... var sa ... if g.Allocator != nil ... else ... g.Float32Arena = ... }

unsafe_block_pattern = r'''\s*if g\.([A-Za-z0-9]+Arena) == nil \{
\s*slabSize := ([^\n]+)
\s*if slabSize < 1024\*1024 \{
\s*slabSize = 1024 \* 1024
\s*\}
\s*var sa \*memory\.SlabArena
\s*if g\.Allocator != nil \{
\s*sa = memory\.NewSlabArenaWithAllocator\(slabSize, g\.Allocator\)
\s*\} else \{
\s*sa = memory\.NewSlabArena\(slabSize\)
\s*\}
\s*g\.\1 = memory\.NewTypedArena\[([a-zA-Z0-9_.]+)\]\(sa\)
\s*\}'''

replacement = r'''
				slabSize := \2
				if slabSize < 1024*1024 {
					slabSize = 1024 * 1024
				}
				initArenaSafe(&g.\1, slabSize, g.Allocator)'''

content = re.sub(unsafe_block_pattern, replacement, content)

with open('/Users/rsd/REPOS/longbow/internal/store/types/graph_data.go', 'w') as f:
    f.write(content)

