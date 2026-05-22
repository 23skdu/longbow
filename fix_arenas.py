import re

with open('/Users/rsd/REPOS/longbow/internal/store/types/graph_data.go', 'r') as f:
    content = f.read()

# Replace block like:
# if g.Uint32Arena == nil {
#     slabSize := ...
#     if ... { ... }
#     g.Uint32Arena = memory.NewTypedArena[uint32](memory.NewSlabArena(slabSize))
# }

def replacer(match):
    arena_name = match.group(1)
    inner_content = match.group(2)
    type_name = match.group(3)
    
    # We want to replace it with:
    # slabSize := ...
    # if ... { ... }
    # initArenaSafe(&g.ArenaName, slabSize)
    
    # Remove the assignment line
    assignment_pattern = r'\s*g\.' + arena_name + r'\s*=\s*memory\.NewTypedArena\[' + type_name + r'\]\(memory\.NewSlabArena\(slabSize\)\)\n'
    inner_cleaned = re.sub(assignment_pattern, '\n', inner_content)
    
    # Add initArenaSafe
    res = inner_cleaned + f'\t\t\t\tinitArenaSafe(&g.{arena_name}, slabSize)\n'
    
    return res

# The pattern matches:
# if g.ArenaName == nil {
#   <inner_content>
#   g.ArenaName = memory.NewTypedArena[type_name](memory.NewSlabArena(slabSize))
# }

pattern = r'if g\.([A-Za-z0-9]+Arena) == nil \{((?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*)g\.\1 = memory\.NewTypedArena\[([a-zA-Z0-9_.]+)\]\(memory\.NewSlabArena\(slabSize\)\)\s*\}'

new_content = re.sub(pattern, replacer, content)

# There's another pattern without slabSize logic inside (e.g. g.Int32Arena)
# if g.Int32Arena == nil {
#    g.Int32Arena = memory.NewTypedArena[int32](memory.NewSlabArena(4 * 1024 * 1024))
# }

def replacer_simple(match):
    arena_name = match.group(1)
    type_name = match.group(2)
    slab_size_expr = match.group(3)
    return f'initArenaSafe(&g.{arena_name}, {slab_size_expr})'

pattern_simple = r'if g\.([A-Za-z0-9]+Arena) == nil \{\s*g\.\1 = memory\.NewTypedArena\[([a-zA-Z0-9_.]+)\]\(memory\.NewSlabArena\(([^)]+)\)\)\s*\}'
new_content = re.sub(pattern_simple, replacer_simple, new_content)

# Fix the specific Float16Arena case (where it might be float16.Num)
# actually handled by the regex.

with open('/Users/rsd/REPOS/longbow/internal/store/types/graph_data.go', 'w') as f:
    f.write(new_content)

