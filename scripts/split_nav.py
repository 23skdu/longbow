import re

with open('internal/store/index/navigation.go', 'r') as f:
    content = f.read()

# Helper to extract functions/types by matching balanced braces.
def extract_block(text, start_pattern):
    match = re.search(start_pattern, text)
    if not match: return None, text
    
    start_idx = match.start()
    
    # find the first '{' after start_idx
    brace_idx = text.find('{', start_idx)
    if brace_idx == -1:
        # maybe it's just a type alias without braces?
        nl_idx = text.find('\n', start_idx)
        return text[start_idx:nl_idx+1], text[:start_idx] + text[nl_idx+1:]
        
    open_braces = 0
    in_string = False
    in_char = False
    escape = False
    
    for i in range(brace_idx, len(text)):
        char = text[i]
        if escape:
            escape = False
            continue
        if char == '\\':
            escape = True
            continue
            
        if char == '"' and not in_char:
            in_string = not in_string
        elif char == "'" and not in_string:
            in_char = not in_char
            
        if not in_string and not in_char:
            if char == '{':
                open_braces += 1
            elif char == '}':
                open_braces -= 1
                if open_braces == 0:
                    end_idx = i + 1
                    block = text[start_idx:end_idx]
                    rem = text[:start_idx] + text[end_idx:]
                    return block, rem
                    
    return None, text

parallel_blocks = []
search_blocks = []

parallel_patterns = [
    r'type parallelSearchHostF32 struct\s*\{',
    r'func \(p parallelSearchHostF32\) ',
    r'type parallelSearchHostF64 struct\s*\{',
    r'func \(p parallelSearchHostF64\) ',
    r'func \(h \*ArrowHNSW\) SearchForParallel',
    r'func \(h \*ArrowHNSW\) ExtractVectorToBufferForParallel',
    r'func \(h \*ArrowHNSW\) ExtractVectorByIDToBufferForParallel',
]

search_patterns = [
    r'func \(h \*ArrowHNSW\) SearchVectorsWithBitmap',
    r'func \(h \*ArrowHNSW\) SearchVectors\(',
    r'func \(h \*ArrowHNSW\) SearchVectorsInRange',
    r'func \(h \*ArrowHNSW\) ProcessResultsParallel',
    r'func \(h \*ArrowHNSW\) resolveHNSWComputer',
]

for pat in parallel_patterns:
    while True:
        b, content = extract_block(content, pat)
        if b:
            parallel_blocks.append(b)
        else:
            break

for pat in search_patterns:
    while True:
        b, content = extract_block(content, pat)
        if b:
            search_blocks.append(b)
        else:
            break

# Write back the remainder to navigation.go
with open('internal/store/index/navigation.go', 'w') as f:
    f.write(content)

imports = """package index

import (
	"context"

	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)
"""

with open('internal/store/index/navigation_parallel.go', 'w') as f:
    f.write(imports + '\n' + '\n\n'.join(parallel_blocks))

with open('internal/store/index/navigation_search.go', 'w') as f:
    f.write(imports + '\n' + '\n\n'.join(search_blocks))

