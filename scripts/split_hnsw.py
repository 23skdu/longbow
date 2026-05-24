import re

with open('internal/store/index/arrow_hnsw.go', 'r') as f:
    content = f.read()

def extract_block(text, start_pattern):
    match = re.search(start_pattern, text)
    if not match: return None, text
    
    start_idx = match.start()
    
    brace_idx = text.find('{', start_idx)
    if brace_idx == -1:
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

insert_blocks = []
delete_blocks = []

insert_patterns = [
    r'func \(h \*ArrowHNSW\) commitID',
    r'func \(h \*ArrowHNSW\) updateMetadata\(',
    r'func \(h \*ArrowHNSW\) updateMetadataIfHigher',
    r'func \(h \*ArrowHNSW\) AddByLocation',
    r'func \(h \*ArrowHNSW\) AddByRecord',
    r'func \(h \*ArrowHNSW\) generateLevel',
    r'func \(h \*ArrowHNSW\) AddBatch',
]

delete_patterns = [
    r'func \(h \*ArrowHNSW\) Delete\(',
    r'func \(h \*ArrowHNSW\) DeleteBatch',
    r'func \(h \*ArrowHNSW\) CleanupTombstones',
]

for pat in insert_patterns:
    while True:
        b, content = extract_block(content, pat)
        if b:
            insert_blocks.append(b)
        else:
            break

for pat in delete_patterns:
    while True:
        b, content = extract_block(content, pat)
        if b:
            delete_blocks.append(b)
        else:
            break

with open('internal/store/index/arrow_hnsw.go', 'w') as f:
    f.write(content)

imports = """package index

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
)
"""

with open('internal/store/index/arrow_hnsw_insert.go', 'w') as f:
    f.write(imports + '\n' + '\n\n'.join(insert_blocks))

with open('internal/store/index/arrow_hnsw_delete.go', 'w') as f:
    f.write(imports + '\n' + '\n\n'.join(delete_blocks))

