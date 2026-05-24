import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Add missing imports for store and cluster
if '"github.com/23skdu/longbow/internal/store"' not in content:
    content = content.replace('"github.com/23skdu/longbow/internal/store/types"', '"github.com/23skdu/longbow/internal/store/types"\n\t"github.com/23skdu/longbow/internal/store"\n\t"github.com/23skdu/longbow/internal/store/cluster"')

# Fix putDataViastore
content = content.replace('putDataViastore', 'putDataViaVectorStore')

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
