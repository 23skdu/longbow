import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Add the import
if '"github.com/23skdu/longbow/internal/store/cluster"' not in content:
    content = content.replace('"github.com/23skdu/longbow/internal/store"', '"github.com/23skdu/longbow/internal/store"\n\t"github.com/23skdu/longbow/internal/store/cluster"')

# Restore cluster. prefix
content = content.replace('NewDataServer(', 'cluster.NewDataServer(')
content = content.replace('NewMetaServer(', 'cluster.NewMetaServer(')
content = content.replace('MockMeshClient', 'cluster.MockMeshClient')

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
