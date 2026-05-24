import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Remove the import of cluster
content = content.replace('	"github.com/23skdu/longbow/internal/store/cluster"\n', '')

# Remove cluster. from cluster.NewDataServer and cluster.NewMetaServer
content = content.replace('cluster.NewDataServer', 'NewDataServer')
content = content.replace('cluster.NewMetaServer', 'NewMetaServer')
content = content.replace('cluster.MockMeshClient', 'MockMeshClient') # If any

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
