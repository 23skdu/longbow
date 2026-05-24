import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

content = content.replace('"github.com/23skdu/longbow/internal/storage"', '"github.com/23skdu/longbow/internal/storage"\n\t"github.com/23skdu/longbow/internal/store"\n\t"github.com/23skdu/longbow/internal/store/cluster"')

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
