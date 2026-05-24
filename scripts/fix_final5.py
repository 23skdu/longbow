import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

content = content.replace('func putDataViastore.VectorStore', 'func putDataViaVectorStore')

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
