import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

content = content.replace('TestResetstore.DatasetAction', 'TestResetDatasetAction')
content = content.replace('TestDropstore.DatasetAction', 'TestDropDatasetAction')

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
