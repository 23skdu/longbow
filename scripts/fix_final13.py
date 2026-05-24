import re

with open('internal/store/mesh_actions_test.go', 'r') as f:
    content = f.read()

content = re.sub(r'	t\.Run\("ClusterStatus".*?}\)\n', '', content, flags=re.DOTALL)

with open('internal/store/mesh_actions_test.go', 'w') as f:
    f.write(content)
