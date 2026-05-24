with open('internal/store/types/search_pool_test.go', 'r') as f:
    content = f.read()

content = content.replace('package types_test', 'package types')
content = content.replace('index.New', 'New')
content = content.replace('index.index', 'index')
# just remove index. completely for those cases
content = content.replace('index.', '')

with open('internal/store/types/search_pool_test.go', 'w') as f:
    f.write(content)
