import re

with open('internal/store/index/learned_index_embedding_test.go', 'r') as f:
    content = f.read()
content = content.replace('package store', 'package index')
# Remove TestActiveEmbedding
content = re.sub(r'func TestActiveEmbedding\(t \*testing\.T\) \{.*?\n\}', '', content, flags=re.DOTALL)
with open('internal/store/index/learned_index_embedding_test.go', 'w') as f:
    f.write(content)

with open('internal/store/cluster/cdc_test.go', 'r') as f:
    content = f.read()
if 'mu sync.RWMutex' not in content:
    content = content.replace('type MockCDCStore struct {', 'type MockCDCStore struct {\n\tmu sync.RWMutex\n')
    with open('internal/store/cluster/cdc_test.go', 'w') as f:
        f.write(content)

