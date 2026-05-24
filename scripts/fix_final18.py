import re

def add_import(file_path, imp):
    with open(file_path, 'r') as f:
        content = f.read()
    if imp not in content:
        content = re.sub(r'import \(', f'import (\n\t"{imp}"', content, count=1)
        with open(file_path, 'w') as f:
            f.write(content)

add_import('internal/store/mesh_actions_bench_test.go', 'github.com/23skdu/longbow/internal/store/cluster')
add_import('internal/store/mesh_actions_test.go', 'github.com/23skdu/longbow/internal/store/cluster')

with open('internal/store/store.go', 'r') as f:
    content = f.read()

content = content.replace('chan<- CDCEvent', 'chan arrow.RecordBatch')
content = content.replace('func (s *VectorStore) RegisterCDCSubscriber(dataset string, sub chan arrow.RecordBatch)', 'func (s *VectorStore) RegisterCDCSubscriber(dataset string, sub chan arrow.RecordBatch)')
content = content.replace('func (s *VectorStore) UnregisterCDCSubscriber(dataset string, sub chan arrow.RecordBatch)', 'func (s *VectorStore) UnregisterCDCSubscriber(dataset string, sub chan arrow.RecordBatch)')

with open('internal/store/store.go', 'w') as f:
    f.write(content)
