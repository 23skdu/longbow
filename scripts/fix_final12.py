import re

for filename in ['internal/store/cluster/delete_action_test.go', 'internal/store/cluster/benchmark_test.go']:
    with open(filename, 'r') as f:
        content = f.read()

    # Add missing imports for store and cluster
    if '"github.com/23skdu/longbow/internal/store"' not in content:
        content = content.replace('"github.com/stretchr/testify/assert"', '"github.com/23skdu/longbow/internal/store"\n\t"github.com/23skdu/longbow/internal/store/cluster"\n\t"github.com/stretchr/testify/assert"')

    # Fix VectorStore to store.VectorStore
    content = content.replace('VectorStore', 'store.VectorStore')
    content = content.replace('store.store.VectorStore', 'store.VectorStore')
    
    # Fix NewVectorStore to store.NewVectorStore
    content = content.replace('NewVectorStore', 'store.NewVectorStore')
    content = content.replace('store.store.NewVectorStore', 'store.NewVectorStore')
    
    # Fix Dataset to store.Dataset
    content = content.replace('Dataset', 'store.Dataset')
    content = content.replace('store.store.Dataset', 'store.Dataset')

    # Fix setupDataServerTest call signature
    content = content.replace('client := setupDataServerTest(t)', 'client, vs := setupDataServerTest(t)')
    content = content.replace('client := setupDataServerTest(b)', 'client, vs := setupDataServerTest(b)')
    
    with open(filename, 'w') as f:
        f.write(content)
