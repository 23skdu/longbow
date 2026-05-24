import re

def replace_in_file(file_path, old, new):
    with open(file_path, 'r') as f:
        content = f.read()
    content = content.replace(old, new)
    with open(file_path, 'w') as f:
        f.write(content)

replace_in_file('internal/store/sq8_loss_validation_test.go', 'core.', 'index.')
replace_in_file('internal/store/vector_search_action_test.go', 'vs.handleVectorSearchAction', 'vs.HandleVectorSearchAction')

with open('internal/store/vectorstore_index_test.go', 'r') as f:
    content = f.read()
content = content.replace('func() types.IndexDataProvider {', 'func() *Dataset {')
content = content.replace('return &index.MockDataset{}', 'return &Dataset{}')
content = content.replace('index.MockDataset{}', 'Dataset{}')
with open('internal/store/vectorstore_index_test.go', 'w') as f:
    f.write(content)

