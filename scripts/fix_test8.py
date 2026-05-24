import re

with open('internal/store/cluster/cdc_test.go', 'r') as f:
    content = f.read()

content = content.replace('sub.Close()', 'cdc.Unsubscribe(sub.ID)')

with open('internal/store/cluster/cdc_test.go', 'w') as f:
    f.write(content)
