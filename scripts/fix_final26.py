with open('internal/store/cluster/cdc_test.go', 'r') as f:
    content = f.read()

if '"sync"' not in content:
    content = content.replace('import (', 'import (\n\t"sync"\n', 1)

with open('internal/store/cluster/cdc_test.go', 'w') as f:
    f.write(content)
