with open('internal/store/structured_errors_test.go', 'r') as f:
    content = f.read()

if 'github.com/23skdu/longbow/internal/store/types' not in content:
    content = content.replace('import (', 'import (\n\t"github.com/23skdu/longbow/internal/store/types"\n', 1)

with open('internal/store/structured_errors_test.go', 'w') as f:
    f.write(content)
