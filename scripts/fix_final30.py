import re

def fix_file(file_path):
    with open(file_path, 'r') as f:
        content = f.read()

    if '"github.com/23skdu/longbow/internal/store/wal"' not in content:
        content = content.replace('import (', 'import (\n\t"github.com/23skdu/longbow/internal/store/wal"\n', 1)

    content = content.replace('NewFlightWALReplicator', 'wal.NewFlightWALReplicator')
    with open(file_path, 'w') as f:
        f.write(content)

fix_file('internal/store/store.go')
fix_file('internal/store/store_persistence.go')

