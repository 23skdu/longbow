import re

with open('internal/store/cluster/servers_test.go', 'r') as f:
    content = f.read()

# Fix unused vs by regex
content = re.sub(r'client, vs := setupDataServerTest\(t\)', r'client, _ := setupDataServerTest(t)', content)
# Restore the one we actually use!
content = content.replace('''// TestDataServerDoGet tests successful data retrieval
func TestDataServerDoGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	client, _ := setupDataServerTest(t)''', '''// TestDataServerDoGet tests successful data retrieval
func TestDataServerDoGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	client, vs := setupDataServerTest(t)''')

# Add missing assert import if not present
if '"github.com/stretchr/testify/assert"' not in content:
    content = content.replace('"github.com/stretchr/testify/require"', '"github.com/stretchr/testify/assert"\n\t"github.com/stretchr/testify/require"')

# Fix duplicate time and storage
def remove_duplicate_imports(text):
    lines = text.split('\n')
    in_import = False
    imported = set()
    out_lines = []
    for line in lines:
        if line.startswith('import ('):
            in_import = True
            out_lines.append(line)
        elif in_import and line == ')':
            in_import = False
            out_lines.append(line)
        elif in_import:
            pkg = line.strip()
            if pkg and not pkg.startswith('//'):
                if pkg in imported:
                    continue # Skip duplicate
                imported.add(pkg)
            out_lines.append(line)
        else:
            out_lines.append(line)
    return '\n'.join(out_lines)

content = remove_duplicate_imports(content)

with open('internal/store/cluster/servers_test.go', 'w') as f:
    f.write(content)
