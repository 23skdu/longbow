with open('internal/store/cluster/cdc_test.go', 'r') as f:
    content = f.read()

import re

# TestChangeDataCapture_HandleCDCBatch_DropOnFull
content = re.sub(
    r'assert\.Equal\(t, int64\(1\), received\)\s*'
    r'assert\.Equal\(t, int64\(1\), sent\)\s*'
    r'assert\.Equal\(t, int64\(0\), dropped\)\s*'
    r'assert\.Equal\(t, int64\(0\), filtered\)\s*'
    r'assert\.Equal\(t, int64\(1\), subs\)\s*'
    r'assert\.Equal\(t, int64\(0\), full\)',
    'assert.Equal(t, int64(1), received)\n\t'
    'assert.Equal(t, int64(0), sent)\n\t'
    'assert.Equal(t, int64(1), dropped)\n\t'
    'assert.Equal(t, int64(0), filtered)\n\t'
    'assert.Equal(t, int64(1), subs)\n\t'
    'assert.Equal(t, int64(1), full)',
    content
)

with open('internal/store/cluster/cdc_test.go', 'w') as f:
    f.write(content)
