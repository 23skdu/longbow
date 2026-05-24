import re

imports_block = """import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/memory"
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/query"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
)"""

def replace_imports(filepath):
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Remove existing import block
    content = re.sub(r'import \([\s\S]*?\)', '', content, count=1)
    # Insert new import block after package declaration
    content = content.replace('package index', f'package index\n\n{imports_block}')
    
    # Remove unused imports manually to avoid errors
    # A simple pass: if the package name isn't used (except context, time, etc), remove it
    # We will just write it and see what errors go gives us.
    with open(filepath, 'w') as f:
        f.write(content)

replace_imports('internal/store/index/navigation_parallel.go')
replace_imports('internal/store/index/navigation_search.go')
