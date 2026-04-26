#!/bin/bash
# Longbow CLI Verification Script
# Validates CLI commands against a running Longbow server

set -e

# Configuration
SERVER_URI="grpc://127.0.0.1:3000"
CLI="./bin/longbow-cli"
TEST_DS="cli_verification_test"

echo "=== Building Longbow CLI ==="
go build -o bin/longbow-cli ./cmd/cli

echo "=== Starting Feature Matrix Validation ==="

DIMS=(128 384)
DTYPES=("float32" "int8" "turboquant")

for dim in "${DIMS[@]}"; do
    for dtype in "${DTYPES[@]}"; do
        echo "[TEST] Testing Dimension: $dim, Datatype: $dtype"
        
        # 1. Clean up
        $CLI delete-namespace -name "$TEST_DS" -uri "$SERVER_URI" > /dev/null 2>&1 || true
        
        # 2. Create Namespace
        $CLI create-namespace -name "$TEST_DS" -dims "$dim" -data_type "$dtype" -uri "$SERVER_URI"
        
        # 3. Import Data
        $CLI import -dataset "$TEST_DS" -dim "$dim" -count 100 -uri "$SERVER_URI"
        
        # 4. Stats
        $CLI stats -dataset "$TEST_DS" -uri "$SERVER_URI"
        
        # 5. Search
        # Generate a dummy vector of the correct dimension
        VEC=$(printf '0.1,%.0s' $(seq 1 "$dim") | sed 's/,$//')
        $CLI search -dataset "$TEST_DS" -mode dense -vector "$VEC" -k 5 -uri "$SERVER_URI"
        
        echo "[PASS] Dimension: $dim, Datatype: $dtype"
        echo "---------------------------------------"
    done
done

echo "=== Testing Specialized Features ==="

# Geo-spatial
echo "[TEST] Geospatial Search"
$CLI create-dataset -name "geo_cli_test" -dims 128 -geo=true -uri "$SERVER_URI"
$CLI import -dataset "geo_cli_test" -dim 128 -count 50 -uri "$SERVER_URI"
$CLI geo-search -dataset "geo_cli_test" -lat 34.0522 -lon -118.2437 -radius 10.0 -uri "$SERVER_URI"
$CLI delete-namespace -name "geo_cli_test" -uri "$SERVER_URI"

# Graph
echo "[TEST] Graph Operations"
$CLI create-namespace -name "graph_cli_test" -dims 128 -uri "$SERVER_URI"
$CLI import -dataset "graph_cli_test" -dim 128 -count 50 -uri "$SERVER_URI"
$CLI add-edge -dataset "graph_cli_test" -subject 1 -object 2 -weight 0.9 -uri "$SERVER_URI"
$CLI get-graph-stats -dataset "graph_cli_test" -uri "$SERVER_URI"
$CLI pagerank -dataset "graph_cli_test" -uri "$SERVER_URI"
$CLI delete-namespace -name "graph_cli_test" -uri "$SERVER_URI"

echo "=== ALL CLI TESTS PASSED ==="
