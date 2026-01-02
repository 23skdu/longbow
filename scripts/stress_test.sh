#!/bin/bash
END_TIME=$((SECONDS + 300))
echo "Starting fixed stress test for 5 minutes..."
while [ $SECONDS -lt $END_TIME ]; do
    ./.venv/bin/python scripts/ops_test.py --data-uri grpc://0.0.0.0:3000 --meta-uri grpc://0.0.0.0:3001 validate
    ./.venv/bin/python scripts/ops_test.py --data-uri grpc://0.0.0.0:3010 --meta-uri grpc://0.0.0.0:3011 put --dataset load_test --rows 5000 --dim 128
    ./.venv/bin/python scripts/ops_test.py --data-uri grpc://0.0.0.0:3000 --meta-uri grpc://0.0.0.0:3001 search --dataset load_test --k 10 --dim 128 --global
    sleep 1
done
echo "Stress test complete."
