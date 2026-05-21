export LONGBOW_MAX_MEMORY=18000000000
export LONGBOW_GPU_ENABLED=true
./bin/longbow > server_float16.log 2>&1 &
SERVER_PID=$!
sleep 2
./bin/bench-tool -mode vec -uri grpc://127.0.0.1:3000 -dim 128 -dtype float16 -scale 500000 -queries 100 -workers 8 -dataset test_float16_500k -search-modes all
kill -9 $SERVER_PID
cat server_float16.log | tail -n 100
