export LONGBOW_MAX_MEMORY=18000000000
./bin/longbow > server.log 2>&1 &
SERVER_PID=$!
sleep 2
./bin/bench-tool -mode vec -uri grpc://127.0.0.1:3000 -dim 128 -dtype int32 -scale 5000 -queries 100 -workers 8 -dataset test_graphrag -search-modes all
kill -9 $SERVER_PID
cat server.log
